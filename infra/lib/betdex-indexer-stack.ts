import * as cdk from 'aws-cdk-lib';
import { Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import * as appsync from 'aws-cdk-lib/aws-appsync';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as cloudfront from 'aws-cdk-lib/aws-cloudfront';
import * as origins from 'aws-cdk-lib/aws-cloudfront-origins';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as opensearch from 'aws-cdk-lib/aws-opensearchservice';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as timestream from 'aws-cdk-lib/aws-timestream';
import { Construct } from 'constructs';
import * as path from 'path';

export class BetDexIndexerStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const imageUri = this.node.tryGetContext('imageUri') as string | undefined;
    const enableTimestream = this.node.tryGetContext('enableTimestream') === 'true';

    const imageRepository = new ecr.Repository(this, 'IndexerImageRepository', {
      imageScanOnPush: true,
      lifecycleRules: [
        {
          maxImageCount: 20
        }
      ],
      removalPolicy: RemovalPolicy.RETAIN
    });

    const webBucket = new s3.Bucket(this, 'WebBucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      removalPolicy: RemovalPolicy.RETAIN
    });

    const originAccessIdentity = new cloudfront.OriginAccessIdentity(this, 'WebOriginAccessIdentity');
    webBucket.grantRead(originAccessIdentity);

    const webDistribution = new cloudfront.Distribution(this, 'WebDistribution', {
      defaultRootObject: 'index.html',
      defaultBehavior: {
        origin: origins.S3BucketOrigin.withOriginAccessIdentity(webBucket, { originAccessIdentity }),
        viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS
      },
      errorResponses: [
        {
          httpStatus: 403,
          responseHttpStatus: 200,
          responsePagePath: '/index.html',
          ttl: Duration.minutes(5)
        },
        {
          httpStatus: 404,
          responseHttpStatus: 200,
          responsePagePath: '/index.html',
          ttl: Duration.minutes(5)
        }
      ]
    });

    const betdexSecret = new secretsmanager.Secret(this, 'BetDexCredentials', {
      description: 'BetDEX app/API credentials for the stream indexer',
      generateSecretString: {
        secretStringTemplate: JSON.stringify({ appKey: 'replace-me' }),
        generateStringKey: 'apiKey'
      }
    });

    const domain = new opensearch.Domain(this, 'OpenSearchDomain', {
      version: opensearch.EngineVersion.OPENSEARCH_2_11,
      capacity: {
        dataNodes: 1,
        dataNodeInstanceType: 't3.small.search',
        multiAzWithStandbyEnabled: false
      },
      zoneAwareness: {
        enabled: false
      },
      ebs: {
        volumeSize: 20
      },
      enforceHttps: true,
      nodeToNodeEncryption: true,
      encryptionAtRest: {
        enabled: true
      },
      removalPolicy: RemovalPolicy.RETAIN
    });

    let timestreamDatabase: timestream.CfnDatabase | undefined;
    let timestreamTable: timestream.CfnTable | undefined;
    let indexerContainer: ecs.ContainerDefinition | undefined;
    if (enableTimestream) {
      timestreamDatabase = new timestream.CfnDatabase(this, 'TimestreamDatabase', {
        databaseName: 'betdex'
      });

      timestreamTable = new timestream.CfnTable(this, 'TimestreamTable', {
        databaseName: timestreamDatabase.databaseName!,
        tableName: 'market_price_metrics',
        retentionProperties: {
          memoryStoreRetentionPeriodInHours: '24',
          magneticStoreRetentionPeriodInDays: '365'
        }
      });
      timestreamTable.addDependency(timestreamDatabase);
    }

    if (imageUri) {
      const vpc = new ec2.Vpc(this, 'Vpc', {
        maxAzs: 2,
        natGateways: 1
      });

      const cluster = new ecs.Cluster(this, 'Cluster', { vpc });

      const logGroup = new logs.LogGroup(this, 'IndexerLogGroup', {
        retention: logs.RetentionDays.ONE_MONTH,
        removalPolicy: RemovalPolicy.DESTROY
      });

      const taskDefinition = new ecs.FargateTaskDefinition(this, 'TaskDefinition', {
        cpu: 512,
        memoryLimitMiB: 1024
      });

      betdexSecret.grantRead(taskDefinition.taskRole);
      domain.grantReadWrite(taskDefinition.taskRole);
      if (enableTimestream) {
        taskDefinition.taskRole.addToPrincipalPolicy(new iam.PolicyStatement({
          actions: ['timestream:WriteRecords', 'timestream:DescribeEndpoints'],
          resources: ['*']
        }));
      }

      const containerImage = ecs.ContainerImage.fromEcrRepository(imageRepository, imageTagFromUri(imageUri));
      const container = taskDefinition.addContainer('Indexer', {
        image: containerImage,
        logging: ecs.LogDrivers.awsLogs({
          streamPrefix: 'betdex-indexer',
          logGroup
        }),
        environment: {
          BETDEX_REST_BASE_URL: this.node.tryGetContext('betdexRestBaseUrl') ?? 'https://sandbox.api.monacoprotocol.xyz',
          BETDEX_STREAM_URL: this.node.tryGetContext('betdexStreamUrl') ?? 'wss://sandbox.stream.api.monacoprotocol.xyz',
          OPENSEARCH_ENDPOINT: `https://${domain.domainEndpoint}`,
          TIMESTREAM_ENABLED: enableTimestream ? 'true' : 'false',
          TIMESTREAM_DATABASE: enableTimestream ? timestreamDatabase!.databaseName! : '',
          TIMESTREAM_TABLE: enableTimestream ? timestreamTable!.tableName! : '',
          AWS_REGION: Stack.of(this).region
        },
        secrets: {
          BETDEX_APP_KEY: ecs.Secret.fromSecretsManager(betdexSecret, 'appKey'),
          BETDEX_API_KEY: ecs.Secret.fromSecretsManager(betdexSecret, 'apiKey')
        },
        portMappings: [{ containerPort: 8080 }]
      });
      indexerContainer = container;

      const service = new ecs.FargateService(this, 'IndexerService', {
        cluster,
        taskDefinition,
        desiredCount: 1,
        assignPublicIp: false,
        vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
        minHealthyPercent: 0
      });

      new cloudwatch.Alarm(this, 'IndexerTaskCountLowAlarm', {
        metric: new cloudwatch.Metric({
          namespace: 'AWS/ECS',
          metricName: 'RunningTaskCount',
          dimensionsMap: {
            ClusterName: cluster.clusterName,
            ServiceName: service.serviceName
          },
          statistic: 'Minimum',
          period: Duration.minutes(1)
        }),
        threshold: 1,
        evaluationPeriods: 1,
        comparisonOperator: cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD
      });

      new cdk.CfnOutput(this, 'ContainerImage', { value: container.imageName });
    } else {
      new cdk.CfnOutput(this, 'IndexerService', {
        value: 'Not deployed. Pass -c imageUri=<ecr-image-uri> to create the Fargate service.'
      });
    }

    const api = new appsync.GraphqlApi(this, 'GraphqlApi', {
      name: 'betdex-indexer',
      definition: appsync.Definition.fromFile(path.join(__dirname, '..', 'schema', 'schema.graphql')),
      authorizationConfig: {
        defaultAuthorization: {
          authorizationType: appsync.AuthorizationType.API_KEY,
          apiKeyConfig: {
            expires: cdk.Expiration.after(Duration.days(30))
          }
        }
      },
      logConfig: {
        fieldLogLevel: appsync.FieldLogLevel.ERROR
      }
    });
    indexerContainer?.addEnvironment('APPSYNC_GRAPHQL_URL', api.graphqlUrl);
    indexerContainer?.addEnvironment('APPSYNC_API_KEY', api.apiKey ?? '');

    const openSearchDataSource = api.addHttpDataSource('OpenSearchDataSource', `https://${domain.domainEndpoint}`, {
      authorizationConfig: {
        signingRegion: Stack.of(this).region,
        signingServiceName: 'es'
      }
    });
    domain.grantRead(openSearchDataSource.grantPrincipal);
    openSearchDataSource.grantPrincipal.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: ['es:ESHttpPost'],
      resources: [`${domain.domainArn}/*`]
    }));

    openSearchDataSource.createResolver('SearchMarketsResolver', {
      typeName: 'Query',
      fieldName: 'searchMarkets',
      requestMappingTemplate: appsync.MappingTemplate.fromString(searchMarketsRequestTemplate()),
      responseMappingTemplate: appsync.MappingTemplate.fromString(searchMarketsResponseTemplate())
    });

    openSearchDataSource.createResolver('GetMarketResolver', {
      typeName: 'Query',
      fieldName: 'getMarket',
      requestMappingTemplate: appsync.MappingTemplate.fromString(getMarketRequestTemplate()),
      responseMappingTemplate: appsync.MappingTemplate.fromString(getMarketResponseTemplate())
    });

    openSearchDataSource.createResolver('GetMarketUpdatesResolver', {
      typeName: 'Query',
      fieldName: 'getMarketUpdates',
      requestMappingTemplate: appsync.MappingTemplate.fromString(getMarketUpdatesRequestTemplate()),
      responseMappingTemplate: appsync.MappingTemplate.fromString(getMarketUpdatesResponseTemplate())
    });

    const noneDataSource = api.addNoneDataSource('RealtimeStubDataSource');
    noneDataSource.createResolver('PublishMarketUpdatedResolver', {
      typeName: 'Mutation',
      fieldName: 'publishMarketUpdated',
      requestMappingTemplate: appsync.MappingTemplate.fromString('{ "version": "2018-05-29", "payload": $util.toJson($ctx.args.input) }'),
      responseMappingTemplate: appsync.MappingTemplate.fromString('$util.toJson($ctx.result)')
    });

    noneDataSource.createResolver('PriceSeriesStubResolver', {
      typeName: 'Query',
      fieldName: 'getMarketPriceSeries',
      requestMappingTemplate: appsync.MappingTemplate.fromString('{ "version": "2018-05-29", "payload": [] }'),
      responseMappingTemplate: appsync.MappingTemplate.fromString('$util.toJson($ctx.result)')
    });

    new cloudwatch.Alarm(this, 'OpenSearchClusterStatusRedAlarm', {
      metric: domain.metricClusterStatusRed({ period: Duration.minutes(1) }),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
    });

    new cdk.CfnOutput(this, 'GraphqlApiUrl', { value: api.graphqlUrl });
    new cdk.CfnOutput(this, 'GraphqlApiKey', { value: api.apiKey ?? '' });
    new cdk.CfnOutput(this, 'OpenSearchEndpoint', { value: `https://${domain.domainEndpoint}` });
    new cdk.CfnOutput(this, 'BetDexSecretArn', { value: betdexSecret.secretArn });
    new cdk.CfnOutput(this, 'EcrRepositoryUri', { value: imageRepository.repositoryUri });
    new cdk.CfnOutput(this, 'WebBucketName', { value: webBucket.bucketName });
    new cdk.CfnOutput(this, 'WebDistributionId', { value: webDistribution.distributionId });
    new cdk.CfnOutput(this, 'WebDistributionUrl', { value: `https://${webDistribution.distributionDomainName}` });
  }
}

function imageTagFromUri(imageUri: string): string {
  const separator = imageUri.lastIndexOf(':');
  if (separator <= imageUri.lastIndexOf('/')) {
    throw new Error(`imageUri must include a tag, for example ${imageUri}:latest`);
  }
  return imageUri.slice(separator + 1);
}

function searchMarketsRequestTemplate(): string {
  return `
#set($input = $ctx.args.input)
#set($must = [])
#set($filter = [])
#set($sort = [])
#set($hasQuery = false)
#set($pageSize = $util.defaultIfNull($input.pageSize, 25))
#if($pageSize < 1)
  #set($pageSize = 25)
#elseif($pageSize > 100)
  #set($pageSize = 100)
#end
#set($page = $util.defaultIfNull($input.page, 1))
#if($page < 1)
  #set($page = 1)
#end
#set($from = ($page - 1) * $pageSize)
#if($input.text)
  #set($hasQuery = true)
  $util.qr($must.add({"query_string":{"query":"*$input.text*","fields":["raw.name^4","name^4","raw.eventName^3","eventName^3","raw.event.name^3","raw.categoryName","raw.subCategoryName","raw.marketName","raw.outcomeSearchText^2","outcomeSearchText^2","raw.enrichmentSearchText^2","enrichmentSearchText^2","raw.marketOutcomes.name^2","raw.marketOutcomes.outcomeName^2","latestPrices.name^2","latestPrices.outcomeName^2","raw.id","raw.marketId","raw.eventId"],"default_operator":"AND","analyze_wildcard":true}}))
#end
#if(!$util.isNull($input.eventId))
  #set($hasQuery = true)
  $util.qr($filter.add({"bool":{"should":[{"term":{"eventId.keyword":$input.eventId}},{"term":{"eventId":$input.eventId}},{"match_phrase":{"eventId":$input.eventId}},{"term":{"raw.eventId.keyword":$input.eventId}},{"term":{"raw.eventId":$input.eventId}},{"match_phrase":{"raw.eventId":$input.eventId}},{"term":{"raw.event_id.keyword":$input.eventId}},{"term":{"raw.event_id":$input.eventId}},{"match_phrase":{"raw.event_id":$input.eventId}}],"minimum_should_match":1}}))
#end
#if(!$util.isNull($input.categoryId))
  #set($hasQuery = true)
  $util.qr($filter.add({"bool":{"should":[{"term":{"categoryId.keyword":$input.categoryId}},{"term":{"categoryId":$input.categoryId}},{"match_phrase":{"categoryId":$input.categoryId}},{"term":{"raw.categoryId.keyword":$input.categoryId}},{"term":{"raw.categoryId":$input.categoryId}},{"match_phrase":{"raw.categoryId":$input.categoryId}},{"term":{"raw.category_id.keyword":$input.categoryId}},{"term":{"raw.category_id":$input.categoryId}},{"match_phrase":{"raw.category_id":$input.categoryId}}],"minimum_should_match":1}}))
#end
#if(!$util.isNull($input.subCategoryId))
  #set($hasQuery = true)
  $util.qr($filter.add({"bool":{"should":[{"term":{"subCategoryId.keyword":$input.subCategoryId}},{"term":{"subCategoryId":$input.subCategoryId}},{"match_phrase":{"subCategoryId":$input.subCategoryId}},{"term":{"raw.subCategoryId.keyword":$input.subCategoryId}},{"term":{"raw.subCategoryId":$input.subCategoryId}},{"match_phrase":{"raw.subCategoryId":$input.subCategoryId}},{"term":{"raw.subCategory_id.keyword":$input.subCategoryId}},{"term":{"raw.subCategory_id":$input.subCategoryId}},{"match_phrase":{"raw.subCategory_id":$input.subCategoryId}}],"minimum_should_match":1}}))
#end
#if(!$util.isNull($input.eventGroupId))
  #set($hasQuery = true)
  $util.qr($filter.add({"bool":{"should":[{"term":{"eventGroupId.keyword":$input.eventGroupId}},{"term":{"eventGroupId":$input.eventGroupId}},{"match_phrase":{"eventGroupId":$input.eventGroupId}},{"term":{"raw.eventGroupId.keyword":$input.eventGroupId}},{"term":{"raw.eventGroupId":$input.eventGroupId}},{"match_phrase":{"raw.eventGroupId":$input.eventGroupId}},{"term":{"raw.eventGroup_id.keyword":$input.eventGroupId}},{"term":{"raw.eventGroup_id":$input.eventGroupId}},{"match_phrase":{"raw.eventGroup_id":$input.eventGroupId}}],"minimum_should_match":1}}))
#end
#if(!$util.isNull($input.status))
  #set($hasQuery = true)
  $util.qr($filter.add({"term":{"raw.status.keyword":$input.status}}))
#end
#if(!$util.isNull($input.categoryIds) && $input.categoryIds.size() > 0)
  #set($hasQuery = true)
  $util.qr($filter.add({"bool":{"should":[{"terms":{"categoryId.keyword":$input.categoryIds}},{"terms":{"categoryId":$input.categoryIds}},{"terms":{"raw.categoryId.keyword":$input.categoryIds}},{"terms":{"raw.categoryId":$input.categoryIds}},{"terms":{"raw.category_id.keyword":$input.categoryIds}},{"terms":{"raw.category_id":$input.categoryIds}}],"minimum_should_match":1}}))
#end
#if(!$util.isNull($input.subCategoryIds) && $input.subCategoryIds.size() > 0)
  #set($hasQuery = true)
  $util.qr($filter.add({"bool":{"should":[{"terms":{"subCategoryId.keyword":$input.subCategoryIds}},{"terms":{"subCategoryId":$input.subCategoryIds}},{"terms":{"raw.subCategoryId.keyword":$input.subCategoryIds}},{"terms":{"raw.subCategoryId":$input.subCategoryIds}},{"terms":{"raw.subCategory_id.keyword":$input.subCategoryIds}},{"terms":{"raw.subCategory_id":$input.subCategoryIds}}],"minimum_should_match":1}}))
#end
#if(!$util.isNull($input.eventGroupIds) && $input.eventGroupIds.size() > 0)
  #set($hasQuery = true)
  $util.qr($filter.add({"bool":{"should":[{"terms":{"eventGroupId.keyword":$input.eventGroupIds}},{"terms":{"eventGroupId":$input.eventGroupIds}},{"terms":{"raw.eventGroupId.keyword":$input.eventGroupIds}},{"terms":{"raw.eventGroupId":$input.eventGroupIds}},{"terms":{"raw.eventGroup_id.keyword":$input.eventGroupIds}},{"terms":{"raw.eventGroup_id":$input.eventGroupIds}}],"minimum_should_match":1}}))
#end
#if(!$util.isNull($input.statuses) && $input.statuses.size() > 0)
  #set($hasQuery = true)
  $util.qr($filter.add({"terms":{"raw.status.keyword":$input.statuses}}))
#end
#if($input.hasLiquidity == true)
  #set($hasQuery = true)
  $util.qr($filter.add({"range":{"liquidity":{"gt":0}}}))
#end
#if(!$util.isNull($input.inPlay) && $input.inPlay.size() > 0)
  #set($hasQuery = true)
  #set($wantsLive = false)
  #set($wantsPre = false)
  #foreach($flag in $input.inPlay)
    #if($flag)
      #set($wantsLive = true)
    #else
      #set($wantsPre = true)
    #end
  #end
  #if($wantsLive && !$wantsPre)
    $util.qr($filter.add({"term":{"raw.inPlayStatus.keyword":"InPlay"}}))
  #elseif($wantsPre && !$wantsLive)
    $util.qr($filter.add({"terms":{"raw.inPlayStatus.keyword":["NotApplicable","PrePlay"]}}))
  #end
#end
#if($input.startsAfter || $input.startsBefore)
  #set($hasQuery = true)
  #set($range = {})
  #if($input.startsAfter) $util.qr($range.put("gte", $input.startsAfter)) #end
  #if($input.startsBefore) $util.qr($range.put("lte", $input.startsBefore)) #end
  $util.qr($filter.add({"range":{"raw.lockAt":$range}}))
#end
#if($util.isNull($input.sort) || $input.sort == "START_TIME" || $input.sort == "RELEVANCE")
  $util.qr($sort.add({"raw.lockAt":{"order":"asc","unmapped_type":"date","missing":"_last"}}))
#elseif($input.sort == "MATCHED")
  $util.qr($sort.add({"matched":{"order":"desc","unmapped_type":"double","missing":"_last"}}))
#elseif($input.sort == "LIQUIDITY")
  $util.qr($sort.add({"liquidity":{"order":"desc","unmapped_type":"double","missing":"_last"}}))
#end
#if($hasQuery)
  #set($query = {"bool": { "must": $must, "filter": $filter }})
#else
  #set($query = {"match_all": {}})
#end
{
  "version": "2018-05-29",
  "method": "POST",
  "resourcePath": "/betdex-markets-current/_search",
  "params": {
    "headers": { "Content-Type": "application/json" },
    "body": {
      "from": $from,
      "size": $pageSize,
      "track_total_hits": true,
      "sort": $util.toJson($sort),
      "query": $util.toJson($query)
    }
  }
}`;
}

function searchMarketsResponseTemplate(): string {
  return `
#set($body = $util.parseJson($ctx.result.body))
#if($ctx.result.statusCode < 200 || $ctx.result.statusCode >= 300)
  $util.error($ctx.result.body, "OpenSearchSearchFailed")
#end
#set($hits = $body.hits.hits)
#set($total = $body.hits.total)
#if(!$util.isNull($total.value))
  #set($total = $total.value)
#end
{
  "items": [
    #foreach($hit in $hits)
    #set($hitId = $hit.get("_id"))
    #set($source = $hit.get("_source"))
    { "id": $util.toJson($hitId), "source": $util.toJson($source) }#if($foreach.hasNext),#end
    #end
  ],
  "total": $util.defaultIfNull($total, 0),
  "nextToken": null
}`;
}

function getMarketRequestTemplate(): string {
  return `
{
  "version": "2018-05-29",
  "method": "GET",
  "resourcePath": "/betdex-markets-current/_doc/$ctx.args.marketId"
}`;
}

function getMarketResponseTemplate(): string {
  return `
#set($body = $util.parseJson($ctx.result.body))
#if(!$body.found)
  null
#else
  #set($hitId = $body.get("_id"))
  #set($src = $body.get("_source"))
  #set($raw = $src.raw)
  #set($marketId = $util.defaultIfNull($src.marketId, $util.defaultIfNull($raw.marketId, $util.defaultIfNull($raw.id, $hitId))))
  #set($eventId = $util.defaultIfNull($src.eventId, $raw.eventId))
  #set($name = $raw.name)
  #if($util.isNull($name) || $name == $marketId)
    #set($name = $util.defaultIfNull($src.name, $name))
  #end
  {
    "marketId": $util.toJson($marketId),
    "name": $util.toJson($name),
    "eventId": $util.toJson($eventId),
    "status": $util.toJson($raw.status),
    "raw": $util.toJson($raw)
  }
#end`;
}

function getMarketUpdatesRequestTemplate(): string {
  return `
#set($filter = [{"term":{"marketId":$ctx.args.marketId}}])
#if($ctx.args.from || $ctx.args.to)
  #set($range = {})
  #if($ctx.args.from) $util.qr($range.put("gte", $ctx.args.from)) #end
  #if($ctx.args.to) $util.qr($range.put("lte", $ctx.args.to)) #end
  $util.qr($filter.add({"range":{"receivedAt":$range}}))
#end
{
  "version": "2018-05-29",
  "method": "POST",
  "resourcePath": "/betdex-stream-raw-*/_search",
  "params": {
    "headers": { "Content-Type": "application/json" },
    "body": {
      "size": 100,
      "sort": [{"receivedAt":"desc"}],
      "query": { "bool": { "filter": $util.toJson($filter) } }
    }
  }
}`;
}

function getMarketUpdatesResponseTemplate(): string {
  return `
#set($body = $util.parseJson($ctx.result.body))
[
  #foreach($hit in $body.hits.hits)
    #set($src = $hit.get("_source"))
    {
      "marketId": "$src.marketId",
      "messageType": "$src.messageType",
      "receivedAt": "$src.receivedAt",
      "payload": $util.toJson($util.defaultIfNull($src.payloadJson, "{}"))
    }#if($foreach.hasNext),#end
  #end
]`;
}
