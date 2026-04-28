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

    const imageRepository = new ecr.Repository(this, 'IndexerImageRepository', {
      repositoryName: 'betdex-indexer',
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

    const timestreamDatabase = new timestream.CfnDatabase(this, 'TimestreamDatabase', {
      databaseName: 'betdex'
    });

    const timestreamTable = new timestream.CfnTable(this, 'TimestreamTable', {
      databaseName: timestreamDatabase.databaseName!,
      tableName: 'market_price_metrics',
      retentionProperties: {
        memoryStoreRetentionPeriodInHours: '24',
        magneticStoreRetentionPeriodInDays: '365'
      }
    });
    timestreamTable.addDependency(timestreamDatabase);

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
      taskDefinition.taskRole.addToPrincipalPolicy(new iam.PolicyStatement({
        actions: ['timestream:WriteRecords', 'timestream:DescribeEndpoints'],
        resources: ['*']
      }));

      const container = taskDefinition.addContainer('Indexer', {
        image: ecs.ContainerImage.fromRegistry(imageUri),
        logging: ecs.LogDrivers.awsLogs({
          streamPrefix: 'betdex-indexer',
          logGroup
        }),
        environment: {
          BETDEX_REST_BASE_URL: this.node.tryGetContext('betdexRestBaseUrl') ?? 'https://api.betdex.com',
          BETDEX_STREAM_URL: this.node.tryGetContext('betdexStreamUrl') ?? 'wss://production.stream.api.monacoprotocol.xyz',
          OPENSEARCH_ENDPOINT: `https://${domain.domainEndpoint}`,
          TIMESTREAM_ENABLED: 'true',
          TIMESTREAM_DATABASE: timestreamDatabase.databaseName!,
          TIMESTREAM_TABLE: timestreamTable.tableName!,
          AWS_REGION: Stack.of(this).region
        },
        secrets: {
          BETDEX_APP_KEY: ecs.Secret.fromSecretsManager(betdexSecret, 'appKey'),
          BETDEX_API_KEY: ecs.Secret.fromSecretsManager(betdexSecret, 'apiKey')
        },
        portMappings: [{ containerPort: 8080 }]
      });

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

    const openSearchDataSource = api.addHttpDataSource('OpenSearchDataSource', `https://${domain.domainEndpoint}`, {
      authorizationConfig: {
        signingRegion: Stack.of(this).region,
        signingServiceName: 'es'
      }
    });
    domain.grantRead(openSearchDataSource.grantPrincipal);

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
    new cdk.CfnOutput(this, 'OpenSearchEndpoint', { value: `https://${domain.domainEndpoint}` });
    new cdk.CfnOutput(this, 'BetDexSecretArn', { value: betdexSecret.secretArn });
    new cdk.CfnOutput(this, 'EcrRepositoryUri', { value: imageRepository.repositoryUri });
    new cdk.CfnOutput(this, 'WebBucketName', { value: webBucket.bucketName });
    new cdk.CfnOutput(this, 'WebDistributionId', { value: webDistribution.distributionId });
    new cdk.CfnOutput(this, 'WebDistributionUrl', { value: `https://${webDistribution.distributionDomainName}` });
  }
}

function searchMarketsRequestTemplate(): string {
  return `
#set($input = $ctx.args.input)
#set($must = [])
#set($filter = [])
#if($input.text)
  $util.qr($must.add({"multi_match":{"query":$input.text,"fields":["raw.name^3","raw.eventName","raw.categoryName","raw.*"]}}))
#else
  $util.qr($must.add({"match_all":{}}))
#end
#foreach($field in ["eventId","categoryId","subCategoryId","status"])
  #if(!$util.isNull($input[$field]))
    $util.qr($filter.add({"term":{"raw.$field":$input[$field]}}))
  #end
#end
#if(!$util.isNull($input.inPlay))
  #if($input.inPlay)
    $util.qr($filter.add({"term":{"raw.inPlayStatus":"InPlay"}}))
  #else
    $util.qr($filter.add({"terms":{"raw.inPlayStatus":["NotApplicable","PrePlay"]}}))
  #end
#end
#if($input.startsAfter || $input.startsBefore)
  #set($range = {})
  #if($input.startsAfter) $util.qr($range.put("gte", $input.startsAfter)) #end
  #if($input.startsBefore) $util.qr($range.put("lte", $input.startsBefore)) #end
  $util.qr($filter.add({"range":{"raw.lockAt":$range}}))
#end
{
  "version": "2018-05-29",
  "method": "POST",
  "resourcePath": "/betdex-markets-current/_search",
  "params": {
    "headers": { "Content-Type": "application/json" },
    "body": {
      "size": $util.defaultIfNull($input.pageSize, 25),
      "query": { "bool": { "must": $util.toJson($must), "filter": $util.toJson($filter) } }
    }
  }
}`;
}

function searchMarketsResponseTemplate(): string {
  return `
#set($body = $util.parseJson($ctx.result.body))
#set($hits = $body.hits.hits)
{
  "items": [
    #foreach($hit in $hits)
    { "id": "$hit._id", "source": $util.toJson($hit._source.raw) }#if($foreach.hasNext),#end
    #end
  ],
  "total": $util.defaultIfNull($body.hits.total.value, 0),
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
  #set($src = $body._source)
  {
    "marketId": "$src.marketId",
    "name": $util.toJson($src.raw.name),
    "eventId": $util.toJson($src.eventId),
    "status": $util.toJson($src.raw.status),
    "raw": $util.toJson($src.raw)
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
    #set($src = $hit._source)
    {
      "marketId": "$src.marketId",
      "messageType": "$src.messageType",
      "receivedAt": "$src.receivedAt",
      "payload": $util.toJson($src.payload)
    }#if($foreach.hasNext),#end
  #end
]`;
}
