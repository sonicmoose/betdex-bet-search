# BetDEX AWS Indexer

Java Spring Boot service and AWS CDK infrastructure for ingesting BetDEX market stream data into OpenSearch, with optional Timestream metrics and AppSync read access.

## Layout

- `app/` - Java 21 Spring Boot stream indexer
- `infra/` - AWS CDK TypeScript infrastructure
- `web/` - Vite React market search UI, currently backed by mock data

## Service

Required environment:

- `BETDEX_REST_BASE_URL`
- `BETDEX_STREAM_URL`
- `BETDEX_APP_KEY` - Monaco/BetDEX app id
- `BETDEX_API_KEY`
- `OPENSEARCH_ENDPOINT`
- `TIMESTREAM_DATABASE`
- `TIMESTREAM_TABLE`
- `AWS_REGION`

Optional:

- `TIMESTREAM_ENABLED=false`
- `INGEST_QUEUE_CAPACITY=10000`
- `INGEST_WORKER_COUNT=2`

Run locally:

```bash
gradle :app:test
gradle :app:bootRun
```

The WebSocket frames follow the Monaco/BetDEX Stream API docs:

- authenticate with `{ "action": "authenticate", "accessToken": "..." }`
- subscribe to `EventUpdate`, `MarketUpdate`, `MarketPriceUpdate`, and `MarketStatusUpdate`
- use `subscriptionIds: ["*"]`

The REST session endpoint is isolated in `BetDexSessionClient` with a TODO because the exact session path/body should be confirmed against the current REST API docs.

## Infrastructure

```bash
cd infra
npm install
npm run build
npx cdk synth
```

Deploy with a container image published to ECR and pass `imageUri`:

```bash
npx cdk deploy -c imageUri=123456789012.dkr.ecr.eu-west-2.amazonaws.com/betdex-indexer:latest
```

The stack creates an ECR repository and static web hosting bucket/distribution. ECS/Fargate is only created when `imageUri` is passed, which keeps the first infra deploy lighter for personal-account testing.

## Web UI

```bash
cd web
npm install
npm run dev
```

The UI is wired to `src/mockApi.ts` for now. Replace that module with AppSync calls once the API is deployed.

## GitHub Actions

Manual workflows are in `.github/workflows/`:

- `Deploy Infra` deploys the CDK stack without ECS unless an image has already been passed separately.
- `Deploy Java Indexer` runs tests, ensures base infra exists, pushes the Docker image to ECR, then redeploys CDK with `imageUri`.
- `Deploy Web` builds `web/`, syncs `dist/` to S3, and invalidates CloudFront.

Configure repository secret `AWS_ROLE_ARN` with a GitHub OIDC deploy role ARN before running them. The default stack name is `BetDexIndexerStack`.
