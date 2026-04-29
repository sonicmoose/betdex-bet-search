#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { BetDexIndexerStack } from '../lib/betdex-indexer-stack';

const app = new cdk.App();

new BetDexIndexerStack(app, 'BetDexIndexerStack', {
	env: {
	  account: process.env.CDK_DEFAULT_ACCOUNT,
	  region: process.env.CDK_DEFAULT_REGION ?? 'eu-west-2'
	}
});
