package com.betsearch.betdex.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;

@Configuration
public class AwsClientsConfig {
  @Bean
  Region awsRegion(@Value("${aws.region}") String region) {
    return Region.of(region);
  }

  @Bean
  AwsCredentialsProvider awsCredentialsProvider() {
    return DefaultCredentialsProvider.create();
  }

  @Bean
  TimestreamWriteClient timestreamWriteClient(Region region) {
    return TimestreamWriteClient.builder().region(region).build();
  }
}
