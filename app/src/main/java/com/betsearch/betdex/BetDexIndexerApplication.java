package com.betsearch.betdex;

import com.betsearch.betdex.config.BetDexProperties;
import com.betsearch.betdex.config.AppSyncProperties;
import com.betsearch.betdex.config.IngestProperties;
import com.betsearch.betdex.config.OpenSearchProperties;
import com.betsearch.betdex.config.TimestreamProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({
    BetDexProperties.class,
    AppSyncProperties.class,
    IngestProperties.class,
    OpenSearchProperties.class,
    TimestreamProperties.class
})
public class BetDexIndexerApplication {
  public static void main(String[] args) {
    SpringApplication.run(BetDexIndexerApplication.class, args);
  }
}
