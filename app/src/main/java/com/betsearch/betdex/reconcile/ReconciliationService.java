package com.betsearch.betdex.reconcile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ReconciliationService {
  private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);

  public void reconcile() {
    // TODO Fetch the full REST world and rebuild OpenSearch current-state indexes.
    log.info("BetDEX reconciliation is not implemented yet");
  }
}
