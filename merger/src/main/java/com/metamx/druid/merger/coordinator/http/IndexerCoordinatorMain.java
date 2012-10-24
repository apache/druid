package com.metamx.druid.merger.coordinator.http;

import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.log.LogLevelAdjuster;

/**
 */
public class IndexerCoordinatorMain
{
  private static final Logger log = new Logger(IndexerCoordinatorMain.class);

  public static void main(String[] args) throws Exception
  {
    LogLevelAdjuster.register();

    Lifecycle lifecycle = new Lifecycle();

    lifecycle.addManagedInstance(IndexerCoordinatorNode.builder().build());

    try {
      lifecycle.start();
    }
    catch (Throwable t) {
      log.info(t, "Throwable caught at startup, committing seppuku");
      System.exit(2);
    }

    lifecycle.join();
  }
}