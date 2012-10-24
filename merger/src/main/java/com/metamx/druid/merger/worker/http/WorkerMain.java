package com.metamx.druid.merger.worker.http;

import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.log.LogLevelAdjuster;

/**
 */
public class WorkerMain
{
  private static final Logger log = new Logger(WorkerMain.class);

  public static void main(String[] args) throws Exception
  {
    LogLevelAdjuster.register();

    Lifecycle lifecycle = new Lifecycle();

    lifecycle.addManagedInstance(WorkerNode.builder().build());

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
