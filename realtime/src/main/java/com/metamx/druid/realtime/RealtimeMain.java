package com.metamx.druid.realtime;

import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.log.LogLevelAdjuster;

/**
 */
public class RealtimeMain
{
  private static final Logger log = new Logger(RealtimeMain.class);

  public static void main(String[] args) throws Exception
  {
    LogLevelAdjuster.register();

    Lifecycle lifecycle = new Lifecycle();

    lifecycle.addManagedInstance(
        RealtimeNode.builder().build()
    );

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