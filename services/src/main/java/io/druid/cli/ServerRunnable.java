package io.druid.cli;

import com.google.common.base.Throwables;
import com.google.inject.Injector;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.log.LogLevelAdjuster;

/**
 */
public abstract class ServerRunnable implements Runnable
{
  private final Logger log;

  public ServerRunnable(Logger log)
  {
    this.log = log;
  }

  protected abstract Injector getInjector();

  @Override
  public void run()
  {
    try {
      LogLevelAdjuster.register();

      final Lifecycle lifecycle = getInjector().getInstance(Lifecycle.class);

      try {
        lifecycle.start();
      }
      catch (Throwable t) {
        log.error(t, "Error when starting up.  Failing.");
        System.exit(1);
      }

      lifecycle.join();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
