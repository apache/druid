package io.druid.cli;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.druid.initialization.LogLevelAdjuster;

import java.util.List;

/**
 */
public abstract class GuiceRunnable implements Runnable
{
  private final Logger log;

  private Injector baseInjector;

  public GuiceRunnable(Logger log)
  {
    this.log = log;
  }

  @Inject
  public void configure(Injector injector)
  {
    this.baseInjector = injector;
  }

  public Injector getBaseInjector()
  {
    return baseInjector;
  }

  protected abstract List<Object> getModules();

  public Injector makeInjector()
  {
    try {
      return Initialization.makeInjectorWithModules(
          baseInjector, getModules()
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public Lifecycle initLifecycle(Injector injector)
  {
    try {
      LogLevelAdjuster.register();
      final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);

      try {
        lifecycle.start();
      }
      catch (Throwable t) {
        log.error(t, "Error when starting up.  Failing.");
        System.exit(1);
      }

      return lifecycle;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void run()
  {
    initLifecycle(makeInjector());
  }
}
