package com.metamx.druid.metrics;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.logger.Logger;
import com.metamx.druid.concurrent.Execs;
import com.metamx.druid.guice.JsonConfigProvider;
import com.metamx.druid.guice.LazySingleton;
import com.metamx.druid.guice.ManageLifecycle;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.metrics.JvmMonitor;
import com.metamx.metrics.Monitor;
import com.metamx.metrics.MonitorScheduler;
import com.metamx.metrics.SysMonitor;

import java.util.List;

/**
 * Sets up the {@link MonitorScheduler} to monitor things on a regular schedule.  {@link Monitor}s must be explicitly
 * bound in order to be loaded.
 */
public class MetricsModule implements Module
{
  private static final Logger log = new Logger(MetricsModule.class);

  private final Class<? extends Monitor>[] monitors;

  /**
   * A constructor that takes a list of {@link Monitor} classes to explicitly bind so that they will be instantiated
   *
   * @param monitors list of {@link Monitor} classes to explicitly bind
   */
  public MetricsModule(
      Class<? extends Monitor>... monitors
  )
  {
    this.monitors = monitors;
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.monitoring", DruidMonitorSchedulerConfig.class);

    binder.bind(JvmMonitor.class).in(LazySingleton.class);
    binder.bind(SysMonitor.class).in(LazySingleton.class); // TODO: allow for disabling of this monitor

    for (Class<? extends Monitor> monitor : monitors) {
      binder.bind(monitor).in(LazySingleton.class);
    }
  }

  @Provides @ManageLifecycle
  public MonitorScheduler getMonitorScheduler(
      Supplier<DruidMonitorSchedulerConfig> config,
      ServiceEmitter emitter,
      Injector injector
  )
  {
    List<Monitor> monitors = Lists.newArrayList();

    for (Key<?> key: injector.getBindings().keySet()) {
      if (Monitor.class.isAssignableFrom(key.getClass())) {
        final Monitor monitor = (Monitor) injector.getInstance(key);

        log.info("Adding monitor[%s]", monitor);

        monitors.add(monitor);
      }
    }

    return new MonitorScheduler(
        config.get(),
        Execs.scheduledSingleThreaded("MonitorScheduler-%s"),
        emitter,
        monitors
    );
  }
}
