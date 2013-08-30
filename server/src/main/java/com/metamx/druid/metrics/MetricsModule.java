/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.metrics;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.metamx.common.logger.Logger;
import com.metamx.druid.concurrent.Execs;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.metrics.Monitor;
import com.metamx.metrics.MonitorScheduler;
import io.druid.guice.guice.JsonConfigProvider;
import io.druid.guice.guice.JsonConfigurator;
import io.druid.guice.guice.LazySingleton;
import io.druid.guice.guice.ManageLifecycle;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Sets up the {@link MonitorScheduler} to monitor things on a regular schedule.  {@link Monitor}s must be explicitly
 * bound in order to be loaded.
 */
public class MetricsModule implements Module
{
  private static final Logger log = new Logger(MetricsModule.class);

  private final List<Class<? extends Monitor>> monitors = new CopyOnWriteArrayList<Class<? extends Monitor>>();
  public boolean configured = false;

  public MetricsModule register(Class<? extends Monitor> monitorClazz)
  {
    synchronized (monitors) {
      Preconditions.checkState(!configured, "Cannot register monitor[%s] after configuration.", monitorClazz);
    }
    monitors.add(monitorClazz);
    return this;
  }

  @Inject
  public void setProperties(Properties props, JsonConfigurator configurator)
  {
    final MonitorsConfig config = configurator.configurate(
        props,
        "druid.monitoring",
        MonitorsConfig.class
    );

    for (Class<? extends Monitor> monitorClazz : config.getMonitors()) {
      register(monitorClazz);
    }
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.monitoring", DruidMonitorSchedulerConfig.class);

    for (Class<? extends Monitor> monitor : monitors) {
      binder.bind(monitor).in(LazySingleton.class);
    }

    // Instantiate eagerly so that we get everything registered and put into the Lifecycle
    binder.bind(Key.get(MonitorScheduler.class, Names.named("ForTheEagerness")))
          .to(MonitorScheduler.class)
          .asEagerSingleton();
  }

  @Provides
  @ManageLifecycle
  public MonitorScheduler getMonitorScheduler(
      Supplier<DruidMonitorSchedulerConfig> config,
      ServiceEmitter emitter,
      Injector injector
  )
  {
    List<Monitor> monitors = Lists.newArrayList();

    for (Key<?> key : injector.getBindings().keySet()) {
      if (Monitor.class.isAssignableFrom(key.getTypeLiteral().getRawType())) {
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
