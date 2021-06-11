/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.metrics;

import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.timeandspace.cronscheduler.CronScheduler;
import org.apache.druid.guice.DruidBinders;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.BasicMonitorScheduler;
import org.apache.druid.java.util.metrics.ClockDriftSafeMonitorScheduler;
import org.apache.druid.java.util.metrics.JvmCpuMonitor;
import org.apache.druid.java.util.metrics.JvmMonitor;
import org.apache.druid.java.util.metrics.JvmThreadsMonitor;
import org.apache.druid.java.util.metrics.Monitor;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.apache.druid.java.util.metrics.SysMonitor;
import org.apache.druid.query.ExecutorServiceMonitor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Sets up the {@link MonitorScheduler} to monitor things on a regular schedule.  {@link Monitor}s must be explicitly
 * bound in order to be loaded.
 */
public class MetricsModule implements Module
{
  static final String MONITORING_PROPERTY_PREFIX = "druid.monitoring";
  private static final Logger log = new Logger(MetricsModule.class);

  public static void register(Binder binder, Class<? extends Monitor> monitorClazz)
  {
    DruidBinders.metricMonitorBinder(binder).addBinding().toInstance(monitorClazz);
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, MONITORING_PROPERTY_PREFIX, DruidMonitorSchedulerConfig.class);
    JsonConfigProvider.bind(binder, MONITORING_PROPERTY_PREFIX, MonitorsConfig.class);

    DruidBinders.metricMonitorBinder(binder); // get the binder so that it will inject the empty set at a minimum.

    binder.bind(DataSourceTaskIdHolder.class).in(LazySingleton.class);

    binder.bind(EventReceiverFirehoseRegister.class).in(LazySingleton.class);
    binder.bind(ExecutorServiceMonitor.class).in(LazySingleton.class);

    // Instantiate eagerly so that we get everything registered and put into the Lifecycle
    binder.bind(Key.get(MonitorScheduler.class, Names.named("ForTheEagerness")))
          .to(MonitorScheduler.class)
          .asEagerSingleton();
  }

  @Provides
  @ManageLifecycle
  public MonitorScheduler getMonitorScheduler(
      Supplier<DruidMonitorSchedulerConfig> config,
      MonitorsConfig monitorsConfig,
      Set<Class<? extends Monitor>> monitorSet,
      ServiceEmitter emitter,
      Injector injector
  )
  {
    List<Monitor> monitors = new ArrayList<>();

    for (Class<? extends Monitor> monitorClass : Iterables.concat(monitorsConfig.getMonitors(), monitorSet)) {
      monitors.add(injector.getInstance(monitorClass));
    }

    if (!monitors.isEmpty()) {
      log.info(
          "Loaded %d monitors: %s",
          monitors.size(),
          monitors.stream().map(monitor -> monitor.getClass().getName()).collect(Collectors.joining(", "))
      );
    }

    if (ClockDriftSafeMonitorScheduler.class.getName().equals(config.get().getSchedulerClassName())) {
      return new ClockDriftSafeMonitorScheduler(
          config.get(),
          emitter,
          monitors,
          CronScheduler.newBuilder(Duration.ofSeconds(1L)).setThreadName("MonitorScheduler").build(),
          Execs.singleThreaded("MonitorRunner")
      );
    } else if (BasicMonitorScheduler.class.getName().equals(config.get().getSchedulerClassName())) {
      return new BasicMonitorScheduler(
          config.get(),
          emitter,
          monitors,
          Execs.scheduledSingleThreaded("MonitorScheduler-%s")
      );
    } else {
      throw new IAE("Unknown monitor scheduler[%s]", config.get().getSchedulerClassName());
    }
  }

  @Provides
  @ManageLifecycle
  public JvmMonitor getJvmMonitor(
      DataSourceTaskIdHolder dataSourceTaskIdHolder
  )
  {
    Map<String, String[]> dimensions = MonitorsConfig.mapOfDatasourceAndTaskID(
        dataSourceTaskIdHolder.getDataSource(),
        dataSourceTaskIdHolder.getTaskId()
    );
    return new JvmMonitor(dimensions);
  }

  @Provides
  @ManageLifecycle
  public JvmCpuMonitor getJvmCpuMonitor(
      DataSourceTaskIdHolder dataSourceTaskIdHolder
  )
  {
    Map<String, String[]> dimensions = MonitorsConfig.mapOfDatasourceAndTaskID(
        dataSourceTaskIdHolder.getDataSource(),
        dataSourceTaskIdHolder.getTaskId()
    );
    return new JvmCpuMonitor(dimensions);
  }

  @Provides
  @ManageLifecycle
  public JvmThreadsMonitor getJvmThreadsMonitor(DataSourceTaskIdHolder dataSourceTaskIdHolder)
  {
    Map<String, String[]> dimensions = MonitorsConfig.mapOfDatasourceAndTaskID(
        dataSourceTaskIdHolder.getDataSource(),
        dataSourceTaskIdHolder.getTaskId()
    );
    return new JvmThreadsMonitor(dimensions);
  }

  @Provides
  @ManageLifecycle
  public SysMonitor getSysMonitor(
      DataSourceTaskIdHolder dataSourceTaskIdHolder
  )
  {
    Map<String, String[]> dimensions = MonitorsConfig.mapOfDatasourceAndTaskID(
        dataSourceTaskIdHolder.getDataSource(),
        dataSourceTaskIdHolder.getTaskId()
    );
    return new SysMonitor(dimensions);
  }
}
