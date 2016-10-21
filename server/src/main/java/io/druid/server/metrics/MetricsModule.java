/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.metrics;

import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.metrics.JvmCpuMonitor;
import com.metamx.metrics.JvmMonitor;
import com.metamx.metrics.Monitor;
import com.metamx.metrics.MonitorScheduler;
import com.metamx.metrics.SysMonitor;
import io.druid.concurrent.Execs;
import io.druid.guice.DruidBinders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.ManageLifecycle;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.ExecutorServiceMonitor;

import java.util.List;
import java.util.Set;

/**
 * Sets up the {@link MonitorScheduler} to monitor things on a regular schedule.  {@link Monitor}s must be explicitly
 * bound in order to be loaded.
 */
public class MetricsModule implements Module
{
  private static final Logger log = new Logger(MetricsModule.class);

  public static void register(Binder binder, Class<? extends Monitor> monitorClazz)
  {
    DruidBinders.metricMonitorBinder(binder).addBinding().toInstance(monitorClazz);
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.monitoring", DruidMonitorSchedulerConfig.class);
    JsonConfigProvider.bind(binder, "druid.monitoring", MonitorsConfig.class);

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
    List<Monitor> monitors = Lists.newArrayList();

    for (Class<? extends Monitor> monitorClass : Iterables.concat(monitorsConfig.getMonitors(), monitorSet)) {
      final Monitor monitor = injector.getInstance(monitorClass);

      log.info("Adding monitor[%s]", monitor);

      monitors.add(monitor);
    }

    return new MonitorScheduler(
        config.get(),
        Execs.scheduledSingleThreaded("MonitorScheduler-%s"),
        emitter,
        monitors
    );
  }

  @Provides
  @ManageLifecycle
  public JvmMonitor getJvmMonitor(
      DataSourceTaskIdHolder dataSourceTaskIdHolder
  )
  {
    return new JvmMonitor(MonitorsConfig.mapOfDatasourceAndTaskID(
        dataSourceTaskIdHolder.getDataSource(),
        dataSourceTaskIdHolder.getTaskId()
    ));
  }

  @Provides
  @ManageLifecycle
  public JvmCpuMonitor getJvmCpuMonitor(
      DataSourceTaskIdHolder dataSourceTaskIdHolder
  )
  {
    return new JvmCpuMonitor(MonitorsConfig.mapOfDatasourceAndTaskID(
        dataSourceTaskIdHolder.getDataSource(),
        dataSourceTaskIdHolder.getTaskId()
    ));
  }

  @Provides
  @ManageLifecycle
  public SysMonitor getSysMonitor(
      DataSourceTaskIdHolder dataSourceTaskIdHolder
  )
  {
    return new SysMonitor(MonitorsConfig.mapOfDatasourceAndTaskID(
        dataSourceTaskIdHolder.getDataSource(),
        dataSourceTaskIdHolder.getTaskId()
    ));
  }
}
