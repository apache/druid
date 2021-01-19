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

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.BasicMonitorScheduler;
import org.apache.druid.java.util.metrics.ClockDriftSafeMonitorScheduler;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.apache.druid.server.DruidNode;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;

public class MetricsModuleTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSimpleInjection()
  {
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            JsonConfigProvider.bindInstance(
                binder,
                Key.get(DruidNode.class, Self.class),
                new DruidNode("test-inject", null, false, null, null, true, false)
            );
          }
        })
    );
    final DataSourceTaskIdHolder dimensionIdHolder = new DataSourceTaskIdHolder();
    injector.injectMembers(dimensionIdHolder);
    Assert.assertNull(dimensionIdHolder.getDataSource());
    Assert.assertNull(dimensionIdHolder.getTaskId());
  }

  @Test
  public void testSimpleInjectionWithValues()
  {
    final String dataSource = "some datasource";
    final String taskId = "some task";
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            JsonConfigProvider.bindInstance(
                binder,
                Key.get(DruidNode.class, Self.class),
                new DruidNode("test-inject", null, false, null, null, true, false)
            );
            binder.bind(Key.get(String.class, Names.named(DataSourceTaskIdHolder.DATA_SOURCE_BINDING)))
                  .toInstance(dataSource);
            binder.bind(Key.get(String.class, Names.named(DataSourceTaskIdHolder.TASK_ID_BINDING)))
                  .toInstance(taskId);
          }
        })
    );
    final DataSourceTaskIdHolder dimensionIdHolder = new DataSourceTaskIdHolder();
    injector.injectMembers(dimensionIdHolder);
    Assert.assertEquals(dataSource, dimensionIdHolder.getDataSource());
    Assert.assertEquals(taskId, dimensionIdHolder.getTaskId());
  }

  @Test
  public void testGetBasicMonitorSchedulerByDefault()
  {
    final MonitorScheduler monitorScheduler = createInjector(new Properties()).getInstance(MonitorScheduler.class);
    Assert.assertSame(BasicMonitorScheduler.class, monitorScheduler.getClass());
  }

  @Test
  public void testGetClockDriftSafeMonitorSchedulerViaConfig()
  {
    final Properties properties = new Properties();
    properties.setProperty(
        StringUtils.format("%s.schedulerClassName", MetricsModule.MONITORING_PROPERTY_PREFIX),
        ClockDriftSafeMonitorScheduler.class.getName()
    );
    final MonitorScheduler monitorScheduler = createInjector(properties).getInstance(MonitorScheduler.class);
    Assert.assertSame(ClockDriftSafeMonitorScheduler.class, monitorScheduler.getClass());
  }

  @Test
  public void testGetBasicMonitorSchedulerViaConfig()
  {
    final Properties properties = new Properties();
    properties.setProperty(
        StringUtils.format("%s.schedulerClassName", MetricsModule.MONITORING_PROPERTY_PREFIX),
        BasicMonitorScheduler.class.getName()
    );
    final MonitorScheduler monitorScheduler = createInjector(properties).getInstance(MonitorScheduler.class);
    Assert.assertSame(BasicMonitorScheduler.class, monitorScheduler.getClass());
  }

  @Test
  public void testGetMonitorSchedulerUnknownSchedulerException()
  {
    final Properties properties = new Properties();
    properties.setProperty(
        StringUtils.format("%s.schedulerClassName", MetricsModule.MONITORING_PROPERTY_PREFIX),
        "UnknownScheduler"
    );
    expectedException.expect(CreationException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(IllegalArgumentException.class));
    expectedException.expectMessage("Unknown monitor scheduler[UnknownScheduler]");
    createInjector(properties).getInstance(MonitorScheduler.class);
  }

  private static Injector createInjector(Properties properties)
  {
    return Guice.createInjector(
        new JacksonModule(),
        new LifecycleModule(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(ServiceEmitter.class).toInstance(new NoopServiceEmitter());
          binder.bind(Properties.class).toInstance(properties);
        },
        new MetricsModule()
    );
  }
}
