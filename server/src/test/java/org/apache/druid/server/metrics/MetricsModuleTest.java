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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.DefaultServerHolderModule;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.initialization.ServerInjectorBuilder;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.java.util.metrics.BasicMonitorScheduler;
import org.apache.druid.java.util.metrics.ClockDriftSafeMonitorScheduler;
import org.apache.druid.java.util.metrics.Monitor;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.apache.druid.java.util.metrics.NoopOshiSysMonitor;
import org.apache.druid.java.util.metrics.NoopSysMonitor;
import org.apache.druid.java.util.metrics.NoopTaskHolder;
import org.apache.druid.java.util.metrics.OshiSysMonitor;
import org.apache.druid.java.util.metrics.OshiSysMonitorConfig;
import org.apache.druid.java.util.metrics.SysMonitor;
import org.apache.druid.java.util.metrics.TaskHolder;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.DruidNode;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class MetricsModuleTest
{
  private static final String CPU_ARCH = System.getProperty("os.arch");

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
    Assert.assertTrue(injector.getInstance(TaskHolder.class) instanceof NoopTaskHolder);
    Assert.assertTrue(injector.getInstance(LoadSpecHolder.class) instanceof DefaultLoadSpecHolder);
  }

  @Test
  public void testSimpleInjectionWithValues()
  {
    final String dataSource = "some_datasource";
    final String taskId = "some_taskid";
    final String taskType = "some_task_type";
    final String groupId = "some_groupid";
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
            binder.bind(TaskHolder.class).toInstance(new TestTaskHolder(dataSource, taskId, taskType, groupId));
            binder.bind(LoadSpecHolder.class).to(DefaultLoadSpecHolder.class).in(LazySingleton.class);
          }
        })
    );
    TaskHolder taskHolder = injector.getInstance(TaskHolder.class);
    Assert.assertEquals(dataSource, taskHolder.getDataSource());
    Assert.assertEquals(taskId, taskHolder.getTaskId());
    Assert.assertEquals(taskType, taskHolder.getTaskType());
    Assert.assertEquals(groupId, taskHolder.getGroupId());
    Map<String, String> expectedTaskDims = Map.of(
        DruidMetrics.DATASOURCE, dataSource,
        DruidMetrics.TASK_ID, taskId,
        DruidMetrics.ID, taskId,
        DruidMetrics.TASK_TYPE, taskType,
        DruidMetrics.GROUP_ID, groupId
    );

    Assert.assertEquals(expectedTaskDims, taskHolder.getMetricDimensions());
  }

  @Test
  public void testGetBasicMonitorSchedulerByDefault()
  {
    final MonitorScheduler monitorScheduler =
        createInjector(new Properties(), ImmutableSet.of()).getInstance(MonitorScheduler.class);
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
    final MonitorScheduler monitorScheduler =
        createInjector(properties, ImmutableSet.of()).getInstance(MonitorScheduler.class);
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
    final MonitorScheduler monitorScheduler =
        createInjector(properties, ImmutableSet.of()).getInstance(MonitorScheduler.class);
    Assert.assertSame(BasicMonitorScheduler.class, monitorScheduler.getClass());
  }

  @Test
  public void test_monitorScheduler_addsMonitor_ifNodeRoleIsInLoadScope()
  {
    final Properties properties = new Properties();
    properties.setProperty(
        "druid.monitoring.monitors",
        StringUtils.format("[\"%s\"]", OverlordOnlyMonitor.class.getName())
    );

    verifyThatMonitorIsLoadedOnlyOn(
        OverlordOnlyMonitor.class,
        properties,
        NodeRole.OVERLORD
    );
  }

  @Test
  public void test_monitorScheduler_addsMonitor_ifNodeRoleIsInLoadScopeOfSuperClass()
  {
    final Properties properties = new Properties();
    properties.setProperty(
        "druid.monitoring.monitors",
        StringUtils.format("[\"%s\"]", OverlordAndCoordinatorMonitor2.class.getName())
    );

    verifyThatMonitorIsLoadedOnlyOn(
        OverlordAndCoordinatorMonitor2.class,
        properties,
        NodeRole.COORDINATOR,
        NodeRole.OVERLORD
    );
  }

  @Test
  public void test_monitorScheduler_addsMonitor_ifNoLoadScopeIsDefined()
  {
    final Properties properties = new Properties();
    properties.setProperty(
        "druid.monitoring.monitors",
        StringUtils.format("[\"%s\"]", AllNodeMonitor.class.getName())
    );

    verifyThatMonitorIsLoadedOnlyOn(
        AllNodeMonitor.class,
        properties,
        NodeRole.values()
    );
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
    createInjector(properties, ImmutableSet.of()).getInstance(MonitorScheduler.class);
  }

  @Test
  public void testGetSysMonitorViaInjector()
  {
    // Do not run the tests on ARM64. Sigar library has no binaries for ARM64
    Assume.assumeFalse("aarch64".equals(CPU_ARCH));

    final Injector injector = createInjector(new Properties(), ImmutableSet.of(NodeRole.PEON));
    final SysMonitor sysMonitor = injector.getInstance(SysMonitor.class);
    final ServiceEmitter emitter = Mockito.mock(ServiceEmitter.class);
    sysMonitor.doMonitor(emitter);

    Assert.assertTrue(sysMonitor instanceof NoopSysMonitor);
    Mockito.verify(emitter, Mockito.never()).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
  }

  @Test
  public void testGetSysMonitorWhenNull()
  {
    // Do not run the tests on ARM64. Sigar library has no binaries for ARM64
    Assume.assumeFalse("aarch64".equals(CPU_ARCH));

    Injector injector = createInjector(new Properties(), ImmutableSet.of());
    final SysMonitor sysMonitor = injector.getInstance(SysMonitor.class);
    final ServiceEmitter emitter = Mockito.mock(ServiceEmitter.class);
    sysMonitor.doMonitor(emitter);

    Assert.assertFalse(sysMonitor instanceof NoopSysMonitor);
    Mockito.verify(emitter, Mockito.atLeastOnce()).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
  }
  @Test
  public void testGetOshiSysMonitorViaInjector()
  {

    final Injector injector = createInjector(new Properties(), ImmutableSet.of(NodeRole.PEON));
    final OshiSysMonitor sysMonitor = injector.getInstance(OshiSysMonitor.class);
    final ServiceEmitter emitter = Mockito.mock(ServiceEmitter.class);
    sysMonitor.doMonitor(emitter);

    Assert.assertTrue(sysMonitor instanceof NoopOshiSysMonitor);
    Mockito.verify(emitter, Mockito.never()).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
  }

  @Test
  public void testGetOshiSysMonitorViaInjectorBroker()
  {
    Properties properties = new Properties();
    properties.setProperty("druid.monitoring.sys.categories", "[\"mem\"]");
    final Injector injector = createInjector(properties, ImmutableSet.of(NodeRole.BROKER));
    final OshiSysMonitor sysMonitor = injector.getInstance(OshiSysMonitor.class);
    final ServiceEmitter emitter = Mockito.mock(ServiceEmitter.class);
    sysMonitor.doMonitor(emitter);

    Assert.assertTrue(sysMonitor instanceof OshiSysMonitor);
    Mockito.verify(emitter, Mockito.atLeastOnce()).emit(ArgumentMatchers.any(ServiceEventBuilder.class));

    Assert.assertTrue(injector.getInstance(OshiSysMonitorConfig.class).shouldEmitMetricCategory("mem"));
    Assert.assertFalse(injector.getInstance(OshiSysMonitorConfig.class).shouldEmitMetricCategory("swap"));
  }

  @Test
  public void testGetOshiSysMonitorWhenNull()
  {

    Injector injector = createInjector(new Properties(), ImmutableSet.of());
    final OshiSysMonitor sysMonitor = injector.getInstance(OshiSysMonitor.class);
    final ServiceEmitter emitter = Mockito.mock(ServiceEmitter.class);
    sysMonitor.doMonitor(emitter);

    Assert.assertFalse(sysMonitor instanceof NoopOshiSysMonitor);
    Mockito.verify(emitter, Mockito.atLeastOnce()).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
  }

  private static Injector createInjector(Properties properties, ImmutableSet<NodeRole> nodeRoles)
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
        ServerInjectorBuilder.registerNodeRoleModule(nodeRoles),
        new MetricsModule(),
        new DefaultServerHolderModule()
    );
  }

  private <T extends Monitor> void verifyThatMonitorIsLoadedOnlyOn(
      Class<T> monitorClass,
      Properties properties,
      NodeRole... supportedRoles
  )
  {
    final Set<NodeRole> supportedRoleSet = Set.of(supportedRoles);
    for (NodeRole role : NodeRole.values()) {
      final MonitorScheduler monitorScheduler = createInjector(properties, ImmutableSet.of(role))
          .getInstance(MonitorScheduler.class);
      Assert.assertEquals(
          supportedRoleSet.contains(role),
          monitorScheduler.findMonitor(monitorClass).isPresent()
      );
    }
  }

  public static class AllNodeMonitor extends AbstractMonitor
  {
    @Override
    public boolean doMonitor(ServiceEmitter emitter)
    {
      return false;
    }
  }

  @LoadScope(roles = {NodeRole.COORDINATOR_JSON_NAME, NodeRole.OVERLORD_JSON_NAME})
  public static class OverlordAndCoordinatorMonitor extends AllNodeMonitor
  {
  }

  @LoadScope(roles = NodeRole.OVERLORD_JSON_NAME)
  public static class OverlordOnlyMonitor extends OverlordAndCoordinatorMonitor
  {
  }

  /**
   * Uses load scope of super class.
   */
  public static class OverlordAndCoordinatorMonitor2 extends OverlordAndCoordinatorMonitor
  {
  }
}
