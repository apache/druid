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

package org.apache.druid.server.emitter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.DefaultServerHolderModule;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ServerModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.ServerInjectorBuilder;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.core.ParametrizedUriEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.java.util.metrics.StubServiceEmitterModule;
import org.apache.druid.java.util.metrics.TaskHolder;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.metrics.DefaultLoadSpecHolder;
import org.apache.druid.server.metrics.LoadSpecHolder;
import org.apache.druid.server.metrics.TestTaskHolder;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.List;
import java.util.Properties;

public class EmitterModuleTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testParametrizedUriEmitterConfig()
  {
    final Properties props = new Properties();
    props.setProperty("druid.emitter", "parametrized");
    props.setProperty("druid.emitter.parametrized.recipientBaseUrlPattern", "http://example.com:8888/{feed}");
    props.setProperty("druid.emitter.parametrized.httpEmitting.flushMillis", "1");
    props.setProperty("druid.emitter.parametrized.httpEmitting.flushCount", "2");
    props.setProperty("druid.emitter.parametrized.httpEmitting.basicAuthentication", "a:b");
    props.setProperty("druid.emitter.parametrized.httpEmitting.batchingStrategy", "NEWLINES");
    props.setProperty("druid.emitter.parametrized.httpEmitting.maxBatchSize", "4");
    props.setProperty("druid.emitter.parametrized.httpEmitting.flushTimeOut", "1000");

    final Emitter emitter = makeInjectorWithProperties(props).getInstance(Emitter.class);

    // Testing that ParametrizedUriEmitter is successfully deserialized from the above config
    Assert.assertThat(emitter, CoreMatchers.instanceOf(ParametrizedUriEmitter.class));
  }

  @Test
  public void testMissingEmitterType()
  {
    final Properties props = new Properties();
    props.setProperty("druid.emitter", "");

    final Emitter emitter = makeInjectorWithProperties(props).getInstance(Emitter.class);
    Assert.assertThat(emitter, CoreMatchers.instanceOf(NoopEmitter.class));
  }

  @Test
  public void testInvalidEmitterType()
  {
    final Properties props = new Properties();
    props.setProperty("druid.emitter", "invalid");

    expectedException.expectMessage("Unknown emitter type[druid.emitter]=[invalid]");
    makeInjectorWithProperties(props).getInstance(Emitter.class);
  }

  @Test
  public void testEmitterForTaskContainsAllTaskDimensions()
  {
    Properties props = new Properties();
    props.setProperty("druid.emitter", "stub");
    EmitterModule emitterModule = new EmitterModule();
    emitterModule.setProps(props);

    ImmutableSet<NodeRole> nodeRoles = ImmutableSet.of();

    TestTaskHolder testTaskHolder = new TestTaskHolder("wiki", "id1", "type1", "group1");
    Injector injector = Guice.createInjector(
        new JacksonModule(),
        new LifecycleModule(),
        binder -> {
          JsonConfigProvider.bindInstance(
                    binder,
                    Key.get(DruidNode.class, Self.class),
                    new DruidNode("test-inject", null, false, null, null, true, false)
          );
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
          binder.bind(TaskHolder.class).toInstance(testTaskHolder);
          binder.bind(LoadSpecHolder.class).to(DefaultLoadSpecHolder.class).in(LazySingleton.class);
        },
        ServerInjectorBuilder.registerNodeRoleModule(nodeRoles),
        emitterModule,
        new StubServiceEmitterModule()
    );

    ServiceEmitter instance = injector.getInstance(ServiceEmitter.class);
    Assert.assertNotNull(instance);
    instance.start();
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    builder.setDimension("foo", "bar");
    builder.setMetric("metric1", 1);
    instance.emit(builder);

    Emitter instance1 = injector.getInstance(Emitter.class);
    Assert.assertTrue(instance1 instanceof StubServiceEmitter);
    StubServiceEmitter stubEmitter = (StubServiceEmitter) instance1;

    stubEmitter.verifyEmitted("metric1", 1);
    List<Event> events = stubEmitter.getEvents();
    Assert.assertEquals(1, events.size());
    ServiceMetricEvent event = (ServiceMetricEvent) events.get(0);
    EventMap map = event.toMap();
    Assert.assertEquals("id1", map.get(DruidMetrics.TASK_ID));
    Assert.assertEquals("id1", map.get(DruidMetrics.ID));
    Assert.assertEquals("type1", map.get(DruidMetrics.TASK_TYPE));
    Assert.assertEquals("group1", map.get(DruidMetrics.GROUP_ID));
    Assert.assertEquals("wiki", map.get(DruidMetrics.DATASOURCE));
    stubEmitter.flush();

    // Override a dimension and verify that is emitted
    final ServiceMetricEvent.Builder builder2 = new ServiceMetricEvent.Builder();
    builder2.setDimension("taskId", "id2");
    builder2.setMetric("metric2", 1);
    instance.emit(builder2);

    List<Event> events2 = stubEmitter.getEvents();
    Assert.assertEquals(1, events2.size());
    ServiceMetricEvent event2 = (ServiceMetricEvent) events2.get(0);
    EventMap map2 = event2.toMap();
    Assert.assertEquals("id2", map2.get(DruidMetrics.TASK_ID));
    Assert.assertEquals("id1", map2.get(DruidMetrics.ID));
    Assert.assertEquals("type1", map2.get(DruidMetrics.TASK_TYPE));
    Assert.assertEquals("group1", map2.get(DruidMetrics.GROUP_ID));
    Assert.assertEquals("wiki", map2.get(DruidMetrics.DATASOURCE));
  }

  private Injector makeInjectorWithProperties(final Properties props)
  {
    EmitterModule emitterModule = new EmitterModule();
    emitterModule.setProps(props);
    return Guice.createInjector(
        ImmutableList.of(
            new DruidGuiceExtensions(),
            new LifecycleModule(),
            new ServerModule(),
            new JacksonModule(),
            new DefaultServerHolderModule(),
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
                binder.bind(JsonConfigurator.class).in(LazySingleton.class);
                binder.bind(Properties.class).toInstance(props);
              }
            },
            emitterModule
        )
    );
  }
}
