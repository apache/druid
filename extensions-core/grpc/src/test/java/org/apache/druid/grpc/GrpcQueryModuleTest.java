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

package org.apache.druid.grpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.NoopRequestLogger;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import javax.validation.executable.ExecutableValidator;
import javax.validation.metadata.BeanDescriptor;
import java.util.Collections;
import java.util.Set;

public class GrpcQueryModuleTest
{
  private static final QueryToolChestWarehouse WAREHOUSE = new MapQueryToolChestWarehouse(ImmutableMap.of());
  private static final QuerySegmentWalker TEST_SEGMENT_WALKER = new QuerySegmentWalker()
  {
    @Override
    public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
    {
      return (query1, responseContext) -> Sequences.empty();
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
    {
      return getQueryRunnerForIntervals(null, null);
    }
  };

  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void testSimpleBinding() throws Exception
  {
    final Validator validator = new Validator()
    {
      @Override
      public <T> Set<ConstraintViolation<T>> validate(T object, Class<?>... groups)
      {
        return Collections.emptySet();
      }

      @Override
      public <T> Set<ConstraintViolation<T>> validateProperty(T object, String propertyName, Class<?>... groups)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public <T> Set<ConstraintViolation<T>> validateValue(
          Class<T> beanType,
          String propertyName,
          Object value,
          Class<?>... groups
      )
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public BeanDescriptor getConstraintsForClass(Class<?> clazz)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public <T> T unwrap(Class<T> type)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public ExecutableValidator forExecutables()
      {
        throw new UnsupportedOperationException();
      }
    };

    final Capture<DruidNode> announced = Capture.newInstance();
    final Capture<DruidNode> unannounced = Capture.newInstance();
    final ServiceAnnouncer serviceAnnouncer = EasyMock.createStrictMock(ServiceAnnouncer.class);
    serviceAnnouncer.announce(EasyMock.capture(announced));
    EasyMock.expectLastCall().once();
    serviceAnnouncer.unannounce(EasyMock.capture(unannounced));
    EasyMock.expectLastCall().once();

    EasyMock.replay(serviceAnnouncer);
    final Injector injector = Guice.createInjector(
        // Bind this first to make sure late bindings work correctly regardless of bind order
        new GrpcQueryModule(),
        binder -> {
          binder.bind(Key.get(DruidNode.class, Self.class)).toInstance(new DruidNode(
              "some service",
              "somehost",
              true,
              1111,
              -1,
              true,
              false
          ));
          binder.bind(Key.get(ObjectMapper.class, Json.class)).toInstance(MAPPER);
          binder.bind(Key.get(ServiceAnnouncer.class)).toInstance(serviceAnnouncer);
          binder.bind(GenericQueryMetricsFactory.class)
                .toInstance(EasyMock.createStrictMock(GenericQueryMetricsFactory.class));
          binder.bind(QuerySegmentWalker.class).toInstance(TEST_SEGMENT_WALKER);
          binder.bind(QueryToolChestWarehouse.class).toInstance(WAREHOUSE);
          binder.bind(RequestLogger.class).toInstance(new NoopRequestLogger());
          binder.bind(ServiceEmitter.class).toInstance(new NoopServiceEmitter());
          binder.bind(AuthorizerMapper.class).toInstance(EasyMock.createStrictMock(AuthorizerMapper.class));
          binder.bind(Validator.class).toInstance(validator);

          // Actual test class to fail if the lifecycle order is bad
          binder.bind(FailIfAnnouncerStarted.class).in(ManageLifecycle.class);
          LifecycleModule.register(binder, FailIfAnnouncerStarted.class);
        },
        new LifecycleModule(),
        new DruidGuiceExtensions()
    );
    // Can't inject because it is already injected in the module. So we just set the service name instead
    final GrpcConfig config = injector.getInstance(GrpcConfig.class);
    config.serviceName = "foo";
    // Here's where a bunch of the materialization happens.
    final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
    Assert.assertFalse(announced.hasCaptured());
    Assert.assertFalse(unannounced.hasCaptured());
    lifecycle.start();
    Assert.assertTrue(announced.hasCaptured());
    Assert.assertFalse(unannounced.hasCaptured());
    lifecycle.stop();
    Assert.assertTrue(announced.hasCaptured());
    Assert.assertTrue(unannounced.hasCaptured());
    Assert.assertEquals(1, announced.getValues().size());
    Assert.assertEquals(1, unannounced.getValues().size());
    EasyMock.verify(serviceAnnouncer);
  }

  public static class FailIfAnnouncerStarted
  {
    final DruidLastAnnouncer lastAnnouncer;

    @Inject
    public FailIfAnnouncerStarted(DruidLastAnnouncer lastAnnouncer)
    {
      this.lastAnnouncer = lastAnnouncer;
    }


    @LifecycleStop
    @LifecycleStart
    public void check()
    {
      if (lastAnnouncer.isStarted()) {
        throw new IllegalStateException("DruidLastAnnouncer shouldn't be started");
      }
    }
  }
}
