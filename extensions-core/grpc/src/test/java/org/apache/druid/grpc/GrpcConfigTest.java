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
import com.google.inject.Module;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.guice.ConfigModule;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.PropertiesModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.NoopRequestLogger;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class GrpcConfigTest
{
  @Test
  public void testPropertyBasedCreation()
  {
    final QuerySegmentWalker querySegmentWalker = EasyMock.createStrictMock(QuerySegmentWalker.class);
    final Injector injector = Guice.createInjector((Module) binder -> {
      binder.install(new PropertiesModule(Collections.singletonList("simple.grpc.properties")));
      binder.install(new ConfigModule());
      binder.install(new DruidGuiceExtensions());
      binder.install(new GrpcQueryModule());
      binder.install(new LifecycleModule());
      binder.bind(Key.get(DruidNode.class, Self.class)).toInstance(new DruidNode(
          "foo",
          "bar",
          true,
          -1,
          null,
          true,
          false
      ));
      final ObjectMapper mapper = new DefaultObjectMapper();
      binder.bind(Key.get(ObjectMapper.class, Json.class)).toInstance(mapper);
      binder.bind(QueryToolChestWarehouse.class).toInstance(new MapQueryToolChestWarehouse(ImmutableMap.of()));
      binder.bind(QuerySegmentWalker.class).toInstance(querySegmentWalker);
      binder.bind(RequestLogger.class).toInstance(new NoopRequestLogger());
      binder.bind(AuthorizerMapper.class).toInstance(AuthTestUtils.TEST_AUTHORIZER_MAPPER);
      binder.bind(GenericQueryMetricsFactory.class).toInstance(new DefaultGenericQueryMetricsFactory(mapper));
      binder.bind(ServiceEmitter.class).toInstance(new NoopServiceEmitter());
      binder.bind(ServiceAnnouncer.class).toInstance(EasyMock.createStrictMock(ServiceAnnouncer.class));
    });
    final GrpcConfig grpcConfig = injector.getInstance(GrpcConfig.class);
    Assert.assertNotNull(grpcConfig);
    Assert.assertEquals(9988, grpcConfig.port);
    Assert.assertEquals(12345, grpcConfig.shutdownTimeoutMs);
  }
}
