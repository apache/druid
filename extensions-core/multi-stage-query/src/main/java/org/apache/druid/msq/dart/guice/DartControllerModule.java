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

package org.apache.druid.msq.dart.guice;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.msq.dart.DartResourcePermissionMapper;
import org.apache.druid.msq.dart.controller.ControllerMessageListener;
import org.apache.druid.msq.dart.controller.ControllerServerId;
import org.apache.druid.msq.dart.controller.DartControllerContext;
import org.apache.druid.msq.dart.controller.DartControllerRegistry;
import org.apache.druid.msq.dart.controller.DartControllerRegistryImpl;
import org.apache.druid.msq.dart.controller.DartMessageRelayFactoryImpl;
import org.apache.druid.msq.dart.controller.DartMessageRelays;
import org.apache.druid.msq.dart.controller.http.DartSqlResource;
import org.apache.druid.msq.dart.controller.sql.DartSqlClientFactory;
import org.apache.druid.msq.dart.controller.sql.DartSqlClientFactoryImpl;
import org.apache.druid.msq.dart.controller.sql.DartSqlClients;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.rpc.ResourcePermissionMapper;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.server.DruidNode;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.SqlToolbox;

import java.util.Properties;

/**
 * Primary module for Brokers. Checks {@link DartModules#isDartEnabled(Properties)} before installing itself.
 */
@LoadScope(roles = NodeRole.BROKER_JSON_NAME)
public class DartControllerModule implements DruidModule
{
  @Inject
  private Properties properties;

  @Override
  public void configure(Binder binder)
  {
    if (DartModules.isDartEnabled(properties)) {
      binder.install(new ActualModule());
    }
  }

  public static class ActualModule implements Module
  {
    @Override
    public void configure(Binder binder)
    {
      Jerseys.addResource(binder, DartSqlResource.class);
      LifecycleModule.register(binder, DartSqlClients.class);
      LifecycleModule.register(binder, DartMessageRelays.class);
      JsonConfigProvider.bind(binder, DartModules.DART_PROPERTY_BASE + ".controller", DartControllerConfig.class);
      JsonConfigProvider.bind(binder, DartModules.DART_PROPERTY_BASE + ".query", DefaultQueryConfig.class, Dart.class);

      binder.bind(ControllerContext.class)
            .annotatedWith(Dart.class)
            .to(DartControllerContext.class)
            .in(LazySingleton.class);

      binder.bind(DartControllerRegistry.class)
            .to(DartControllerRegistryImpl.class)
            .in(LazySingleton.class);

      binder.bind(ControllerMessageListener.class)
             .in(LazySingleton.class);

      binder.bind(DartMessageRelayFactoryImpl.class)
            .in(LazySingleton.class);

      binder.bind(DartSqlClientFactory.class)
            .to(DartSqlClientFactoryImpl.class)
            .in(LazySingleton.class);

      binder.bind(ResourcePermissionMapper.class)
            .annotatedWith(Dart.class)
            .to(DartResourcePermissionMapper.class);
    }

    @Provides
    @LazySingleton
    public ControllerServerId makeControllerId(final @Self DruidNode selfNode)
    {
      return new ControllerServerId(selfNode.getHostAndPortToUse(), DateTimes.nowUtc().getMillis());
    }

    @Provides
    @Dart
    @LazySingleton
    public SqlStatementFactory makeSqlStatementFactory(
        final DartSqlEngine engine,
        final SqlToolbox toolbox
    )
    {
      return new SqlStatementFactory(toolbox.withEngine(engine));
    }

    @Provides
    @ManageLifecycle
    public DartMessageRelays makeMessageRelays(
        final DruidNodeDiscoveryProvider discoveryProvider,
        final DartMessageRelayFactoryImpl messageRelayFactory
    )
    {
      return new DartMessageRelays(discoveryProvider, messageRelayFactory);
    }

    @Provides
    @LazySingleton
    public DartSqlEngine makeSqlEngine(
        @Dart ControllerContext controllerContext,
        DartControllerRegistry controllerRegistry,
        DartControllerConfig controllerConfig
    )
    {
      return new DartSqlEngine(
          controllerContext,
          controllerRegistry,
          controllerConfig,
          Execs.multiThreaded(controllerConfig.getConcurrentQueries(), "dart-controller-%s")
      );
    }
  }
}
