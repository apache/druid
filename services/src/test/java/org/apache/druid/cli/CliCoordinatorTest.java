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

package org.apache.druid.cli;

import com.google.inject.Binding;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.JSR311Resource;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryConfigProvider;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.metadata.SegmentMetadataQuerySegmentWalker;
import org.apache.druid.server.NoopQuerySegmentWalker;
import org.apache.druid.server.QueryResource;
import org.apache.druid.server.initialization.jetty.JettyBindings;
import org.apache.druid.server.system.handler.SystemTableQueryResource;
import org.apache.druid.server.system.module.SystemTableModule;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class CliCoordinatorTest
{
  private static final String COORDINATOR_QOS_PATH = "/druid/coordinator/v1/*";

  @Test
  public void testQosFilterIsBoundByDefault()
  {
    final Injector injector = makeCoordinatorInjector(new Properties());

    final Set<JettyBindings.QosFilterHolder> qosFilters = getQosFilterHolders(injector);
    Assertions.assertTrue(
        hasCoordinatorQosFilter(qosFilters),
        "Coordinator QoS filter should be bound when maxConcurrentRequests defaults to a positive value"
    );
  }

  @Test
  public void testQosFilterIsNotBoundWhenDisabled()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.coordinator.server.maxConcurrentRequests", "-1");
    final Injector injector = makeCoordinatorInjector(properties);

    final Set<JettyBindings.QosFilterHolder> qosFilters = getQosFilterHolders(injector);
    Assertions.assertFalse(
        hasCoordinatorQosFilter(qosFilters),
        "Coordinator QoS filter should not be bound when maxConcurrentRequests is set to a non-positive value"
    );
  }

  @Test
  public void testLeaderEndpointsExcludedFromQos()
  {
    final Injector injector = makeCoordinatorInjector(new Properties());

    final JettyBindings.QosFilterHolder coordinatorQosFilter =
        getQosFilterHolders(injector).stream()
                                     .filter(holder -> Arrays.asList(holder.getPaths()).contains(COORDINATOR_QOS_PATH))
                                     .findFirst()
                                     .orElseThrow(() -> new AssertionError("Coordinator QoS filter should be bound"));


    final Set<String> excludedPaths = Set.of(coordinatorQosFilter.getExcludedPaths());
    Assertions.assertTrue(
        excludedPaths.contains("/druid/coordinator/v1/isLeader"),
        "isLeader should be exempt from QoS filtering"
    );
    Assertions.assertTrue(
        excludedPaths.contains("/druid/coordinator/v1/leader"),
        "leader should be exempt from QoS filtering"
    );
  }

  @Test
  public void testCoordinatorAsOverlordWithCentralizedDatasourceSchema()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.coordinator.asOverlord.enabled", "true");
    properties.setProperty("druid.centralizedDatasourceSchema.enabled", "true");
    properties.setProperty("druid.coordinator.query.default.context.testKey", "testValue");
    properties.setProperty(SystemTableModule.ENABLE_DRUID_ENGINE_FOR_SYSTEM_TABLES_PROPERTY, "true");

    final Injector injector = makeCoordinatorInjector(properties);

    Assertions.assertNotNull(injector.getInstance(SystemTableQueryResource.class));
    Assertions.assertFalse(jerseyResources(injector).contains(QueryResource.class));
    Assertions.assertTrue(jerseyResources(injector).contains(SystemTableQueryResource.class));
    Assertions.assertInstanceOf(
        SegmentMetadataQuerySegmentWalker.class,
        injector.getInstance(QuerySegmentWalker.class)
    );
    Assertions.assertTrue(queryRunnerFactories(injector).containsKey(ScanQuery.class));
    Assertions.assertTrue(queryRunnerFactories(injector).containsKey(SegmentMetadataQuery.class));
    Assertions.assertEquals("testValue", injector.getInstance(QueryConfigProvider.class).getContext().get("testKey"));
  }

  @Test
  public void testCoordinatorUsesRestrictedQueryResourceWithoutCentralizedDatasourceSchema()
  {
    final Properties properties = new Properties();
    properties.setProperty(SystemTableModule.ENABLE_DRUID_ENGINE_FOR_SYSTEM_TABLES_PROPERTY, "true");
    final Injector injector = makeCoordinatorInjector(properties);

    Assertions.assertNotNull(injector.getInstance(SystemTableQueryResource.class));
    Assertions.assertInstanceOf(NoopQuerySegmentWalker.class, injector.getInstance(QuerySegmentWalker.class));
    Assertions.assertTrue(queryRunnerFactories(injector).containsKey(ScanQuery.class));
    Assertions.assertFalse(queryRunnerFactories(injector).containsKey(SegmentMetadataQuery.class));
  }

  /** Without the Druid engine for system tables, the Coordinator serves no {@code /druid/v2/}. */
  @Test
  public void testCoordinatorWithoutDruidEngineForSystemTables()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.centralizedDatasourceSchema.enabled", "true");
    final Injector injector = makeCoordinatorInjector(properties);

    Assertions.assertFalse(jerseyResources(injector).contains(SystemTableQueryResource.class));
    Assertions.assertFalse(jerseyResources(injector).contains(QueryResource.class));
    Assertions.assertTrue(systemTableDataProviders(injector).isEmpty());
    Assertions.assertInstanceOf(
        SegmentMetadataQuerySegmentWalker.class,
        injector.getInstance(QuerySegmentWalker.class)
    );

    final Injector withoutSchemaCache = makeCoordinatorInjector(new Properties());
    Assertions.assertFalse(jerseyResources(withoutSchemaCache).contains(SystemTableQueryResource.class));
    Assertions.assertTrue(systemTableDataProviders(withoutSchemaCache).isEmpty());
  }

  private static Map<String, SystemTableDataProvider> systemTableDataProviders(final Injector injector)
  {
    final Binding<Map<String, SystemTableDataProvider>> binding =
        injector.getExistingBinding(Key.get(new TypeLiteral<>() {}));
    return binding == null ? Map.of() : binding.getProvider().get();
  }

  private static Map<Class<? extends Query>, QueryRunnerFactory> queryRunnerFactories(final Injector injector)
  {
    return injector.getInstance(Key.get(new TypeLiteral<>() {}));
  }

  private static Set<Class<?>> jerseyResources(final Injector injector)
  {
    return injector.getInstance(Key.get(new TypeLiteral<>() {}, JSR311Resource.class));
  }

  private static boolean hasCoordinatorQosFilter(Set<JettyBindings.QosFilterHolder> qosFilters)
  {
    return qosFilters.stream()
                     .anyMatch(holder -> Arrays.asList(holder.getPaths()).contains(COORDINATOR_QOS_PATH));
  }

  private static Set<JettyBindings.QosFilterHolder> getQosFilterHolders(Injector injector)
  {
    return injector.getInstance(Key.get(new TypeLiteral<Set<JettyBindings.QosFilterHolder>>() {}));
  }

  private static Injector makeCoordinatorInjector(final Properties props)
  {
    final Injector baseInjector = Guice.createInjector(
        new JacksonModule(),
        new LifecycleModule(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Properties.class).toInstance(props);
        }
    );

    final CliCoordinator coordinator = new CliCoordinator();
    baseInjector.injectMembers(coordinator);
    return coordinator.makeInjector(coordinator.getNodeRoles(props));
  }
}
