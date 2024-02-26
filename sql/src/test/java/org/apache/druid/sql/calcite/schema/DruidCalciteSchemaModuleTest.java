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

package org.apache.druid.sql.calcite.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.LookupReferencesManager;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.MapJoinableFactory;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

@RunWith(EasyMockRunner.class)
class DruidCalciteSchemaModuleTest extends CalciteTestBase
{
  private static final String DRUID_SCHEMA_NAME = "druid";

  @Mock
  private QueryLifecycleFactory queryLifecycleFactory;
  @Mock
  private TimelineServerView serverView;
  @Mock
  private PlannerConfig plannerConfig;
  @Mock
  private ViewManager viewManager;
  @Mock
  private Escalator escalator;
  @Mock
  AuthorizerMapper authorizerMapper;
  @Mock
  private FilteredServerInventoryView serverInventoryView;
  @Mock
  private DruidLeaderClient coordinatorDruidLeaderClient;
  @Mock
  private DruidLeaderClient overlordDruidLeaderClient;
  @Mock
  private DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  @Mock
  private ObjectMapper objectMapper;
  @Mock
  private LookupReferencesManager lookupReferencesManager;
  @Mock
  private SegmentManager segmentManager;
  @Mock
  private DruidOperatorTable druidOperatorTable;

  private DruidCalciteSchemaModule target;
  private Injector injector;

  @BeforeEach
  void setUp()
  {
    EasyMock.replay(plannerConfig);
    target = new DruidCalciteSchemaModule();
    injector = Guice.createInjector(
        binder -> {
          binder.bind(QueryLifecycleFactory.class).toInstance(queryLifecycleFactory);
          binder.bind(TimelineServerView.class).toInstance(serverView);
          binder.bind(JoinableFactory.class).toInstance(new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()));
          binder.bind(PlannerConfig.class).toInstance(plannerConfig);
          binder.bind(ViewManager.class).toInstance(viewManager);
          binder.bind(Escalator.class).toInstance(escalator);
          binder.bind(AuthorizerMapper.class).toInstance(authorizerMapper);
          binder.bind(FilteredServerInventoryView.class).toInstance(serverInventoryView);
          binder.bind(SegmentManager.class).toInstance(segmentManager);
          binder.bind(DruidOperatorTable.class).toInstance(druidOperatorTable);
          binder.bind(DruidLeaderClient.class)
                .annotatedWith(Coordinator.class)
                .toInstance(coordinatorDruidLeaderClient);
          binder.bind(DruidLeaderClient.class)
                .annotatedWith(IndexingService.class)
                .toInstance(overlordDruidLeaderClient);
          binder.bind(DruidNodeDiscoveryProvider.class).toInstance(druidNodeDiscoveryProvider);
          binder.bind(DruidSchemaManager.class).toInstance(new NoopDruidSchemaManager());
          binder.bind(ObjectMapper.class).annotatedWith(Json.class).toInstance(objectMapper);
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(LookupExtractorFactoryContainerProvider.class).toInstance(lookupReferencesManager);
          binder.bind(CatalogResolver.class).toInstance(CatalogResolver.NULL_RESOLVER);
          binder.bind(ServiceEmitter.class).toInstance(new ServiceEmitter("", "", null));
          binder.bind(OverlordClient.class).to(NoopOverlordClient.class);
          binder.bind(CoordinatorClient.class).to(NoopCoordinatorClient.class);
        },
        new LifecycleModule(),
        target);
  }

  @Test
  void druidSchemaNameIsInjected()
  {
    String schemaName = injector.getInstance(Key.get(String.class, DruidSchemaName.class));
    assertEquals(DRUID_SCHEMA_NAME, schemaName);
  }

  @Test
  void druidSqlSchemaIsInjectedAsSingleton()
  {
    NamedDruidSchema namedDruidSchema = injector.getInstance(NamedDruidSchema.class);
    assertNotNull(namedDruidSchema);
    NamedDruidSchema other = injector.getInstance(NamedDruidSchema.class);
    assertSame(other, namedDruidSchema);
  }

  @Test
  void systemSqlSchemaIsInjectedAsSingleton()
  {
    NamedSystemSchema namedSystemSchema = injector.getInstance(NamedSystemSchema.class);
    assertNotNull(namedSystemSchema);
    NamedSystemSchema other = injector.getInstance(NamedSystemSchema.class);
    assertSame(other, namedSystemSchema);
  }

  @Test
  void druidCalciteSchemasAreInjected()
  {
    Set<NamedSchema> sqlSchemas = injector.getInstance(Key.get(new TypeLiteral<Set<NamedSchema>>(){}));
    Set<Class<? extends NamedSchema>> expectedSchemas =
        ImmutableSet.of(NamedSystemSchema.class, NamedDruidSchema.class, NamedLookupSchema.class, NamedViewSchema.class);
    assertEquals(expectedSchemas.size(), sqlSchemas.size());
    assertEquals(
        expectedSchemas,
        sqlSchemas.stream().map(NamedSchema::getClass).collect(Collectors.toSet()));
  }

  @Test
  void druidSchemaIsInjectedAsSingleton()
  {
    DruidSchema schema = injector.getInstance(DruidSchema.class);
    assertNotNull(schema);
    DruidSchema other = injector.getInstance(DruidSchema.class);
    assertSame(other, schema);
  }

  @Test
  void systemSchemaIsInjectedAsSingleton()
  {
    SystemSchema schema = injector.getInstance(SystemSchema.class);
    assertNotNull(schema);
    SystemSchema other = injector.getInstance(SystemSchema.class);
    assertSame(other, schema);
  }

  @Test
  void informationSchemaIsInjectedAsSingleton()
  {
    InformationSchema schema = injector.getInstance(InformationSchema.class);
    assertNotNull(schema);
    InformationSchema other = injector.getInstance(InformationSchema.class);
    assertSame(other, schema);
  }

  @Test
  void lookupSchemaIsInjectedAsSingleton()
  {
    LookupSchema schema = injector.getInstance(LookupSchema.class);
    assertNotNull(schema);
    LookupSchema other = injector.getInstance(LookupSchema.class);
    assertSame(other, schema);
  }

  @Test
  void rootSchemaAnnotatedIsInjectedAsSingleton()
  {
    DruidSchemaCatalog rootSchema = injector.getInstance(
        Key.get(DruidSchemaCatalog.class, Names.named(DruidCalciteSchemaModule.INCOMPLETE_SCHEMA))
    );
    assertNotNull(rootSchema);
    DruidSchemaCatalog other = injector.getInstance(
        Key.get(DruidSchemaCatalog.class, Names.named(DruidCalciteSchemaModule.INCOMPLETE_SCHEMA))
    );
    assertSame(other, rootSchema);
  }

  @Test
  void rootSchemaIsInjectedAsSingleton()
  {
    DruidSchemaCatalog rootSchema = injector.getInstance(Key.get(DruidSchemaCatalog.class));
    assertNotNull(rootSchema);
    DruidSchemaCatalog other = injector.getInstance(
        Key.get(DruidSchemaCatalog.class, Names.named(DruidCalciteSchemaModule.INCOMPLETE_SCHEMA))
    );
    assertSame(other, rootSchema);
  }

  @Test
  void rootSchemaIsInjectedAndHasInformationSchema()
  {
    DruidSchemaCatalog rootSchema = injector.getInstance(Key.get(DruidSchemaCatalog.class));
    InformationSchema expectedSchema = injector.getInstance(InformationSchema.class);
    assertNotNull(rootSchema);
    assertSame(expectedSchema, rootSchema.getSubSchema("INFORMATION_SCHEMA").unwrap(InformationSchema.class));
  }
}
