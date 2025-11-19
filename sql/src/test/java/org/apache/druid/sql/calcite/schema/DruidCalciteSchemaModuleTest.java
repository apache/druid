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
import org.apache.druid.catalog.MapMetadataCatalog;
import org.apache.druid.catalog.MetadataCatalog;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.LookupReferencesManager;
import org.apache.druid.rpc.indexing.NoopOverlordClient;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.MapJoinableFactory;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
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
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Set;
import java.util.stream.Collectors;

@ExtendWith(EasyMockExtension.class)
public class DruidCalciteSchemaModuleTest extends CalciteTestBase
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
  private DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  @Mock
  private ObjectMapper objectMapper;
  @Mock
  private LookupReferencesManager lookupReferencesManager;
  @Mock
  private SegmentManager segmentManager;
  @Mock
  private DruidOperatorTable druidOperatorTable;
  @Mock
  private HttpClient httpClient;

  private DruidCalciteSchemaModule target;
  private Injector injector;

  @BeforeEach
  public void setUp()
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
          binder.bind(DruidNodeDiscoveryProvider.class).toInstance(druidNodeDiscoveryProvider);
          binder.bind(DruidSchemaManager.class).toInstance(new NoopDruidSchemaManager());
          binder.bind(ObjectMapper.class).annotatedWith(Json.class).toInstance(objectMapper);
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(LookupExtractorFactoryContainerProvider.class).toInstance(lookupReferencesManager);
          binder.bind(CatalogResolver.class).toInstance(CatalogResolver.NULL_RESOLVER);
          binder.bind(MetadataCatalog.class).toInstance(new MapMetadataCatalog(objectMapper));
          binder.bind(ServiceEmitter.class).toInstance(new ServiceEmitter("", "", null));
          binder.bind(OverlordClient.class).to(NoopOverlordClient.class);
          binder.bind(CoordinatorClient.class).to(NoopCoordinatorClient.class);
          binder.bind(CentralizedDatasourceSchemaConfig.class)
                .toInstance(CentralizedDatasourceSchemaConfig.create());
          binder.bind(HttpClient.class).toInstance(httpClient);
          binder.bind(HttpClient.class).annotatedWith(EscalatedClient.class).toInstance(httpClient);
        },
        new LifecycleModule(),
        target);
  }

  @Test
  public void testDruidSchemaNameIsInjected()
  {
    String schemaName = injector.getInstance(Key.get(String.class, DruidSchemaName.class));
    Assertions.assertEquals(DRUID_SCHEMA_NAME, schemaName);
  }

  @Test
  public void testDruidSqlSchemaIsInjectedAsSingleton()
  {
    NamedDruidSchema namedDruidSchema = injector.getInstance(NamedDruidSchema.class);
    Assertions.assertNotNull(namedDruidSchema);
    NamedDruidSchema other = injector.getInstance(NamedDruidSchema.class);
    Assertions.assertSame(other, namedDruidSchema);
  }

  @Test
  public void testSystemSqlSchemaIsInjectedAsSingleton()
  {
    NamedSystemSchema namedSystemSchema = injector.getInstance(NamedSystemSchema.class);
    Assertions.assertNotNull(namedSystemSchema);
    NamedSystemSchema other = injector.getInstance(NamedSystemSchema.class);
    Assertions.assertSame(other, namedSystemSchema);
  }

  @Test
  public void testDruidCalciteSchemasAreInjected()
  {
    Set<NamedSchema> sqlSchemas = injector.getInstance(Key.get(new TypeLiteral<>() {}));
    Set<Class<? extends NamedSchema>> expectedSchemas = Set.of(
        NamedSystemSchema.class,
        NamedDruidSchema.class,
        NamedLookupSchema.class,
        NamedViewSchema.class
    );
    Assertions.assertEquals(expectedSchemas.size(), sqlSchemas.size());
    Assertions.assertEquals(
        expectedSchemas,
        sqlSchemas.stream().map(NamedSchema::getClass).collect(Collectors.toSet()));
  }

  @Test
  public void testDruidSchemaIsInjectedAsSingleton()
  {
    DruidSchema schema = injector.getInstance(DruidSchema.class);
    Assertions.assertNotNull(schema);
    DruidSchema other = injector.getInstance(DruidSchema.class);
    Assertions.assertSame(other, schema);
  }

  @Test
  public void testSystemSchemaIsInjectedAsSingleton()
  {
    SystemSchema schema = injector.getInstance(SystemSchema.class);
    Assertions.assertNotNull(schema);
    SystemSchema other = injector.getInstance(SystemSchema.class);
    Assertions.assertSame(other, schema);
  }

  @Test
  public void testInformationSchemaIsInjectedAsSingleton()
  {
    InformationSchema schema = injector.getInstance(InformationSchema.class);
    Assertions.assertNotNull(schema);
    InformationSchema other = injector.getInstance(InformationSchema.class);
    Assertions.assertSame(other, schema);
  }

  @Test
  public void testLookupSchemaIsInjectedAsSingleton()
  {
    LookupSchema schema = injector.getInstance(LookupSchema.class);
    Assertions.assertNotNull(schema);
    LookupSchema other = injector.getInstance(LookupSchema.class);
    Assertions.assertSame(other, schema);
  }

  @Test
  public void testRootSchemaAnnotatedIsInjectedAsSingleton()
  {
    DruidSchemaCatalog rootSchema = injector.getInstance(
        Key.get(DruidSchemaCatalog.class, Names.named(DruidCalciteSchemaModule.INCOMPLETE_SCHEMA))
    );
    Assertions.assertNotNull(rootSchema);
    DruidSchemaCatalog other = injector.getInstance(
        Key.get(DruidSchemaCatalog.class, Names.named(DruidCalciteSchemaModule.INCOMPLETE_SCHEMA))
    );
    Assertions.assertSame(other, rootSchema);
  }

  @Test
  public void testRootSchemaIsInjectedAsSingleton()
  {
    DruidSchemaCatalog rootSchema = injector.getInstance(Key.get(DruidSchemaCatalog.class));
    Assertions.assertNotNull(rootSchema);
    DruidSchemaCatalog other = injector.getInstance(
        Key.get(DruidSchemaCatalog.class, Names.named(DruidCalciteSchemaModule.INCOMPLETE_SCHEMA))
    );
    Assertions.assertSame(other, rootSchema);
  }

  @Test
  public void testRootSchemaIsInjectedAndHasInformationSchema()
  {
    DruidSchemaCatalog rootSchema = injector.getInstance(Key.get(DruidSchemaCatalog.class));
    InformationSchema expectedSchema = injector.getInstance(InformationSchema.class);
    Assertions.assertNotNull(rootSchema);
    Assertions.assertSame(expectedSchema, rootSchema.getSubSchema("INFORMATION_SCHEMA").unwrap(InformationSchema.class));
  }
}
