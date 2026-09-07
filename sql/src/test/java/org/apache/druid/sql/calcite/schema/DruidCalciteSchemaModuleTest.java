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
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ExtendWith(EasyMockExtension.class)
public class DruidCalciteSchemaModuleTest extends CalciteTestBase
{
  private static final String DRUID_SCHEMA_NAME = "druid";
  private static final AuthenticationResult AUTH_RESULT =
      new AuthenticationResult("identity", "authorizer", "authenticator", null);

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

  private Injector injector;

  @BeforeEach
  public void setUp()
  {
    EasyMock.expect(plannerConfig.isEnableSysQueriesTable()).andReturn(false).anyTimes();
    EasyMock.expect(plannerConfig.isAuthorizeTableVisibility()).andReturn(false).anyTimes();
    EasyMock.replay(plannerConfig);
    injector = Guice.createInjector(
        binder -> {
          binder.bind(QueryLifecycleFactory.class).toInstance(queryLifecycleFactory);
          binder.bind(TimelineServerView.class).toInstance(serverView);
          binder.bind(JoinableFactory.class).toInstance(new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()));
          binder.bind(PlannerConfig.class).toInstance(plannerConfig);
          binder.bind(ViewManager.class).toInstance(viewManager);
          binder.bind(Escalator.class).toInstance(escalator);
          binder.bind(AuthorizerMapper.class).toInstance(AuthTestUtils.TEST_AUTHORIZER_MAPPER);
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
          binder.bind(new TypeLiteral<Set<SqlEngine>>() {}).toInstance(ImmutableSet.of());
        },
        new LifecycleModule(),
        new DruidCalciteSchemaModule()
    );
  }

  @Test
  public void testDruidSchemaNameIsInjected()
  {
    String schemaName = injector.getInstance(Key.get(String.class, DruidSchemaName.class));
    Assertions.assertEquals(DRUID_SCHEMA_NAME, schemaName);
  }

  @Test
  public void testNamedSchemasAreInjected()
  {
    Set<NamedSchema> namedSchemas = injector.getInstance(Key.get(new TypeLiteral<>() {}));
    Assertions.assertEquals(
        Set.of(NamedLookupSchema.class),
        namedSchemas.stream().map(NamedSchema::getClass).collect(Collectors.toSet())
    );
  }

  @Test
  public void testSchemaProvidersAreInjected()
  {
    Set<SchemaProvider> schemaProviders = injector.getInstance(Key.get(new TypeLiteral<>() {}));
    Assertions.assertEquals(
        Set.of(DruidSchemaProvider.class, SystemSchemaProvider.class, ViewSchemaProvider.class),
        schemaProviders.stream().map(SchemaProvider::getClass).collect(Collectors.toSet())
    );
  }

  @Test
  public void testDruidSchemaProviderIsInjectedAsSingleton()
  {
    DruidSchemaProvider schemaProvider = injector.getInstance(DruidSchemaProvider.class);
    Assertions.assertNotNull(schemaProvider);
    Assertions.assertSame(schemaProvider, injector.getInstance(DruidSchemaProvider.class));
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
  public void testSchemaCatalogProviderIsInjectedAsSingleton()
  {
    DruidSchemaCatalogProvider provider = injector.getInstance(DruidSchemaCatalogProvider.class);
    Assertions.assertInstanceOf(DruidSchemaCatalogProviderImpl.class, provider);
    Assertions.assertSame(provider, injector.getInstance(DruidSchemaCatalogProvider.class));
  }

  @Test
  public void testRootSchemaHasAllSchemasPlusInformationSchema()
  {
    EasyMock.expect(viewManager.getViews()).andReturn(Map.of()).anyTimes();
    EasyMock.replay(viewManager);

    final DruidSchemaCatalog rootSchema =
        injector.getInstance(DruidSchemaCatalogProvider.class).createRootSchema(AUTH_RESULT);

    // Every schema the module binds must be reachable from the root schema.
    Assertions.assertEquals(
        Set.of(
            DRUID_SCHEMA_NAME,
            NamedViewSchema.NAME,
            NamedSystemSchema.NAME,
            NamedLookupSchema.NAME,
            InformationSchema.INFORMATION_SCHEMA_NAME
        ),
        rootSchema.getSubSchemaNames()
    );
    Assertions.assertNotNull(
        rootSchema.getSubSchema(InformationSchema.INFORMATION_SCHEMA_NAME).unwrap(InformationSchema.class)
    );
  }

  @Test
  public void testEscalatedRootSchemaUsesEscalator()
  {
    EasyMock.expect(viewManager.getViews()).andReturn(Map.of()).anyTimes();
    EasyMock.replay(viewManager);
    EasyMock.expect(escalator.createEscalatedAuthenticationResult()).andReturn(AUTH_RESULT).once();
    EasyMock.replay(escalator);

    Assertions.assertNotNull(injector.getInstance(DruidSchemaCatalogProvider.class).createEscalatedRootSchema());
    EasyMock.verify(escalator);
  }
}
