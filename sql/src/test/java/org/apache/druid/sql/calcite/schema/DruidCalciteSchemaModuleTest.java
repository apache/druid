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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.client.InventoryView;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.LookupReferencesManager;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.MapJoinableFactory;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Set;
import java.util.stream.Collectors;

@RunWith(EasyMockRunner.class)
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
  private InventoryView serverInventoryView;
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

  private DruidCalciteSchemaModule target;
  private Injector injector;

  @Before
  public void setUp()
  {
    EasyMock.expect(plannerConfig.isMetadataSegmentCacheEnable()).andStubReturn(false);
    EasyMock.expect(plannerConfig.getMetadataSegmentPollPeriod()).andStubReturn(6000L);
    EasyMock.replay(plannerConfig);
    target = new DruidCalciteSchemaModule();
    injector = Guice.createInjector(
        binder -> {
          binder.bind(QueryLifecycleFactory.class).toInstance(queryLifecycleFactory);
          binder.bind(TimelineServerView.class).toInstance(serverView);
          binder.bind(JoinableFactory.class).toInstance(new MapJoinableFactory(ImmutableMap.of()));
          binder.bind(PlannerConfig.class).toInstance(plannerConfig);
          binder.bind(ViewManager.class).toInstance(viewManager);
          binder.bind(Escalator.class).toInstance(escalator);
          binder.bind(AuthorizerMapper.class).toInstance(authorizerMapper);
          binder.bind(InventoryView.class).toInstance(serverInventoryView);
          binder.bind(SegmentManager.class).toInstance(segmentManager);
          binder.bind(DruidLeaderClient.class)
                .annotatedWith(Coordinator.class)
                .toInstance(coordinatorDruidLeaderClient);
          binder.bind(DruidLeaderClient.class)
                .annotatedWith(IndexingService.class)
                .toInstance(overlordDruidLeaderClient);
          binder.bind(DruidNodeDiscoveryProvider.class).toInstance(druidNodeDiscoveryProvider);
          binder.bind(ObjectMapper.class).toInstance(objectMapper);
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(LookupExtractorFactoryContainerProvider.class).toInstance(lookupReferencesManager);
        },
        new LifecycleModule(),
        target);
  }

  @Test
  public void testDruidSchemaNameIsInjected()
  {
    String schemaName = injector.getInstance(Key.get(String.class, DruidSchemaName.class));
    Assert.assertEquals(DRUID_SCHEMA_NAME, schemaName);
  }

  @Test
  public void testDruidSqlSchemaIsInjectedAsSingleton()
  {
    NamedDruidSchema namedDruidSchema = injector.getInstance(NamedDruidSchema.class);
    Assert.assertNotNull(namedDruidSchema);
    NamedDruidSchema other = injector.getInstance(NamedDruidSchema.class);
    Assert.assertSame(other, namedDruidSchema);
  }

  @Test
  public void testSystemSqlSchemaIsInjectedAsSingleton()
  {
    NamedSystemSchema namedSystemSchema = injector.getInstance(NamedSystemSchema.class);
    Assert.assertNotNull(namedSystemSchema);
    NamedSystemSchema other = injector.getInstance(NamedSystemSchema.class);
    Assert.assertSame(other, namedSystemSchema);
  }

  @Test
  public void testDruidCalciteSchemasAreInjected()
  {
    Set<NamedSchema> sqlSchemas = injector.getInstance(Key.get(new TypeLiteral<Set<NamedSchema>>(){}));
    Set<Class<? extends NamedSchema>> expectedSchemas =
        ImmutableSet.of(NamedSystemSchema.class, NamedDruidSchema.class, NamedLookupSchema.class);
    Assert.assertEquals(expectedSchemas.size(), sqlSchemas.size());
    Assert.assertEquals(
        expectedSchemas,
        sqlSchemas.stream().map(NamedSchema::getClass).collect(Collectors.toSet()));
  }

  @Test
  public void testDruidSchemaIsInjectedAsSingleton()
  {
    DruidSchema schema = injector.getInstance(DruidSchema.class);
    Assert.assertNotNull(schema);
    DruidSchema other = injector.getInstance(DruidSchema.class);
    Assert.assertSame(other, schema);
  }

  @Test
  public void testSystemSchemaIsInjectedAsSingleton()
  {
    SystemSchema schema = injector.getInstance(SystemSchema.class);
    Assert.assertNotNull(schema);
    SystemSchema other = injector.getInstance(SystemSchema.class);
    Assert.assertSame(other, schema);
  }

  @Test
  public void testInformationSchemaIsInjectedAsSingleton()
  {
    InformationSchema schema = injector.getInstance(InformationSchema.class);
    Assert.assertNotNull(schema);
    InformationSchema other = injector.getInstance(InformationSchema.class);
    Assert.assertSame(other, schema);
  }

  @Test
  public void testLookupSchemaIsInjectedAsSingleton()
  {
    LookupSchema schema = injector.getInstance(LookupSchema.class);
    Assert.assertNotNull(schema);
    LookupSchema other = injector.getInstance(LookupSchema.class);
    Assert.assertSame(other, schema);
  }

  @Test
  public void testRootSchemaAnnotatedIsInjectedAsSingleton()
  {
    SchemaPlus rootSchema = injector.getInstance(
        Key.get(SchemaPlus.class, Names.named(DruidCalciteSchemaModule.INCOMPLETE_SCHEMA))
    );
    Assert.assertNotNull(rootSchema);
    SchemaPlus other = injector.getInstance(
        Key.get(SchemaPlus.class, Names.named(DruidCalciteSchemaModule.INCOMPLETE_SCHEMA))
    );
    Assert.assertSame(other, rootSchema);
  }

  @Test
  public void testRootSchemaIsInjectedAsSingleton()
  {
    SchemaPlus rootSchema = injector.getInstance(Key.get(SchemaPlus.class));
    Assert.assertNotNull(rootSchema);
    SchemaPlus other = injector.getInstance(
        Key.get(SchemaPlus.class, Names.named(DruidCalciteSchemaModule.INCOMPLETE_SCHEMA))
    );
    Assert.assertSame(other, rootSchema);
  }

  @Test
  public void testRootSchemaIsInjectedAndHasInformationSchema()
  {
    SchemaPlus rootSchema = injector.getInstance(Key.get(SchemaPlus.class));
    InformationSchema expectedSchema = injector.getInstance(InformationSchema.class);
    Assert.assertNotNull(rootSchema);
    Assert.assertSame(expectedSchema, rootSchema.getSubSchema("INFORMATION_SCHEMA").unwrap(InformationSchema.class));
  }
}
