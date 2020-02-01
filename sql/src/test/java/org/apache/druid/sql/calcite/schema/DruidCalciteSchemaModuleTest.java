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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.client.InventoryView;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
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
public class DruidCalciteSchemaModuleTest
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
  private SchemaPlus rootSchema;
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
          binder.bind(PlannerConfig.class).toInstance(plannerConfig);
          binder.bind(ViewManager.class).toInstance(viewManager);
          binder.bind(Escalator.class).toInstance(escalator);
          binder.bind(AuthorizerMapper.class).toInstance(authorizerMapper);
          binder.bind(SchemaPlus.class).toInstance(rootSchema);
          binder.bind(InventoryView.class).toInstance(serverInventoryView);
          binder.bind(DruidLeaderClient.class)
                .annotatedWith(Coordinator.class)
                .toInstance(coordinatorDruidLeaderClient);
          binder.bind(DruidLeaderClient.class)
                .annotatedWith(IndexingService.class)
                .toInstance(overlordDruidLeaderClient);
          binder.bind(DruidNodeDiscoveryProvider.class).toInstance(druidNodeDiscoveryProvider);
          binder.bind(ObjectMapper.class).toInstance(objectMapper);
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
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
    DruidSqlSchema druidSqlSchema = injector.getInstance(DruidSqlSchema.class);
    Assert.assertNotNull(druidSqlSchema);
    DruidSqlSchema other = injector.getInstance(DruidSqlSchema.class);
    Assert.assertSame(other, druidSqlSchema);
  }

  @Test
  public void testSystemSqlSchemaIsInjectedAsSingleton()
  {
    SystemSqlSchema systemSqlSchema = injector.getInstance(SystemSqlSchema.class);
    Assert.assertNotNull(systemSqlSchema);
    SystemSqlSchema other = injector.getInstance(SystemSqlSchema.class);
    Assert.assertSame(other, systemSqlSchema);
  }

  @Test
  public void testInformationSqlSchemaIsInjectedAsSingleton()
  {
    InformationSqlSchema informationSqlSchema = injector.getInstance(InformationSqlSchema.class);
    Assert.assertNotNull(informationSqlSchema);
    InformationSqlSchema other = injector.getInstance(InformationSqlSchema.class);
    Assert.assertSame(other, informationSqlSchema);
  }

  @Test
  public void testDruidCalciteSchemasAreInjected()
  {
    Set<DruidCalciteSchema> sqlSchemas = injector.getInstance(Key.get(new TypeLiteral<Set<DruidCalciteSchema>>(){}));
    Assert.assertEquals(3, sqlSchemas.size());
    Assert.assertEquals(
        ImmutableSet.of(InformationSqlSchema.class, SystemSqlSchema.class, DruidSqlSchema.class),
        sqlSchemas.stream().map(DruidCalciteSchema::getClass).collect(Collectors.toSet()));
  }
}
