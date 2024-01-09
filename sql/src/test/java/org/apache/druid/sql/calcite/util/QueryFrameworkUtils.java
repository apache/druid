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

package org.apache.druid.sql.calcite.util;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.log.NoopRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.SqlToolbox;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.BrokerSegmentMetadataCache;
import org.apache.druid.sql.calcite.schema.BrokerSegmentMetadataCacheConfig;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.DruidSchemaManager;
import org.apache.druid.sql.calcite.schema.InformationSchema;
import org.apache.druid.sql.calcite.schema.LookupSchema;
import org.apache.druid.sql.calcite.schema.NamedDruidSchema;
import org.apache.druid.sql.calcite.schema.NamedLookupSchema;
import org.apache.druid.sql.calcite.schema.NamedSchema;
import org.apache.druid.sql.calcite.schema.NamedSystemSchema;
import org.apache.druid.sql.calcite.schema.NamedViewSchema;
import org.apache.druid.sql.calcite.schema.NoopDruidSchemaManager;
import org.apache.druid.sql.calcite.schema.PhysicalDatasourceMetadataFactory;
import org.apache.druid.sql.calcite.schema.SystemSchema;
import org.apache.druid.sql.calcite.schema.ViewSchema;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.easymock.EasyMock;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryFrameworkUtils
{
  public static final String INFORMATION_SCHEMA_NAME = "INFORMATION_SCHEMA";

  public static QueryLifecycleFactory createMockQueryLifecycleFactory(
      final QuerySegmentWalker walker,
      final QueryRunnerFactoryConglomerate conglomerate
  )
  {
    return new QueryLifecycleFactory(
        new QueryToolChestWarehouse()
        {
          @Override
          public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
          {
            return conglomerate.findFactory(query).getToolchest();
          }
        },
        walker,
        new DefaultGenericQueryMetricsFactory(),
        new ServiceEmitter("dummy", "dummy", new NoopEmitter()),
        new NoopRequestLogger(),
        new AuthConfig(),
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of()))
    );
  }

  public static SqlStatementFactory createSqlStatementFactory(
      final SqlEngine engine,
      final PlannerFactory plannerFactory,
      final AuthConfig authConfig
  )
  {
    SqlToolbox toolbox = new SqlToolbox(
        engine,
        plannerFactory,
        new ServiceEmitter("dummy", "dummy", new NoopEmitter()),
        new NoopRequestLogger(),
        QueryStackTests.DEFAULT_NOOP_SCHEDULER,
        new DefaultQueryConfig(ImmutableMap.of()),
        new SqlLifecycleManager()
    );
    return new SqlStatementFactory(toolbox);
  }

  public static DruidSchemaCatalog createMockRootSchema(
      final Injector injector,
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      @Nullable final ViewManager viewManager,
      final DruidSchemaManager druidSchemaManager,
      final AuthorizerMapper authorizerMapper,
      final CatalogResolver catalogResolver
  )
  {
    DruidSchema druidSchema = createMockSchema(
        injector,
        conglomerate,
        walker,
        druidSchemaManager,
        catalogResolver
    );
    SystemSchema systemSchema =
        CalciteTests.createMockSystemSchema(druidSchema, walker, authorizerMapper);

    LookupSchema lookupSchema = createMockLookupSchema(injector);
    ViewSchema viewSchema = viewManager != null ? new ViewSchema(viewManager) : null;

    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
    Set<NamedSchema> namedSchemas = new HashSet<>();
    namedSchemas.add(new NamedDruidSchema(druidSchema, CalciteTests.DRUID_SCHEMA_NAME));
    namedSchemas.add(new NamedSystemSchema(plannerConfig, systemSchema));
    namedSchemas.add(new NamedLookupSchema(lookupSchema));

    if (viewSchema != null) {
      namedSchemas.add(new NamedViewSchema(viewSchema));
    }

    DruidSchemaCatalog catalog = new DruidSchemaCatalog(
        rootSchema,
        namedSchemas.stream().collect(Collectors.toMap(NamedSchema::getSchemaName, x -> x))
    );
    InformationSchema informationSchema =
        new InformationSchema(
            catalog,
            authorizerMapper,
            createOperatorTable(injector)
        );
    rootSchema.add(CalciteTests.DRUID_SCHEMA_NAME, druidSchema);
    rootSchema.add(INFORMATION_SCHEMA_NAME, informationSchema);
    rootSchema.add(NamedSystemSchema.NAME, systemSchema);
    rootSchema.add(NamedLookupSchema.NAME, lookupSchema);

    if (viewSchema != null) {
      rootSchema.add(NamedViewSchema.NAME, viewSchema);
    }

    return catalog;
  }

  public static DruidSchemaCatalog createMockRootSchema(
      final Injector injector,
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      final AuthorizerMapper authorizerMapper
  )
  {
    return createMockRootSchema(
        injector,
        conglomerate,
        walker,
        plannerConfig,
        null,
        new NoopDruidSchemaManager(),
        authorizerMapper,
        CatalogResolver.NULL_RESOLVER
    );
  }

  private static DruidSchema createMockSchema(
      final Injector injector,
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final DruidSchemaManager druidSchemaManager,
      final CatalogResolver catalog
  )
  {
    final BrokerSegmentMetadataCache cache = new BrokerSegmentMetadataCache(
        createMockQueryLifecycleFactory(walker, conglomerate),
        new TestTimelineServerView(walker.getSegments()),
        BrokerSegmentMetadataCacheConfig.create(),
        CalciteTests.TEST_AUTHENTICATOR_ESCALATOR,
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        new PhysicalDatasourceMetadataFactory(
            createDefaultJoinableFactory(injector),
            new SegmentManager(EasyMock.createMock(SegmentLoader.class))
            {
              @Override
              public Set<String> getDataSourceNames()
              {
                return ImmutableSet.of(CalciteTests.BROADCAST_DATASOURCE);
              }
            }
        ),
        null
    );

    try {
      cache.start();
      cache.awaitInitialization();
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    cache.stop();
    return new DruidSchema(cache, druidSchemaManager, catalog);
  }

  public static JoinableFactory createDefaultJoinableFactory(Injector injector)
  {
    return QueryStackTests.makeJoinableFactoryFromDefault(
        injector.getInstance(LookupExtractorFactoryContainerProvider.class),
        ImmutableSet.of(TestDataBuilder.CUSTOM_ROW_TABLE_JOINABLE),
        ImmutableMap.of(TestDataBuilder.CUSTOM_ROW_TABLE_JOINABLE.getClass(), GlobalTableDataSource.class)
    );
  }

  public static DruidOperatorTable createOperatorTable(final Injector injector)
  {
    try {
      return injector.getInstance(DruidOperatorTable.class);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static LookupSchema createMockLookupSchema(final Injector injector)
  {
    return new LookupSchema(injector.getInstance(LookupExtractorFactoryContainerProvider.class));
  }
}
