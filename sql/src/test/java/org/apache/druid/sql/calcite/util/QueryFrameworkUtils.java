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
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.log.NoopRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.PreparedStatement;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.SqlToolbox;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryFrameworkUtils
{
  public static final String INFORMATION_SCHEMA_NAME = "INFORMATION_SCHEMA";

  public static QueryLifecycleFactory createMockQueryLifecycleFactory(
      final QuerySegmentWalker walker,
      final QueryRunnerFactoryConglomerate conglomerate,
      final AuthorizerMapper authorizerMapper
  )
  {
    return new QueryLifecycleFactory(
        conglomerate,
        walker,
        new DefaultGenericQueryMetricsFactory(),
        NoopServiceEmitter.instance(),
        NoopRequestLogger.instance(),
        new AuthConfig(),
        NoopPolicyEnforcer.instance(),
        authorizerMapper,
        Suppliers.ofInstance(new DefaultQueryConfig(Map.of())),
        null  // BrokerConfigManager - null for tests
    );
  }

  /**
   * Create a standard {@link SqlStatementFactory} for testing with a new {@link SqlToolbox} created by
   * {@link #createTestToolbox(SqlEngine, PlannerFactory)}
   */
  public static SqlStatementFactory createSqlStatementFactory(
      final SqlEngine engine,
      final PlannerFactory plannerFactory
  )
  {
    return new SqlStatementFactory(createTestToolbox(engine, plannerFactory));
  }

  /**
   * Create a {@link TestMultiStatementFactory}, a special {@link SqlStatementFactory} which allows multi-statement SET
   * parsing for {@link SqlStatementFactory#directStatement(SqlQueryPlus)} and
   * {@link SqlStatementFactory#preparedStatement(SqlQueryPlus)}.
   */
  public static SqlStatementFactory createSqlMultiStatementFactory(
      final SqlEngine engine,
      final PlannerFactory plannerFactory
  )
  {
    return new TestMultiStatementFactory(createTestToolbox(engine, plannerFactory), engine, plannerFactory);
  }

  private static SqlToolbox createTestToolbox(SqlEngine engine, PlannerFactory plannerFactory)
  {
    return new SqlToolbox(
        engine,
        plannerFactory,
        NoopServiceEmitter.instance(),
        NoopRequestLogger.instance(),
        QueryStackTests.DEFAULT_NOOP_SCHEDULER,
        new SqlLifecycleManager()
    );
  }

  public static DruidSchemaCatalog createMockRootSchema(
      final Injector injector,
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      @Nullable final ViewManager viewManager,
      final DruidSchemaManager druidSchemaManager,
      final AuthorizerMapper authorizerMapper,
      final CatalogResolver catalogResolver)
  {
    TimelineServerView timelineServerView = new TestTimelineServerView(walker.getSegments());
    return createMockRootSchema(
        injector,
        conglomerate,
        walker,
        plannerConfig,
        viewManager,
        druidSchemaManager,
        authorizerMapper,
        catalogResolver,
        timelineServerView
    );
  }

  public static DruidSchemaCatalog createMockRootSchema(
      final Injector injector,
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      @Nullable final ViewManager viewManager,
      final DruidSchemaManager druidSchemaManager,
      final AuthorizerMapper authorizerMapper,
      final CatalogResolver catalogResolver,
      final TimelineServerView timelineServerView
  )
  {
    DruidSchema druidSchema = createMockSchema(
        injector,
        conglomerate,
        walker,
        druidSchemaManager,
        catalogResolver,
        timelineServerView
    );
    SystemSchema systemSchema =
        CalciteTests.createMockSystemSchema(druidSchema, timelineServerView, authorizerMapper);

    LookupSchema lookupSchema = createMockLookupSchema(injector);
    DruidOperatorTable createOperatorTable = createOperatorTable(injector);

    return createMockRootSchema(
        plannerConfig,
        viewManager,
        authorizerMapper,
        druidSchema,
        systemSchema,
        lookupSchema,
        createOperatorTable
    );
  }

  public static DruidSchemaCatalog createMockRootSchema(
      final PlannerConfig plannerConfig,
      final ViewManager viewManager,
      final AuthorizerMapper authorizerMapper,
      DruidSchema druidSchema,
      SystemSchema systemSchema,
      LookupSchema lookupSchema,
      DruidOperatorTable createOperatorTable
  )
  {
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
            createOperatorTable
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

  public static DruidSchema createMockSchema(
      final Injector injector,
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final DruidSchemaManager druidSchemaManager,
      final CatalogResolver catalog,
      final TimelineServerView timelineServerView
  )
  {
    final BrokerSegmentMetadataCache cache = new BrokerSegmentMetadataCache(
        createMockQueryLifecycleFactory(walker, conglomerate, CalciteTests.TEST_AUTHORIZER_MAPPER),
        timelineServerView,
        BrokerSegmentMetadataCacheConfig.create(),
        CalciteTests.TEST_AUTHENTICATOR_ESCALATOR,
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        new PhysicalDatasourceMetadataFactory(
            createDefaultJoinableFactory(injector),
            new SegmentManager(EasyMock.createMock(SegmentCacheManager.class))
            {
              @Override
              public Set<String> getDataSourceNames()
              {
                return ImmutableSet.of(CalciteTests.BROADCAST_DATASOURCE, CalciteTests.RESTRICTED_BROADCAST_DATASOURCE);
              }
            }
        ),
        null,
        CentralizedDatasourceSchemaConfig.create()
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



  /**
   * SqlStatementFactory which overrides direct statement creation to allow calcite tests to test multi-part set
   * statements e.g. like 'SET vectorize = 'force'; SET useApproxCountDistinct = true; SELECT 1 + 1'
   */
  static class TestMultiStatementFactory extends SqlStatementFactory
  {
    private final SqlToolbox toolbox;
    private final SqlEngine engine;
    private final PlannerFactory plannerFactory;

    public TestMultiStatementFactory(SqlToolbox lifecycleToolbox, SqlEngine engine, PlannerFactory plannerFactory)
    {
      super(lifecycleToolbox);
      this.toolbox = lifecycleToolbox;
      this.engine = engine;
      this.plannerFactory = plannerFactory;
    }

    @Override
    public DirectStatement directStatement(SqlQueryPlus sqlRequest)
    {
      // override direct statement creation to allow calcite tests to test multi-part set statements
      return new DirectStatement(toolbox, sqlRequest)
      {
        @Override
        protected DruidPlanner createPlanner()
        {
          return plannerFactory.createPlanner(
              engine,
              queryPlus.sql(),
              queryPlus.sqlNode(),
              queryPlus.authContextKeys(),
              queryContext,
              hook
          );
        }
      };
    }

    @Override
    public PreparedStatement preparedStatement(SqlQueryPlus sqlRequest)
    {
      return new PreparedStatement(toolbox, sqlRequest)
      {
        @Override
        protected DruidPlanner getPlanner()
        {
          return plannerFactory.createPlanner(
              engine,
              queryPlus.sql(),
              queryPlus.sqlNode(),
              queryPlus.authContextKeys(),
              queryContext,
              hook
          );
        }

        @Override
        public DirectStatement execute(List<TypedValue> parameters)
        {
          return directStatement(queryPlus.withParameters(parameters));
        }
      };
    }
  }
}
