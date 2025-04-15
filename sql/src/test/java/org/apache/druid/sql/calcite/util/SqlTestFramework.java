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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.druid.client.TestHttpClient;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.QueryRunnerFactoryModule;
import org.apache.druid.guice.QueryableModule;
import org.apache.druid.guice.SegmentWranglerModule;
import org.apache.druid.guice.ServerModule;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.guice.StorageNodeModule;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.initialization.ServiceInjectorBuilder;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.TestBufferPool;
import org.apache.druid.query.groupby.DefaultGroupByQueryMetricsFactory;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryMetricsFactory;
import org.apache.druid.query.groupby.GroupByResourcesReservationPool;
import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.quidem.ProjectPathUtils;
import org.apache.druid.quidem.TestSqlModule;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.realtime.ChatHandlerProvider;
import org.apache.druid.segment.realtime.NoopChatHandlerProvider;
import org.apache.druid.server.ClientQuerySegmentWalker;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.LocalQuerySegmentWalker;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.SubqueryGuardrailHelper;
import org.apache.druid.server.TestClusterQuerySegmentWalker;
import org.apache.druid.server.TestClusterQuerySegmentWalker.TestSegmentsBroker;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.log.NoopRequestLogger;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.metrics.SubqueryCountStatsProvider;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.PreparedStatement;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.SqlToolbox;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.rule.ExtensionCalciteRuleProvider;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.DruidSchemaManager;
import org.apache.druid.sql.calcite.schema.LookupSchema;
import org.apache.druid.sql.calcite.schema.NoopDruidSchemaManager;
import org.apache.druid.sql.calcite.schema.SystemSchema;
import org.apache.druid.sql.calcite.util.datasets.TestDataSet;
import org.apache.druid.sql.calcite.view.DruidViewMacroFactory;
import org.apache.druid.sql.calcite.view.InProcessViewManager;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.apache.druid.sql.guice.SqlModule;
import org.apache.druid.sql.hook.DruidHookDispatcher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.JvmUtils;

import javax.inject.Named;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Builds the infrastructure needed to run Calcite tests. Building splits into
 * two parts: a constant part and a per-test part. The constant part includes
 * the congolmerate and walker (that is, the implementation of the test data),
 * while the per-query part includes the schema, planner factory and SQL
 * statement factory. The key reason for the split is that the per-test part
 * depends on the value of the {@link PlannerConfig} object, which varies between
 * tests.
 * <p>
 * The builder accepts the injector to use. "Calcite tests" use the injector
 * defined in {@link CalciteTests#INJECTOR}, while other tests can pass in the
 * preferred injector.
 * <p>
 * The framework allows the test to customize many of the framework components.
 * Since those components tend to depend on other framework components, we use
 * an indirection, {@link SqlTestFramework.QueryComponentSupplier QueryComponentSupplier}
 * to build those components. The methods of this interface match the methods
 * in @link org.apache.druid.sql.calcite.BaseCalciteQueryTest BaseCalciteQueryTest}
 * so that those tests can customize the framework just by overriding methods.
 * (This was done to maintain backward compatibility with earlier versions of the
 * code.) Other tests can implement {@code QueryComponentSupplier} directly by
 * extending {@link SqlTestFramework.StandardComponentSupplier StandardComponentSupplier}.
 * <p>
 * The framework should be built once per test class (not once per test method.)
 * Then, for each planner setup, call
 * {@link #plannerFixture(PlannerConfig, AuthConfig)}
 * to get a {@link PlannerFixture} with a view manager and planner factory. Call
 * {@link PlannerFixture#statementFactory()} to
 * obtain a the test-specific planner and wrapper classes for that test. After
 * that, tests use the various SQL statement classes to run tests. For tests
 * based on {@code BaseCalciteQueryTest}, the statements are wrapped by the
 * various {@code testQuery()} methods.
 * <p>
 * For tests that use non-standard views, first create the {@code PlannerFixture},
 * populate the views, then use the {@code QueryTestBuilder} directly, passing in
 * the {@code PlannerFixture} with views populated.
 * <p>
 * The framework holds on to the framework components. You can obtain the injector,
 * object mapper and other items by calling the various methods. The objects
 * are those created by the provided injector, or in this class, using objects
 * from that injector.
 */
public class SqlTestFramework
{
  /**
   * Interface to provide various framework components. Extend to customize,
   * use {@link StandardComponentSupplier} for the "standard" components.
   * <p>
   * Note that the methods here are named to match methods that already
   * exist in {@code BaseCalciteQueryTest}. Any changes here will impact that
   * base class, and possibly many test cases that extend that class.
   */
  public interface QueryComponentSupplier extends Closeable
  {
    /**
     * Gather properties to be used within tests. Particularly useful when choosing
     * among aggregator implementations: avoids the need to copy/paste code to select
     * the desired implementation.
     */
    void gatherProperties(Properties properties);

    Class<? extends SqlEngine> getSqlEngineClass();

    SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(SpecificSegmentsQuerySegmentWalker walker);

    /**
     * Should return a module which provides the core Druid components.
     */
    DruidModule getCoreModule();

    /**
     * Provides the overrides the core Druid components.
     */
    DruidModule getOverrideModule();

    default CatalogResolver createCatalogResolver()
    {
      return CatalogResolver.NULL_RESOLVER;
    }

    /**
     * Configure the JSON mapper.
     */
    @Deprecated
    default void configureJsonMapper(ObjectMapper mapper)
    {
    }

    JoinableFactoryWrapper createJoinableFactoryWrapper(LookupExtractorFactoryContainerProvider lookupProvider);

    void finalizeTestFramework(SqlTestFramework sqlTestFramework);

    PlannerComponentSupplier getPlannerComponentSupplier();

    @Override
    default void close() throws IOException
    {
    }

    /**
     * Configures modules and overrides.
     *
     * New classes should use the {@link QueryComponentSupplier#getCoreModule()}
     * and {@link QueryComponentSupplier#getOverrideModule()} methods.
     */
    @Deprecated
    void configureGuice(DruidInjectorBuilder injectorBuilder, List<Module> overrideModules);

    /**
     * Communicates if explain are supported.
     *
     * MSQ right now needs a full query run.
     */
    Boolean isExplainSupported();

    QueryRunnerFactoryConglomerate wrapConglomerate(QueryRunnerFactoryConglomerate conglomerate, Closer resourceCloser);

    TempDirProducer getTempDirProducer();
  }

  public abstract static class QueryComponentSupplierDelegate implements QueryComponentSupplier
  {
    private final QueryComponentSupplier delegate;

    public QueryComponentSupplierDelegate(QueryComponentSupplier delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public void gatherProperties(Properties properties)
    {
      delegate.gatherProperties(properties);
    }

    @Override
    public void configureGuice(DruidInjectorBuilder builder, List<Module> overrideModules)
    {
      delegate.configureGuice(builder, overrideModules);
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(SpecificSegmentsQuerySegmentWalker walker)
    {
      return delegate.addSegmentsToWalker(walker);
    }

    @Override
    public void configureJsonMapper(ObjectMapper mapper)
    {
      delegate.configureJsonMapper(mapper);
    }

    @Override
    public JoinableFactoryWrapper createJoinableFactoryWrapper(LookupExtractorFactoryContainerProvider lookupProvider)
    {
      return delegate.createJoinableFactoryWrapper(lookupProvider);
    }

    @Override
    public void finalizeTestFramework(SqlTestFramework sqlTestFramework)
    {
      delegate.finalizeTestFramework(sqlTestFramework);
    }

    @Override
    public PlannerComponentSupplier getPlannerComponentSupplier()
    {
      return delegate.getPlannerComponentSupplier();
    }

    @Override
    public void close() throws IOException
    {
      delegate.close();
    }

    @Override
    public Boolean isExplainSupported()
    {
      return delegate.isExplainSupported();
    }

    @Override
    public QueryRunnerFactoryConglomerate wrapConglomerate(
        QueryRunnerFactoryConglomerate conglomerate,
        Closer resourceCloser
    )
    {
      return delegate.wrapConglomerate(conglomerate, resourceCloser);
    }

    @Override
    public DruidModule getCoreModule()
    {
      return delegate.getCoreModule();
    }

    @Override
    public DruidModule getOverrideModule()
    {
      return delegate.getOverrideModule();
    }

    @Override
    public Class<? extends SqlEngine> getSqlEngineClass()
    {
      return delegate.getSqlEngineClass();
    }

    @Override
    public TempDirProducer getTempDirProducer()
    {
      return delegate.getTempDirProducer();
    }
  }

  public interface PlannerComponentSupplier
  {
    Set<ExtensionCalciteRuleProvider> extensionCalciteRules();

    ViewManager createViewManager();

    void populateViews(ViewManager viewManager, PlannerFactory plannerFactory);

    DruidSchemaManager createSchemaManager();

    void finalizePlanner(PlannerFixture plannerFixture);
  }

  /**
   * Provides a "standard" set of query components, where "standard" just means
   * those you would get if you used {@code BaseCalciteQueryTest} with no
   * customization. {@code BaseCalciteQueryTest} uses this class to provide those
   * standard components.
   */
  public static class StandardComponentSupplier implements QueryComponentSupplier
  {
    protected final TempDirProducer tempDirProducer;
    private final PlannerComponentSupplier plannerComponentSupplier;

    public StandardComponentSupplier(
        final TempDirProducer tempDirProducer
    )
    {
      this.tempDirProducer = tempDirProducer;
      this.plannerComponentSupplier = buildPlannerComponentSupplier();
    }

    /**
     * Build the {@link PlannerComponentSupplier}.
     *
     * Implementations may override how this is being built.
     */
    protected PlannerComponentSupplier buildPlannerComponentSupplier()
    {
      return new StandardPlannerComponentSupplier();
    }

    @Override
    public void gatherProperties(Properties properties)
    {
    }

    @Override
    public DruidModule getCoreModule()
    {
      return DruidModuleCollection.of(
          new LookylooModule(),
          new SegmentWranglerModule(),
          new ExpressionModule(),
          DruidModule.override(
              new QueryRunnerFactoryModule(),
              new Module()
              {
                @Override
                public void configure(Binder binder)
                {

                }

                @Provides
                @Named("isExplainSupported")
                public Boolean isExplainSupported(Builder builder)
                {
                  return builder.componentSupplier.isExplainSupported();
                }

                @Provides
                public QueryComponentSupplier getQueryComponentSupplier(Builder builder)
                {
                  return builder.componentSupplier;
                }

                @Provides
                @LazySingleton
                GroupByQueryMetricsFactory groupByQueryMetricsFactory()
                {
                  return DefaultGroupByQueryMetricsFactory.instance();
                }

                @Provides
                @LazySingleton
                public TopNQueryConfig makeTopNQueryConfig(Builder builder)
                {
                  return new TopNQueryConfig()
                  {
                    @Override
                    public int getMinTopNThreshold()
                    {
                      return builder.minTopNThreshold;
                    }
                  };
                }

                @Provides
                public QueryWatcher getQueryWatcher()
                {
                  return QueryRunnerTestHelper.NOOP_QUERYWATCHER;
                }
              }
          ),
          new BuiltInTypesModule(),
          new TestSqlModule(),
          DruidModule.override(
              new ServerModule(),
              new Module()
              {
                @Provides
                @Self
                @LazySingleton
                public DruidNode makeSelfDruidNode()
                {
                  return new DruidNode("druid/broker", "local-test-host", false, 12345, 443, true, false);
                }

                @Override
                public void configure(Binder binder)
                {
                }
              }
              ),
          new LifecycleModule(),
          DruidModule.override(
              new QueryableModule(),
              binder -> {
                TestRequestLogger testRequestLogger = new TestRequestLogger();
                binder.bind(RequestLogger.class).toInstance(testRequestLogger);
              }
          ),
          DruidModule.override(
              new SqlModule(),
              new Module()
              {
                @Override
                public void configure(Binder binder)
                {
                }

                @Provides
                @LazySingleton
                ViewManager createViewManager(Builder builder)
                {
                  return builder.componentSupplier.getPlannerComponentSupplier().createViewManager();
                }

                @Provides
                @LazySingleton
                private DruidSchema makeDruidSchema(
                    final Injector injector,
                    QueryRunnerFactoryConglomerate conglomerate,
                    QuerySegmentWalker walker,
                    Builder builder,
                    TimelineServerView timelineServerView
                )
                {
                  return QueryFrameworkUtils.createMockSchema(
                      injector,
                      conglomerate,
                      (SpecificSegmentsQuerySegmentWalker) walker,
                      builder.componentSupplier.getPlannerComponentSupplier().createSchemaManager(),
                      builder.catalogResolver,
                      timelineServerView
                  );
                }

                @Provides
                @LazySingleton
                private SystemSchema makeSystemSchema(
                    AuthorizerMapper authorizerMapper,
                    DruidSchema druidSchema,
                    TimelineServerView timelineServerView)
                {
                  return CalciteTests.createMockSystemSchema(druidSchema, timelineServerView, authorizerMapper);
                }

                @Provides
                @LazySingleton
                private TimelineServerView makeTimelineServerView(SpecificSegmentsQuerySegmentWalker walker)
                {
                  return new TestTimelineServerView(walker.getSegments());
                }

                @Provides
                @LazySingleton
                private LookupSchema makeLookupSchema(final Injector injector)
                {
                  return QueryFrameworkUtils.createMockLookupSchema(injector);
                }

                @Provides
                @LazySingleton
                private DruidSchemaCatalog makeCatalog(
                    final PlannerConfig plannerConfig,
                    final ViewManager viewManager,
                    AuthorizerMapper authorizerMapper,
                    DruidSchema druidSchema,
                    SystemSchema systemSchema,
                    LookupSchema lookupSchema,
                    DruidOperatorTable createOperatorTable
                )
                {
                  final DruidSchemaCatalog rootSchema = QueryFrameworkUtils.createMockRootSchema(
                      plannerConfig,
                      viewManager,
                      authorizerMapper,
                      druidSchema,
                      systemSchema,
                      lookupSchema,
                      createOperatorTable
                  );
                  return rootSchema;
                }
              }
          ),
          new TestSetupModule(),
          new TestSchemaSetupModule(),
          new StorageNodeModule()
      );
    }

    @Override
    public DruidModule getOverrideModule()
    {
      return DruidModuleCollection.of();
    }

    /**
     * Configures Guice modules (mostly).
     *
     * Deprecated; see: {@link QueryComponentSupplier#configureGuice(DruidInjectorBuilder, List)}
     */
    @Deprecated
    protected void configureGuice(DruidInjectorBuilder builder)
    {
    }

    @Override
    @Deprecated
    public void configureGuice(DruidInjectorBuilder builder, List<Module> overrideModules)
    {
      configureGuice(builder);
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(SpecificSegmentsQuerySegmentWalker walker)
    {
      return TestDataBuilder.addDataSetsToWalker(tempDirProducer.newTempFolder("segments"), walker);
    }

    @Override
    public Class<? extends SqlEngine> getSqlEngineClass()
    {
      return NativeSqlEngine.class;
    }

    @Override
    public JoinableFactoryWrapper createJoinableFactoryWrapper(LookupExtractorFactoryContainerProvider lookupProvider)
    {
      return new JoinableFactoryWrapper(
          QueryStackTests.makeJoinableFactoryFromDefault(
              lookupProvider,
              ImmutableSet.of(TestDataBuilder.CUSTOM_ROW_TABLE_JOINABLE),
              ImmutableMap.of(TestDataBuilder.CUSTOM_ROW_TABLE_JOINABLE.getClass(), GlobalTableDataSource.class)
          )
      );
    }

    @Override
    public void finalizeTestFramework(SqlTestFramework sqlTestFramework)
    {
    }

    @Override
    public PlannerComponentSupplier getPlannerComponentSupplier()
    {
      return plannerComponentSupplier;
    }

    @Override
    public void close() throws IOException
    {
      tempDirProducer.close();
    }

    @Override
    public Boolean isExplainSupported()
    {
      return true;
    }

    @Override
    public QueryRunnerFactoryConglomerate wrapConglomerate(
        QueryRunnerFactoryConglomerate conglomerate,
        Closer resourceCloser
    )
    {
      return conglomerate;
    }

    @Override
    public final TempDirProducer getTempDirProducer()
    {
      return tempDirProducer;
    }
  }

  public static class StandardPlannerComponentSupplier implements PlannerComponentSupplier
  {
    @Override
    public Set<ExtensionCalciteRuleProvider> extensionCalciteRules()
    {
      return ImmutableSet.of();
    }

    @Override
    public ViewManager createViewManager()
    {
      return new InProcessViewManager(DRUID_VIEW_MACRO_FACTORY);
    }

    @Override
    public void populateViews(ViewManager viewManager, PlannerFactory plannerFactory)
    {
      viewManager.createView(
          plannerFactory,
          "aview",
          "SELECT SUBSTRING(dim1, 1, 1) AS dim1_firstchar FROM foo WHERE dim2 = 'a'"
      );

      viewManager.createView(
          plannerFactory,
          "bview",
          "SELECT COUNT(*) FROM druid.foo\n"
          + "WHERE __time >= CURRENT_TIMESTAMP + INTERVAL '1' DAY AND __time < TIMESTAMP '2002-01-01 00:00:00'"
      );

      viewManager.createView(
          plannerFactory,
          "cview",
          "SELECT SUBSTRING(bar.dim1, 1, 1) AS dim1_firstchar, bar.dim2 as dim2, dnf.l2 as l2\n"
          + "FROM (SELECT * from foo WHERE dim2 = 'a') as bar INNER JOIN druid.numfoo dnf ON bar.dim2 = dnf.dim2"
      );

      viewManager.createView(
          plannerFactory,
          "dview",
          "SELECT SUBSTRING(dim1, 1, 1) AS numfoo FROM foo WHERE dim2 = 'a'"
      );

      viewManager.createView(
          plannerFactory,
          "forbiddenView",
          "SELECT __time, SUBSTRING(dim1, 1, 1) AS dim1_firstchar, dim2 FROM foo WHERE dim2 = 'a'"
      );

      viewManager.createView(
          plannerFactory,
          "restrictedView",
          "SELECT __time, dim1, dim2, m1 FROM druid.forbiddenDatasource WHERE dim2 = 'a'"
      );

      viewManager.createView(
          plannerFactory,
          "invalidView",
          "SELECT __time, dim1, dim2, m1 FROM druid.invalidDatasource WHERE dim2 = 'a'"
      );
    }

    @Override
    public DruidSchemaManager createSchemaManager()
    {
      return new NoopDruidSchemaManager();
    }

    @Override
    public void finalizePlanner(PlannerFixture plannerFixture)
    {
    }
  }

  /**
   * Builder for the framework. The component supplier and injector are
   * required; all other items are optional.
   */
  public static class Builder
  {
    private final QueryComponentSupplier componentSupplier;
    private int minTopNThreshold = TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD;
    private int mergeBufferCount;
    private CatalogResolver catalogResolver = CatalogResolver.NULL_RESOLVER;
    private List<Module> overrideModules = new ArrayList<>();
    private SqlTestFrameworkConfig config;
    private Closer resourceCloser = Closer.create();

    public Builder(QueryComponentSupplier componentSupplier)
    {
      this.componentSupplier = componentSupplier;
    }

    public Builder minTopNThreshold(int minTopNThreshold)
    {
      this.minTopNThreshold = minTopNThreshold;
      return this;
    }

    public Builder mergeBufferCount(int mergeBufferCount)
    {
      this.mergeBufferCount = mergeBufferCount;
      return this;
    }

    public Builder catalogResolver(CatalogResolver catalogResolver)
    {
      this.catalogResolver = catalogResolver;
      return this;
    }

    public Builder withOverrideModule(Module m)
    {
      this.overrideModules.add(m);
      return this;
    }

    public SqlTestFramework build()
    {
      return new SqlTestFramework(this);
    }

    public Builder withConfig(SqlTestFrameworkConfig config)
    {
      this.config = config;
      return this;
    }

    public QueryComponentSupplier getComponentSupplier()
    {
      return componentSupplier;
    }

    public CatalogResolver getCatalogResolver()
    {
      return catalogResolver;
    }

    public Closer getResourceCloser()
    {
      return resourceCloser;
    }
  }

  /**
   * Builds the statement factory, which also builds all the infrastructure
   * behind the factory by calling methods on this test class. As a result, each
   * factory is specific to one test and one planner config. This method can be
   * overridden to control the objects passed to the factory.
   */
  public static class PlannerFixture
  {
    private final ViewManager viewManager;
    private final PlannerFactory plannerFactory;
    private final SqlStatementFactory statementFactory;

    public PlannerFixture(
        final SqlTestFramework framework,
        final PlannerComponentSupplier componentSupplier,
        final PlannerConfig plannerConfig,
        final AuthConfig authConfig
    )
    {
      this.viewManager = componentSupplier.createViewManager();
      final DruidSchemaCatalog rootSchema = QueryFrameworkUtils.createMockRootSchema(
          framework.injector,
          framework.conglomerate(),
          framework.walker(),
          plannerConfig,
          viewManager,
          componentSupplier.createSchemaManager(),
          framework.authorizerMapper,
          framework.builder.catalogResolver,
          framework.injector.getInstance(TimelineServerView.class)
      );

      this.plannerFactory = new PlannerFactory(
          rootSchema,
          framework.operatorTable(),
          framework.macroTable(),
          plannerConfig,
          framework.authorizerMapper,
          framework.queryJsonMapper(),
          CalciteTests.DRUID_SCHEMA_NAME,
          new CalciteRulesManager(componentSupplier.extensionCalciteRules()),
          framework.injector.getInstance(JoinableFactoryWrapper.class),
          framework.builder.catalogResolver,
          authConfig != null ? authConfig : new AuthConfig(),
          new DruidHookDispatcher()
      );
      componentSupplier.finalizePlanner(this);
      final SqlToolbox toolbox = new SqlToolbox(
          framework.engine,
          plannerFactory,
          new ServiceEmitter("dummy", "dummy", new NoopEmitter()),
          new NoopRequestLogger(),
          QueryStackTests.DEFAULT_NOOP_SCHEDULER,
          new DefaultQueryConfig(ImmutableMap.of()),
          new SqlLifecycleManager()
      );
      this.statementFactory = new TestMultiStatementFactory(toolbox, framework.engine, plannerFactory);
      componentSupplier.populateViews(viewManager, plannerFactory);
    }

    public ViewManager viewManager()
    {
      return viewManager;
    }

    public PlannerFactory plannerFactory()
    {
      return plannerFactory;
    }

    public SqlStatementFactory statementFactory()
    {
      return statementFactory;
    }
  }

  /**
   * Guice module to create the various query framework items. By creating items within
   * a module, later items can depend on those created earlier by grabbing them from the
   * injector. This avoids the race condition that otherwise occurs if we try to build
   * some of the items directly code, while others depend on the injector.
   * <p>
   * To allow customization, the instances are created via provider methods that pull
   * dependencies from Guice, then call the component provider to create the instance.
   * Tests customize the instances by overriding the instance creation methods.
   * <p>
   * This is an intermediate solution: the ultimate solution is to create things
   * in Guice itself.
   */
  public static class TestSetupModule implements DruidModule
  {
    @Provides
    TempDirProducer getTempDirProducer(Builder builder)
    {
      return builder.componentSupplier.getTempDirProducer();
    }

    @Provides
    ServiceEmitter getServiceEmitter()
    {
      return NoopServiceEmitter.instance();
    }

    @Provides
    @LazySingleton
    public QuerySegmentWalker getQuerySegmentWalker(SpecificSegmentsQuerySegmentWalker walker)
    {
      return walker;
    }

    @Provides
    ChatHandlerProvider getChatHandlerProvider()
    {
      return new NoopChatHandlerProvider();
    }

    @Override
    public void configure(Binder binder)
    {
      binder.bind(DruidOperatorTable.class).in(LazySingleton.class);
      binder.bind(DataSegment.PruneSpecsHolder.class).toInstance(DataSegment.PruneSpecsHolder.DEFAULT);
    }

    @Provides
    @Global
    NonBlockingPool<ByteBuffer> getGlobalPool(TestBufferPool pool)
    {
      return pool;
    }

    @Provides
    @Merging
    BlockingPool<ByteBuffer> getMergingPool(TestBufferPool pool)
    {
      return pool;
    }

    @Provides
    AuthorizerMapper getAuthorizerMapper()
    {
      return AuthTestUtils.TEST_AUTHORIZER_MAPPER;
    }

    @Provides
    @LazySingleton
    private GroupByResourcesReservationPool makeGroupByResourcesReservationPool(
        final GroupByQueryConfig config,
        final TestGroupByBuffers bufferPools
    )
    {
      return new GroupByResourcesReservationPool(bufferPools.getMergePool(), config);
    }

    @Provides
    @LazySingleton
    private GroupingEngine makeGroupingEngine(
        final ObjectMapper mapper,
        final DruidProcessingConfig processingConfig,
        final GroupByStatsProvider statsProvider,
        final GroupByQueryConfig config,
        final GroupByResourcesReservationPool groupByResourcesReservationPool
    )
    {
      final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
      return new GroupingEngine(
          processingConfig,
          configSupplier,
          groupByResourcesReservationPool,
          mapper,
          mapper,
          QueryRunnerTestHelper.NOOP_QUERYWATCHER,
          statsProvider
      );
    }

    @Provides
    @LazySingleton
    @Merging
    GroupByResourcesReservationPool makeMergingGroupByResourcesReservationPool(
        final GroupByResourcesReservationPool groupByResourcesReservationPool
    )
    {
      return groupByResourcesReservationPool;
    }

    @Provides
    @LazySingleton
    public DruidProcessingConfig makeProcessingConfig(Builder builder)
    {
      return QueryStackTests.getProcessingConfig(builder.mergeBufferCount);
    }

    @Provides
    @LazySingleton
    public TestBufferPool makeTestBufferPool(Builder builder)
    {
      return QueryStackTests.makeTestBufferPool(builder.resourceCloser);
    }

    @Provides
    @LazySingleton
    public TestGroupByBuffers makeTestGroupByBuffers(DruidProcessingConfig processingConfig, Builder builder)
    {
      return QueryStackTests.makeGroupByBuffers(builder.resourceCloser, processingConfig);
    }

    @Provides
    @LazySingleton
    public JoinableFactoryWrapper joinableFactoryWrapper(final Injector injector, Builder builder)
    {
      return builder.componentSupplier.createJoinableFactoryWrapper(
          injector.getInstance(LookupExtractorFactoryContainerProvider.class)
      );
    }

    @Provides
    @LazySingleton
    public QueryLifecycleFactory queryLifecycleFactory(final Injector injector)
    {
      return QueryFrameworkUtils.createMockQueryLifecycleFactory(
          injector.getInstance(QuerySegmentWalker.class),
          injector.getInstance(QueryRunnerFactoryConglomerate.class),
          injector.getInstance(AuthorizerMapper.class)
      );
    }

    @Provides
    SqlTestFrameworkConfig getTestConfig(Builder builder)
    {
      return builder.config;
    }

    @Provides
    @Named("quidem")
    public URI getDruidTestURI(SqlTestFrameworkConfig config)
    {
      return config.getDruidTestURI();
    }
  }

  public static class TestSchemaSetupModule implements DruidModule
  {
    @Provides
    @LazySingleton
    public SpecificSegmentsQuerySegmentWalker specificSegmentsQuerySegmentWalker(
        @Named("empty") SpecificSegmentsQuerySegmentWalker walker, Builder builder,
        List<TestDataSet> testDataSets)
    {
      builder.resourceCloser.register(walker);
      if (testDataSets.isEmpty()) {
        builder.componentSupplier.addSegmentsToWalker(walker);
      } else {
        for (TestDataSet testDataSet : testDataSets) {
          walker.add(testDataSet, builder.componentSupplier.getTempDirProducer().newTempFolder());
        }
      }

      return walker;
    }

    @Provides
    @LazySingleton
    public List<TestDataSet> buildCustomTables(ObjectMapper objectMapper, TempDirProducer tdp,
        SqlTestFrameworkConfig cfg)
    {
      String datasets = cfg.datasets;
      if (datasets.isEmpty()) {
        return Collections.emptyList();
      }
      final File[] inputFiles = getTableIngestFiles(datasets);
      List<TestDataSet> ret = new ArrayList<TestDataSet>();
      for (File src : inputFiles) {
        ret.add(FakeIndexTaskUtil.makeDS(objectMapper, src));
      }
      return ret;
    }

    private File[] getTableIngestFiles(String datasets)
    {
      File datasetsFile = ProjectPathUtils.getPathFromProjectRoot(datasets);
      if (!datasetsFile.exists()) {
        throw new RE("Table config file does not exist: %s", datasetsFile);
      }
      if (!datasetsFile.isDirectory()) {
        throw new RE("The option datasets [%s] must point to a directory relative to the project root!", datasetsFile);
      }
      final File[] inputFiles = datasetsFile.listFiles(this::jsonFiles);
      if (inputFiles.length == 0) {
        throw new RE("There are no json files found in datasets directory [%s]!", datasetsFile);
      }

      return inputFiles;
    }

    boolean jsonFiles(File f)
    {
      return !f.isDirectory() && f.getName().endsWith(".json");
    }

    @Provides
    @LazySingleton
    public TestSegmentsBroker makeTimelines()
    {
      return new TestSegmentsBroker();
    }

    @Provides
    @LazySingleton
    private HttpClient makeHttpClient(ObjectMapper objectMapper)
    {
      return new TestHttpClient(objectMapper);
    }

    @Provides
    @Named("empty")
    @LazySingleton
    public SpecificSegmentsQuerySegmentWalker createEmptyWalker(
        TestSegmentsBroker testSegmentsBroker,
        ClientQuerySegmentWalker clientQuerySegmentWalker)
    {
      return new SpecificSegmentsQuerySegmentWalker(
          testSegmentsBroker.timelines,
          clientQuerySegmentWalker
      );
    }

    @Provides
    @LazySingleton
    private ClientQuerySegmentWalker makeClientQuerySegmentWalker(QueryRunnerFactoryConglomerate conglomerate,
        JoinableFactoryWrapper joinableFactory, Injector injector, ServiceEmitter emitter,
        TestClusterQuerySegmentWalker testClusterQuerySegmentWalker,
        LocalQuerySegmentWalker testLocalQuerySegmentWalker, ServerConfig serverConfig)
    {
      return new ClientQuerySegmentWalker(
          emitter,
          testClusterQuerySegmentWalker,
          testLocalQuerySegmentWalker,
          conglomerate,
          joinableFactory.getJoinableFactory(),
          new RetryQueryRunnerConfig(),
          injector.getInstance(ObjectMapper.class),
          serverConfig,
          injector.getInstance(Cache.class),
          injector.getInstance(CacheConfig.class),
          new SubqueryGuardrailHelper(null, JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes(), 1),
          new SubqueryCountStatsProvider()
      );
    }

    @Provides
    @LazySingleton
    public SubqueryGuardrailHelper makeSubqueryGuardrailHelper()
    {
      return new SubqueryGuardrailHelper(null, JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes(), 1);
    }

    @Override
    public void configure(Binder binder)
    {
    }
  }

  public static final DruidViewMacroFactory DRUID_VIEW_MACRO_FACTORY = new TestDruidViewMacroFactory();

  private final Builder builder;
  private final QueryComponentSupplier componentSupplier;
  private final Injector injector;
  private final AuthorizerMapper authorizerMapper = CalciteTests.TEST_AUTHORIZER_MAPPER;
  private final SqlEngine engine;

  private SqlTestFramework(Builder builder)
  {
    this.builder = builder;
    this.componentSupplier = builder.componentSupplier;
    Properties properties = new Properties();
    this.componentSupplier.gatherProperties(properties);
    Injector startupInjector = new StartupInjectorBuilder()
        .withProperties(properties)
        .build();
    CoreInjectorBuilder injectorBuilder = (CoreInjectorBuilder) new CoreInjectorBuilder(startupInjector)
        // Ignore load scopes. This is a unit test, not a Druid node. If a
        // test pulls in a module, then pull in that module, even though we are
        // not the Druid node to which the module is scoped.
        .ignoreLoadScopes();

    injectorBuilder.addAll(DruidModuleCollection.flatten(componentSupplier.getCoreModule()));
    injectorBuilder.addModule(binder -> binder.bind(Builder.class).toInstance(builder));

    ArrayList<Module> overrideModules = new ArrayList<>(builder.overrideModules);
    overrideModules.addAll(DruidModuleCollection.flatten(componentSupplier.getOverrideModule()));
    builder.componentSupplier.configureGuice(injectorBuilder, overrideModules);

    ServiceInjectorBuilder serviceInjector = new ServiceInjectorBuilder(injectorBuilder);
    serviceInjector.addAll(overrideModules);

    this.injector = serviceInjector.build();
    this.engine = injector.getInstance(componentSupplier.getSqlEngineClass());

    componentSupplier.configureJsonMapper(queryJsonMapper());
    componentSupplier.finalizeTestFramework(this);
  }

  public Injector injector()
  {
    return injector;
  }

  public SqlEngine engine()
  {
    return engine;
  }

  public ObjectMapper queryJsonMapper()
  {
    return injector.getInstance(ObjectMapper.class);
  }

  public QueryLifecycleFactory queryLifecycleFactory()
  {
    return injector.getInstance(QueryLifecycleFactory.class);
  }

  public QueryLifecycle queryLifecycle()
  {
    return queryLifecycleFactory().factorize();
  }

  public ExprMacroTable macroTable()
  {
    return injector.getInstance(ExprMacroTable.class);
  }

  public DruidOperatorTable operatorTable()
  {
    return injector.getInstance(DruidOperatorTable.class);
  }

  public SpecificSegmentsQuerySegmentWalker walker()
  {
    return injector.getInstance(SpecificSegmentsQuerySegmentWalker.class);
  }

  public QueryRunnerFactoryConglomerate conglomerate()
  {
    return injector.getInstance(QueryRunnerFactoryConglomerate.class);
  }

  /**
   * Creates an object (a "fixture") to hold the planner factory, view manager
   * and related items. Most tests need just the statement factory. View-related
   * tests also use the view manager. The fixture builds the infrastructure
   * behind the factory by calling methods on the {@link QueryComponentSupplier}
   * interface. That Calcite tests that interface, so the components can be customized
   * by overriding methods in a particular tests. As a result, each
   * planner fixture is specific to one test and one planner config.
   */
  public PlannerFixture plannerFixture(
      PlannerConfig plannerConfig,
      AuthConfig authConfig
  )
  {
    PlannerComponentSupplier plannerComponentSupplier = componentSupplier.getPlannerComponentSupplier();
    return new PlannerFixture(this, plannerComponentSupplier, plannerConfig, authConfig);
  }

  public void close()
  {
    try {
      builder.resourceCloser.close();
      componentSupplier.close();
    }
    catch (IOException e) {
      throw new RE(e);
    }
  }

  public URI getDruidTestURI()
  {
    return builder.config.getDruidTestURI();
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
        protected DruidPlanner getPlanner()
        {
          return plannerFactory.createPlanner(
              engine,
              queryPlus.sql(),
              queryContext,
              hook,
              true
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
              queryContext,
              hook,
              true
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
