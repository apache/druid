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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.SegmentWranglerModule;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.initialization.ServiceInjectorBuilder;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryLogic;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.TestBufferPool;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.union.UnionQuery;
import org.apache.druid.query.union.UnionQueryLogic;
import org.apache.druid.quidem.TestSqlModule;
import org.apache.druid.segment.DefaultColumnFormatConfig;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.aggregation.SqlAggregationModule;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.rule.ExtensionCalciteRuleProvider;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.DruidSchemaManager;
import org.apache.druid.sql.calcite.schema.NoopDruidSchemaManager;
import org.apache.druid.sql.calcite.view.DruidViewMacroFactory;
import org.apache.druid.sql.calcite.view.InProcessViewManager;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.apache.druid.sql.hook.DruidHookDispatcher;
import org.apache.druid.timeline.DataSegment;

import javax.inject.Named;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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


    /**
     * Configure modules needed for tests. This is the preferred way to configure
     * Jackson: include the production module in this method that includes the
     * required Jackson configuration.
     */
    void configureGuice(DruidInjectorBuilder builder);

    SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
        QueryRunnerFactoryConglomerate conglomerate,
        JoinableFactoryWrapper joinableFactory,
        Injector injector
    );

    SqlEngine createEngine(
        QueryLifecycleFactory qlf,
        ObjectMapper objectMapper,
        Injector injector
    );

    default CatalogResolver createCatalogResolver()
    {
      return CatalogResolver.NULL_RESOLVER;
    }

    /**
     * Configure the JSON mapper.
     *
     * @see #configureGuice(DruidInjectorBuilder) for the preferred solution.
     */
    void configureJsonMapper(ObjectMapper mapper);

    JoinableFactoryWrapper createJoinableFactoryWrapper(LookupExtractorFactoryContainerProvider lookupProvider);

    void finalizeTestFramework(SqlTestFramework sqlTestFramework);

    PlannerComponentSupplier getPlannerComponentSupplier();
    @Override
    default void close() throws IOException
    {
    }

    default void configureGuice(CoreInjectorBuilder injectorBuilder, List<Module> overrideModules)
    {
      configureGuice(injectorBuilder);
    }

    /**
     * Communicates if explain are supported.
     *
     * MSQ right now needs a full query run.
     */
    Boolean isExplainSupported();

    QueryRunnerFactoryConglomerate wrapConglomerate(QueryRunnerFactoryConglomerate conglomerate, Closer resourceCloser);

    Map<? extends Class<? extends Query>, ? extends QueryRunnerFactory> makeRunnerFactories(Injector injector);

    Map<? extends Class<? extends Query>, ? extends QueryToolChest> makeToolChests(Injector injector);
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
    public void configureGuice(DruidInjectorBuilder builder)
    {
      delegate.configureGuice(builder);
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
        QueryRunnerFactoryConglomerate conglomerate,
        JoinableFactoryWrapper joinableFactory,
        Injector injector)
    {
      return delegate.createQuerySegmentWalker(conglomerate, joinableFactory, injector);
    }

    @Override
    public SqlEngine createEngine(
        QueryLifecycleFactory qlf,
        ObjectMapper objectMapper,
        Injector injector)
    {
      return delegate.createEngine(qlf, objectMapper, injector);
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
    public QueryRunnerFactoryConglomerate wrapConglomerate(QueryRunnerFactoryConglomerate conglomerate,
        Closer resourceCloser)
    {
      return delegate.wrapConglomerate(conglomerate, resourceCloser);
    }

    @Override
    public Map<? extends Class<? extends Query>, ? extends QueryRunnerFactory> makeRunnerFactories(Injector injector)
    {
      return delegate.makeRunnerFactories(injector);
    }

    @Override
    public Map<? extends Class<? extends Query>, ? extends QueryToolChest> makeToolChests(Injector injector)
    {
      return delegate.makeToolChests(injector);
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
    public void configureGuice(DruidInjectorBuilder builder)
    {
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
        final QueryRunnerFactoryConglomerate conglomerate,
        final JoinableFactoryWrapper joinableFactory,
        final Injector injector
    )
    {
      return TestDataBuilder.createMockWalker(
          injector,
          conglomerate,
          tempDirProducer.newTempFolder("segments"),
          QueryStackTests.DEFAULT_NOOP_SCHEDULER,
          joinableFactory
      );
    }

    @Override
    public SqlEngine createEngine(
        QueryLifecycleFactory qlf,
        ObjectMapper objectMapper,
        Injector injector
    )
    {
      return new NativeSqlEngine(
          qlf,
          objectMapper
      );
    }

    @Override
    public void configureJsonMapper(ObjectMapper mapper)
    {
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
    public QueryRunnerFactoryConglomerate wrapConglomerate(QueryRunnerFactoryConglomerate conglomerate,
        Closer resourceCloser)
    {
      return conglomerate;
    }

    @Override
    public Map<? extends Class<? extends Query>, ? extends QueryRunnerFactory> makeRunnerFactories(Injector injector)
    {
      return Collections.emptyMap();
    }

    @Override
    public Map<? extends Class<? extends Query>, ? extends QueryToolChest> makeToolChests(Injector injector)
    {
      return ImmutableMap.<Class<? extends Query>, QueryToolChest>builder()
          .build();
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
          framework.builder.catalogResolver
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
      this.statementFactory = QueryFrameworkUtils.createSqlStatementFactory(
          framework.engine,
          plannerFactory,
          authConfig
      );
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

  public static final String SQL_TEST_FRAME_WORK = "sqlTestFrameWork";

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
  private class TestSetupModule implements DruidModule
  {
    private final Builder builder;
    private final List<DruidModule> subModules = Arrays.asList(new BuiltInTypesModule(), new TestSqlModule());

    public TestSetupModule(Builder builder)
    {
      this.builder = builder;
    }

    @Override
    public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
    {
      ImmutableList.Builder<com.fasterxml.jackson.databind.Module> builder = ImmutableList.builder();
      for (DruidModule druidModule : subModules) {
        builder.addAll(druidModule.getJacksonModules());
      }
      return builder.build();
    }

    @Override
    public void configure(Binder binder)
    {
      for (DruidModule module : subModules) {
        binder.install(module);
      }
      binder.bind(DruidOperatorTable.class).in(LazySingleton.class);
      binder.bind(DataSegment.PruneSpecsHolder.class).toInstance(DataSegment.PruneSpecsHolder.DEFAULT);
      binder.bind(DefaultColumnFormatConfig.class).toInstance(new DefaultColumnFormatConfig(null, null));
    }


    @Provides
    @LazySingleton
    public @Named(SQL_TEST_FRAME_WORK) Map<Class<? extends Query>, QueryRunnerFactory> makeRunnerFactories(
        ObjectMapper jsonMapper,
        final TestBufferPool testBufferPool,
        final TestGroupByBuffers groupByBuffers,
        @Named(SqlTestFramework.SQL_TEST_FRAME_WORK) DruidProcessingConfig processingConfig)
    {
      return ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>builder()
          .putAll(
              QueryStackTests
                  .makeDefaultQueryRunnerFactories(
                      processingConfig,
                      builder.minTopNThreshold,
                      jsonMapper,
                      testBufferPool,
                      groupByBuffers
                  )
          )
          .putAll(componentSupplier.makeRunnerFactories(injector))
          .build();
    }

    @Provides
    @LazySingleton
    public @Named(SQL_TEST_FRAME_WORK) Map<Class<? extends Query>, QueryToolChest> makeToolchests(
        @Named(SQL_TEST_FRAME_WORK) Map<Class<? extends Query>, QueryRunnerFactory> factories)
    {
      return ImmutableMap.<Class<? extends Query>, QueryToolChest>builder()
          .putAll(Maps.transformValues(factories, f -> f.getToolchest()))
          .putAll(componentSupplier.makeToolChests(injector))
          .build();
    }

    @Provides
    @LazySingleton
    public @Named(SQL_TEST_FRAME_WORK) Map<Class<? extends Query>, QueryLogic> makeQueryLogics(
        UnionQueryLogic unionQueryLogic)
    {
      return ImmutableMap.<Class<? extends Query>, QueryLogic>builder()
          .put(UnionQuery.class, unionQueryLogic)
          .build();
    }

    /*
     * Ideally this should not have a Named annotation, but it clashes with {@link DruidProcessingModule}.
     */
    @Named(SQL_TEST_FRAME_WORK)
    @Provides
    @LazySingleton
    public DruidProcessingConfig makeProcessingConfig()
    {
      return QueryStackTests.getProcessingConfig(builder.mergeBufferCount);
    }

    @Provides
    @LazySingleton
    public TestBufferPool makeTestBufferPool()
    {
      return QueryStackTests.makeTestBufferPool(resourceCloser);
    }

    @Provides
    @LazySingleton
    public TestGroupByBuffers makeTestGroupByBuffers(@Named(SQL_TEST_FRAME_WORK) DruidProcessingConfig processingConfig)
    {
      return QueryStackTests.makeGroupByBuffers(resourceCloser, processingConfig);
    }

    @Provides
    @LazySingleton
    public QueryRunnerFactoryConglomerate conglomerate(
        @Named(SQL_TEST_FRAME_WORK) Map<Class<? extends Query>, QueryRunnerFactory> factories,
        @Named(SQL_TEST_FRAME_WORK) Map<Class<? extends Query>, QueryToolChest> toolchests,
        @Named(SQL_TEST_FRAME_WORK) Map<Class<? extends Query>, QueryLogic> querylogics)
    {
      QueryRunnerFactoryConglomerate conglomerate = new DefaultQueryRunnerFactoryConglomerate(factories, toolchests, querylogics);
      return componentSupplier.wrapConglomerate(conglomerate, resourceCloser);
    }

    @Provides
    @LazySingleton
    public JoinableFactoryWrapper joinableFactoryWrapper(final Injector injector)
    {
      return builder.componentSupplier.createJoinableFactoryWrapper(
          injector.getInstance(LookupExtractorFactoryContainerProvider.class)
      );
    }

    @Provides
    @LazySingleton
    public QuerySegmentWalker querySegmentWalker(final Injector injector)
    {
      return injector.getInstance(SpecificSegmentsQuerySegmentWalker.class);
    }

    @Provides
    @LazySingleton
    public SpecificSegmentsQuerySegmentWalker specificSegmentsQuerySegmentWalker(final Injector injector)
    {
      SpecificSegmentsQuerySegmentWalker walker = componentSupplier.createQuerySegmentWalker(
          injector.getInstance(QueryRunnerFactoryConglomerate.class),
          injector.getInstance(JoinableFactoryWrapper.class),
          injector
      );
      resourceCloser.register(walker);
      return walker;
    }

    @Provides
    @LazySingleton
    public QueryLifecycleFactory queryLifecycleFactory(final Injector injector)
    {
      return QueryFrameworkUtils.createMockQueryLifecycleFactory(
          injector.getInstance(QuerySegmentWalker.class),
          injector.getInstance(QueryRunnerFactoryConglomerate.class)
      );
    }

    @Provides
    @LazySingleton
    ViewManager createViewManager()
    {
      return componentSupplier.getPlannerComponentSupplier().createViewManager();
    }

    @Provides
    @LazySingleton
    public DruidSchemaCatalog makeCatalog(
        final Injector injector,
        final PlannerConfig plannerConfig,
        final AuthConfig authConfig,
        final ViewManager viewManager,
        QueryRunnerFactoryConglomerate conglomerate,
        QuerySegmentWalker walker
    )
    {
      final DruidSchemaCatalog rootSchema = QueryFrameworkUtils.createMockRootSchema(
          injector,
          conglomerate,
          (SpecificSegmentsQuerySegmentWalker) walker,
          plannerConfig,
          viewManager,
          componentSupplier.getPlannerComponentSupplier().createSchemaManager(),
          authorizerMapper,
          builder.catalogResolver
      );
      return rootSchema;
    }

    @Provides
    SqlTestFrameworkConfig getTestConfig()
    {
      return builder.config;
    }

    @Provides
    @Named("quidem")
    public URI getDruidTestURI()
    {
      return getTestConfig().getDruidTestURI();
    }

    @Provides
    @Named("isExplainSupported")
    public Boolean isExplainSupported()
    {
      return builder.componentSupplier.isExplainSupported();
    }
  }

  public static final DruidViewMacroFactory DRUID_VIEW_MACRO_FACTORY = new TestDruidViewMacroFactory();

  private final Builder builder;
  private final QueryComponentSupplier componentSupplier;
  private final Closer resourceCloser = Closer.create();
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
    List<Module> overrideModules = new ArrayList<>(builder.overrideModules);
    overrideModules.add(new LookylooModule());
    overrideModules.add(new SqlAggregationModule());
    overrideModules.add(new SegmentWranglerModule());
    overrideModules.add(new ExpressionModule());

    overrideModules.add(testSetupModule());
    builder.componentSupplier.configureGuice(injectorBuilder, overrideModules);

    ServiceInjectorBuilder serviceInjector = new ServiceInjectorBuilder(injectorBuilder);
    serviceInjector.addAll(overrideModules);

    this.injector = serviceInjector.build();
    this.engine = builder.componentSupplier.createEngine(queryLifecycleFactory(), queryJsonMapper(), injector);
    componentSupplier.configureJsonMapper(queryJsonMapper());
    componentSupplier.finalizeTestFramework(this);
  }

  public TestSetupModule testSetupModule()
  {
    return new TestSetupModule(builder);
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
      resourceCloser.close();
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
}
