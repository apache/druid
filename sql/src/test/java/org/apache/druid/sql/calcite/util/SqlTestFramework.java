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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
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
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.segment.DefaultColumnFormatConfig;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.SqlStatementFactory;
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
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
 * {@link #plannerFixture(PlannerComponentSupplier, PlannerConfig, AuthConfig)}
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
  public interface QueryComponentSupplier
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

    QueryRunnerFactoryConglomerate createCongolmerate(
        Builder builder,
        Closer closer
    );

    SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
        QueryRunnerFactoryConglomerate conglomerate,
        JoinableFactoryWrapper joinableFactory,
        Injector injector
    ) throws IOException;

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
    private final File temporaryFolder;

    public StandardComponentSupplier(
        final File temporaryFolder
    )
    {
      this.temporaryFolder = temporaryFolder;
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
    public QueryRunnerFactoryConglomerate createCongolmerate(
        Builder builder,
        Closer resourceCloser
    )
    {
      if (builder.mergeBufferCount == 0) {
        return QueryStackTests.createQueryRunnerFactoryConglomerate(
            resourceCloser,
            () -> builder.minTopNThreshold
        );
      } else {
        return QueryStackTests.createQueryRunnerFactoryConglomerate(
            resourceCloser,
            QueryStackTests.getProcessingConfig(builder.mergeBufferCount)
        );
      }
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
          temporaryFolder,
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
          authConfig != null ? authConfig : new AuthConfig()
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

    public TestSetupModule(Builder builder)
    {
      this.builder = builder;
    }

    @Override
    public void configure(Binder binder)
    {
      binder.bind(DruidOperatorTable.class).in(LazySingleton.class);
      binder.bind(DataSegment.PruneSpecsHolder.class).toInstance(DataSegment.PruneSpecsHolder.DEFAULT);
      binder.bind(DefaultColumnFormatConfig.class).toInstance(new DefaultColumnFormatConfig(null));
    }

    @Provides
    @LazySingleton
    public QueryRunnerFactoryConglomerate conglomerate()
    {
      return componentSupplier.createCongolmerate(builder, resourceCloser);
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
    public SpecificSegmentsQuerySegmentWalker segmentsQuerySegmentWalker(final Injector injector)
    {
      try {
        SpecificSegmentsQuerySegmentWalker walker = componentSupplier.createQuerySegmentWalker(
            injector.getInstance(QueryRunnerFactoryConglomerate.class),
            injector.getInstance(JoinableFactoryWrapper.class),
            injector
        );
        resourceCloser.register(walker);
        return walker;
      }
      catch (IOException e) {
        throw new RE(e);
      }
    }

    @Provides
    @LazySingleton
    public QueryLifecycleFactory queryLifecycleFactory(final Injector injector)
    {
      return QueryFrameworkUtils.createMockQueryLifecycleFactory(
          injector.getInstance(SpecificSegmentsQuerySegmentWalker.class),
          injector.getInstance(QueryRunnerFactoryConglomerate.class)
      );
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
        .ignoreLoadScopes()
        .addModule(new LookylooModule())
        .addModule(new SegmentWranglerModule())
        .addModule(new SqlAggregationModule())
        .addModule(new ExpressionModule())
        .addModule(new TestSetupModule(builder));

    builder.componentSupplier.configureGuice(injectorBuilder);

    ServiceInjectorBuilder serviceInjector = new ServiceInjectorBuilder(injectorBuilder);
    serviceInjector.addAll(builder.overrideModules);

    this.injector = serviceInjector.build();
    this.engine = builder.componentSupplier.createEngine(queryLifecycleFactory(), queryJsonMapper(), injector);
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
      PlannerComponentSupplier componentSupplier,
      PlannerConfig plannerConfig,
      AuthConfig authConfig
  )
  {
    return new PlannerFixture(this, componentSupplier, plannerConfig, authConfig);
  }

  public void close()
  {
    try {
      resourceCloser.close();
    }
    catch (IOException e) {
      throw new RE(e);
    }
  }
}
