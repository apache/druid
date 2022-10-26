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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Injector;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.lookup.LookupSerdeModule;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.NoopDruidSchemaManager;
import org.apache.druid.sql.calcite.view.DruidViewMacroFactory;
import org.apache.druid.sql.calcite.view.InProcessViewManager;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * Then, per method, call {@link #statementFactory(PlannerConfig, AuthConfig)} to
 * obtain a the test-specific planner and wrapper classes for that test. After
 * that, tests use the various SQL statement classes to run tests. For tests
 * based on {@code BaseCalciteQueryTest}, the statements are wrapped by the
 * various {@code testQuery()} methods.
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
    SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
        QueryRunnerFactoryConglomerate conglomerate
    ) throws IOException;

    SqlEngine createEngine(
        QueryLifecycleFactory qlf,
        ObjectMapper objectMapper
    );

    DruidOperatorTable createOperatorTable();

    ExprMacroTable createMacroTable();

    Iterable<? extends Module> getJacksonModules();

    Map<String, Object> getJacksonInjectables();

    void configureJsonMapper(ObjectMapper mapper);

    void configureGuice(DruidInjectorBuilder builder);
  }

  /**
   * Provides a "standard" set of query components, where "standard" just means
   * those you would get if you used {@code BaseCalciteQueryTest} with no
   * customization. {@code BaseCalciteQueryTest} uses this class to provide those
   * standard components.
   */
  public static class StandardComponentSupplier implements QueryComponentSupplier
  {
    private final Injector injector;
    private final File temporaryFolder;

    public StandardComponentSupplier(
        final Injector injector,
        final File temporaryFolder
    )
    {
      this.injector = injector;
      this.temporaryFolder = temporaryFolder;
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
        QueryRunnerFactoryConglomerate conglomerate
    )
    {
      return TestDataBuilder.createMockWalker(
          injector,
          conglomerate,
          temporaryFolder
      );
    }

    @Override
    public SqlEngine createEngine(QueryLifecycleFactory qlf, ObjectMapper objectMapper)
    {
      return new NativeSqlEngine(
          qlf,
          objectMapper
      );
    }

    @Override
    public DruidOperatorTable createOperatorTable()
    {
      return QueryFrameworkUtils.createOperatorTable(injector);
    }

    @Override
    public ExprMacroTable createMacroTable()
    {
      return QueryFrameworkUtils.createExprMacroTable(injector);
    }

    @Override
    public Iterable<? extends Module> getJacksonModules()
    {
      final List<Module> modules = new ArrayList<>(new LookupSerdeModule().getJacksonModules());
      modules.add(new SimpleModule().registerSubtypes(ExternalDataSource.class));
      return modules;
    }

    @Override
    public Map<String, Object> getJacksonInjectables()
    {
      return new HashMap<>();
    }

    @Override
    public void configureJsonMapper(ObjectMapper mapper)
    {
    }

    @Override
    public void configureGuice(DruidInjectorBuilder builder)
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

    public SqlTestFramework build()
    {
      return new SqlTestFramework(this);
    }
  }

  public static final DruidViewMacroFactory DRUID_VIEW_MACRO_FACTORY = new TestDruidViewMacroFactory();

  private final Closer resourceCloser = Closer.create();
  private final Injector injector;
  private final AuthorizerMapper authorizerMapper = CalciteTests.TEST_AUTHORIZER_MAPPER;
  private final SqlEngine engine;

  private SqlTestFramework(Builder builder)
  {
    this.injector = buildInjector(builder, resourceCloser);
    this.engine = builder.componentSupplier.createEngine(queryLifecycleFactory(), queryJsonMapper());
    builder.componentSupplier.configureJsonMapper(queryJsonMapper());
  }

  public Injector buildInjector(Builder builder, Closer resourceCloser)
  {
    CalciteTestInjectorBuilder injectorBuilder = new CalciteTestInjectorBuilder();

    final QueryRunnerFactoryConglomerate conglomerate;
    if (builder.mergeBufferCount == 0) {
      conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(
          resourceCloser,
          () -> builder.minTopNThreshold
      );
    } else {
      conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(
          resourceCloser,
          QueryStackTests.getProcessingConfig(true, builder.mergeBufferCount)
      );
    }

    final SpecificSegmentsQuerySegmentWalker walker;
    try {
      walker = builder.componentSupplier.createQuerySegmentWalker(conglomerate);
    }
    catch (IOException e) {
      throw new RE(e);
    }
    this.resourceCloser.register(walker);

    final QueryLifecycleFactory qlf = QueryFrameworkUtils.createMockQueryLifecycleFactory(walker, conglomerate);

    final DruidOperatorTable operatorTable = builder.componentSupplier.createOperatorTable();
    final ExprMacroTable macroTable = builder.componentSupplier.createMacroTable();

    injectorBuilder.addModule(new DruidModule()
    {
      @Override
      public void configure(Binder binder)
      {
        binder.bind(QueryRunnerFactoryConglomerate.class).toInstance(conglomerate);
        binder.bind(SpecificSegmentsQuerySegmentWalker.class).toInstance(walker);
        binder.bind(QueryLifecycleFactory.class).toInstance(qlf);
        binder.bind(DruidOperatorTable.class).toInstance(operatorTable);
        binder.bind(ExprMacroTable.class).toInstance(macroTable);
        binder.bind(DataSegment.PruneSpecsHolder.class).toInstance(DataSegment.PruneSpecsHolder.DEFAULT);
      }

      @Override
      public List<? extends Module> getJacksonModules()
      {
        return Lists.newArrayList(builder.componentSupplier.getJacksonModules());
      }
    });

    return injectorBuilder.build();
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
   * Build the statement factory, which also builds all the infrastructure
   * behind the factory by calling methods on this test class. As a result, each
   * factory is specific to one test and one planner config. This method can be
   * overridden to control the objects passed to the factory.
   */
  public SqlStatementFactory statementFactory(
      PlannerConfig plannerConfig,
      AuthConfig authConfig
  )
  {
    final InProcessViewManager viewManager = new InProcessViewManager(DRUID_VIEW_MACRO_FACTORY);
    DruidSchemaCatalog rootSchema = QueryFrameworkUtils.createMockRootSchema(
        injector,
        conglomerate(),
        walker(),
        plannerConfig,
        viewManager,
        new NoopDruidSchemaManager(),
        authorizerMapper
    );

    final PlannerFactory plannerFactory = new PlannerFactory(
        rootSchema,
        operatorTable(),
        macroTable(),
        plannerConfig,
        authorizerMapper,
        queryJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of())
    );
    final SqlStatementFactory sqlStatementFactory = QueryFrameworkUtils.createSqlStatementFactory(
        engine,
        plannerFactory,
        authConfig
    );

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
    return sqlStatementFactory;
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
