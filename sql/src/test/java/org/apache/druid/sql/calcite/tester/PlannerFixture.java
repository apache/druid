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

package org.apache.druid.sql.calcite.tester;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.calcite.tools.RelConversionException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.lookup.LookupSerdeModule;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.run.QueryMakerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.RootSchemaBuilder;
import org.apache.druid.sql.calcite.util.RootSchemaBuilder.CatalogResult;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.view.InProcessViewManager;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.apache.druid.sql.http.SqlParameter;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configures and holds the Druid planner and its associated
 * helper classes. By default, sets up the planner to mimic the
 * {@code BaseCalciteTest} class, but all bits are configurable for
 * special cases. (To do that, extend the {@link Builder} class
 * with the required methods.)
 */
public class PlannerFixture
{
  /**
   * Builds the planner fixture by allowing the test case to customize
   * parts of the build process without copy/pasting the entire messy
   * setup process. The builder is also a "rebuilder" to build a second
   * planner factory when the planner settings change. Since the planner
   * settings holds more than just <b>planner</b> settings, it also
   * is used in code that supports the planner. The structure works fine
   * when Druid is run normally, but is awkward in tests. This builder
   * hides all that cruft.
   */
  public static class Builder
  {
    static {
      Calcites.setSystemProperties();
      ExpressionProcessing.initializeForTests(null);
    }

    final File temporaryFolder;
    List<Module> jacksonModules;
    Map<String, Object> jacksonInjectables = new HashMap<>();

    // Planner config contains values use by the planner, but also
    // by the Druid schema to control the refresh interval. The
    // value here is used by the mock schema objects. It is also
    // used when planning unless a case provides its own config.
    // Test-specific configs do not contain values that influence
    // the schema usage of the config. Rather confusing.
    PlannerConfig plannerConfig = new PlannerConfig();
    DruidSchemaCatalog rootSchema;
    AuthConfig authConfig = new AuthConfig();
    DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();
    ExprMacroTable macroTable = CalciteTests.createExprMacroTable();
    AuthorizerMapper authorizerMapper = CalciteTests.TEST_AUTHORIZER_MAPPER;
    ObjectMapper objectMapper;
    String druidSchemaName = CalciteTests.DRUID_SCHEMA_NAME;
    QueryMakerFactory queryMakerFactory;
    Closer resourceCloser = Closer.create();
    int minTopNThreshold = TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD;
    ViewManager viewManager;
    File resultsDir = new File("target/actual");
    List<Pair<String, String>> views = new ArrayList<>();
    Map<String, Object> defaultQueryOptions;
    AuthenticationResult defaultAuthResult = CalciteTests.REGULAR_USER_AUTH_RESULT;
    QueryRunnerFactoryConglomerate conglomerate;
    SpecificSegmentsQuerySegmentWalker walker;
    boolean includeLookups;
    int mergeBufferCount = -1;

    public Builder(File temporaryFolder)
    {
      this.temporaryFolder = temporaryFolder;

      // See BaseCalciteQueryTest.getJacksonModules()
      jacksonModules = new ArrayList<>(new LookupSerdeModule().getJacksonModules());
      jacksonModules.add(new SimpleModule().registerSubtypes(ExternalDataSource.class));

      // See BaseCalciteQueryTest.createQueryJsonMapper()
      objectMapper = new DefaultObjectMapper().registerModules(jacksonModules);
      BaseCalciteQueryTest.setMapperInjectableValues(objectMapper, jacksonInjectables, macroTable);
    }

    public Builder withView(String viewName, String stmt)
    {
      views.add(Pair.of(viewName, stmt));
      return this;
    }

    public Builder withPlannerConfig(PlannerConfig plannerConfig)
    {
      this.plannerConfig = plannerConfig;
      return this;
    }

    public Builder withQueryMaker(QueryMakerFactory queryMakerFactory)
    {
      this.queryMakerFactory = queryMakerFactory;
      return this;
    }

    public Builder defaultQueryOptions(Map<String, Object> defaultQueryOptions)
    {
      this.defaultQueryOptions = defaultQueryOptions;
      return this;
    }

    public Builder withLookups()
    {
      this.includeLookups = true;
      return this;
    }

    public Builder withMergeBufferCount(int count)
    {
      this.mergeBufferCount = count;
      return this;
    }

    public Builder withAuthResult(AuthenticationResult authResult)
    {
      this.defaultAuthResult = authResult;
      return this;
    }

    public ObjectMapper jsonMapper()
    {
      return this.objectMapper;
    }

    public Builder copy()
    {
      Builder copy = new Builder(temporaryFolder);
      copy.jacksonModules = jacksonModules;
      copy.jacksonInjectables = jacksonInjectables;
      copy.plannerConfig = plannerConfig;
      copy.authConfig = authConfig;
      copy.operatorTable = operatorTable;
      copy.macroTable = macroTable;
      copy.authorizerMapper = authorizerMapper;
      copy.objectMapper = objectMapper;
      copy.druidSchemaName = druidSchemaName;
      copy.queryMakerFactory = queryMakerFactory;
      copy.minTopNThreshold = minTopNThreshold;
      copy.viewManager = viewManager;
      copy.resultsDir = resultsDir;
      copy.defaultQueryOptions = defaultQueryOptions;
      copy.defaultAuthResult = defaultAuthResult;
      copy.includeLookups = includeLookups;
      // Don't copy the conglomerate or walker: one of them
      // caches the null handling setting and causes tests to
      // fail if they are reused.
      // Don't copy the views: they are already in the view manager.
      copy.views = new ArrayList<>();
      return copy;
    }

    public PlannerFixture build()
    {
      return new PlannerFixture(this);
    }
  }

  public static class ExplainFixture
  {
    final PlannerFixture plannerFixture;
    final String sql;
    final Map<String, Object> context;
    final List<SqlParameter> parameters;
    final AuthenticationResult authenticationResult;
    private List<Object[]> results;

    public ExplainFixture(
        PlannerFixture plannerFixture,
        String sql,
        Map<String, Object> context,
        List<SqlParameter> parameters,
        AuthenticationResult authenticationResult)
    {
      this.plannerFixture = plannerFixture;
      this.sql = sql;
      this.context = context;
      this.parameters = parameters;
      this.authenticationResult = authenticationResult;
    }

    public ExplainFixture(PlannerFixture plannerFixture, String sql, Map<String, Object> context)
    {
      this(
          plannerFixture,
          sql, context,
          Collections.emptyList(),
          CalciteTests.REGULAR_USER_AUTH_RESULT);
    }

    public void explain() throws RelConversionException
    {
      results = plannerFixture.sqlLifecycleFactory
          .factorize()
          .runSimple(sql, context, parameters, authenticationResult)
          .toList();
    }

    public Pair<String, String> results()
    {
      Object[] row = results.get(0);
      return Pair.of((String) row[0], (String) row[1]);
    }
  }

  final Builder builder;
  final QueryRunnerFactoryConglomerate conglomerate;
  final SpecificSegmentsQuerySegmentWalker walker;
  final SqlLifecycleFactory sqlLifecycleFactory;
  final ObjectMapper jsonMapper;
  final File resultsDir;
  final ViewManager viewManager;
  final Map<String, Object> defaultQueryOptions;
  final AuthenticationResult defaultAuthResult;
  final QueryRunner queryRunner;

  public PlannerFixture(Builder builder)
  {
    this.builder = builder;

    // Must rebuild the schema (and its mock data) each time since
    // a change to global options will change the generated mock segments.
    if (builder.conglomerate == null) {
      if (builder.mergeBufferCount > -1) {
        conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(
            builder.resourceCloser,
            QueryStackTests.getProcessingConfig(true, builder.mergeBufferCount),
            () -> builder.minTopNThreshold);
      } else {
        conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(
            builder.resourceCloser,
            () -> builder.minTopNThreshold);
      }
    } else {
      conglomerate = builder.conglomerate;
    }
    if (builder.walker == null) {
      walker = CalciteTests.createMockWalker(
          conglomerate,
          builder.temporaryFolder
      );
    } else {
      walker = builder.walker;
    }
    RootSchemaBuilder rootSchemaBuilder = new RootSchemaBuilder(
            builder.plannerConfig,
            builder.authorizerMapper)
        .congolomerate(conglomerate)
        .walker(walker)
        .withLookupSchema(builder.includeLookups);
    if (builder.viewManager == null) {
      viewManager = new InProcessViewManager(CalciteTests.DRUID_VIEW_MACRO_FACTORY);
    } else {
      viewManager = builder.viewManager;
    }
    if (viewManager != null) {
      rootSchemaBuilder.viewManager(viewManager);
    }
    CatalogResult result = rootSchemaBuilder.build();
    PlannerFactory plannerFactory = new PlannerFactory(
        result.catalog,
        result.createQueryMakerFactory(builder.objectMapper),
        builder.operatorTable,
        builder.macroTable,
        builder.plannerConfig,
        builder.authorizerMapper,
        builder.objectMapper,
        builder.druidSchemaName
    );
    this.queryRunner = new QueryRunner(plannerFactory, builder.authorizerMapper);
    for (Pair<String, String> view : builder.views) {
      viewManager.createView(plannerFactory, view.lhs, view.rhs);
    }
    this.sqlLifecycleFactory = CalciteTests.createSqlLifecycleFactory(
        plannerFactory,
        builder.authConfig);
    this.resultsDir = builder.resultsDir;
    this.jsonMapper = builder.objectMapper;
    this.defaultQueryOptions = builder.defaultQueryOptions;
    this.defaultAuthResult = builder.defaultAuthResult;
  }

  public static Builder builder(File tempDir)
  {
    return new Builder(tempDir);
  }

  /**
   * Create a copy of the builder to change planner options.
   * Leaves the conglomerate and walker, as they depend on
   * null handling which must not change in the copy.
   */
  public Builder toBuilder()
  {
    Builder newBuilder = builder.copy();
    newBuilder.conglomerate = conglomerate;
    newBuilder.walker = walker;
    return newBuilder;
  }

  public File resultsDir()
  {
    return resultsDir;
  }

  public File tempDir()
  {
    return builder.temporaryFolder;
  }

  public PlannerConfig plannerConfig()
  {
    return builder.plannerConfig;
  }

  public AuthenticationResult authResultFor(String user)
  {
    if (user == null) {
      return defaultAuthResult;
    }
    throw new UOE("Not yet");
  }

  public QueryRunner queryRunner()
  {
    return queryRunner;
  }

  public Map<String, Object> applyDefaultContext(Map<String, Object> context)
  {
    if (defaultQueryOptions != null) {
      context = QueryContexts.override(defaultQueryOptions, context);
    }
    return context;
  }

  public ActualResults runTestCase(QueryTestCase testCase)
  {
    return new QueryTestCaseRunner(this, testCase).run();
  }
}
