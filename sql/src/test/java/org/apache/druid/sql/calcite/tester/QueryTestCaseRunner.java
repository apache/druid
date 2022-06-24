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

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlInsert;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.math.expr.ExpressionProcessingConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryContexts.Vectorize;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest.QueryContextForJoinProvider;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.tester.PlannerFixture.ExplainFixture;
import org.apache.druid.sql.calcite.tester.QueryRunner.PlanDetails;
import org.apache.druid.sql.calcite.util.CalciteTests;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Runs a test case and captures the planning-related aspects
 * of the query that the test case says to verify.
 * <p>
 * Druid is irritating in that several options are global, yet tests
 * want to test variations. This appears to normally be done by running
 * tests with different command-line settings, which is clunky. We want
 * to set those options in-line. Further, some of the the global options
 * are cached in the planner, forcing us to rebuild the entire planner
 * when the options change. This is clearly an opportunity for improvement.
 */
public class QueryTestCaseRunner
{
  public static final Logger log = new Logger(QueryTestCaseRunner.class);

  private static final Map<String, Object> ENABLE_VECTORIZE_CONTEXT =
      ImmutableMap.of(
          QueryContexts.VECTORIZE_KEY,
          Vectorize.FORCE.name(),
          QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY,
          Vectorize.FORCE.name(),
          QueryContexts.VECTOR_SIZE_KEY,
          2); // Small vector size to ensure we use more than one.
  private static final Map<String, Object> DISABLE_VECTORIZE_CONTEXT =
      ImmutableMap.of(
          QueryContexts.VECTORIZE_KEY,
          Vectorize.FALSE.name(),
          QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY,
          Vectorize.FALSE.name());

  private final PlannerFixture baseFixture;
  private final QueryTestCase testCase;
  private final ActualResults results;
  private PlannerFixture plannerFixture;
  private PlanDetails planDetails;
  private ExplainFixture explainFixture;

  public QueryTestCaseRunner(PlannerFixture plannerFixture, QueryTestCase testCase)
  {
    this.baseFixture = plannerFixture;
    this.plannerFixture = plannerFixture;
    this.testCase = testCase;
    this.results = new ActualResults(testCase);
  }

  public ActualResults run()
  {
    return runWithExpressionOptions();
  }

  private ActualResults runWithExpressionOptions()
  {
    // Horrible, hacky way to change the way Druid handles
    // expressions. The config is meant to be global, initialized
    // on startup. This is a cheap workaround.
    // Only works for single-threaded tests.
    boolean allowNestedArrays = testCase.booleanOption(OptionsSection.ALLOW_NESTED_ARRAYS);
    boolean homogenizeNullMultiValueStrings = testCase.booleanOption(OptionsSection.HOMOGENIZE_NULL_MULTI_VALUE_STRINGS);
    if (allowNestedArrays == ExpressionProcessing.allowNestedArrays() &&
        homogenizeNullMultiValueStrings == ExpressionProcessing.isHomogenizeNullMultiValueStringArrays()) {
      return runWithNullHandingOptions();
    }
    ExpressionProcessingConfig prevExprConfig = ExpressionProcessing.currentConfig();
    try {
      ExpressionProcessing.initializeForTests(allowNestedArrays);
      if (homogenizeNullMultiValueStrings) {
        ExpressionProcessing.initializeForHomogenizeNullMultiValueStrings();
      }
      plannerFixture = null;
      return runWithNullHandingOptions();
    }
    finally {
      ExpressionProcessing.restoreConfig(prevExprConfig);
    }
  }

  private ActualResults runWithNullHandingOptions()
  {
    // Horrible, hacky way to change the way Druid handles
    // nulls. The config is meant to be global, initialized
    // on startup. This is a cheap workaround.
    // Only works for single-threaded tests.
    String sqlNullHandling = testCase.option(OptionsSection.SQL_COMPATIBLE_NULLS);
    if (sqlNullHandling == null) {
      return runWithCustomPlanner();
    }
    boolean useSqlNulls = QueryContexts.getAsBoolean(
            OptionsSection.SQL_COMPATIBLE_NULLS,
            sqlNullHandling,
            true);
    if (useSqlNulls != NullHandling.sqlCompatible()) {
      return null;
    }
    return runWithCustomPlanner();
  }

  /**
   * The planner factory and surrounding objects are designed to be created once
   * at the start of a Druid run. Test cases, however, want to try variations.
   * If the test case has planner settings, create a new planner fixture
   * (and all its associated knick-knacks), just for that one test. The custom
   * planner starts with the configuration for the "global" planner.
   * <p>
   * To do: since we want to test the planner, restructure the code to allow
   * changing just the planner config without needing to rebuild everything
   * else.
   * <p>
   * The planner fixture (and its associated mock segments) also must be
   * recreated if the global options change, such as null handling. Again, ugly,
   * but the best we can do.
   * <p>
   * The planner fixture is not global, so we can create a new one just for
   * this test, leaving the original one unchanged.
   */
  private ActualResults runWithCustomPlanner()
  {
    boolean hasCustomConfig = testCase.requiresCustomPlanner();
    if (plannerFixture != null && !hasCustomConfig) {
      return doRun();
    }
    PlannerFixture.Builder builder = baseFixture.toBuilder();
    if (hasCustomConfig) {
      PlannerConfig customConfig = QueryTestCases.applyOptions(
          plannerFixture.plannerConfig(),
          testCase.optionsSection().options());
      builder.withPlannerConfig(customConfig);
    }
    plannerFixture = builder.build();
    return doRun();
  }

  private ActualResults doRun()
  {
    gatherResults();
    results.verify();
    return results;
  }

  // Lazy planning evaluation in case the test only wants to EXPLAIN,
  // but not capture detail plan results.
  private void preparePlan() throws Exception
  {
    if (planDetails != null) {
      return;
    }
    QueryDefn queryDefn = QueryDefn
        .builder(testCase.sql())
        // Plan with only the context in the test case. Ensures that the
        // case with no extra context works. Makes native queries smaller.
        .context(testCase.context())
        .parameters(testCase.parameters())
        .authResult(plannerFixture.authResultFor(testCase.user()))
        .build();
    planDetails = plannerFixture.queryRunner().introspectPlan(queryDefn);
  }

  private void gatherResults()
  {
    try {
      // Planning is done on demand. If we should fail in planning,
      // go ahead and try now. If the query succeeds, no need to try
      // the other items as success and failure are mutually exclusive.
      if (testCase.shouldFail()) {
        preparePlan();
        return;
      }

      // Gather actual plan results to compare against expected values.
      gatherParseTree();
      gatherUnparse();
      gatherSchema();
      gatherPlan();
      gatherNativeQuery();
      gatherResources();
      gatherTargetSchema();
      gatherExplain();
      gatherExecPlan();
    }
    catch (Exception e) {
      results.exception(e);
      return;
    }

    // Run the query with the requested options
    for (QueryRun run : testCase.runs()) {
      runQuery(run);
    }
  }

  private void gatherParseTree() throws Exception
  {
    PatternSection ast = testCase.ast();
    if (ast == null) {
      return;
    }
    preparePlan();
    ParseTreeVisualizer visitor = new ParseTreeVisualizer();
    planDetails.planState().sqlNode.accept(visitor);
    String output = visitor.result();
    results.ast(ast, output);
  }

  private void gatherUnparse() throws Exception
  {
    PatternSection testSection = testCase.unparsed();
    if (testSection == null) {
      return;
    }
    preparePlan();
    String unparsed = planDetails.planState().sqlNode.toString();
    results.unparsed(testSection, unparsed);
  }

  private void gatherPlan() throws Exception
  {
    PatternSection testSection = testCase.plan();
    if (testSection == null) {
      return;
    }
    preparePlan();
    if (planDetails.planState().bindableRel != null) {
      gatherBindablePlan(testSection);
    } else if (planDetails.planState().relRoot != null) {
      gatherDruidPlan(testSection);
    } else {
      throw new ISE(
          StringUtils.format(
              "Test case [%s] has a plan but the planner did not produce one.",
              testCase.label()));
    }
  }

  private void gatherDruidPlan(PatternSection testSection)
  {
    // Do-it-ourselves plan since the actual plan omits insert.
    String queryPlan = RelOptUtil.dumpPlan(
        "",
        planDetails.planState().relRoot.rel,
        SqlExplainFormat.TEXT,
        SqlExplainLevel.DIGEST_ATTRIBUTES);
    String plan;
    SqlInsert insertNode = planDetails.planState().insertNode;
    if (insertNode == null) {
      plan = queryPlan;
    } else if (insertNode instanceof DruidSqlInsert) {
      DruidSqlInsert druidInsertNode = (DruidSqlInsert) insertNode;
      // The target is a SQLIdentifier literal, pre-resolution, so does
      // not include the schema.
      plan = StringUtils.format(
          "LogicalInsert(target=[%s], granularity=[%s])\n",
          druidInsertNode.getTargetTable(),
          druidInsertNode.getPartitionedBy() == null ? "<none>" : druidInsertNode.getPartitionedBy());
      if (druidInsertNode.getClusteredBy() != null) {
        plan += "  Clustered By: " + druidInsertNode.getClusteredBy();
      }
      plan +=
          "  " + StringUtils.replace(queryPlan, "\n ", "\n   ");
    } else if (insertNode instanceof DruidSqlReplace) {
      DruidSqlReplace druidInsertNode = (DruidSqlReplace) insertNode;
      // The target is a SQLIdentifier literal, pre-resolution, so does
      // not include the schema.
      plan = StringUtils.format(
          "LogicalInsert(target=[%s], granularity=[%s])\n",
          druidInsertNode.getTargetTable(),
          druidInsertNode.getPartitionedBy() == null ? "<none>" : druidInsertNode.getPartitionedBy());
      if (druidInsertNode.getClusteredBy() != null) {
        plan += "  Clustered By: " + druidInsertNode.getClusteredBy();
      }
      plan +=
          "  " + StringUtils.replace(queryPlan, "\n ", "\n   ");
    } else {
      plan = queryPlan;
    }
    results.plan(testSection, plan);
  }

  private void gatherBindablePlan(PatternSection testSection)
  {
    String queryPlan = RelOptUtil.dumpPlan(
        "",
        planDetails.planState().bindableRel,
        SqlExplainFormat.TEXT,
        SqlExplainLevel.DIGEST_ATTRIBUTES);
    results.plan(testSection, queryPlan);
  }

  private void gatherExecPlan()
  {
    PatternSection testSection = testCase.execPlan();
    if (testSection == null) {
      return;
    }
    results.execPlan(testSection,
        QueryTestCases.formatJson(
            plannerFixture.jsonMapper,
            planDetails.planState().execPlan));
  }

  private void gatherNativeQuery() throws Exception
  {
    PatternSection testSection = testCase.nativeQuery();
    if (testSection == null) {
      return;
    }
    preparePlan();
    DruidRel<?> druidRel = planDetails.planState().druidRel;
    if (druidRel == null) {
      throw new ISE(
          StringUtils.format(
              "Test case [%s] has a native query but the planner did not produce one.",
              testCase.label()));
    }
    results.nativeQuery(
        testSection,
        QueryTestCases.serializeDruidRel(plannerFixture.jsonMapper, druidRel));
  }

  private void gatherSchema() throws Exception
  {
    PatternSection section = testCase.schema();
    if (section == null) {
      return;
    }
    preparePlan();
    results.schema(
        section,
        QueryTestCases.formatSchema(planDetails.plannerResult()));
  }

  private void gatherResources() throws Exception
  {
    ResourcesSection section = testCase.resourceActions();
    if (section == null) {
      return;
    }
    preparePlan();
    results.resourceActions(
        section,
        planDetails.validationResult().getResourceActions());
  }

  private void gatherTargetSchema() throws Exception
  {
    PatternSection section = testCase.targetSchema();
    if (section == null) {
      return;
    }
    preparePlan();
    if (planDetails.planState().insertNode == null) {
      results.errors().add(
          StringUtils.format(
              "Query [%s] expects a target schema, but the SQL is not an INSERT statement.",
              testCase.label()));
      return;
    }

    List<RelDataTypeField> fields = planDetails.planState().relRoot.validatedRowType.getFieldList();
    String[] actual = new String[fields.size()];
    for (int i = 0; i < actual.length; i++) {
      RelDataTypeField field = fields.get(i);
      actual[i] = field.getName() + " " + field.getType();
    }
    results.targetSchema(section, actual);
  }

  private void gatherExplain() throws Exception
  {
    PatternSection testSection = testCase.explain();
    if (testSection == null) {
      return;
    }
    // User mapping is a bit lame: there are only two: the regular user (default)
    // or the super user. The super user is required for tests with an extern data
    // source as the regular user test setup doesn't provide access.
    AuthenticationResult authenticationResult;
    String user = testCase.user();
    if (user != null && user.equals(CalciteTests.TEST_SUPERUSER_NAME)) {
      authenticationResult = CalciteTests.SUPER_USER_AUTH_RESULT;
    } else {
      authenticationResult = CalciteTests.REGULAR_USER_AUTH_RESULT;
    }
    explainFixture = new ExplainFixture(
        plannerFixture,
        testCase.sql(),
        testCase.context(),
        Collections.emptyList(),
        authenticationResult);
    explainFixture.explain();
    Pair<String, String> explained = explainFixture.results();
    results.explain(
        testSection,
        QueryTestCases.formatExplain(
            plannerFixture.jsonMapper,
            explained.lhs,
            explained.rhs));
  }

  private interface QueryExec
  {
    void run(QueryDefn queryDefn, Map<String, String> options);
  }

  private class ConcreteExec implements QueryExec
  {
    private QueryRun queryRun;

    private ConcreteExec(QueryRun queryRun)
    {
      this.queryRun = queryRun;
    }

    @Override
    public void run(QueryDefn queryDefn, Map<String, String> options)
    {
      try {
        List<Object[]> rows = plannerFixture.queryRunner.run(queryDefn).toList();
        results.run(queryRun, queryDefn.context(), rows, plannerFixture.jsonMapper);
      }
      catch (Exception e) {
        results.runFailed(queryRun, queryDefn.context(), e);
      }
    }
  }

  private static class VectorizeExec implements QueryExec
  {
    private final QueryExec child;

    public VectorizeExec(QueryExec child)
    {
      this.child = child;
    }

    @Override
    public void run(QueryDefn queryDefn, Map<String, String> options)
    {
      child.run(queryDefn.withOverrides(DISABLE_VECTORIZE_CONTEXT), options);
      boolean canVectorize = QueryTestCases.booleanOption(
          options,
          OptionsSection.VECTORIZE_OPTION,
          true);
      if (!canVectorize) {
        return;
      }
      child.run(queryDefn.withOverrides(ENABLE_VECTORIZE_CONTEXT), options);
    }
  }

  /**
   * Filter to only pass along runs that match the current "replace with
   * null" setting initialized externally. It matches if no options is given
   * for the run, the option is "both", or the option Boolean value matches
   * the current setting.
   */
  private static class NullStrategyFilter implements QueryExec
  {
    private final boolean sqlCompatible = NullHandling.sqlCompatible();
    private final QueryExec child;

    public NullStrategyFilter(QueryExec child)
    {
      this.child = child;
    }

    @Override
    public void run(QueryDefn queryDefn, Map<String, String> options)
    {
      String sqlNullOption = options.get(OptionsSection.SQL_COMPATIBLE_NULLS);
      if (sqlNullOption == null ||
          OptionsSection.NULL_HANDLING_BOTH.equals(sqlNullOption) ||
          QueryContexts.getAsBoolean(
              OptionsSection.SQL_COMPATIBLE_NULLS,
              sqlNullOption,
              false) == sqlCompatible) {
        child.run(queryDefn, options);
      }
    }
  }

  /**
   * Iterates over the contexts provided by QueryContextForJoinProvider,
   * which is a provider class used in JUnit, but adapted for use here.
   * The class provides not just the join options, but also a set of
   * "default" options which are the same as the defaults used in the
   * JUnit tests, so no harm in applying them.
   */
  private static class JoinContextProvider implements QueryExec
  {
    private final QueryExec child;

    public JoinContextProvider(QueryExec child)
    {
      this.child = child;
    }

    @Override
    public void run(QueryDefn queryDefn, Map<String, String> options)
    {
      for (Object obj : QueryContextForJoinProvider.provideQueryContexts()) {
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) obj;
        QueryDefn rewritten = queryDefn.withOverrides(context);
        child.run(rewritten, options);
      }
    }
  }

  /**
   * Special version of {@link JoinContextProvider} that filters on
   * {@code enableJoinFilterRewrite} to handle bugs in
   * {@code testLeftJoinSubqueryWithNullKeyFilter}.
   */
  private static class JoinContextProviderFilterRewriteFilter implements QueryExec
  {
    private final QueryExec child;
    private final boolean value;

    public JoinContextProviderFilterRewriteFilter(QueryExec child, boolean value)
    {
      this.child = child;
      this.value = value;
    }

    @Override
    public void run(QueryDefn queryDefn, Map<String, String> options)
    {
      for (Object obj : QueryContextForJoinProvider.provideQueryContexts()) {
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) obj;
        // Per testLeftJoinSubqueryWithNullKeyFilter(), the default value is true.
        if (QueryTestCases.booleanOption(context, QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, true) == value) {
          QueryDefn rewritten = queryDefn.withOverrides(context);
          child.run(rewritten, options);
        }
      }
    }
  }


  /**
   * Special version of {@link JoinContextProvider} that filters on
   * {@code enableJoinFilterRewrite} to handle bugs in
   * {@code testLeftJoinSubqueryWithNullKeyFilter}.
   */
  private static class JoinContextProviderJoinToFilterRewriteFilter implements QueryExec
  {
    private final QueryExec child;
    private final boolean value;

    public JoinContextProviderJoinToFilterRewriteFilter(QueryExec child, boolean value)
    {
      this.child = child;
      this.value = value;
    }

    @Override
    public void run(QueryDefn queryDefn, Map<String, String> options)
    {
      for (Object obj : QueryContextForJoinProvider.provideQueryContexts()) {
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) obj;
        // Per testLeftJoinSubqueryWithNullKeyFilter(), the default value is true.
        if (BaseCalciteQueryTest.isRewriteJoinToFilter(context) == value) {
          QueryDefn rewritten = queryDefn.withOverrides(context);
          child.run(rewritten, options);
        }
      }
    }
  }

  private void runQuery(QueryRun run)
  {
    QueryDefn queryDefn = QueryDefn
        .builder(run.testCase().sql())
        // Run with the same defaults as used in the original JUnit-based
        // tests to ensure results are consistent.
        .context(plannerFixture.applyDefaultContext(run.context()))
        .parameters(run.testCase().parameters())
        .authResult(plannerFixture.authResultFor(run.testCase().user()))
        .build();
    QueryExec exec = new VectorizeExec(
        new ConcreteExec(run));
    // Hard-coded support for the known providers.
    String provider = run.option(OptionsSection.PROVIDER_CLASS);
    if (provider != null) {
      switch (provider) {
        case "QueryContextForJoinProvider":
          exec = new JoinContextProvider(exec);
          break;
        case "QueryContextForJoinProviderNoFilterRewrite":
          exec = new JoinContextProviderFilterRewriteFilter(exec, false);
          break;
        case "QueryContextForJoinProviderWithFilterRewrite":
          exec = new JoinContextProviderFilterRewriteFilter(exec, true);
          break;
        case "QueryContextForJoinProviderNoRewriteJoinToFilter":
          exec = new JoinContextProviderJoinToFilterRewriteFilter(exec, false);
          break;
        case "QueryContextForJoinProviderWithRewriteJoinToFilter":
          exec = new JoinContextProviderJoinToFilterRewriteFilter(exec, true);
          break;
        default:
          log.warn("Undefined provider: %s", provider);
      }
    }
    exec = new NullStrategyFilter(exec);
    exec.run(queryDefn, run.options());
  }
}
