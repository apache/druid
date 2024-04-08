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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlInsert;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.PreparedStatement;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.QueryTestBuilder.QueryTestConfig;
import org.apache.druid.sql.calcite.parser.DruidSqlIngest;
import org.apache.druid.sql.calcite.planner.PlannerCaptureHook;
import org.apache.druid.sql.calcite.planner.PrepareResult;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Runs a test built up by {@link QueryTestBuilder}. Running a SQL query test
 * is somewhat complex; with different modes and items to verify. To manage the
 * complexity, test execution is done in two steps:
 * <ol>
 * <li>Execute the query and capture results.</li>
 * <li>Verify those results as requested in the builder.</li>
 * </ol>
 * Verification is sometimes tied to some context or other. The steps can be
 * defined specific to that context when needed.
 */
public class QueryTestRunner
{
  public interface QueryVerifyStepFactory
  {
    QueryVerifyStep make(BaseExecuteQuery execStep);
  }

  public interface QueryRunStepFactory
  {
    QueryRunStep make(QueryTestBuilder builder, QueryTestRunner.BaseExecuteQuery execStep);
  }

  /**
   * Test step that executes or prepares a query.
   */
  public abstract static class QueryRunStep
  {
    protected final QueryTestBuilder builder;

    public QueryRunStep(final QueryTestBuilder builder)
    {
      this.builder = builder;
    }

    public final QueryTestBuilder builder()
    {
      return builder;
    }

    public abstract void run();
  }

  /**
   * Test step that verifies some aspect of a query.
   */
  public interface QueryVerifyStep
  {
    void verify();
  }

  /**
   * Results from running a single query.
   */
  public static class QueryResults
  {
    public final Map<String, Object> queryContext;
    public final String vectorizeOption;
    public final RelDataType sqlSignature;
    public final RowSignature signature;
    public final List<Object[]> results;
    public final List<Query<?>> recordedQueries;
    public final Set<ResourceAction> resourceActions;
    public final RuntimeException exception;
    public final PlannerCaptureHook capture;

    public QueryResults(
        final Map<String, Object> queryContext,
        final String vectorizeOption,
        final RelDataType sqlSignature,
        final List<Object[]> results,
        final List<Query<?>> recordedQueries,
        final PlannerCaptureHook capture
    )
    {
      this.queryContext = queryContext;
      this.vectorizeOption = vectorizeOption;
      this.sqlSignature = sqlSignature;
      this.signature = RowSignatures.fromRelDataType(sqlSignature.getFieldNames(), sqlSignature);
      this.results = results;
      this.recordedQueries = recordedQueries;
      this.resourceActions = null;
      this.exception = null;
      this.capture = capture;
    }

    public QueryResults(
        final Map<String, Object> queryContext,
        final String vectorizeOption,
        final RuntimeException exception
    )
    {
      this.queryContext = queryContext;
      this.vectorizeOption = vectorizeOption;
      this.signature = null;
      this.results = null;
      this.recordedQueries = null;
      this.resourceActions = null;
      this.exception = exception;
      this.capture = null;
      this.sqlSignature = null;
    }

    public QueryResults withResults(List<Object[]> newResults)
    {
      return new QueryResults(queryContext, vectorizeOption, sqlSignature, newResults, recordedQueries, capture);
    }
  }

  /**
   * Prepare a query and save the resources. Runs the query only once
   * since the vectorization, etc. options do not change the set of
   * resources.
   */
  public static class PrepareQuery extends QueryRunStep
  {
    public Set<ResourceAction> resourceActions;
    public RelDataType sqlSignature;

    public PrepareQuery(QueryTestBuilder builder)
    {
      super(builder);
    }

    public Set<ResourceAction> resourceActions()
    {
      return resourceActions;
    }

    @Override
    public void run()
    {
      final QueryTestBuilder builder = builder();
      final SqlQueryPlus sqlQuery = SqlQueryPlus.builder(builder.sql)
                                                .context(builder.queryContext)
                                                .sqlParameters(builder.parameters)
                                                .auth(builder.authenticationResult)
                                                .build();
      final SqlStatementFactory sqlStatementFactory = builder.statementFactory();
      final PreparedStatement stmt = sqlStatementFactory.preparedStatement(sqlQuery);
      final PrepareResult prepareResult = stmt.prepare();
      resourceActions = stmt.allResources();
      sqlSignature = prepareResult.getReturnedRowType();
    }
  }

  public abstract static class BaseExecuteQuery extends QueryRunStep
  {
    protected final List<QueryResults> results = new ArrayList<>();

    public BaseExecuteQuery(QueryTestBuilder builder)
    {
      super(builder);
    }

    public List<QueryResults> results()
    {
      return results;
    }
  }

  /**
   * Runs a query up to three times with different vectorization options.
   * Captures the results, signature and native queries for each run.
   */
  public static class ExecuteQuery extends BaseExecuteQuery
  {
    public ExecuteQuery(QueryTestBuilder builder)
    {
      super(builder);
    }

    @Override
    public void run()
    {
      final QueryTestBuilder builder = builder();

      BaseCalciteQueryTest.log.info("SQL: %s", builder.sql);

      final SqlStatementFactory sqlStatementFactory = builder.statementFactory();
      final SqlQueryPlus sqlQuery = SqlQueryPlus.builder(builder.sql)
                                                .sqlParameters(builder.parameters)
                                                .auth(builder.authenticationResult)
                                                .build();

      final List<String> vectorizeValues = new ArrayList<>();
      vectorizeValues.add("false");
      if (!builder.skipVectorize) {
        vectorizeValues.add("force");
      }

      for (final String vectorize : vectorizeValues) {
        final Map<String, Object> theQueryContext = new HashMap<>(builder.queryContext);
        theQueryContext.put(QueryContexts.VECTORIZE_KEY, vectorize);
        theQueryContext.put(QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize);

        if (!"false".equals(vectorize)) {
          theQueryContext.put(QueryContexts.VECTOR_SIZE_KEY, 2); // Small vector size to ensure we use more than one.
        }

        results.add(runQuery(
            sqlStatementFactory,
            sqlQuery.withContext(theQueryContext),
            vectorize
        ));
      }
    }

    public QueryResults runQuery(
        final SqlStatementFactory sqlStatementFactory,
        final SqlQueryPlus query,
        final String vectorize
    )
    {
      try {
        final PlannerCaptureHook capture = getCaptureHook();
        final DirectStatement stmt = sqlStatementFactory.directStatement(query);
        stmt.setHook(capture);
        AtomicReference<List<Object[]>> resultListRef = new AtomicReference<>();
        QueryLogHook queryLogHook = new QueryLogHook(builder().config.jsonMapper());
        queryLogHook.logQueriesFor(
            () -> {
              resultListRef.set(stmt.execute().getResults().toList());
            }
        );
        return new QueryResults(
            query.context(),
            vectorize,
            stmt.prepareResult().getReturnedRowType(),
            resultListRef.get(),
            queryLogHook.getRecordedQueries(),
            capture
        );
      }
      catch (RuntimeException e) {
        return new QueryResults(
            query.context(),
            vectorize,
            e
        );
      }
    }

    private PlannerCaptureHook getCaptureHook()
    {
      if (builder.getQueryContext().containsKey(PlannerCaptureHook.NEED_CAPTURE_HOOK) || builder.expectedLogicalPlan != null) {
        return new PlannerCaptureHook();
      }
      return null;
    }

    public static Pair<RowSignature, List<Object[]>> getResults(
        final SqlStatementFactory sqlStatementFactory,
        final SqlQueryPlus query
    )
    {
      final DirectStatement stmt = sqlStatementFactory.directStatement(query);
      Sequence<Object[]> results = stmt.execute().getResults();
      RelDataType rowType = stmt.prepareResult().getReturnedRowType();
      return new Pair<>(
          RowSignatures.fromRelDataType(rowType.getFieldNames(), rowType),
          results.toList()
      );
    }
  }

  /**
   * Verify query results.
   */
  public static class VerifyResults implements QueryVerifyStep
  {
    protected final BaseExecuteQuery execStep;
    protected final boolean verifyRowSignature;

    public VerifyResults(
        BaseExecuteQuery execStep,
        boolean verifyRowSignature
    )
    {
      this.execStep = execStep;
      this.verifyRowSignature = verifyRowSignature;
    }

    @Override
    public void verify()
    {
      for (QueryResults queryResults : execStep.results()) {
        verifyResults(queryResults);
      }
    }

    private void verifyResults(QueryResults queryResults)
    {
      if (queryResults.exception != null) {
        return;
      }
      List<Object[]> results = queryResults.results;
      for (int i = 0; i < results.size(); i++) {
        BaseCalciteQueryTest.log.info("row #%d: %s", i, Arrays.toString(results.get(i)));
      }

      QueryTestBuilder builder = execStep.builder();
      if (verifyRowSignature) {
        builder.expectedResultsVerifier.verifyRowSignature(queryResults.signature);
      }
      builder.expectedResultsVerifier.verify(builder.sql, queryResults);
    }
  }

  /**
   * Verify the native queries generated by an execution run against a set
   * provided in the builder.
   */
  public static class VerifyNativeQueries implements QueryVerifyStep
  {
    protected final BaseExecuteQuery execStep;

    public VerifyNativeQueries(BaseExecuteQuery execStep)
    {
      this.execStep = execStep;
    }

    @Override
    public void verify()
    {
      for (QueryResults queryResults : execStep.results()) {
        verifyQuery(queryResults);
      }
    }

    private void verifyQuery(QueryResults queryResults)
    {
      if (queryResults.exception != null) {
        return;
      }
      QueryTestBuilder builder = execStep.builder();
      final List<Query<?>> expectedQueries = new ArrayList<>();
      ObjectMapper queryJsonMapper = builder.config.jsonMapper();
      for (Query<?> query : builder.expectedQueries) {
        // The tests set a lot of various values in the context that are not relevant to how the query actually planned,
        // so we effectively ignore these keys in the context during query validation by overwriting whatever
        // context had been set in the test with the context produced by the test setup code.  This means that any
        // context parameter that the tests choose to set will never actually be tested (it will always be overridden)
        // while parameters that don't get set by the test can be tested.
        //
        // This is pretty magical, it would probably be a good thing to move away from this hard-to-predict setting
        // of context parameters towards a test setup that is much more explicit and easier to understand.  Perhaps
        // we could have validations of query objects that are a bit more intelligent.  That is, instead of relying on
        // equals, perhaps we could have a context validator that only validates that keys set on the expected query
        // are set, allowing any other context keys to also be set?
        expectedQueries.add(BaseCalciteQueryTest.recursivelyClearContext(query, queryJsonMapper));
      }

      final List<Query<?>> recordedQueries = queryResults.recordedQueries
          .stream()
          .map(q -> BaseCalciteQueryTest.recursivelyClearContext(q, queryJsonMapper))
          .collect(Collectors.toList());

      Assert.assertEquals(
          StringUtils.format("query count: %s", builder.sql),
          expectedQueries.size(),
          recordedQueries.size()
      );
      for (int i = 0; i < expectedQueries.size(); i++) {
        Assert.assertEquals(
            StringUtils.format("query #%d: %s", i + 1, builder.sql),
            expectedQueries.get(i),
            recordedQueries.get(i)
        );

        try {
          // go through some JSON serde and back, round tripping both queries and comparing them to each other, because
          // Assert.assertEquals(recordedQueries.get(i), stringAndBack) is a failure due to a sorted map being present
          // in the recorded queries, but it is a regular map after deserialization
          final String recordedString = queryJsonMapper.writeValueAsString(recordedQueries.get(i));
          final Query<?> stringAndBack = queryJsonMapper.readValue(recordedString, Query.class);
          final String expectedString = queryJsonMapper.writeValueAsString(expectedQueries.get(i));
          final Query<?> expectedStringAndBack = queryJsonMapper.readValue(expectedString, Query.class);
          Assert.assertEquals(expectedStringAndBack, stringAndBack);
        }
        catch (JsonProcessingException e) {
          Assert.fail(e.getMessage());
        }
      }
    }
  }

  /**
   * Verify resources for a prepared query against the expected list.
   */
  public static class VerifyResources implements QueryVerifyStep
  {
    private final PrepareQuery prepareStep;

    public VerifyResources(PrepareQuery prepareStep)
    {
      this.prepareStep = prepareStep;
    }

    @Override
    public void verify()
    {
      QueryTestBuilder builder = prepareStep.builder();
      Assert.assertEquals(
          ImmutableSet.copyOf(builder.expectedResources),
          prepareStep.resourceActions()
      );
    }
  }

  /**
   * Verify resources for a prepared query against the expected list.
   */
  public static class VerifyPrepareSignature implements QueryVerifyStep
  {
    private final PrepareQuery prepareStep;

    public VerifyPrepareSignature(PrepareQuery prepareStep)
    {
      this.prepareStep = prepareStep;
    }

    @Override
    public void verify()
    {
      QueryTestBuilder builder = prepareStep.builder();
      Assert.assertEquals(
          builder.expectedSqlSchema,
          SqlSchema.of(prepareStep.sqlSignature)
      );
    }
  }


  /**
   * Verify resources for a prepared query against the expected list.
   */
  public static class VerifyExecuteSignature implements QueryVerifyStep
  {
    private final BaseExecuteQuery execStep;

    public VerifyExecuteSignature(BaseExecuteQuery execStep)
    {
      this.execStep = execStep;
    }

    @Override
    public void verify()
    {
      QueryTestBuilder builder = execStep.builder();
      for (QueryResults queryResults : execStep.results()) {
        Assert.assertEquals(
            builder.expectedSqlSchema,
            SqlSchema.of(queryResults.sqlSignature)
        );
      }
    }
  }

  public static class VerifyLogicalPlan implements QueryVerifyStep
  {
    private final BaseExecuteQuery execStep;

    public VerifyLogicalPlan(BaseExecuteQuery execStep)
    {
      this.execStep = execStep;
    }

    @Override
    public void verify()
    {
      for (QueryResults queryResults : execStep.results()) {
        verifyLogicalPlan(queryResults);
      }
    }

    private void verifyLogicalPlan(QueryResults queryResults)
    {
      String expectedPlan = execStep.builder().expectedLogicalPlan;
      String actualPlan = visualizePlan(queryResults.capture);
      Assert.assertEquals(expectedPlan, actualPlan);
    }

    private String visualizePlan(PlannerCaptureHook hook)
    {
      // Do-it-ourselves plan since the actual plan omits insert.
      String queryPlan = RelOptUtil.dumpPlan(
          "",
          hook.relRoot().rel,
          SqlExplainFormat.TEXT,
          SqlExplainLevel.DIGEST_ATTRIBUTES);
      String plan;
      SqlInsert insertNode = hook.insertNode();
      if (insertNode == null) {
        plan = queryPlan;
      } else {
        DruidSqlIngest druidInsertNode = (DruidSqlIngest) insertNode;
        // The target is a SQLIdentifier literal, pre-resolution, so does
        // not include the schema.
        plan = StringUtils.format(
            "LogicalInsert(target=[%s], partitionedBy=[%s], clusteredBy=[%s])\n",
            druidInsertNode.getTargetTable(),
            druidInsertNode.getPartitionedBy() == null ? "<none>" : druidInsertNode.getPartitionedBy(),
            druidInsertNode.getClusteredBy() == null ? "<none>" : druidInsertNode.getClusteredBy()
        ) + "  " + StringUtils.replace(queryPlan, "\n ", "\n   ");
      }
      return plan;
    }
  }

  /**
   * Verify the exception thrown by a query using a JUnit expected
   * exception. This is actually an awkward way to to the job, but it is
   * what the Calcite queries have long used. There are three modes.
   * In the first, the exception is simply thrown and the expected
   * exception setup in the test method does the checks. In the second
   * mode, the builder says how to initialize the expected exception in
   * the test. In the third, for one vectorization option, this code
   * sets up the expected exception.
   * <p>
   * A query can be run up to three times. All three variations will
   * run. But, because this code throws an exception, we stop checking
   * after the first failure. It would be better to check all three
   * runs, but that's an exercise for later.
   */

  public static class VerifyExpectedException implements QueryVerifyStep
  {
    protected final BaseExecuteQuery execStep;

    public VerifyExpectedException(BaseExecuteQuery execStep)
    {
      this.execStep = execStep;
    }

    @Override
    public void verify()
    {
      QueryTestBuilder builder = execStep.builder();

      // The builder specifies one exception, but the query can run multiple
      // times. Pick the first failure as that emulates the original code flow
      // where the first exception ended the test.
      for (QueryResults queryResults : execStep.results()) {
        if (queryResults.exception == null) {
          continue;
        }

        // Delayed exception checking to let other verify steps run before running vectorized checks
        if (builder.queryCannotVectorize && "force".equals(queryResults.vectorizeOption)) {
          MatcherAssert.assertThat(
              queryResults.exception,
              CoreMatchers.allOf(
                  CoreMatchers.instanceOf(RuntimeException.class),
                  ThrowableMessageMatcher.hasMessage(
                      CoreMatchers.containsString("Cannot vectorize!")
                  )
              )
          );
        } else {
          throw queryResults.exception;
        }
      }
    }
  }

  private final List<QueryTestRunner.QueryRunStep> runSteps = new ArrayList<>();
  private final List<QueryTestRunner.QueryVerifyStep> verifySteps = new ArrayList<>();

  /**
   * Create a test runner based on the options set in the builder.
   */
  public QueryTestRunner(QueryTestBuilder builder)
  {
    QueryTestConfig config = builder.config;
    if (builder.expectedResultsVerifier == null && builder.expectedResults != null) {
      builder.expectedResultsVerifier = config.defaultResultsVerifier(
          builder.expectedResults,
          builder.expectedResultMatchMode,
          builder.expectedResultSignature
      );
    }

    // Historically, a test either prepares the query (to check resources), or
    // runs the query (to check the native query and results.) In the future we
    // may want to do both in a single test; but we have no such tests today.
    if (builder.expectedResources != null) {
      Preconditions.checkArgument(
          builder.expectedResultsVerifier == null,
          "Cannot check both results and resources"
      );
      PrepareQuery execStep = new PrepareQuery(builder);
      runSteps.add(execStep);
      verifySteps.add(new VerifyResources(execStep));
      if (builder.expectedSqlSchema != null) {
        verifySteps.add(new VerifyPrepareSignature(execStep));
      }
    } else {
      BaseExecuteQuery execStep = new ExecuteQuery(builder);
      runSteps.add(execStep);
      if (!builder.customRunners.isEmpty()) {
        for (QueryRunStepFactory factory : builder.customRunners) {
          runSteps.add(factory.make(builder, (BaseExecuteQuery) runSteps.get(runSteps.size() - 1)));
        }
      }
      BaseExecuteQuery finalExecStep = (BaseExecuteQuery) runSteps.get(runSteps.size() - 1);

      // Verify the logical plan, if requested.
      if (builder.expectedLogicalPlan != null) {
        verifySteps.add(new VerifyLogicalPlan(finalExecStep));
      }

      if (builder.expectedSqlSchema != null) {
        verifySteps.add(new VerifyExecuteSignature(finalExecStep));
      }

      // Verify native queries before results. (Note: change from prior pattern
      // that reversed the steps.
      if (builder.expectedQueries != null && builder.verifyNativeQueries.test(builder.expectedQueries)) {
        verifySteps.add(new VerifyNativeQueries(finalExecStep));
      }
      if (builder.expectedResultsVerifier != null) {
        // Don't verify the row signature when MSQ is running, since the broker receives the task id, and the signature
        // would be {TASK:STRING} instead of the expected results signature
        verifySteps.add(new VerifyResults(finalExecStep, !config.isRunningMSQ()));
      }

      if (!builder.customVerifications.isEmpty()) {
        for (QueryVerifyStepFactory customVerification : builder.customVerifications) {
          verifySteps.add(customVerification.make(finalExecStep));
        }
      }

      // The exception is always verified: either there should be no exception
      // (the other steps ran), or there should be the defined exception.
      verifySteps.add(new VerifyExpectedException(finalExecStep));
    }
  }

  /**
   * All testQuery roads lead to this method.
   */
  public void run()
  {
    for (QueryRunStep runStep : runSteps) {
      runStep.run();
    }
    for (QueryVerifyStep verifyStep : verifySteps) {
      verifyStep.verify();
    }
  }

  public QueryResults resultsOnly()
  {
    ExecuteQuery execStep = (ExecuteQuery) runSteps.get(0);
    execStep.run();
    return execStep.results().get(0);
  }
}
