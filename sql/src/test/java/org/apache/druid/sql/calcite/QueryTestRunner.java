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
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.ISE;
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
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.junit.Assert;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Runs a test build up by {@link QueryTestBuilder}. Running a SQL query test
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
  /**
   * Test step that executes or prepares a query.
   */
  public abstract static class QueryRunStep
  {
    private final QueryTestBuilder builder;

    public QueryRunStep(final QueryTestBuilder builder)
    {
      this.builder = builder;
    }

    public QueryTestBuilder builder()
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
    public final RowSignature signature;
    public final List<Object[]> results;
    public final List<Query<?>> recordedQueries;
    public final Set<ResourceAction> resourceActions;
    public final RuntimeException exception;

    public QueryResults(
        final Map<String, Object> queryContext,
        final String vectorizeOption,
        final RowSignature signature,
        final List<Object[]> results,
        final List<Query<?>> recordedQueries
    )
    {
      this.queryContext = queryContext;
      this.vectorizeOption = vectorizeOption;
      this.signature = signature;
      this.results = results;
      this.recordedQueries = recordedQueries;
      this.resourceActions = null;
      this.exception = null;
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
    }

    public QueryResults(
        final Map<String, Object> queryContext,
        final String vectorizeOption,
        final Set<ResourceAction> resourceActions
    )
    {
      this.queryContext = queryContext;
      this.vectorizeOption = vectorizeOption;
      this.signature = null;
      this.results = null;
      this.recordedQueries = null;
      this.resourceActions = resourceActions;
      this.exception = null;
    }
  }

  /**
   * Prepare a query and save the resources. Runs the query only once
   * since the vectorization, etc. options do not change the set of
   * resources.
   */
  public static class PrepareQuery extends QueryRunStep
  {
    private Set<ResourceAction> resourceActions;

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
      final SqlStatementFactory sqlStatementFactory = builder.config.statementFactory(
          builder.plannerConfig,
          builder.authConfig
      );
      PreparedStatement stmt = sqlStatementFactory.preparedStatement(sqlQuery);
      stmt.prepare();
      resourceActions = stmt.allResources();
    }
  }

  /**
   * Runs a query up to three times with different vectorization options.
   * Captures the results, signature and native queries for each run.
   */
  public static class ExecuteQuery extends QueryRunStep
  {
    private final List<QueryResults> results = new ArrayList<>();

    public ExecuteQuery(QueryTestBuilder builder)
    {
      super(builder);
    }

    public List<QueryResults> results()
    {
      return results;
    }

    @Override
    public void run()
    {
      final QueryTestBuilder builder = builder();

      BaseCalciteQueryTest.log.info("SQL: %s", builder.sql);

      final SqlStatementFactory sqlStatementFactory = builder.config.statementFactory(
          builder.plannerConfig,
          builder.authConfig
      );
      final SqlQueryPlus sqlQuery = SqlQueryPlus.builder(builder.sql)
          .sqlParameters(builder.parameters)
          .auth(builder.authenticationResult)
          .build();

      final List<String> vectorizeValues = new ArrayList<>();
      vectorizeValues.add("false");
      if (!builder.skipVectorize) {
        vectorizeValues.add("force");
      }

      QueryLogHook queryLogHook = builder.config.queryLogHook();
      for (final String vectorize : vectorizeValues) {
        queryLogHook.clearRecordedQueries();

        final Map<String, Object> theQueryContext = new HashMap<>(builder.queryContext);
        theQueryContext.put(QueryContexts.VECTORIZE_KEY, vectorize);
        theQueryContext.put(QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize);

        if (!"false".equals(vectorize)) {
          theQueryContext.put(QueryContexts.VECTOR_SIZE_KEY, 2); // Small vector size to ensure we use more than one.
        }

        try {
          final Pair<RowSignature, List<Object[]>> plannerResults = getResults(
              sqlStatementFactory,
              sqlQuery.withContext(theQueryContext));
          results.add(new QueryResults(
              theQueryContext,
              vectorize,
              plannerResults.lhs,
              plannerResults.rhs,
              queryLogHook.getRecordedQueries()
          ));
        }
        catch (RuntimeException e) {
          results.add(new QueryResults(
              theQueryContext,
              vectorize,
              e
          ));
        }
      }
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
   * Base class for steps which validate query execution results.
   */
  public abstract static class VerifyExecStep implements QueryVerifyStep
  {
    protected final ExecuteQuery execStep;

    public VerifyExecStep(ExecuteQuery execStep)
    {
      this.execStep = execStep;
    }
  }

  /**
   * Verify query results.
   */
  public static class VerifyResults extends VerifyExecStep
  {
    public VerifyResults(ExecuteQuery execStep)
    {
      super(execStep);
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
      builder.expectedResultsVerifier.verifyRowSignature(queryResults.signature);
      builder.expectedResultsVerifier.verify(builder.sql, results);
    }
  }

  /**
   * Verify the native queries generated by an execution run against a set
   * provided in the builder.
   */
  public static class VerifyNativeQueries extends VerifyExecStep
  {
    public VerifyNativeQueries(ExecuteQuery execStep)
    {
      super(execStep);
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
      for (Query<?> query : builder.expectedQueries) {
        expectedQueries.add(BaseCalciteQueryTest.recursivelyOverrideContext(query, queryResults.queryContext));
      }

      final List<Query<?>> recordedQueries = queryResults.recordedQueries;
      Assert.assertEquals(
          StringUtils.format("query count: %s", builder.sql),
          expectedQueries.size(),
          recordedQueries.size()
      );
      ObjectMapper queryJsonMapper = builder.config.jsonMapper();
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
   * Verify rsources for a prepared query against the expected list.
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
   * Verify the exception thrown by a query using a jUnit expected
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
  public static class VerifyExpectedException extends VerifyExecStep
  {
    public VerifyExpectedException(ExecuteQuery execStep)
    {
      super(execStep);
    }

    @Override
    public void verify()
    {
      QueryTestBuilder builder = execStep.builder();

      // The builder specifies one exception, but the query can run multiple
      // times. Pick the first failure as that emulates the original code flow
      // where the first exception ended the test.
      ExpectedException expectedException = builder.config.expectedException();
      for (QueryResults queryResults : execStep.results()) {
        if (queryResults.exception == null) {
          continue;
        }

        // This variation uses JUnit exception validation: we configure the expected
        // exception, then throw the exception from the run.
        // If the expected exception is not configured here, then the test may
        // have done it outside of the test builder.
        if (builder.queryCannotVectorize && "force".equals(queryResults.vectorizeOption)) {
          expectedException.expect(RuntimeException.class);
          expectedException.expectMessage("Cannot vectorize");
        } else if (builder.expectedExceptionInitializer != null) {
          builder.expectedExceptionInitializer.accept(expectedException);
        }
        throw queryResults.exception;
      }
      if (builder.expectedExceptionInitializer != null) {
        throw new ISE("Expected query to throw an exception, but none was thrown.");
      }
    }
  }

  private final List<QueryTestRunner.QueryRunStep> runSteps;
  private final List<QueryTestRunner.QueryVerifyStep> verifySteps;

  QueryTestRunner(
      final List<QueryTestRunner.QueryRunStep> runSteps,
      final List<QueryTestRunner.QueryVerifyStep> verifySteps
  )
  {
    this.runSteps = runSteps;
    this.verifySteps = verifySteps;
  }

  /**
   * All testQuery roads lead to this method.
   */
  public void run()
  {
    for (QueryTestRunner.QueryRunStep runStep : runSteps) {
      runStep.run();
    }
    for (QueryTestRunner.QueryVerifyStep verifyStep : verifySteps) {
      verifyStep.verify();
    }
  }
}
