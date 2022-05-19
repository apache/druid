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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import junitparams.Parameters;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.Query;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest.DefaultResultsVerifier;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest.ResultsVerifier;
import org.apache.druid.sql.calcite.QueryDefn;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Captures a single Calcite<foo>TestCase test in a form that
 * can be converted to a Planner test case.
 */
public class CalciteTestCapture
{
  private final PlannerConfig plannerConfig;
  private final QueryDefn queryDefn;
  private final List<Query<?>> expectedQueries;
  private final ResultsVerifier expectedResultsVerifier;
  @Nullable private final Consumer<ExpectedException> expectedExceptionInitializer;
  private PlannerResult plannerResult;
  private List<Object[]> results;
  protected String methodName;
  private Exception exception;
  private boolean cannotVectorize;
  private int mergeBufCount;
  private final boolean sqlCompatibleNulls;
  private final boolean allowNestedArrays;
  protected final String provider;
  private String user;

  /**
   * Grab information available in {code testQuery()}.
   */
  public CalciteTestCapture(
      final PlannerConfig plannerConfig,
      final QueryDefn queryDefn,
      final List<Query<?>> expectedQueries,
      final ResultsVerifier expectedResultsVerifier,
      @Nullable final Consumer<ExpectedException> expectedExceptionInitializer
  )
  {
    this.plannerConfig = plannerConfig;
    this.queryDefn = queryDefn;
    this.expectedQueries = expectedQueries;
    this.expectedResultsVerifier = expectedResultsVerifier;
    this.expectedExceptionInitializer = expectedExceptionInitializer;
    Pair<String, String> testMethod = captureMethod();
    this.methodName = testMethod.rhs;
    if (testMethod.lhs == null) {
      this.provider = null;
    } else {
      this.provider = getProvider(testMethod.lhs, testMethod.rhs);
    }
    if (queryDefn.authResult() == CalciteTests.SUPER_USER_AUTH_RESULT) {
      user(CalciteTests.TEST_SUPERUSER_NAME);
    }
    this.sqlCompatibleNulls = NullHandling.sqlCompatible();
    this.allowNestedArrays = ExpressionProcessing.allowNestedArrays();
  }

  /**
   * Find the last Druid method before we hit the first JUnit method.
   * That is usually the test name. The stack trace is listed lowest
   * method first.
   */
  private static Pair<String, String> captureMethod()
  {
    String testClass = null;
    String methodName = "unknown";
    StackTraceElement[] trace = Thread.currentThread().getStackTrace();
    for (StackTraceElement element : trace) {
      String className = element.getClassName();
      if (className.startsWith("org.apache.druid")) {
        testClass = className;
        methodName = element.getMethodName();
      } else if (className.startsWith("org.junit")) {
        break;
      }
    }
    return Pair.of(testClass, methodName);
  }

  private String getProvider(String className, String methodName)
  {
    Class<?> testClass;
    try {
      testClass = getClass().getClassLoader().loadClass(className);
    }
    catch (ClassNotFoundException e) {
      return null;
    }
    if (testClass == null) {
      return null;
    }
    for (Method method : testClass.getDeclaredMethods()) {
      if (method.getName().equals(methodName)) {
        Parameters params = method.getAnnotation(Parameters.class);
        if (params == null) {
          return null;
        }
        return params.source().getSimpleName();
      }
    }
    return null;
  }

  /**
   * Capture the results from the planner.
   */
  public void plannerResult(PlannerResult plannerResult)
  {
    this.plannerResult = plannerResult;
  }

  /**
   * Capture results as a list of objects.
   */
  public void results(List<Object[]> runResults)
  {
    this.results = runResults;
  }

  /**
   * Capture the options specified via various methods and held in
   * variables.
   */
  public void options(
      boolean cannotVectorize,
      int mergeBufCount
  )
  {
    this.cannotVectorize = cannotVectorize;
    this.mergeBufCount = mergeBufCount;
  }

  /**
   * Write the gathered information in test case format.
   */
  protected void write(
      boolean includeRun,
      TestCaseWriter writer,
      ObjectMapper jsonMapper
  ) throws IOException
  {
    writeCase(includeRun, writer);
    if (exception != null) {
      writeException(writer);
    } else {
      writeSchema(writer);
      writer.emitPlan("unavailable\n");
      writeNative(writer, jsonMapper);
      if (includeRun) {
        writeResults(writer, jsonMapper);
      }
    }
  }

  private void writeException(TestCaseWriter writer) throws IOException
  {
    writer.emitException(exception);
    writer.emitError(exception);
  }

  private void writeCase(boolean includeRun, TestCaseWriter writer) throws IOException
  {
    String comment = "Converted from " + methodName + "()";
    writer.emitComment(Collections.singletonList(comment));
    String label = decodeMethod(methodName);
    writer.emitCase(label);

    writer.emitSql(queryDefn.sql());
    writer.emitContext(QueryTestCases.rewriteContext(queryDefn.context()));
    if (!queryDefn.parameters().isEmpty()) {
      writer.emitParameters(queryDefn.parameters());
    }
    Map<String, Object> options = new HashMap<>();
    if (allowNestedArrays) {
      options.put(OptionsSection.ALLOW_NESTED_ARRAYS, allowNestedArrays);
    }
    if (provider != null) {
      options.put(OptionsSection.PROVIDER_CLASS, provider);
    }
    if (user != null) {
      options.put(OptionsSection.USER_OPTION, user);
    }
    options.put(OptionsSection.VECTORIZE_OPTION, !cannotVectorize);
    savePlannerConfig(options);
    if (includeRun) {
      if (mergeBufCount != 0) {
        options.put(OptionsSection.MERGE_BUFFER_COUNT, mergeBufCount);
      }
    }
    writer.emitOptions(options);
  }

  /**
   * Convert the method name into an English-like test label.
   */
  private String decodeMethod(String methodName)
  {
    if (methodName.startsWith("test")) {
      methodName = methodName.substring(4);
    }
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < methodName.length(); i++) {
      char c = methodName.charAt(i);
      if (i == 0) {
        buf.append(Character.toUpperCase(c));
      } else if (Character.isUpperCase(c)) {
        buf.append(' ').append(Character.toLowerCase(c));
      } else {
        buf.append(c);
      }
    }
    return buf.toString();
  }

  /**
   * Convert selected planner config options into test case options
   * that will then be used to recreate the planner config.
   */
  private void savePlannerConfig(Map<String, Object> options)
  {
    PlannerConfig base = BaseCalciteQueryTest.PLANNER_CONFIG_DEFAULT;
    if (base.getMaxTopNLimit() != plannerConfig.getMaxTopNLimit()) {
      options.put(
          OptionsSection.PLANNER_MAX_TOP_N,
          plannerConfig.getMaxTopNLimit());
    }
    if (base.isUseApproximateCountDistinct() != plannerConfig.isUseApproximateCountDistinct()) {
      options.put(
          OptionsSection.PLANNER_APPROX_COUNT_DISTINCT,
          plannerConfig.isUseApproximateCountDistinct());
    }
    if (base.isUseApproximateTopN() != plannerConfig.isUseApproximateTopN()) {
      options.put(
          OptionsSection.PLANNER_APPROX_TOP_N,
          plannerConfig.isUseApproximateTopN());
    }
    if (base.isRequireTimeCondition() != plannerConfig.isRequireTimeCondition()) {
      options.put(
          OptionsSection.PLANNER_REQUIRE_TIME_CONDITION,
          plannerConfig.isRequireTimeCondition());
    }
    if (base.getSqlTimeZone() != plannerConfig.getSqlTimeZone()) {
      options.put(
          OptionsSection.PLANNER_SQL_TIME_ZONE,
          plannerConfig.getSqlTimeZone());
    }
    if (base.isUseGroupingSetForExactDistinct() != plannerConfig.isUseGroupingSetForExactDistinct()) {
      options.put(
          OptionsSection.PLANNER_USE_GROUPING_SET_FOR_EXACT_DISTINCT,
          plannerConfig.isUseGroupingSetForExactDistinct());
    }
    if (base.isComputeInnerJoinCostAsFilter() != plannerConfig.isComputeInnerJoinCostAsFilter()) {
      options.put(
          OptionsSection.PLANNER_COMPUTE_INNER_JOIN_COST_AS_FILTER,
          plannerConfig.isComputeInnerJoinCostAsFilter());
    }
    if (base.isUseNativeQueryExplain() != plannerConfig.isUseNativeQueryExplain()) {
      options.put(
          OptionsSection.PLANNER_NATIVE_QUERY_EXPLAIN,
          plannerConfig.isUseNativeQueryExplain());
    }
    if (base.getMaxNumericInFilters() != plannerConfig.getMaxNumericInFilters()) {
      options.put(
          OptionsSection.PLANNER_MAX_NUMERIC_IN_FILTERS,
          plannerConfig.getMaxNumericInFilters());
    }
  }

  private void writeSchema(TestCaseWriter writer) throws IOException
  {
    if (plannerResult != null) {
      writer.emitSchema(QueryTestCases.formatSchema(plannerResult));
    }
  }

  private void writeNative(TestCaseWriter writer, ObjectMapper mapper) throws IOException
  {
    if (expectedQueries.isEmpty()) {
      return;
    }
    if (expectedQueries.size() == 1) {
      writer.emitNative(QueryTestCases.serializeQuery(mapper, expectedQueries.get(0)));
      return;
    }
    // Create a fake "union query" to hold the expected queries.
    writer.emitNative(
        QueryTestCases.serializeQuery(
            mapper,
            ImmutableMap.of(
                "artificialQueryType",
                "union",
                "inputs",
                expectedQueries)));
  }

  private void writeResults(TestCaseWriter writer, ObjectMapper mapper) throws IOException
  {
    if (results == null && expectedResultsVerifier == null) {
      return;
    }
    writer.emitSection("run");
    writer.emitOptions(ImmutableMap.of(OptionsSection.SQL_COMPATIBLE_NULLS, sqlCompatibleNulls));
    if (results != null) {
      writer.emitResults(QueryTestCases.resultsToJson(results, mapper));
      return;
    }
    if (!(expectedResultsVerifier instanceof DefaultResultsVerifier)) {
      CalciteTestRecorder.log.warn(
          "%s(): Results verifier is of type %s - cannot record.",
          methodName,
          expectedResultsVerifier.getClass().getSimpleName());
      return;
    }
    DefaultResultsVerifier verifier = (DefaultResultsVerifier) expectedResultsVerifier;
    writer.emitResults(QueryTestCases.resultsToJson(verifier.expectedResults(), mapper));
  }

  public void exception(Exception e)
  {
    this.exception = e;
  }

  public void user(String user)
  {
    this.user = user;
  }
}
