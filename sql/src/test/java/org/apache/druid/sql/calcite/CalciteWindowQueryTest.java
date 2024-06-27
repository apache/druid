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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.CalciteWindowQueryTest.WindowQueryTestInputClass.TestType;
import org.apache.druid.sql.calcite.QueryTestRunner.QueryResults;
import org.apache.druid.sql.calcite.QueryVerification.QueryResultsVerifier;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * These tests are file-based, look in resources -> calcite/tests/window for the set of test specifications.
 */
public class CalciteWindowQueryTest extends BaseCalciteQueryTest
{

  public static final boolean DUMP_ACTUAL_RESULTS = Boolean.parseBoolean(
      System.getProperty("druid.tests.sql.dumpActualResults")
  ) || developerIDEdetected();

  private static final ObjectMapper YAML_JACKSON = new DefaultObjectMapper(new YAMLFactory(), "tests");

  public static Object[] parametersForWindowQueryTest() throws Exception
  {
    final URL windowFolderUrl = ClassLoader.getSystemResource("calcite/tests/window");
    File windowFolder = new File(windowFolderUrl.toURI());

    final File[] listedFiles = windowFolder.listFiles(pathname -> pathname.getName().toLowerCase(Locale.ROOT).endsWith(".sqltest"));

    return Arrays
        .stream(Objects.requireNonNull(listedFiles))
        .map(File::getName)
        .toArray();
  }

  class TestCase implements QueryResultsVerifier
  {
    private WindowQueryTestInputClass input;
    private ObjectMapper queryJackson;

    public TestCase(String filename) throws Exception
    {
      final URL systemResource = ClassLoader.getSystemResource("calcite/tests/window/" + filename);

      final Object objectFromYaml = YAML_JACKSON.readValue(systemResource, Object.class);

      queryJackson = queryFramework().queryJsonMapper();
      input = queryJackson.convertValue(objectFromYaml, WindowQueryTestInputClass.class);

    }

    public TestType getType()
    {
      return input.type;
    }

    public String getSql()
    {
      return input.sql;
    }

    @Override
    public void verifyResults(QueryResults results) throws Exception
    {
      if (results.exception != null) {
        throw new RE(results.exception, "Failed to execute because of exception.");
      }
      Assert.assertEquals(1, results.recordedQueries.size());

      maybeDumpActualResults(results.results);
      if (input.expectedOperators != null) {
        final WindowOperatorQuery query = getWindowOperatorQuery(results.recordedQueries);
        validateOperators(input.expectedOperators, query.getOperators());
      }

      final RowSignature outputSignature = results.signature;
      ColumnType[] types = new ColumnType[outputSignature.size()];
      for (int i = 0; i < outputSignature.size(); ++i) {
        types[i] = outputSignature.getColumnType(i).get();
        Assert.assertEquals(types[i], results.signature.getColumnType(i).get());
      }

      for (Object[] result : input.expectedResults) {
        for (int i = 0; i < result.length; i++) {
          // Jackson deserializes numbers as the minimum size required to
          // store the value. This means that
          // Longs can become Integer objects and then they fail equality
          // checks. We read the expected
          // results using Jackson, so, we coerce the expected results to the
          // type expected.
          if (result[i] != null) {
            if (result[i] instanceof Number) {
              switch (types[i].getType()) {
                case LONG:
                  result[i] = ((Number) result[i]).longValue();
                  break;
                case DOUBLE:
                  result[i] = ((Number) result[i]).doubleValue();
                  break;
                case FLOAT:
                  result[i] = ((Number) result[i]).floatValue();
                  break;
                default:
                  throw new ISE("result[%s] was type[%s]!?  Expected it to be numerical", i, types[i].getType());
              }
            }
          }
        }
      }
      assertResultsValid(ResultMatchMode.RELAX_NULLS, input.expectedResults, results);
    }

    private void validateOperators(List<OperatorFactory> expectedOperators, List<OperatorFactory> currentOperators)
        throws Exception
    {
      for (int i = 0; i < expectedOperators.size(); ++i) {
        final OperatorFactory expectedOperator = expectedOperators.get(i);
        final OperatorFactory actualOperator = currentOperators.get(i);
        if (!expectedOperator.validateEquivalent(actualOperator)) {
          assertEquals("Operator Mismatch, index[" + i + "]",
              queryJackson.writeValueAsString(expectedOperator),
              queryJackson.writeValueAsString(actualOperator));
          fail("validateEquivalent failed; but textual comparision of operators didn't reported the mismatch!");
        }
      }
      assertEquals("Operator count mismatch!", expectedOperators.size(), currentOperators.size());
    }

    private void maybeDumpActualResults(List<Object[]> results) throws Exception
    {
      if (DUMP_ACTUAL_RESULTS) {
        StringBuilder sb = new StringBuilder();
        for (Object[] row : results) {
          sb.append("  - ");
          sb.append(queryJackson.writeValueAsString(row));
          sb.append("\n");
        }
        log.info("Actual results:\n%s", sb.toString());
      }
    }
  }

  @MethodSource("parametersForWindowQueryTest")
  @ParameterizedTest(name = "{0}")
  @SuppressWarnings("unchecked")
  public void windowQueryTest(String filename) throws Exception
  {
    TestCase testCase = new TestCase(filename);

    assumeTrue(testCase.getType() != TestType.failingTest);

    if (testCase.getType() == TestType.operatorValidation) {
      testBuilder()
          .skipVectorize(true)
          .sql(testCase.getSql())
          .queryContext(ImmutableMap.of(
              PlannerContext.CTX_ENABLE_WINDOW_FNS, true,
              QueryContexts.ENABLE_DEBUG, true,
              QueryContexts.WINDOWING_STRICT_VALIDATION, false
              ))
          .addCustomVerification(QueryVerification.ofResults(testCase))
          .run();
    }
  }

  @MethodSource("parametersForWindowQueryTest")
  @ParameterizedTest(name = "{0}")
  @SuppressWarnings("unchecked")
  public void windowQueryTestWithCustomContextMaxSubqueryBytes(String filename) throws Exception
  {
    TestCase testCase = new TestCase(filename);

    assumeTrue(testCase.getType() != TestType.failingTest);

    if (testCase.getType() == TestType.operatorValidation) {
      testBuilder()
          .skipVectorize(true)
          .sql(testCase.getSql())
          .queryContext(ImmutableMap.of(QueryContexts.ENABLE_DEBUG, true,
                                        PlannerContext.CTX_ENABLE_WINDOW_FNS, true,
                                        QueryContexts.MAX_SUBQUERY_BYTES_KEY, "100000",
                                        QueryContexts.WINDOWING_STRICT_VALIDATION, false
                        )
          )
          .addCustomVerification(QueryVerification.ofResults(testCase))
          .run();
    }
  }

  @Test
  public void testEmptyWindowInSubquery()
  {
    testBuilder()
        .sql(
            "select c from (\n"
            + "  select channel, row_number() over () as c\n"
            + "  from wikipedia\n"
            + "  group by channel\n"
            + ") LIMIT 5"
        )
        .queryContext(ImmutableMap.of(
            PlannerContext.CTX_ENABLE_WINDOW_FNS, true,
            QueryContexts.ENABLE_DEBUG, true,
            QueryContexts.WINDOWING_STRICT_VALIDATION, false
        ))
        .expectedResults(ImmutableList.of(
            new Object[]{1L},
            new Object[]{2L},
            new Object[]{3L},
            new Object[]{4L},
            new Object[]{5L}
        ))
        .run();
  }

  @Test
  public void testWindow()
  {
    testBuilder()
        .sql("SELECT\n" +
             "(rank() over (order by count(*) desc)),\n" +
             "(rank() over (order by count(*) desc))\n" +
             "FROM \"wikipedia\"")
        .queryContext(ImmutableMap.of(
            PlannerContext.CTX_ENABLE_WINDOW_FNS, true,
            QueryContexts.ENABLE_DEBUG, true,
            QueryContexts.WINDOWING_STRICT_VALIDATION, false
        ))
        .expectedResults(ImmutableList.of(
            new Object[]{1L, 1L}
        ))
        .run();
  }

  @Test
  public void testWindowAllBoundsCombination()
  {
    testBuilder()
        .sql("select\n"
             + "cityName,\n"
             + "count(*) over (partition by cityName order by countryName rows between unbounded preceding and 1 preceding) c1,\n"
             + "count(*) over (partition by cityName order by countryName rows between unbounded preceding and current row) c2,\n"
             + "count(*) over (partition by cityName order by countryName rows between unbounded preceding and 1 following) c3,\n"
             + "count(*) over (partition by cityName order by countryName rows between unbounded preceding and unbounded following) c4,\n"
             + "count(*) over (partition by cityName order by countryName rows between 3 preceding and 1 preceding) c5,\n"
             + "count(*) over (partition by cityName order by countryName rows between 1 preceding and current row) c6,\n"
             + "count(*) over (partition by cityName order by countryName rows between 1 preceding and 1 FOLLOWING) c7,\n"
             + "count(*) over (partition by cityName order by countryName rows between 1 preceding and unbounded FOLLOWING) c8,\n"
             + "count(*) over (partition by cityName order by countryName rows between 1 FOLLOWING and unbounded FOLLOWING) c9,\n"
             + "count(*) over (partition by cityName order by countryName rows between 1 FOLLOWING and 3 FOLLOWING) c10,\n"
             + "count(*) over (partition by cityName order by countryName rows between current row and 1 following) c11,\n"
             + "count(*) over (partition by cityName order by countryName rows between current row and unbounded following) c12\n"
             + "from wikipedia\n"
             + "where cityName in ('Vienna', 'Seoul')\n"
             + "group by countryName, cityName, added")
        .queryContext(ImmutableMap.of(
            PlannerContext.CTX_ENABLE_WINDOW_FNS, true,
            QueryContexts.ENABLE_DEBUG, true
        ))
        .expectedResults(ImmutableList.of(
            new Object[]{"Seoul", 0L, 1L, 2L, 13L, 0L, 1L, 2L, 13L, 12L, 3L, 2L, 13L},
            new Object[]{"Seoul", 1L, 2L, 3L, 13L, 1L, 2L, 3L, 13L, 11L, 3L, 2L, 12L},
            new Object[]{"Seoul", 2L, 3L, 4L, 13L, 2L, 2L, 3L, 12L, 10L, 3L, 2L, 11L},
            new Object[]{"Seoul", 3L, 4L, 5L, 13L, 3L, 2L, 3L, 11L, 9L, 3L, 2L, 10L},
            new Object[]{"Seoul", 4L, 5L, 6L, 13L, 3L, 2L, 3L, 10L, 8L, 3L, 2L, 9L},
            new Object[]{"Seoul", 5L, 6L, 7L, 13L, 3L, 2L, 3L, 9L, 7L, 3L, 2L, 8L},
            new Object[]{"Seoul", 6L, 7L, 8L, 13L, 3L, 2L, 3L, 8L, 6L, 3L, 2L, 7L},
            new Object[]{"Seoul", 7L, 8L, 9L, 13L, 3L, 2L, 3L, 7L, 5L, 3L, 2L, 6L},
            new Object[]{"Seoul", 8L, 9L, 10L, 13L, 3L, 2L, 3L, 6L, 4L, 3L, 2L, 5L},
            new Object[]{"Seoul", 9L, 10L, 11L, 13L, 3L, 2L, 3L, 5L, 3L, 3L, 2L, 4L},
            new Object[]{"Seoul", 10L, 11L, 12L, 13L, 3L, 2L, 3L, 4L, 2L, 2L, 2L, 3L},
            new Object[]{"Seoul", 11L, 12L, 13L, 13L, 3L, 2L, 3L, 3L, 1L, 1L, 2L, 2L},
            new Object[]{"Seoul", 12L, 13L, 13L, 13L, 3L, 2L, 2L, 2L, 0L, 0L, 1L, 1L},
            new Object[]{"Vienna", 0L, 1L, 2L, 3L, 0L, 1L, 2L, 3L, 2L, 2L, 2L, 3L},
            new Object[]{"Vienna", 1L, 2L, 3L, 3L, 1L, 2L, 3L, 3L, 1L, 1L, 2L, 2L},
            new Object[]{"Vienna", 2L, 3L, 3L, 3L, 2L, 2L, 2L, 2L, 0L, 0L, 1L, 1L}
        ))
        .run();
  }

  private WindowOperatorQuery getWindowOperatorQuery(List<Query<?>> queries)
  {
    assertEquals(1, queries.size());
    Object query = queries.get(0);
    assertTrue(query instanceof WindowOperatorQuery);
    return (WindowOperatorQuery) query;
  }


  public static class WindowQueryTestInputClass
  {
    enum TestType
    {
      failingTest,
      operatorValidation
    }
    @JsonProperty
    public TestType type;

    @JsonProperty
    public String sql;

    @JsonProperty
    public List<OperatorFactory> expectedOperators;

    @JsonProperty
    public List<Object[]> expectedResults;
  }
}
