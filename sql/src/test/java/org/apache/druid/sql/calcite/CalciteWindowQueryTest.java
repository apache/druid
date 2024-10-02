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
import org.apache.druid.error.DruidException;
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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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

  private static final Map<String, Object> DEFAULT_QUERY_CONTEXT = ImmutableMap.of(
      QueryContexts.ENABLE_DEBUG, true,
      QueryContexts.CTX_SQL_STRINGIFY_ARRAYS, false
  );

  private static final Map<String, Object> DEFAULT_QUERY_CONTEXT_WITH_SUBQUERY_BYTES =
      ImmutableMap.<String, Object>builder()
                  .putAll(DEFAULT_QUERY_CONTEXT)
                  .put(QueryContexts.MAX_SUBQUERY_BYTES_KEY, "100000")
                  .put(QueryContexts.MAX_SUBQUERY_ROWS_KEY, "0")
                  .build();

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
      assertResultsValid(ResultMatchMode.RELAX_NULLS_EPS, input.expectedResults, results);
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

    public Map<? extends String, ? extends Object> getQueryContext()
    {
      return input.queryContext == null ? Collections.emptyMap() : input.queryContext;
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
          .queryContext(
              ImmutableMap.<String, Object>builder()
                  .putAll(DEFAULT_QUERY_CONTEXT)
                  .putAll(testCase.getQueryContext())
                  .build()
          )
          .addCustomVerification(QueryVerification.ofResults(testCase))
          .run();
    }
  }

  @MethodSource("parametersForWindowQueryTest")
  @ParameterizedTest(name = "{0}")
  @SuppressWarnings("unchecked")
  public void windowQueryTestsWithSubqueryBytes(String filename) throws Exception
  {
    TestCase testCase = new TestCase(filename);

    assumeTrue(testCase.getType() != TestType.failingTest);

    if (testCase.getType() == TestType.operatorValidation) {
      testBuilder()
          .skipVectorize(true)
          .sql(testCase.getSql())
          .queryContext(
              ImmutableMap.<String, Object>builder()
                          .putAll(DEFAULT_QUERY_CONTEXT_WITH_SUBQUERY_BYTES)
                          .putAll(testCase.getQueryContext())
                          .build()
          )
          .addCustomVerification(QueryVerification.ofResults(testCase))
          .run();
    }
  }

  @Test
  public void testWithArrayConcat()
  {
    testBuilder()
        .sql("select countryName, cityName, channel, "
             + "array_concat_agg(ARRAY['abc', channel], 10000) over (partition by cityName order by countryName) as c\n"
             + "from wikipedia\n"
             + "where countryName in ('Austria', 'Republic of Korea') "
             + "and (cityName in ('Vienna', 'Seoul') or cityName is null)\n"
             + "group by countryName, cityName, channel")
        .queryContext(DEFAULT_QUERY_CONTEXT)
        .expectedResults(
            ResultMatchMode.RELAX_NULLS,
            ImmutableList.of(
              new Object[]{"Austria", null, "#de.wikipedia", ImmutableList.of("abc", "#de.wikipedia")},
              new Object[]{"Republic of Korea", null, "#en.wikipedia", ImmutableList.of("abc", "#de.wikipedia", "abc", "#en.wikipedia", "abc", "#ja.wikipedia", "abc", "#ko.wikipedia")},
              new Object[]{"Republic of Korea", null, "#ja.wikipedia", ImmutableList.of("abc", "#de.wikipedia", "abc", "#en.wikipedia", "abc", "#ja.wikipedia", "abc", "#ko.wikipedia")},
              new Object[]{"Republic of Korea", null, "#ko.wikipedia", ImmutableList.of("abc", "#de.wikipedia", "abc", "#en.wikipedia", "abc", "#ja.wikipedia", "abc", "#ko.wikipedia")},
              new Object[]{"Republic of Korea", "Seoul", "#ko.wikipedia", ImmutableList.of("abc", "#ko.wikipedia")},
              new Object[]{"Austria", "Vienna", "#de.wikipedia", ImmutableList.of("abc", "#de.wikipedia", "abc", "#es.wikipedia", "abc", "#tr.wikipedia")},
              new Object[]{"Austria", "Vienna", "#es.wikipedia", ImmutableList.of("abc", "#de.wikipedia", "abc", "#es.wikipedia", "abc", "#tr.wikipedia")},
              new Object[]{"Austria", "Vienna", "#tr.wikipedia", ImmutableList.of("abc", "#de.wikipedia", "abc", "#es.wikipedia", "abc", "#tr.wikipedia")}
            )
        )
        .run();
  }

  @Test
  public void testFailure_partitionByMVD()
  {
    final DruidException e = Assert.assertThrows(
        DruidException.class,
        () -> testBuilder()
            .sql("select cityName, countryName, array_to_mv(array[1,length(cityName)]),\n"
                 + "row_number() over (partition by  array_to_mv(array[1,length(cityName)]) order by countryName, cityName)\n"
                 + "from wikipedia\n"
                 + "where countryName in ('Austria', 'Republic of Korea') and cityName is not null\n"
                 + "order by 1, 2, 3")
            .queryContext(DEFAULT_QUERY_CONTEXT)
            .run()
    );

    assertEquals(
        "Encountered a multi value column [v0]. Window processing does not support MVDs. "
        + "Consider using UNNEST or MV_TO_ARRAY.",
        e.getMessage()
    );

    final DruidException e1 = Assert.assertThrows(
        DruidException.class,
        () -> testBuilder()
            .sql("select cityName, countryName, array_to_mv(array[1,length(cityName)]),\n"
                 + "row_number() over (partition by  array_to_mv(array[1,length(cityName)]) order by countryName, cityName)\n"
                 + "from wikipedia\n"
                 + "where countryName in ('Austria', 'Republic of Korea') and cityName is not null\n"
                 + "order by 1, 2, 3")
            .queryContext(ImmutableMap.of(
                QueryContexts.ENABLE_DEBUG, true,
                QueryContexts.CTX_SQL_STRINGIFY_ARRAYS, false,
                PlannerContext.CTX_ENABLE_RAC_TRANSFER_OVER_WIRE, true
            ))
            .run()
    );

    assertEquals(
        "Encountered a multi value column. Window processing does not support MVDs. "
        + "Consider using UNNEST or MV_TO_ARRAY.",
        e1.getMessage()
    );
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
    public Map<String, String> queryContext;

    @JsonProperty
    public String sql;

    @JsonProperty
    public List<OperatorFactory> expectedOperators;

    @JsonProperty
    public List<Object[]> expectedResults;
  }
}
