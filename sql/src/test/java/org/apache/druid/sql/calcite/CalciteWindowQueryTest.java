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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
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
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

/**
 * These tests are file-based, look in resources -> calcite/tests/window for the set of test specifications.
 */
@RunWith(Parameterized.class)
public class CalciteWindowQueryTest extends BaseCalciteQueryTest
{

  public static final boolean DUMP_ACTUAL_RESULTS = Boolean.parseBoolean(
      System.getProperty("druid.tests.sql.dumpActualResults")
  );

  static {
    NullHandling.initializeForTests();
  }

  private static final ObjectMapper YAML_JACKSON = new DefaultObjectMapper(new YAMLFactory(), "tests");

  @Parameterized.Parameters(name = "{0}")
  public static Object parametersForWindowQueryTest() throws Exception
  {
    final URL windowFolderUrl = ClassLoader.getSystemResource("calcite/tests/window");
    File windowFolder = new File(windowFolderUrl.toURI());

    final File[] listedFiles = windowFolder.listFiles(pathname -> pathname.getName().toLowerCase(Locale.ROOT).endsWith(".sqltest"));

    return Arrays
        .stream(Objects.requireNonNull(listedFiles))
        .map(File::getName)
        .toArray();
  }

  private final String filename;

  public CalciteWindowQueryTest(String filename)
  {
    this.filename = filename;
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

  @Test
  @SuppressWarnings("unchecked")
  public void windowQueryTest() throws Exception
  {
    TestCase testCase = new TestCase(filename);

    assumeThat(testCase.getType(), Matchers.not(TestType.failingTest));

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

  @Test
  @SuppressWarnings("unchecked")
  public void windowQueryTestWithCustomContextMaxSubqueryBytes() throws Exception
  {
    TestCase testCase = new TestCase(filename);

    assumeThat(testCase.getType(), Matchers.not(TestType.failingTest));

    if (testCase.getType() == TestType.operatorValidation) {
      testBuilder()
          .skipVectorize(true)
          .sql(testCase.getSql())
          .queryContext(ImmutableMap.of(PlannerContext.CTX_ENABLE_WINDOW_FNS, true,
                                        QueryContexts.ENABLE_DEBUG, true,
                                        QueryContexts.MAX_SUBQUERY_BYTES_KEY, "100000",
                                        QueryContexts.WINDOWING_STRICT_VALIDATION, false
                        )
          )
          .addCustomVerification(QueryVerification.ofResults(testCase))
          .run();
    }
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
