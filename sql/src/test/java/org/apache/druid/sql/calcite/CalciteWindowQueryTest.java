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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableMap;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.query.Query;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Pattern;

@RunWith(JUnitParamsRunner.class)
public class CalciteWindowQueryTest extends BaseCalciteQueryTest
{

  public static final boolean DUMP_EXPECTED_RESULTS = Boolean.parseBoolean(
      System.getProperty("druid.tests.sql.dumpExpectedResults")
  );

  static {
    NullHandling.initializeForTests();
  }

  private static final ObjectMapper YAML_JACKSON = new DefaultObjectMapper(new YAMLFactory(), "tests");

  private static final AtomicLong EXPECTED_TESTS = new AtomicLong();
  private static final AtomicLong TEST_COUNTER = new AtomicLong();

  public Object parametersForWindowQueryTest() throws Exception
  {
    final URL windowFolderUrl = ClassLoader.getSystemResource("calcite/tests/window");
    File windowFolder = new File(windowFolderUrl.toURI());

    final File[] listedFiles = windowFolder.listFiles(
        pathname -> pathname.getName().toLowerCase(Locale.ROOT).endsWith(".sqltest")
    );
    EXPECTED_TESTS.set(listedFiles.length);

    Pattern matcher = Pattern.compile(".*");

    return Arrays
        .stream(Objects.requireNonNull(listedFiles))
        .map(File::getName)
        .filter(matcher.asPredicate())
        .toArray();
  }

  @AfterClass
  public static void testRanAllTests()
  {
    // This validation exists to catch issues with the filter Pattern accidentally getting checked in.  It validates
    // that we ran all of the tests from the directory.  If this is failing, most likely, the filter Pattern in
    // parametersForWindowQueryTest accidentally got checked in as something other than ".*"
    Assert.assertEquals(EXPECTED_TESTS.get(), TEST_COUNTER.get());
  }

  @Test
  @Parameters(method = "parametersForWindowQueryTest")
  @SuppressWarnings("unchecked")
  @TestCaseName("{0}")
  public void windowQueryTest(String filename) throws IOException
  {
    final Function<String, String> stringManipulator;
    if (NullHandling.sqlCompatible()) {
      stringManipulator = s -> "".equals(s) ? null : s;
    } else {
      stringManipulator = Function.identity();
    }

    TEST_COUNTER.incrementAndGet();
    final URL systemResource = ClassLoader.getSystemResource("calcite/tests/window/" + filename);

    final Object objectFromYaml = YAML_JACKSON.readValue(systemResource.openStream(), Object.class);

    final ObjectMapper queryJackson = queryFramework().queryJsonMapper();
    final WindowQueryTestInputClass input = queryJackson.convertValue(objectFromYaml, WindowQueryTestInputClass.class);

    Function<Object, String> jacksonToString = value -> {
      try {
        return queryJackson.writeValueAsString(value);
      }
      catch (JsonProcessingException e) {
        throw new RE(e);
      }
    };

    if ("operatorValidation".equals(input.type)) {
      testBuilder()
          .skipVectorize(true)
          .sql(input.sql)
          .queryContext(ImmutableMap.of("windowsAreForClosers", true))
          .addCustomVerification(QueryVerification.ofResults(results -> {
            if (results.exception != null) {
              throw new RE(results.exception, "Failed to execute because of exception.");
            }

            Assert.assertEquals(1, results.recordedQueries.size());

            final WindowOperatorQuery query = (WindowOperatorQuery) results.recordedQueries.get(0);
            for (int i = 0; i < input.expectedOperators.size(); ++i) {
              final OperatorFactory expectedOperator = input.expectedOperators.get(i);
              final OperatorFactory actualOperator = query.getOperators().get(i);
              if (!expectedOperator.validateEquivalent(actualOperator)) {
                // This assertion always fails because the validate equivalent failed, but we do it anyway
                // so that we get values in the output of the failed test to make it easier to
                // debug what happened.  Note, we use the Jackson representation when showing the diff.  There is
                // a chance that this representation is exactly equivalent, but the validation call is still failing
                // this is probably indicative of a bug where something that needs to be serialized by Jackson
                // currently is not.  Check your getters.

                // prepend different values so that we are guaranteed that it is always different
                String expected = "e " + jacksonToString.apply(expectedOperator);
                String actual = "a " + jacksonToString.apply(actualOperator);

                Assert.assertEquals("Operator Mismatch, index[" + i + "]", expected, actual);
              }
            }
            final RowSignature outputSignature = query.getRowSignature();
            ColumnType[] types = new ColumnType[outputSignature.size()];
            for (int i = 0; i < outputSignature.size(); ++i) {
              types[i] = outputSignature.getColumnType(i).get();
              Assert.assertEquals(types[i], results.signature.getColumnType(i).get());
            }

            maybeDumpExpectedResults(jacksonToString, results.results);
            for (Object[] result : input.expectedResults) {
              for (int i = 0; i < types.length; i++) {
                // Jackson deserializes numbers as the minimum size required to store the value.  This means that
                // Longs can become Integer objects and then they fail equality checks.  We read the expected
                // results using Jackson, so, we coerce the expected results to the type expected.
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
                  } else if (result[i] instanceof String) {
                    result[i] = stringManipulator.apply((String) result[i]);
                  }
                }
              }
            }
            assertResultsEquals(filename, input.expectedResults, results.results);
          }))
          .run();
    }
  }

  private void maybeDumpExpectedResults(
      Function<Object, String> toStrFn, List<Object[]> results
  )
  {
    if (DUMP_EXPECTED_RESULTS) {
      for (Object[] result : results) {
        System.out.println("  - " + toStrFn.apply(result));
      }
    }
  }

  public static class WindowQueryTestInputClass
  {
    @JsonProperty
    public String type;

    @JsonProperty
    public String sql;

    @JsonProperty
    public Query nativeQuery;

    @JsonProperty
    public List<OperatorFactory> expectedOperators;

    @JsonProperty
    public List<Object[]> expectedResults;
  }
}
