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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.config.NullValueHandlingConfig;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.calcite.util.CalciteTests;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Holds a set of test cases to execute, runs the test, and applies
 * filters to select which tests to run.
 */
public class QueryTestSet
{
  public static final Logger log = new Logger(QueryTestSet.class);

  /**
   * Tests are run in multiple passes: one with SQL compatible null
   * handling, another without. This class correlates the actual results
   * for the two passes. Then, within each pass, the query is planned,
   * run zero or more times, and verified. This class correlates those
   * multiple passes.
   *
   * If a test run fails, then we emit the full set of tests. If this
   * test was OK, we just repeat the whole expected test case. Else,
   * we emit the merged expected/actuals test case for each variation
   * which failed.
   */
  private static class TestResults
  {
    final QueryTestCase testCase;
    final List<ActualResults> results = new ArrayList<>();
    boolean ok = true;

    private TestResults(QueryTestCase testCase)
    {
      this.testCase = testCase;
    }

    public void add(ActualResults caseResults)
    {
      if (caseResults == null) {
        return;
      }
      results.add(caseResults);
      ok = ok && caseResults.ok();
    }

    public void write(TestCaseWriter testWriter) throws IOException
    {
      if (ok) {
        testCase.write(testWriter);
      } else {
        for (ActualResults actual : results) {
          if (!actual.ok()) {
            actual.write(testWriter);
            break;
          }
        }
      }
    }
  }

  private final String label;
  private final List<TestResults> results = new ArrayList<>();

  public QueryTestSet(String label, List<QueryTestCase> testCases)
  {
    this.label = label;
    for (QueryTestCase testCase : testCases) {
      results.add(new TestResults(testCase));
    }
  }

  public static QueryTestSet fromFile(File file)
  {
    return new QueryTestSet(
        file.getName(),
        TestCaseLoader.loadFile(file)
        );
  }

  public static QueryTestSet fromResource(String resource)
  {
    int posn = resource.lastIndexOf('/');
    return new QueryTestSet(
        posn == -1 ? resource : resource.substring(posn + 1),
        TestCaseLoader.loadResource(resource)
        );
  }

  public static QueryTestSet fromString(String label, String body)
  {
    return new QueryTestSet(
        label,
        TestCaseLoader.loadString(body)
        );
  }

  public boolean run(PlannerFixture.Builder builder)
  {
    if (isDebugMode()) {
      setSqlCompatibleNulls(true);
      boolean sqlCompatOK = run(builder.build());
      setSqlCompatibleNulls(false);
      boolean classicOK = run(builder.build());
      return sqlCompatOK && classicOK;
    } else {
      return run(builder.build());
    }
  }

  /**
   * Check if "debug mode" is set. If the system property
   * {code -Ddruid.debug=true`} is set, then the test will run
   * both the SQL-compatible nulls, and legacy "replace nulls with
   * defaults" modes. This saves you from having to twiddle those
   * values when running tests in your IDE. When run from Maven,
   * tests will run in whatever mode is current, as set by the
   * Travis job.
   */
  private boolean isDebugMode()
  {
    return Boolean.parseBoolean(
        System.getProperty("druid.debug", Boolean.FALSE.toString()));
  }

  private void setSqlCompatibleNulls(boolean option)
  {
    System.setProperty(
        NullValueHandlingConfig.NULL_HANDLING_CONFIG_STRING,
        Boolean.toString(!option));
    NullHandling.initializeForTests();
    CalciteTests.reset();
  }

  public boolean run(PlannerFixture fixture)
  {
    boolean ok = true;
    for (TestResults testCase : results) {
      ActualResults caseResults = fixture.runTestCase(testCase.testCase);
      testCase.add(caseResults);
      ok = ok && testCase.ok;
    }
    File dest = new File(fixture.resultsDir(), label);
    if (ok) {
      // This run is clean. Remove any output files from previous
      // runs to prevent confusion.
      dest.delete();
    } else {
      reportResults();
      try {
        FileUtils.mkdirp(fixture.resultsDir());
      }
      catch (IOException e) {
        throw new ISE(e, "Could not make results dir: " + fixture.resultsDir());
      }
      writeResults(dest);
    }
    return ok;
  }

  private void reportResults()
  {
    log.error("Test case failed: %s", label);
    for (TestResults testCase : results) {
      if (testCase.ok) {
        continue;
      }
      log.error("=== " + testCase.testCase.label() + " ===");
      for (ActualResults caseResults : testCase.results) {
        for (String error : caseResults.errors().errors()) {
          log.error(error);
        }
      }
    }
  }

  private void writeResults(File dest)
  {
    try {
      try (Writer writer = new OutputStreamWriter(new FileOutputStream(dest), StandardCharsets.UTF_8)) {
        TestCaseWriter testWriter = new TestCaseWriter(writer);
        for (TestResults testCase : results) {
          testCase.write(testWriter);
        }
      }
    }
    catch (IOException e) {
      throw new IAE(e, "Could not write test results to " + dest);
    }
  }
}
