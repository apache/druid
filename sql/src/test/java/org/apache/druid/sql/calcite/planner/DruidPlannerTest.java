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

package org.apache.druid.sql.calcite.planner;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.tester.PlannerFixture;
import org.apache.druid.sql.calcite.tester.QueryTestSet;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Test runner for query planner tests defined in ".case" files.
 * If the test fails, the test itself won't report many details.
 * Instead, look in target/actual for the "actual" files for failed
 * test. Diff them with the cases in test/resources/calcite/cases
 * to determine what changed.
 * <p>
 * Planner setup is mostly handled by the {@code PlannerFixture}
 * class, with some additional test-specific configuration
 * applied for each group of test (each case file or set of case
 * files).
 * <p>
 * All tests use the set of hard-coded, in-memory segments defined
 * by {@code CalciteTests}. Tests can optionally include lookups
 * and views, if required for those tests.
 */
public class DruidPlannerTest
{
  public static final Logger log = new Logger(DruidPlannerTest.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  // Converted from CalciteInsertDmlTest
  @Test
  public void testInsertDml() throws IOException
  {
    assertTrue(
        QueryTestSet
        .fromResource("/calcite/cases/insertDML.case")
        .run(
            standardBuilder()
            // To allow access to external tables
            .withAuthResult(CalciteTests.SUPER_USER_AUTH_RESULT)
            .withView(
                "aview",
                "SELECT SUBSTRING(dim1, 1, 1) AS dim1_firstchar FROM foo WHERE dim2 = 'a'")));
  }

  private PlannerFixture.Builder standardBuilder() throws IOException
  {
    return new PlannerFixture
        .Builder(temporaryFolder.newFolder())
        .defaultQueryOptions(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
        .withLookups();
  }

  // Converted from CalciteArraysQueryTest
  @Test
  public void testArrayQuery() throws IOException
  {
    assertTrue(
        QueryTestSet
        .fromResource("/calcite/cases/arrayQuery.case")
        .run(standardBuilder()));
  }

  // Converted from CalciteCorrelatedQueryTest
  @Test
  public void testCorrelatedQuery() throws IOException
  {
    assertTrue(
        QueryTestSet
        .fromResource("/calcite/cases/correlatedQuery.case")
        .run(standardBuilder()));
  }

  // Converted from CalciteJoinQueryTest
  @Test
  public void testJoinQuery() throws IOException
  {
    assertTrue(
        runMultiple(
            standardBuilder(),
            "joinQuery",
            7));
  }

  private boolean runMultiple(
      PlannerFixture.Builder builder,
      String base,
      int count
  )
  {
    boolean ok = true;
    for (int i = 1; i <= count; i++) {
      String testCase = StringUtils.format("%s%02d.case", base, i);
      QueryTestSet testSet = QueryTestSet.fromResource("/calcite/cases/" + testCase);
      if (!testSet.run(builder)) {
        log.warn("Test failed: " + testCase);
        ok = false;
      }
    }
    return ok;
  }

  // Converted from CalciteMultiValueStringQueryTest
  @Test
  public void testMultiValueStringQuery() throws IOException
  {
    assertTrue(
        QueryTestSet
        .fromResource("/calcite/cases/multiValueStringQuery.case")
        .run(standardBuilder()));
  }

  // Converted from CalciteParameterQueryTest
  @Test
  public void testParameterQuery() throws IOException
  {
    assertTrue(
        QueryTestSet
        .fromResource("/calcite/cases/parameterQuery.case")
        .run(standardBuilder()));
  }

  // Converted from CalciteQueryTest
  // The original file is huge. The tests are split into multiple files,
  // in groups of around 25, in the same order as they appear in the
  // Java file.
  @Test
  public void testQuery() throws IOException
  {
    PlannerFixture fixture = new PlannerFixture
        .Builder(temporaryFolder.newFolder())
        .defaultQueryOptions(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
        .withLookups()
        .withMergeBufferCount(4)
        .withView(
            "aview",
            "SELECT SUBSTRING(dim1, 1, 1) AS dim1_firstchar FROM foo WHERE dim2 = 'a'")
        .withView(
            "bview",
            "SELECT COUNT(*) FROM druid.foo\n"
            + "WHERE __time >= CURRENT_TIMESTAMP + INTERVAL '1' DAY AND __time < TIMESTAMP '2002-01-01 00:00:00'")
        .withView(
            "cview",
            "SELECT SUBSTRING(bar.dim1, 1, 1) AS dim1_firstchar, bar.dim2 as dim2, dnf.l2 as l2\n"
            + "FROM (SELECT * from foo WHERE dim2 = 'a') as bar INNER JOIN druid.numfoo dnf ON bar.dim2 = dnf.dim2")
        .withView(
            "dview",
            "SELECT SUBSTRING(dim1, 1, 1) AS numfoo FROM foo WHERE dim2 = 'a'")
        .withView(
            "forbiddenView",
            "SELECT __time, SUBSTRING(dim1, 1, 1) AS dim1_firstchar, dim2 FROM foo WHERE dim2 = 'a'")
        .withView(
            "restrictedView",
            "SELECT __time, dim1, dim2, m1 FROM druid.forbiddenDatasource WHERE dim2 = 'a'")
        .withView(
            "invalidView",
            "SELECT __time, dim1, dim2, m1 FROM druid.invalidDatasource WHERE dim2 = 'a'")
        .build();
    boolean ok = true;
    for (int i = 1; i <= 15; i++) {
      String testCase = StringUtils.format("query%02d.case", i);
      QueryTestSet testSet = QueryTestSet.fromResource("/calcite/cases/" + testCase);
      if (!testSet.run(fixture)) {
        log.warn("Test failed: " + testCase);
        ok = false;
      }
    }
    assertTrue(ok);
  }
}
