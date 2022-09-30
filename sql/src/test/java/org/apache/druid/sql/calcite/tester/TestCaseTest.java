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

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the test case class.
 */
public class TestCaseTest
{
  private List<QueryTestCase> load(String input)
  {
    TestSetSpec testSet = TestCaseLoader.loadString(input);
    TestCaseAnalyzer analyzer = new TestCaseAnalyzer(testSet);
    return analyzer.analyze();
  }

  @Test
  public void testSql()
  {
    String input =
        "=== case\n" +
        "=== SQL\n" +
        "SELECT 1\n";
    List<QueryTestCase> cases = load(input);
    assertEquals("SELECT 1", cases.get(0).sql());
  }

  public void expectOK(ExpectedPattern expected, String actual)
  {
    ActualResults.ErrorCollector errors = new ActualResults.ErrorCollector();
    expected.verify(actual, errors);
    assertTrue(errors.ok());
  }

  public void expectError(ExpectedPattern expected, String actual)
  {
    ActualResults.ErrorCollector errors = new ActualResults.ErrorCollector();
    expected.verify(actual, errors);
    assertFalse(errors.ok());
  }

  @Test
  public void testOneLiteral()
  {
    String input =
        "=== case\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== plan\n" +
        "  a plan  \n";
    List<QueryTestCase> cases = load(input);
    QueryTestCase testCase = cases.get(0);

    expectOK(testCase.plan(), "a plan");
    expectOK(testCase.plan(), "   a plan   ");
    expectOK(testCase.plan(), "  a plan  \n");
    expectOK(testCase.plan(), "a plan\n\n");

    expectError(testCase.plan(), "");
    expectError(testCase.plan(), "wrong");
    String actual =
        "a plan\n" +
        "bogus";
    expectError(testCase.plan(), actual);

    actual =
        "a plan\n" +
        "\n" +
        "bogus";
    expectError(testCase.plan(), actual);
  }

  @Test
  public void testMultipleLiterals()
  {
    String input =
        "=== case\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== plan\n" +
        "  a plan  \n" +
        "    second\n" +
        "  third\n";
    List<QueryTestCase> cases = load(input);
    QueryTestCase testCase = cases.get(0);

    {
      final String actual =
          "  a plan  \n" +
          "    second\n" +
          "  third\n";
      expectOK(testCase.plan(), actual);
    }

    {
      final String actual =
          "a plan\n" +
          "second   \n" +
          "third\n";
      expectOK(testCase.plan(), actual);
    }

    {
      final String actual =
          "  a plan  \n" +
          "    second'n" +
          "  third\n" +
          "  extra\n";
      expectError(testCase.plan(), actual);
    }

    {
      final String actual =
          "  a plan  \n" +
          "    second\n";
      expectError(testCase.plan(), actual);
    }
  }

  @Test
  public void testRegex()
  {
    String input =
        "=== case\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== plan\n" +
        "!count \\d+\n" +
        "!timestamp .+\n";
    List<QueryTestCase> cases = load(input);
    QueryTestCase testCase = cases.get(0);

    {
      final String actual =
          "  count 1234  \n" +
          "timestamp 2021-04-29T12:13:14 ";
      expectOK(testCase.plan(), actual);
    }

    {
      final String actual =
          "  count 1234x  \n" +
          "timestamp 2021-04-29T12:13:14 \n";
      expectError(testCase.plan(), actual);
    }

    {
      final String actual =
          "  count 1234  \n" +
          "timestamp\n";
      expectError(testCase.plan(), actual);
    }

    {
      final String actual =
          "  count 1234  \n" +
          "bogus\n";
      expectError(testCase.plan(), actual);
    }
  }

  @Test
  public void testSkip()
  {
    String input =
        "=== case\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== plan\n" +
        "!count \\d+\n" +
        "**\n" +
        "end\n";
    List<QueryTestCase> cases = load(input);
    QueryTestCase testCase = cases.get(0);

    {
      final String actual =
          "  count 1234  \n" +
          "end\n";
      expectOK(testCase.plan(), actual);
    }

    {
      final String actual =
          "  count 1234  \n" +
          " ignored \n" +
          "   abc 123\n" +
          "end\n";
      expectOK(testCase.plan(), actual);
    }

    {
      final String actual =
          "  count 1234  \n" +
          "bogus\n";
      expectError(testCase.plan(), actual);
    }

    {
      final String actual =
          "  count 1234  \n" +
          " ignored \n" +
          "   abc 123\n" +
          "bogus\n";
      expectError(testCase.plan(), actual);
    }
  }
}
