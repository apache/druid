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

import org.apache.calcite.avatica.SqlType;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.security.Action;
import org.apache.druid.sql.http.SqlParameter;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests the test case loader (parser).
 */
public class TestCaseLoaderTest
{
  @Test
  public void testEmpty()
  {
    String input = "";
    assertTrue(TestCaseLoader.loadString(input).isEmpty());
    input = "  ";
    assertTrue(TestCaseLoader.loadString(input).isEmpty());
    input = "\n";
    assertTrue(TestCaseLoader.loadString(input).isEmpty());
    input = "  \n\n";
    assertTrue(TestCaseLoader.loadString(input).isEmpty());
  }

  @Test
  public void testLeadingComments()
  {
    String input =
        "I'm a comment";
    assertTrue(TestCaseLoader.loadString(input).isEmpty());
    input =
        "I'm a comment\n";
    assertTrue(TestCaseLoader.loadString(input).isEmpty());
    input =
        "I'm a comment\n" +
        "and so am I\n";
    assertTrue(TestCaseLoader.loadString(input).isEmpty());
    input =
        "I'm a comment\n" +
        "====\n" +
        "and so am I\n";
    assertTrue(TestCaseLoader.loadString(input).isEmpty());
  }

  @Test
  public void testAllComments()
  {
    String input =
          "====\n" +
          "Ignore me\n" +
          "=====\n" +
          "Ignore me also\n" +
          "==== foo\n" +
          "===#\n" +
          "===# foo\\n" +
          "=== #\\n" +
          "=== # foo\n";
    assertTrue(TestCaseLoader.loadString(input).isEmpty());
    input =
        "====";
    assertTrue(TestCaseLoader.loadString(input).isEmpty());
  }

  @Test
  public void testMissingCase()
  {
    final String input =
        "=== plan\n";
    assertThrows(
        IAE.class,
        () -> TestCaseLoader.loadString(input));
  }

  @Test
  public void testCase()
  {
    String input =
        "=== case\n" +
        "=== SQL\n" +
        "SELECT 1\n";
    List<QueryTestCase> cases = TestCaseLoader.loadString(input);
    assertEquals(1, cases.size());
    assertEquals("SELECT 1", cases.get(0).sql());
    assertEquals("Case at line 1", cases.get(0).label);

    input =
        "\n" +
        "====\n" +
        "some comment\n" +
        "=== case\n" +
        "=== SQL\n" +
        "SELECT 1\n";
    cases = TestCaseLoader.loadString(input);
    assertEquals(1, cases.size());
    assertEquals("SELECT 1", cases.get(0).sql());
    assertEquals("Case at line 4", cases.get(0).label);

    input =
        "=== case\n" +
        "second\n" +
        "=== SQL\n" +
        "SELECT foo\n" +
        " FROM bar\n";
    cases = TestCaseLoader.loadString(input);
    assertEquals(1, cases.size());
    assertEquals("SELECT foo\n FROM bar", cases.get(0).sql());
    assertEquals("second", cases.get(0).label);
  }

  @Test
  public void testEmptySql()
  {
    {
      final String input =
          "=== case\n" +
          "=== sql\n";
      assertThrows(
          IAE.class,
          () -> TestCaseLoader.loadString(input));
    }
    {
      final String input =
          "=== case\n" +
          "=== sql\n" +
          "\n";
      assertThrows(
          IAE.class,
          () -> TestCaseLoader.loadString(input));
    }
    {
      final String input =
          "=== case\n" +
          "=== sql\n" +
          "   \n";
      assertThrows(
          IAE.class,
          () -> TestCaseLoader.loadString(input));
    }
    {
      final String input =
          "=== case\n" +
          "=== sql\n" +
          "=== case\n" +
          "=== sql";
      assertThrows(
          IAE.class,
          () -> TestCaseLoader.loadString(input));
    }
    {
      final String input =
          "=== case\n" +
          "=== sql\n" +
          "\n" +
          "=== case\n" +
          "=== sql";
      assertThrows(
          IAE.class,
          () -> TestCaseLoader.loadString(input));
    }
  }

  @Test
  public void testInvalidSection()
  {
    {
      final String input =
          "=== case\n" +
          "=== SQL\n" +
          "SELECT 1\n" +
          "=== bogus";
      assertThrows(
          IAE.class,
          () -> TestCaseLoader.loadString(input));
    }
  }

  @Test
  public void testPlan()
  {
    String input =
        "=== case\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== plan\n";
    List<QueryTestCase> cases = TestCaseLoader.loadString(input);
    assertEquals(1, cases.size());
    PatternSection.ExpectedText plan = cases.get(0).plan().expected;
    assertTrue(plan.lines.isEmpty());

    input =
        "=== case\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== plan\n" +
        "  a plan  \n";
    cases = TestCaseLoader.loadString(input);
    assertEquals(1, cases.size());
    plan = cases.get(0).plan().expected;
    assertEquals(1, plan.lines.size());
    assertEquals("  a plan  ", ((PatternSection.ExpectedLiteral) plan.lines.get(0)).line);

    input =
        "=== case\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== plan\n" +
        "**\n" +
        "a plan\n" +
        "!count \\d+  \n" +
        "  \n" +
        "\\!foo \n";
    cases = TestCaseLoader.loadString(input);
    assertEquals(1, cases.size());
    plan = cases.get(0).plan().expected;
    assertEquals(5, plan.lines.size());
    assertTrue(plan.lines.get(0) instanceof PatternSection.SkipAny);
    assertEquals("a plan", ((PatternSection.ExpectedLiteral) plan.lines.get(1)).line);
    assertEquals("count \\d+  ", ((PatternSection.ExpectedRegex) plan.lines.get(2)).line);
    assertEquals("  ", ((PatternSection.ExpectedLiteral) plan.lines.get(3)).line);
    assertEquals("!foo ", ((PatternSection.ExpectedLiteral) plan.lines.get(4)).line);
  }

  @Test
  public void testSections()
  {
    String input =
        "Example input file\n" +
        "=== case\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== plan\n" +
        "  a plan  \n" +
        "=== explain\n" +
        "  explanation  \n";
    List<QueryTestCase> cases = TestCaseLoader.loadString(input);
    assertEquals(1, cases.size());
    assertNotNull(cases.get(0).plan());
    assertNotNull(cases.get(0).explain());
  }

  @Test
  public void testTrailingComments()
  {
    String input =
        "=== case\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== plan\n" +
        "====\n" +
        "that's all, folks";
    List<QueryTestCase> cases = TestCaseLoader.loadString(input);
    assertEquals(1, cases.size());
    assertTrue(cases.get(0).plan().expected.lines.isEmpty());

    input =
        "=== case\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== plan\n" +
        "===\n";
    cases = TestCaseLoader.loadString(input);
    assertEquals(1, cases.size());
    assertTrue(cases.get(0).plan().expected.lines.isEmpty());
  }

  @Test
  public void testMultipleCases()
  {
    String input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== case\n" +
        "second\n" +
        "=== SQL\n" +
        "SELECT 2\n" +
        "=== plan\n" +
        "second plan\n";
    List<QueryTestCase> cases = TestCaseLoader.loadString(input);
    assertEquals(2, cases.size());
    assertEquals("first", cases.get(0).label);
    assertEquals("second", cases.get(1).label);

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== plan\n" +
        "first plan\n" +
        "=== case\n" +
        "second\n" +
        "=== SQL\n" +
        "SELECT 2\n" +
        "=== plan\n" +
        "second plan\n";
    cases = TestCaseLoader.loadString(input);
    assertEquals(2, cases.size());
    assertEquals("first", cases.get(0).label);
    assertEquals("second", cases.get(1).label);
  }

  @Test
  public void testContext()
  {
    String input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== context\n";
    List<QueryTestCase> cases = TestCaseLoader.loadString(input);
    assertEquals(1, cases.size());
    assertNull(cases.get(0).contextSection());
    assertFalse(cases.get(0).hasRuns());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== context\n" +
        "foo=bar\n" +
        QueryContexts.USE_CACHE_KEY + "=true\n" +
        QueryContexts.TIMEOUT_KEY + "=10\n";
    cases = TestCaseLoader.loadString(input);
    assertEquals(1, cases.size());
    Map<String, Object> context = cases.get(0).contextSection().context;
    assertEquals(3, context.size());
    assertEquals("bar", context.get("foo"));
    assertEquals(true, context.get(QueryContexts.USE_CACHE_KEY));
    assertEquals(10, context.get(QueryContexts.TIMEOUT_KEY));
  }

  @Test
  public void testResources()
  {
    String input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== resources\n";
    List<QueryTestCase> cases = TestCaseLoader.loadString(input);
    assertTrue(cases.get(0).resourceActions().resourceActions.isEmpty());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== resources\n" +
        "foo/bar/" + Action.READ.name() + "\n";
    cases = TestCaseLoader.loadString(input);
    assertEquals(1, cases.get(0).resourceActions().resourceActions.size());
    ResourcesSection.Resource resource = cases.get(0).resourceActions().resourceActions.get(0);
    assertEquals("foo", resource.type);
    assertEquals("bar", resource.name);
    assertEquals(Action.READ, resource.action);
  }

  @Test
  public void testParameters()
  {
    String input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== parameters\n";
    List<QueryTestCase> cases = TestCaseLoader.loadString(input);
    assertTrue(cases.get(0).parameters().isEmpty());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== parameters\n" +
        "int: 10\n" +
        "integer: 20 \n" +
        "long: 30\n" +
        "bigint: 40\n" +
        "float: 50.1  \n" +
        "double: 60.2\n" +
        "string: foo \n" +
        "varchar: bar  \n";
    cases = TestCaseLoader.loadString(input);
    List<SqlParameter> params = cases.get(0).parameters();
    assertEquals(8, params.size());

    assertEquals(SqlType.INTEGER, params.get(0).getType());
    assertEquals(10, params.get(0).getValue());

    assertEquals(SqlType.INTEGER, params.get(1).getType());
    assertEquals(20, params.get(1).getValue());

    assertEquals(SqlType.BIGINT, params.get(2).getType());
    assertEquals(30L, params.get(2).getValue());

    assertEquals(SqlType.BIGINT, params.get(3).getType());
    assertEquals(40L, params.get(3).getValue());

    assertEquals(SqlType.FLOAT, params.get(4).getType());
    assertEquals(50.1F, params.get(4).getValue());

    assertEquals(SqlType.DOUBLE, params.get(5).getType());
    assertEquals(60.2D, params.get(5).getValue());

    assertEquals(SqlType.VARCHAR, params.get(6).getType());
    assertEquals("foo", params.get(6).getValue());

    assertEquals(SqlType.VARCHAR, params.get(7).getType());
    assertEquals("bar", params.get(7).getValue());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== parameters\n" +
        "string: ' foo '\n" +
        "varchar: \"  bar  \"\n";
    cases = TestCaseLoader.loadString(input);
    params = cases.get(0).parameters();
    assertEquals(2, params.size());

    assertEquals(SqlType.VARCHAR, params.get(0).getType());
    assertEquals(" foo ", params.get(0).getValue());

    assertEquals(SqlType.VARCHAR, params.get(1).getType());
    assertEquals("  bar  ", params.get(1).getValue());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== parameters\n" +
        "date: \"2022-05-09\"\n" +
        "timestamp: \"2022-05-09 01:02:03\"\n";
    cases = TestCaseLoader.loadString(input);
    params = cases.get(0).parameters();
    assertEquals(2, params.size());

    assertEquals(SqlType.DATE, params.get(0).getType());
    assertEquals("2022-05-09", params.get(0).getValue());

    assertEquals(SqlType.TIMESTAMP, params.get(1).getType());
    assertEquals("2022-05-09 01:02:03", params.get(1).getValue());
  }

  @Test
  public void testOptions()
  {
    String input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== options\n";
    List<QueryTestCase> cases = TestCaseLoader.loadString(input);
    assertNull(cases.get(0).optionsSection());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== options\n" +
        "p1=foo\n" +
        " p2 = bar \n" +
        "p3=\"  mumble  \"\n";
    cases = TestCaseLoader.loadString(input);
    Map<String, String> options = cases.get(0).optionsSection().options;
    assertEquals(3, options.size());
    assertEquals("foo", options.get("p1"));
    assertEquals("bar", options.get("p2"));
    assertEquals("  mumble  ", options.get("p3"));
    assertFalse(cases.get(0).hasRuns());
  }

  @Test
  public void testCopy()
  {
    {
      final String input =
          "=== case\n" +
          "first\n" +
          "=== SQL copy\n";
      assertThrows(
          IAE.class,
          () -> TestCaseLoader.loadString(input));
    }

    String input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== case\n" +
        "second\n" +
        "=== SQL copy\n";
    List<QueryTestCase> cases = TestCaseLoader.loadString(input);
    assertEquals(cases.get(0).sql(), cases.get(1).sql());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== plan\n" +
        "the plan\n" +
        "=== case\n" +
        "second\n" +
        "=== SQL copy\n" +
        "=== plan copy\n";
    cases = TestCaseLoader.loadString(input);
    assertEquals(cases.get(0).plan(), cases.get(1).plan());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== schema\n" +
        "foo VARCHAR\n" +
        "=== case\n" +
        "second\n" +
        "=== SQL copy\n" +
        "=== schema copy\n";
    cases = TestCaseLoader.loadString(input);
    assertEquals(cases.get(0).schema(), cases.get(1).schema());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== native\n" +
        "foo\n" +
        "=== case\n" +
        "second\n" +
        "=== SQL copy\n" +
        "=== native copy\n";
    cases = TestCaseLoader.loadString(input);
    assertEquals(cases.get(0).nativeQuery(), cases.get(1).nativeQuery());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== resources\n" +
        "druid/foo/READ\n" +
        "=== case\n" +
        "second\n" +
        "=== SQL copy\n" +
        "=== resources copy\n";
    cases = TestCaseLoader.loadString(input);
    assertEquals(cases.get(0).resourceActions(), cases.get(1).resourceActions());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== context\n" +
        "foo=bar\n" +
        "=== case\n" +
        "second\n" +
        "=== SQL copy\n" +
        "=== context copy\n";
    cases = TestCaseLoader.loadString(input);
    assertEquals(cases.get(0).contextSection(), cases.get(1).contextSection());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== options\n" +
        "foo=bar\n" +
        "=== case\n" +
        "second\n" +
        "=== SQL copy\n" +
        "=== options copy\n";
    cases = TestCaseLoader.loadString(input);
    assertEquals(cases.get(0).options(), cases.get(1).options());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== parameters\n" +
        "VARCHAR: foo\n" +
        "=== case\n" +
        "second\n" +
        "=== SQL copy\n" +
        "=== parameters copy\n";
    cases = TestCaseLoader.loadString(input);
    assertEquals(cases.get(0).parameters(), cases.get(1).parameters());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== results\n" +
        "10, 20\n" +
        "=== case\n" +
        "second\n" +
        "=== SQL copy\n" +
        "=== results copy\n";
    cases = TestCaseLoader.loadString(input);
    QueryRun run1 = cases.get(0).runs().get(0);
    QueryRun run2 = cases.get(1).runs().get(0);
    assertEquals(run1.results(), run2.results());
  }

  @Test
  public void testRuns()
  {
    String input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== results\n" +
        "[\"a\", 10]\n";
    List<QueryTestCase> cases = TestCaseLoader.loadString(input);
    QueryRun run = cases.get(0).runs().get(0);
    assertEquals("", run.label());
    assertEquals(1, run.results().size());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== run\n" +
        "=== options\n" +
        "foo=bar\n";
    cases = TestCaseLoader.loadString(input);
    run = cases.get(0).runs().get(0);
    assertEquals("", run.label());
    assertEquals("Run 1", run.displayLabel());
    assertEquals("bar", run.option("foo"));

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== options\n" +
        "x=a\n" +
        "foo=mumble\n" +
        "=== run\n" +
        "=== options\n" +
        "foo=bar\n";
    cases = TestCaseLoader.loadString(input);
    run = cases.get(0).runs().get(0);
    assertEquals("Run 1", run.displayLabel());
    assertEquals("bar", run.option("foo"));
    assertEquals("a", run.option("x"));

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== run\n" +
        "=== options\n" +
        "foo=bar\n" +
        "=== results\n" +
        "[\"a\", 10]\n" +
        "=== run\n" +
        "=== options\n" +
        "user=bob\n" +
        "=== results\n" +
        "[\"b\", 20]\n";
    cases = TestCaseLoader.loadString(input);
    QueryTestCase testCase = cases.get(0);
    assertEquals(2, testCase.runs().size());
    run = testCase.runs().get(0);
    assertEquals("", run.label());
    assertEquals("Run 1", run.displayLabel());
    assertEquals("bar", run.optionsSection().get("foo"));
    assertEquals(1, run.results().size());
    run = testCase.runs().get(1);
    assertEquals("Run 2", run.displayLabel());
    assertEquals("bob", run.optionsSection().get("user"));
    assertEquals(1, run.results().size());

    input =
        "=== case\n" +
        "first\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== run\n" +
        "fast\n" +
        "=== options\n" +
        "user=bob\n";
    cases = TestCaseLoader.loadString(input);
    run = cases.get(0).runs().get(0);
    assertEquals("fast", run.label());
  }
}
