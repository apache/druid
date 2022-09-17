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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.sql.calcite.tester.TestSetSpec.SectionSpec;
import org.apache.druid.sql.calcite.tester.TestSetSpec.TestCaseSpec;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
    assertTrue(TestCaseLoader.loadString(input).cases().isEmpty());
    input = "  ";
    assertTrue(TestCaseLoader.loadString(input).cases().isEmpty());
    input = "\n";
    assertTrue(TestCaseLoader.loadString(input).cases().isEmpty());
    input = "  \n\n";
    assertTrue(TestCaseLoader.loadString(input).cases().isEmpty());
  }

  @Test
  public void testLeadingComments()
  {
    String input =
        "I'm a comment";
    assertTrue(TestCaseLoader.loadString(input).cases().isEmpty());
    input =
        "I'm a comment\n";
    assertTrue(TestCaseLoader.loadString(input).cases().isEmpty());
    input =
        "I'm a comment\n" +
        "and so am I\n";
    assertTrue(TestCaseLoader.loadString(input).cases().isEmpty());
    input =
        "I'm a comment\n" +
        "====\n" +
        "and so am I\n";
    assertTrue(TestCaseLoader.loadString(input).cases().isEmpty());
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
    assertTrue(TestCaseLoader.loadString(input).cases().isEmpty());
    input =
        "====";
    assertTrue(TestCaseLoader.loadString(input).cases().isEmpty());
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
    List<TestCaseSpec> cases = TestCaseLoader.loadString(input).cases();
    assertEquals(1, cases.size());
    assertEquals("SELECT 1", cases.get(0).section("sql").toText().trim());
    assertEquals("Case at line 1", cases.get(0).label());

    input =
        "\n" +
        "====\n" +
        "some comment\n" +
        "=== case\n" +
        "=== SQL\n" +
        "SELECT 1\n";
    cases = TestCaseLoader.loadString(input).cases();
    assertEquals(1, cases.size());
    assertEquals("SELECT 1", cases.get(0).section("sql").toText().trim());
    assertEquals("Case at line 4", cases.get(0).label());

    input =
        "=== case\n" +
        "second\n" +
        "=== SQL\n" +
        "SELECT foo\n" +
        " FROM bar\n";
    cases = TestCaseLoader.loadString(input).cases();
    assertEquals(1, cases.size());
    assertEquals("SELECT foo\n FROM bar", cases.get(0).section("sql").toText().trim());
    assertEquals("second", cases.get(0).label());
  }

  @Test
  public void testSections()
  {
    String input =
        "Example input file\n" +
        "====\n" +
        "Example case\n" +
        "=== case\n" +
        "My case\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== plan\n" +
        "  a plan  \n" +
        "=== explain\n" +
        "  explanation  \n";
    List<TestCaseSpec> cases = TestCaseLoader.loadString(input).cases();
    assertEquals(1, cases.size());
    TestCaseSpec testCase = cases.get(0);
    SectionSpec section = testCase.section("plan");
    assertNotNull(section);
    assertEquals(1, section.lines.size());
    assertEquals(8, section.startLine);
    section = testCase.section("explain");
    assertEquals(1, section.lines.size());
    assertEquals(10, section.startLine);
    List<String> comments = testCase.comments();
    assertEquals(1, comments.size());
    assertEquals("Example case", comments.get(0));
    assertEquals("My case", testCase.label());
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
    List<TestCaseSpec> cases = TestCaseLoader.loadString(input).cases();
    assertEquals(1, cases.size());
    assertTrue(cases.get(0).section("plan").lines.isEmpty());

    input =
        "=== case\n" +
        "=== SQL\n" +
        "SELECT 1\n" +
        "=== plan\n" +
        "===\n";
    cases = TestCaseLoader.loadString(input).cases();
    assertEquals(1, cases.size());
    assertTrue(cases.get(0).section("plan").lines.isEmpty());
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
    List<TestCaseSpec> cases = TestCaseLoader.loadString(input).cases();
    assertEquals(2, cases.size());
    assertEquals("first", cases.get(0).label());
    assertEquals(1, cases.get(0).startLine());
    assertEquals("second", cases.get(1).label());
    assertEquals(5, cases.get(1).startLine());

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
    cases = TestCaseLoader.loadString(input).cases();
    assertEquals(2, cases.size());
    assertEquals("first", cases.get(0).label());
    assertEquals("second", cases.get(1).label());
  }
}
