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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.tester.TestSection.Section;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Ad-hoc utilities used to convert a JUnit test to the test framework.
 * Merges the converted test file with an existing file to produce a new
 * file that has the combined results. Handy for picking up changes to
 * the Java code, or for combining a SQL-compatible run with a
 * "replace nulls with defaults" run. Done only once in a while, by hand.
 * The code must be configured (by hand) for the desired test case.
 *
 * Also contains utilities to rewrite test cases in various useful ways.
 * You'll know you need them (or a new version) if you find yourself making
 * repetitive changes.
 */
public class TestCaseMerger
{
  public static void main(String[] args)
  {
    //rewriteFile("query15.case");
    buildQueryTest();
    //buildArrayTest();
    //buildInsertDMLTest();
    //buildJoinTest();
    //buildCorrelatedTest();
    //buildMultiValueTest();
    //buildParameterTest();
  }

  @SuppressWarnings("unused")
  private static void buildParameterTest()
  {
    List<QueryTestCase> existing = TestCaseLoader.loadResource("/calcite/cases/parameterQuery.case");
    rewrite(existing, "org.apache.druid.sql.calcite.CalciteParameterQueryTest");
  }

  @SuppressWarnings("unused")
  private static void buildMultiValueTest()
  {
    List<QueryTestCase> existing = TestCaseLoader.loadResource("/calcite/cases/multiValueStringQuery.case");
    rewrite(existing, "org.apache.druid.sql.calcite.CalciteMultiValueStringQueryTest");
  }

  @SuppressWarnings("unused")
  private static void buildCorrelatedTest()
  {
    List<QueryTestCase> existing = TestCaseLoader.loadResource("/calcite/cases/correlatedQuery.case");
    rewrite(existing, "org.apache.druid.sql.calcite.CalciteCorrelatedQueryTest");
  }

  @SuppressWarnings("unused")
  private static void buildJoinTest()
  {
    List<QueryTestCase> existing = loadSet("joinQuery", 7);
    rewrite(existing, "org.apache.druid.sql.calcite.CalciteJoinQueryTest");
  }

  @SuppressWarnings("unused")
  private static void buildInsertDMLTest()
  {
    // Doesn't work because of the special structure of this test case.
    List<QueryTestCase> existing = TestCaseLoader.loadResource("/calcite/cases/insertDML.case");
    rewrite(existing, "org.apache.druid.sql.calcite.CalciteInsertDmlTest");
  }

  @SuppressWarnings("unused")
  private static void buildArrayTest()
  {
    List<QueryTestCase> existing = TestCaseLoader.loadResource("/calcite/cases/arrayQuery.case");
    rewrite(existing, "org.apache.druid.sql.calcite.CalciteArraysQueryTest");
  }

  @SuppressWarnings("unused")
  private static void buildQueryTest()
  {
    List<QueryTestCase> existing = loadSet("query", 15);
    rewrite(existing, "org.apache.druid.sql.calcite.CalciteQueryTest");
  }

  public static List<QueryTestCase> loadSet(String base, int count)
  {
    List<QueryTestCase> fullList = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      fullList.addAll(TestCaseLoader.loadResource(
          StringUtils.format("/calcite/cases/%s%02d.case", base, i + 1)));
    }
    return fullList;
  }

  public static List<QueryTestCase> loadFile(String filePath)
  {
    return TestCaseLoader.loadFile(new File(filePath));
  }

  private static void rewrite(List<QueryTestCase> existing, String className)
  {
    new TestCaseMerger(existing, className).rewrite();
  }

  public static class TestWrapper
  {
    boolean merged;
    final QueryTestCase testCase;

    public TestWrapper(QueryTestCase testCase)
    {
      this.testCase = testCase;
    }
  }

  private final List<QueryTestCase> existing;
  private final List<QueryTestCase> recorded;
  private final List<String> methods;

  private TestCaseMerger(List<QueryTestCase> existing, String className)
  {
    this.existing = existing;
    this.recorded = TestCaseLoader.loadFile(new File("target/actual/recorded.case"));
    try {
      this.methods = loadTestClass(className);
    }
    catch (Exception e) {
      throw new IAE(e, "Class load failed");
    }
  }

  public static List<String> loadTestClass(String testClass) throws IOException
  {
    String classPath = StringUtils.replace(testClass, ".", "/");
    File file = new File(new File("src/test/java"), classPath + ".java");
    List<String> methods = new ArrayList<>();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                new FileInputStream(file), StandardCharsets.UTF_8))) {
      String line = reader.readLine();
      if (line == null) {
        return methods;
      }
      line = line.trim();
      while (true) {
        while (!line.startsWith("@")) {
          line = reader.readLine();
          if (line == null) {
            return methods;
          }
          line = line.trim();
        }
        boolean ignore = false;
        while (line.startsWith("@")) {
          if (line.startsWith("@Ignore")) {
            ignore = true;
          }
          line = reader.readLine();
          if (line == null) {
            return methods;
          }
          line = line.trim();
        }
        if (ignore) {
          continue;
        }
        while (!line.startsWith("public void test")) {
          line = reader.readLine();
          if (line == null) {
            return methods;
          }
          line = line.trim();
        }
        Pattern p = Pattern.compile("public void (.*)\\(.*\\).*");
        Matcher m = p.matcher(line);
        if (m.matches()) {
          methods.add(m.group(1));
        }
      }
    }
  }

  public void rewrite()
  {
    List<QueryTestCase> rewritten;
    if (existing == null) {
      rewritten = reorder(recorded);
    } else {
      rewritten = merge();
    }
    write(rewritten);
  }

  private List<QueryTestCase> merge()
  {
    // Create an index onto the recorded tests by method name and SQL
    List<TestWrapper> wrapped = new ArrayList<>();
    Map<String, List<TestWrapper>> index = new HashMap<>();
    for (QueryTestCase testCase : recorded) {
      TestWrapper wrapper = new TestWrapper(testCase);
      wrapped.add(wrapper);
      String key = testKey(testCase);
      List<TestWrapper> tests = index.get(key);
      if (tests == null) {
        tests = new ArrayList<>();
        index.put(key, tests);
      }
      tests.add(wrapper);
    }

    // Group existing tests by key. There may be multiple per key.
    List<Pair<String, List<QueryTestCase>>> existingGroups = new ArrayList<>();
    for (QueryTestCase testCase : existing) {
      String key = testKey(testCase);
      if (!existingGroups.isEmpty()) {
        Pair<String, List<QueryTestCase>> prev = existingGroups.get(existingGroups.size() - 1);
        if (prev.lhs.equals(key)) {
          prev.rhs.add(testCase);
          continue;
        }
      }
      List<QueryTestCase> tests = new ArrayList<>();
      tests.add(testCase);
      existingGroups.add(new Pair<>(key, tests));
    }

    // Match up existing tests in file order with recorded tests using
    // method name and SQL.
    List<QueryTestCase> rewritten = new ArrayList<>();
    for (Pair<String, List<QueryTestCase>> tests : existingGroups) {
      List<TestWrapper> recordedCases = index.get(tests.lhs);
      if (recordedCases == null) {
        rewritten.addAll(tests.rhs);
      } else {
        rewritten.addAll(mergeCases(tests.rhs, recordedCases));
      }
    }

    // If any recorded tests remain, add them in method name order.
    List<QueryTestCase> remainder = new ArrayList<>();
    for (TestWrapper wrapper : wrapped) {
      if (!wrapper.merged) {
        remainder.add(wrapper.testCase);
      }
    }
    rewritten.addAll(reorder(remainder));
    return rewritten;
  }

  private List<QueryTestCase> mergeCases(List<QueryTestCase> existingCases, List<TestWrapper> recordedCases)
  {
    // Simple case: only one test case in each category.
    if (existingCases.size() == 1 && recordedCases.size() == 1) {
      TestWrapper wrapper = recordedCases.get(0);
      wrapper.merged = true;
      return Collections.singletonList(mergeCases(existingCases.get(0), wrapper.testCase));
    }

    // Harder case: multiple runs for the same test and SQL, typically differentiated
    // by options or context. Try to match up.
    List<TestWrapper> recordedCopy = new ArrayList<>(recordedCases);
    List<QueryTestCase> merged = new ArrayList<>();
    for (QueryTestCase existing : existingCases) {
      TestWrapper found = null;
      for (int i = 0; i < recordedCopy.size(); i++) {
        if (existing.matches(recordedCopy.get(i).testCase)) {
          found = recordedCopy.remove(i);
          break;
        }
      }
      if (found == null) {
        merged.add(existing);
      } else {
        found.merged = true;
        merged.add(mergeCases(existing, found.testCase));
      }
    }
    for (TestWrapper wrapper : recordedCopy) {
      wrapper.merged = true;
      merged.add(wrapper.testCase);
    }
    return merged;
  }

  private String testKey(QueryTestCase testCase)
  {
    // Compensate for any formatting applied to SQL
    String sql = StringUtils.toUpperCase(testCase.sql());
    sql = StringUtils.replaceAll(sql, "\\s+", " ");
    sql = StringUtils.replace(sql, "( ", "(");
    sql = StringUtils.replace(sql, " )", ")");
    sql = StringUtils.replace(sql, ", ", ",");
    return methodName(testCase) + " - " + sql;
  }

  /**
   * Merge an existing and recorded test case. Prefer the recorded sections,
   * but fill in existing sections where no recorded section exists.
   */
  private QueryTestCase mergeCases(QueryTestCase testCase, QueryTestCase recordedCase)
  {
    QueryTestCase.Builder builder = new QueryTestCase.Builder(testCase.label);
    List<TestSection> recordedSections = new ArrayList<>(recordedCase.fileOrder);

    // First merge the "setup" sections
    outer1:
    for (TestSection existingSection : testCase.fileOrder) {
      switch (existingSection.section()) {
        case COMMENTS:
        case CASE:
        case SQL:
        case CONTEXT:
        case OPTIONS:
        case PARAMETERS:
          break;
        default:
          continue outer1;
      }
      TestSection recordedSection = recordedCase.section(existingSection.section());
      if (recordedSection != null) {
        recordedSections.remove(recordedSection);
      }
      switch (existingSection.section()) {
        case CONTEXT:
        case OPTIONS:
        case PARAMETERS:
          if (recordedSection == null) {
            builder.add(existingSection);
          } else {
            builder.add(recordedSection);
          }
          break;
        default:
          builder.add(existingSection);
          break;
        }
    }

    // Then merge the "plan output" sections
    outer2:
    for (TestSection existingSection : testCase.fileOrder) {
      switch (existingSection.section()) {
        case COMMENTS:
        case CASE:
        case SQL:
        case CONTEXT:
        case OPTIONS:
        case PARAMETERS:
          continue outer2;
        default:
          break;
      }
      TestSection recordedSection = recordedCase.section(existingSection.section());
      if (recordedSection != null) {
        recordedSections.remove(recordedSection);
      }
      switch (existingSection.section()) {
        case ERROR:
        case EXCEPTION:
        case NATIVE:
        case RESOURCES:
        case SCHEMA:
          if (recordedSection == null) {
            builder.add(existingSection);
          } else {
            builder.add(recordedSection);
          }
          break;
        default:
          builder.add(existingSection);
          break;
        }
    }
    for (TestSection recordedSection : recordedSections) {
      builder.add(recordedSection);
    }
    QueryTestCase rewritten = builder.build();

    List<QueryRun> recordedCopy = new ArrayList<>(recordedCase.runs());
    for (QueryRun run : testCase.runs()) {
      boolean found = false;
      // List search because there will usually be only 1 or 2 runs.
      for (int i = 0; i < recordedCopy.size(); i++) {
        QueryRun candidate = recordedCopy.get(i);
        if (Objects.equals(run.results(), candidate.results())) {
          rewritten.addRuns(mergeRuns(rewritten, run, candidate));
          recordedCopy.remove(i);
          found = true;
          break;
        }
      }
      if (!found) {
        rewritten.addRun(run);
      }
    }
    rewritten.addRuns(recordedCopy);
    return rewritten;
  }

  private List<QueryRun> mergeRuns(QueryTestCase newCase, QueryRun existing, QueryRun recorded)
  {
    QueryRun newRun = doMerge(newCase, existing, recorded);
    if (newRun == null) {
      return Arrays.asList(existing.copy(newCase), recorded.copy(newCase, true));
    } else {
      return Collections.singletonList(newRun);
    }
  }

  private QueryRun doMerge(QueryTestCase newCase, QueryRun existing, QueryRun recorded)
  {
    if (!Objects.equals(existing.context(), recorded.context())) {
      return null;
    }
    OptionsSection existingOptions = existing.optionsSection();
    OptionsSection recordedOptions = recorded.optionsSection();
    if (existingOptions == null || recordedOptions == null) {
      return null;
    }
    if (existingOptions.options.size() != 1 || recordedOptions.options.size() != 1) {
      return null;
    }
    String existingOption = existingOptions.options.get(OptionsSection.SQL_COMPATIBLE_NULLS);
    String recordedOption = recordedOptions.options.get(OptionsSection.SQL_COMPATIBLE_NULLS);
    if (existingOption == null || recordedOption == null) {
      return null;
    }

    // Merge. Change the replace nulls option to "both" to indicate the results
    // are the same whether we replace nulls with default or not.
    String newNullsOption;
    if (existingOption.equals(recordedOption)) {
      newNullsOption = existingOption;
    } else {
      newNullsOption = OptionsSection.NULL_HANDLING_BOTH;
    }

    QueryRun.Builder builder = new QueryRun
        .Builder(existing.label())
        .explicit(existing.isExplicit());
    for (TestSection section : existing.sections()) {
      if (section.section() == Section.OPTIONS) {
        builder.add(new OptionsSection(
            ImmutableMap.of(
                OptionsSection.SQL_COMPATIBLE_NULLS,
                newNullsOption)));
      } else {
        builder.add(section);
      }
    }
    return builder.build(newCase);
  }

  private List<QueryTestCase> reorder(List<QueryTestCase> unordered)
  {
    Map<String, List<QueryTestCase>> index = new HashMap<>();
    for (QueryTestCase testCase : unordered) {
      String methodName = methodName(testCase);
      List<QueryTestCase> items = index.get(methodName);
      if (items == null) {
        items = new ArrayList<>();
        index.put(methodName, items);
      }
      items.add(testCase);
    }
    List<QueryTestCase> reordered = new ArrayList<>();
    for (String methodName : methods) {
      List<QueryTestCase> items = index.remove(methodName);
      if (items != null) {
        reordered.addAll(items);
      }
    }
    for (List<QueryTestCase> items : index.values()) {
      reordered.addAll(items);
    }
    return reordered;
  }

  private String methodName(QueryTestCase testCase)
  {
    Pattern p = Pattern.compile("Converted from (.*)\\(\\)");
    Matcher m = p.matcher(testCase.comment());
    if (m.find()) {
      return m.group(1);
    } else {
      return "unknown";
    }
  }

  private void write(List<QueryTestCase> rewritten)
  {
    try (BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(
            new FileOutputStream(new File("target/actual/rewritten.case")),
            StandardCharsets.UTF_8))) {
      TestCaseWriter testWriter = new TestCaseWriter(writer);
      for (QueryTestCase testCase : rewritten) {
        testCase.write(testWriter);
      }
    }
    catch (Exception e) {
      throw new IAE(e, "Can't write output file");
    }
  }

  @SuppressWarnings("unused")
  private static void rewriteFile(String fileName)
  {
    String path = "/calcite/cases/" + fileName;
    try (InputStream is = TestCaseLoader.class.getResourceAsStream(path)) {
      if (is == null) {
        throw new IAE("Cannot open resource: " + path);
      }
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        List<String> lines = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
          lines.add(line);
        }
        rewrite(fileName, lines);
      }
    }
    catch (IOException e) {
      throw new IAE("Cannot close resource: " + path);
    }
  }

  private static void rewrite(String fileName, List<String> lines) throws IOException
  {
    List<List<String>> cases = new ArrayList<>();
    List<String> curCase = new ArrayList<>();
    cases.add(curCase);
    for (String line : lines) {
      if (line.startsWith("======")) {
        curCase = new ArrayList<>();
        cases.add(curCase);
      }
      curCase.add(line);
    }
    cases = rewriteCases(cases);
    File path = new File(new File("target/actual"), fileName);
    try (BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(
            new FileOutputStream(path),
            StandardCharsets.UTF_8))) {
      for (List<String> testCase : cases) {
        for (String line : testCase) {
          writer.append(line);
          writer.append("\n");
        }
      }
    }
  }

  private static List<List<String>> rewriteCases(List<List<String>> cases)
  {
    List<List<String>> revised = new ArrayList<>();
    for (List<String> testCase : cases) {
      revised.add(rewriteCaseMergeOptions(testCase));
    }
    return revised;
  }

  @SuppressWarnings("unused")
  private static List<String> rewriteCaseMoveOptions(List<String> testCase)
  {
    int optionPosn = -1;
    List<String> options = new ArrayList<>();
    for (int i = 0; i < testCase.size(); i++) {
      String line = testCase.get(i);
      if (line.startsWith("=== options")) {
        optionPosn = i;
        options.add(line);
        continue;
      }
      if (optionPosn == -1) {
        continue;
      }
      if (line.startsWith("=== run")) {
        break;
      }
      if (line.startsWith("=== ")) {
        return testCase;
      }
      options.add(line);
    }
    if (optionPosn == -1 || options.isEmpty()) {
      return testCase;
    }
    List<String> rewritten = new ArrayList<>();
    boolean inOptions = false;
    boolean didOptions = false;
    for (String line : testCase) {
      if (line.startsWith("=== schema")) {
        rewritten.addAll(options);
      }
      if (!didOptions && line.startsWith("=== options")) {
        inOptions = true;
        continue;
      }
      if (line.startsWith("=== ")) {
        if (inOptions) {
          didOptions = true;
        }
        inOptions = false;
      }
      if (!inOptions) {
        rewritten.add(line);
      }
    }
    return rewritten;
  }

  public static class SectionLines
  {
    final String name;
    final String heading;
    final List<String> lines = new ArrayList<>();

    public SectionLines(String line)
    {
      this.heading = line;
      if (line.startsWith("====")) {
        this.name = "=";
      } else {
        Pattern p = Pattern.compile("=== ([^ ]+) ?.*");
        Matcher m = p.matcher(line);
        if (!m.matches()) {
          throw new ISE("Unmatched header: " + line);
        }
        this.name = m.group(1);
      }
    }

    public SectionLines()
    {
      this.name = null;
      this.heading = null;
    }

    public void appendTo(List<String> lines)
    {
      if (this.heading != null) {
        lines.add(this.heading);
      }
      lines.addAll(this.lines);
    }
  }

  private static List<SectionLines> parseSections(List<String> testCase)
  {
    List<SectionLines> sections = new ArrayList<>();
    SectionLines currentSection = null;
    for (String line : testCase) {
      if (line.startsWith("===")) {
        currentSection = new SectionLines(line);
        sections.add(currentSection);
      } else {
        if (currentSection == null) {
          currentSection = new SectionLines();
          sections.add(currentSection);
        }
        currentSection.lines.add(line);
      }
    }
    return sections;
  }

  @SuppressWarnings("unused")
  private static List<String> unparseSections(List<SectionLines> sections)
  {
    List<String> testCase = new ArrayList<>();
    for (SectionLines section : sections) {
      section.appendTo(testCase);
    }
    return testCase;
  }

  private static List<String> rewriteCaseMergeOptions(List<String> testCase)
  {
    List<SectionLines> sections = parseSections(testCase);
    int runCount = 0;
    SectionLines mainOptions = null;
    SectionLines runOptions = null;

    for (SectionLines section : sections) {
      if (section.name == null) {
        return testCase;
      } else if ("options".equals(section.name)) {
        if (runCount == 0) {
          mainOptions = section;
        } else if (runOptions != null) {
          return testCase;
        } else {
          runOptions = section;
        }
      } else if ("run".equals(section.name)) {
        runCount++;
        if (runCount > 1) {
          return testCase;
        }
      }
    }
    if (runCount == 0 || mainOptions == null || runOptions == null) {
      return testCase;
    }

    List<String> rewritten = new ArrayList<>();
    for (SectionLines section : sections) {
      if (section == mainOptions) {
        rewritten.add(section.heading);
        rewritten.addAll(runOptions.lines);
        rewritten.addAll(mainOptions.lines);
      } else if (section == runOptions) {
        // Skip
      } else if ("run".equals(section.name)) {
        // Skip
      } else {
        section.appendTo(rewritten);
      }
    }
    return rewritten;
  }
}
