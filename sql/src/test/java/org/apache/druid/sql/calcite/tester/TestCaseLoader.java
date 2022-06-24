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

import com.google.common.base.Strings;
import org.apache.calcite.avatica.SqlType;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.Action;
import org.apache.druid.sql.calcite.tester.LinesSection.CaseSection;
import org.apache.druid.sql.calcite.tester.LinesSection.CommentsSection;
import org.apache.druid.sql.calcite.tester.LinesSection.ResultsSection;
import org.apache.druid.sql.calcite.tester.PatternSection.ExpectedLine;
import org.apache.druid.sql.calcite.tester.PatternSection.ExpectedRegex;
import org.apache.druid.sql.calcite.tester.PatternSection.ExpectedText;
import org.apache.druid.sql.calcite.tester.PatternSection.SkipAny;
import org.apache.druid.sql.calcite.tester.TestSection.Section;
import org.apache.druid.sql.calcite.tester.TextSection.ExceptionSection;
import org.apache.druid.sql.calcite.tester.TextSection.SqlSection;
import org.apache.druid.sql.http.SqlParameter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Loads and parses a test case file, producing a list of test cases.
 */
public class TestCaseLoader
{
  public static List<QueryTestCase> loadResource(String resource)
  {
    try (InputStream is = TestCaseLoader.class.getResourceAsStream(resource)) {
      if (is == null) {
        throw new IAE("Cannot open resource: " + resource);
      }
      return load(new InputStreamReader(is, StandardCharsets.UTF_8), resource);
    }
    catch (IOException e) {
      throw new IAE("Cannot close resource: " + resource);
    }
  }

  public static List<QueryTestCase> loadFile(File file)
  {
    try {
      try (InputStream is = new FileInputStream(file)) {
        return load(new InputStreamReader(is, StandardCharsets.UTF_8), file.getName());
      }
    }
    catch (IOException e) {
      throw new IAE("Cannot open file: " + file.getAbsolutePath());
    }
  }

  public static List<QueryTestCase> loadString(String string)
  {
    return load(new StringReader(string), "<string>");
  }

  public static List<QueryTestCase> load(Reader reader, String label)
  {
    return new TestCaseLoader(reader, label).load();
  }

  private final String sourceLabel;
  private final LineNumberReader reader;
  private final List<QueryTestCase> testCases = new ArrayList<>();
  private QueryTestCase.Builder testCase;
  private QueryTestCase prevCase;
  private QueryRun.Builder queryRun;
  private String pushed;
  private List<String> comment;
  private int sectionStartLine;

  public TestCaseLoader(Reader reader, String label)
  {
    this.reader = new LineNumberReader(reader);
    this.sourceLabel = label;
  }

  public List<QueryTestCase> load()
  {
    // Ignore leading text
    if (!skipComments()) {
      return testCases;
    }
    while (loadCase()) {
      // Empty
    }
    return testCases;
  }

  private String next()
  {
    if (pushed != null) {
      String ret = pushed;
      pushed = null;
      return ret;
    }
    try {
      return reader.readLine();
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to read query config file: " + sourceLabel);
    }
  }

  private void push(String line)
  {
    pushed = line;
  }

  private Pair<String, String> parseSection(String expected, boolean expectCase)
  {
    while (true) {
      String line = next();
      if (line == null) {
        return null;
      }
      if (line.startsWith("====")) {
        if (!skipComments()) {
          return null;
        }
        continue;
      }
      if (line.startsWith("===#") || "===".equals(line)) {
        continue;
      }
      if (!line.startsWith("=== ")) {
        throw new IAE(
            StringUtils.format(
                "[%s:%d]: Expected comments or === %s",
                sourceLabel,
                reader.getLineNumber(),
                expected));
      }
      String tail = line.substring(4).trim();
      if (tail.length() == 0 || tail.charAt(0) == '#') {
        continue;
      }
      Pair<String, String> result;
      int posn = tail.indexOf(' ');
      if (posn == -1) {
        result = Pair.of(tail, "");
      } else {
        result = Pair.of(
            tail.substring(0, posn),
            tail.substring(posn + 1).trim());
      }
      if (!expectCase && "case".equalsIgnoreCase(result.lhs)) {
        push(line);
      }
      return result;
    }
  }

  private boolean loadCase()
  {
    if (!loadCaseSection()) {
      return false;
    }
    while (loadSection()) {
      comment = null;
    }
    prevCase = testCase.build();
    if (prevCase.sqlSection() == null) {
      throw new IAE(
          StringUtils.format(
              "[%s:%d]: missing sql section",
              sourceLabel,
              sectionStartLine));
    }
    testCases.add(prevCase);
    testCase = null;
    queryRun = null;
    return true;
  }

  private boolean loadSection()
  {
    sectionStartLine = reader.getLineNumber() + 1;
    Pair<String, String> parts = parseSection("<section>", false);
    if (parts == null) {
      return false;
    }
    boolean copy = "copy".equals(parts.rhs);
    if (copy) {
      if (prevCase == null) {
        throw new IAE(
            StringUtils.format(
                "[%s:%d]: Section has \"copy\" option, but is the first case: %s",
                sourceLabel,
                sectionStartLine,
                parts.lhs
                ));
      }
    }
    switch (StringUtils.toLowerCase(parts.lhs)) {
      case "case":
        return false;
      case "sql":
        return loadQuery(copy);
      case "ast":
        return loadPattern(Section.AST, parts.lhs, copy);
      case "plan":
        return loadPattern(Section.PLAN, parts.lhs, copy);
      case "execplan":
        return loadPattern(Section.EXEC_PLAN, parts.lhs, copy);
      case "explain":
        return loadPattern(Section.EXPLAIN, parts.lhs, copy);
      case "unparsed":
        return loadPattern(Section.UNPARSED, parts.lhs, copy);
      case "schema":
        return loadPattern(Section.SCHEMA, parts.lhs, copy);
      case "targetschema":
        return loadPattern(Section.TARGET_SCHEMA, parts.lhs, copy);
      case "native":
        return loadPattern(Section.NATIVE, parts.lhs, copy);
      case "resources":
        return loadResources(copy);
      case "context":
        return loadContext(copy);
      case "exception":
        return loadException(copy);
      case "error":
        return loadError(parts.lhs, copy);
      case "parameters":
        return loadParameters(copy);
      case "results":
        return loadResults(copy);
      case "options":
        return loadOptions(copy);
      case "run":
        return loadRun();
      default:
        throw new IAE(
            StringUtils.format(
                "[%s:%d]: unknown section [%s]",
                sourceLabel,
                sectionStartLine,
                parts.lhs));
    }
  }

  private boolean loadCaseSection()
  {
    Pair<String, String> parts = parseSection("case", true);
    if (parts == null) {
      return false;
    }
    int startLine = reader.getLineNumber();
    if (!"case".equals(parts.lhs)) {
      throw new IAE(
          StringUtils.format(
              "[%s:%d]: First section must be case",
              sourceLabel,
              startLine));
    }
    Pair<String, Boolean> loaded = loadText();
    String label = loaded.lhs.trim();
    if (label.length() == 0) {
      label = StringUtils.format("Case at line %d", startLine);
    }
    testCase = new QueryTestCase.Builder(label);
    if (comment != null) {
      testCase.add(new CommentsSection(comment));
    }
    comment = null;
    testCase.add(new CaseSection(Collections.singletonList(label)));
    return loaded.rhs;
  }

  private boolean loadQuery(boolean copy)
  {
    Pair<String, Boolean> parsed = requireText(Section.SQL, copy);
    TestSection section;
    if (copy) {
      section = prevCase.copySection(Section.SQL);
    } else {
      String sql = parsed.lhs.trim();
      if (Strings.isNullOrEmpty(sql)) {
        throw new IAE(
            StringUtils.format(
                "[%s:%d]: SQL text is missing",
                sourceLabel,
                reader.getLineNumber()));
      }
      section = new SqlSection("SQL", sql);
    }
    testCase.add(section);
    return parsed.rhs;
  }

  private Pair<String, Boolean> requireText(Section section, boolean copy)
  {
    Pair<String, Boolean> parsed = loadText();
    if (copy) {
      if (!Strings.isNullOrEmpty(parsed.lhs)) {
        throw sectionNotEmptyError(section);
      }
    } else {
      if (Strings.isNullOrEmpty(parsed.lhs)) {
        throw new IAE(
            StringUtils.format(
                "[%s:%d]: %s text is missing",
                sourceLabel,
                sectionStartLine,
                section.sectionName()));
      }
    }
    return parsed;
  }

  private boolean loadPattern(Section section, String sectionName, boolean copy)
  {
    Pair<List<ExpectedLine>, Boolean> result = requireExpected(section, copy);
    TestSection patternSection;
    if (copy) {
      patternSection = copySection(section);
    } else {
      patternSection = new PatternSection(section, sectionName, new ExpectedText(result.lhs));
    }
    testCase.add(patternSection);
    return result.rhs;
  }

  private TestSection copySection(Section section)
  {
    TestSection copy = prevCase.copySection(section);
    if (copy == null) {
      throw noPrevSectionError(section);
    }
    return copy;
  }

  private Pair<List<ExpectedLine>, Boolean> requireExpected(Section section, boolean copy)
  {
    Pair<List<ExpectedLine>, Boolean> result = loadExpected();
    if (copy && !result.lhs.isEmpty()) {
      throw sectionNotEmptyError(section);
    }
    return result;
  }

  private IAE sectionNotEmptyError(Section section)
  {
    return new IAE(
        StringUtils.format(
            "[%s:%d]: %s section - \"copy\" option set, but section is not empty",
            sourceLabel,
            sectionStartLine,
            section.sectionName()));
  }

  private IAE noPrevSectionError(Section section)
  {
    throw new IAE(
        StringUtils.format(
            "[%s:%d]: %s section - \"copy\" option set, but previous test doesn't have that section",
            sourceLabel,
            sectionStartLine,
            section.sectionName()));
  }

  private boolean loadException(boolean copy)
  {
    Pair<String, Boolean> loaded = loadText();
    TestSection exSection;
    if (copy) {
      if (!Strings.isNullOrEmpty(loaded.lhs)) {
        throw sectionNotEmptyError(Section.EXCEPTION);
      }
      exSection = copySection(Section.EXCEPTION);
    } else {
      exSection = new ExceptionSection(loaded.lhs.trim());
    }
    addCommonSection(exSection, copy);
    return loaded.rhs;
  }

  private boolean loadError(String sectionName, boolean copy)
  {
    Pair<List<ExpectedLine>, Boolean> result = requireExpected(Section.ERROR, copy);
    TestSection testSection;
    if (copy) {
      testSection = copySection(Section.ERROR);
    } else {
      testSection = new PatternSection(Section.ERROR, sectionName, new ExpectedText(result.lhs));
    }
    addCommonSection(testSection, copy);
    return result.rhs;
  }

  private Pair<List<ExpectedLine>, Boolean> loadExpected()
  {
    Pair<List<String>, Boolean> loaded = loadLines();
    List<ExpectedLine> lines = new ArrayList<>();
    for (String line : loaded.lhs) {
      if (line.startsWith("!")) {
        lines.add(new ExpectedRegex(line.substring(1)));
        continue;
      }
      if ("**".equals(line)) {
        lines.add(new SkipAny());
        continue;
      }
      if (line.startsWith("\\")) {
        line = line.substring(1);
      }
      lines.add(new PatternSection.ExpectedLiteral(line));
    }
    return Pair.of(lines, loaded.rhs);
  }

  private boolean loadContext(boolean copy)
  {
    Pair<String, Boolean> loaded = loadText();
    String text = loaded.lhs;
    TestSection contextSection;
    if (copy) {
      if (!Strings.isNullOrEmpty(text)) {
        throw sectionNotEmptyError(Section.CONTEXT);
      }
      contextSection = copySection(Section.CONTEXT);
    } else {
      Properties props = new Properties();
      try {
        props.load(new StringReader(text));
      }
      catch (IOException e) {
        throw new IAE(
            StringUtils.format(
                "[%s:%d]: failed to parse context: %s",
                sourceLabel,
                sectionStartLine,
                e.getMessage()));
      }
      if (props.isEmpty()) {
        contextSection = null;
      } else {
        Map<String, Object> context = new HashMap<>();
        for (Entry<Object, Object> entry : props.entrySet()) {
          String key = entry.getKey().toString();
          context.put(
              key,
              QueryTestCases.definition(key).parse(
                  entry.getValue().toString()));
        }
        contextSection = new ContextSection(context);
      }
    }
    addCommonSection(contextSection, copy);
    return loaded.rhs;
  }

  private void addCommonSection(TestSection section, boolean copy)
  {
    if (queryRun == null) {
      testCase.add(section);
    } else if (copy) {
      throw new IAE(
          StringUtils.format(
              "[%s:%d]: Cannot use \"copy\" option in run section",
              sourceLabel,
              sectionStartLine));
    } else {
      queryRun.add(section);
    }
  }

  private void addRunSection(TestSection section, boolean copy)
  {
    if (queryRun == null) {
      queryRun = testCase.addRun("", false);
    } else if (copy) {
      throw new IAE(
          StringUtils.format(
              "[%s:%d]: Cannot use \"copy\" option in run section",
              sourceLabel,
              sectionStartLine));
    }
    queryRun.add(section);
  }

  private boolean loadResources(boolean copy)
  {
    Pair<List<String>, Boolean> loaded = loadLines();
    TestSection resourceSection;
    if (copy) {
      resourceSection = copySection(Section.RESOURCES);
    } else {
      List<ResourcesSection.Resource> resourceActions = new ArrayList<>();
      for (String entry : loaded.lhs) {
        String[] parts = entry.split("/");
        if (parts.length != 3) {
          throw new IAE(
              StringUtils.format(
                  "[%s:%d]: Resources is not in type/name/action format: [%s]",
                  sourceLabel,
                  sectionStartLine,
                  entry));
        }
        Action action = Action.fromString(parts[2]);
        if (action == null) {
          throw new IAE(
            StringUtils.format(
                "[%s:%d]: Invalid action: [%s]",
                sourceLabel,
                sectionStartLine,
                parts[2]));
        }
        resourceActions.add(new ResourcesSection.Resource(parts[0], parts[1], action));
      }
      resourceSection = new ResourcesSection(resourceActions);
    }
    testCase.add(resourceSection);
    return loaded.rhs;
  }

  private Pair<List<String>, Boolean> requireLines(Section section, boolean copy)
  {
    Pair<List<String>, Boolean> loaded = loadLines();
    if (loaded.lhs.isEmpty()) {
      return loaded;
    }
    if (copy) {
      throw sectionNotEmptyError(section);
    }
    return loaded;
  }

  private boolean loadParameters(boolean copy)
  {
    Pair<List<String>, Boolean> loaded = requireLines(Section.PARAMETERS, copy);
    TestSection paramsSection;
    if (copy) {
      paramsSection = copySection(Section.PARAMETERS);
    } else {
      List<SqlParameter> parameters = new ArrayList<>();
      for (int i = 0; i < loaded.lhs.size(); i++) {
        String entry = loaded.lhs.get(i);
        if ("null".equals(entry)) {
          parameters.add(null);
          continue;
        }
        int posn = entry.indexOf(':');
        if (posn == -1) {
          throw new IAE(
              StringUtils.format(
                  "[%s:%d]: Parameter is not in type: value format: [%s]",
                  sourceLabel,
                  sectionStartLine,
                  entry));
        }
        String type = StringUtils.toLowerCase(entry.substring(0, posn).trim());
        String value = entry.substring(posn + 1).trim();
        try {
          parameters.add(parseParameter(type, value));
        }
        catch (Exception e) {
          throw new IAE(
              StringUtils.format(
                  "[%s:%d]: parameter [%s]: %s",
                  sourceLabel,
                  sectionStartLine,
                  entry,
                  e.getMessage()));
        }
      }
      paramsSection = new ParametersSection(parameters);
    }
    testCase.add(paramsSection);
    return loaded.rhs;
  }

  public static SqlParameter parseParameter(String type, String value)
  {
    if ("int".equalsIgnoreCase(type)) {
      type = SqlType.INTEGER.name();
    } else if ("long".equalsIgnoreCase(type)) {
      type = SqlType.BIGINT.name();
    } else if ("string".equalsIgnoreCase(type)) {
      type = SqlType.VARCHAR.name();
    }
    SqlType sqlType = SqlType.valueOf(StringUtils.toUpperCase(type));
    if (sqlType == null) {
      throw new RuntimeException("Unsupported parameter type: " + type);
    }
    if ("\\N".equals(value)) {
      return new SqlParameter(sqlType, null);
    }
    Object sqlValue;
    switch (sqlType) {
      case INTEGER:
        sqlValue = Integer.parseInt(value);
        break;
      case BIGINT:
        sqlValue = Long.parseLong(value);
        break;
      case FLOAT:
      case REAL:
        sqlValue = Float.parseFloat(value);
        break;
      case DOUBLE:
        sqlValue = Double.parseDouble(value);
        break;
      case VARCHAR:
        sqlValue = QueryTestCases.unquote(value);
        break;
      case TIMESTAMP:
      case DATE:
        // Timestamps seem to appear as both quoted strings and numbers.
        sqlValue = QueryTestCases.unquote(value);
        break;
      default:
        throw new RuntimeException("Unsupported SQL type: " + type);
    }
    return new SqlParameter(sqlType, sqlValue);
  }

  private boolean loadOptions(boolean copy)
  {
    Pair<List<String>, Boolean> loaded = loadLines();
    TestSection optionsSection;
    if (copy) {
      optionsSection = copySection(Section.OPTIONS);
    } else {
      Map<String, String> options = new HashMap<>();
      for (int i = 0; i < loaded.lhs.size(); i++) {
        String line = loaded.lhs.get(i);
        int posn = line.indexOf('=');
        if (posn == -1) {
          throw new IAE(
              StringUtils.format(
                  "[%s:%d]: Option is not in key=value format: [%s]",
                  sourceLabel,
                  sectionStartLine,
                  line));
        }
        String key = line.substring(0, posn).trim();
        String value = QueryTestCases.unquote(line.substring(posn + 1).trim());
        options.put(key, value);
      }
      if (options.isEmpty()) {
        optionsSection = null;
      } else {
        optionsSection = new OptionsSection(options);
      }
    }
    addCommonSection(optionsSection, copy);
    return loaded.rhs;
  }

  private boolean loadResults(boolean copy)
  {
    Pair<List<String>, Boolean> loaded = loadLines();
    TestSection resultsSection;
    if (copy) {
      if (prevCase.runs().size() != 1) {
        throw new IAE(
            StringUtils.format(
                "[%s:%d]: Can only copy results if previous test has only one run. Previous has %d",
                sourceLabel,
                sectionStartLine,
                prevCase.runs().size()));
      }
      TestSection prevSection = prevCase.runs().get(0).section(Section.RESULTS);
      if (prevSection == null) {
        throw noPrevSectionError(Section.RESULTS);
      }
      resultsSection = prevSection.copy();
    } else {
      resultsSection = new ResultsSection(loaded.lhs);
    }
    addRunSection(resultsSection, copy);
    return loaded.rhs;
  }

  private boolean loadRun()
  {
    Pair<String, Boolean> parsed = loadText();
    String label = parsed.lhs.trim();
    queryRun = testCase.addRun(label, true);
    return parsed.rhs;
  }

  private Pair<List<String>, Boolean> loadLines()
  {
    List<String> lines = new ArrayList<>();
    String line;
    while ((line = next()) != null) {
      if (line.startsWith("===")) {
        push(line);
        break;
      }
      lines.add(line);
    }
    return Pair.of(lines, line != null);
  }

  private Pair<String, Boolean> loadText()
  {
    Pair<List<String>, Boolean> lines = loadLines();
    // Preserve final newline if any text appears
    lines.lhs.add("");
    String text = String.join("\n", lines.lhs);
    return Pair.of(text, lines.rhs);
  }

  private boolean skipComments()
  {
    Pair<List<String>, Boolean> loaded = loadLines();
    comment = loaded.lhs;
    return loaded.rhs;
  }
}
