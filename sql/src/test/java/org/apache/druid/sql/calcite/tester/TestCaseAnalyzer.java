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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.Action;
import org.apache.druid.sql.calcite.tester.LinesSection.CaseSection;
import org.apache.druid.sql.calcite.tester.LinesSection.ResultsSection;
import org.apache.druid.sql.calcite.tester.PatternSection.ExpectedLine;
import org.apache.druid.sql.calcite.tester.PatternSection.ExpectedRegex;
import org.apache.druid.sql.calcite.tester.PatternSection.ExpectedText;
import org.apache.druid.sql.calcite.tester.PatternSection.SkipAny;
import org.apache.druid.sql.calcite.tester.TestSection.Section;
import org.apache.druid.sql.calcite.tester.TestSetSpec.SectionSpec;
import org.apache.druid.sql.calcite.tester.TestSetSpec.TestCaseSpec;
import org.apache.druid.sql.calcite.tester.TextSection.ExceptionSection;
import org.apache.druid.sql.calcite.tester.TextSection.SqlSection;
import org.apache.druid.sql.http.SqlParameter;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public class TestCaseAnalyzer
{
  private final TestSetSpec setSpec;
  private final List<QueryTestCase> testCases = new ArrayList<>();
  private QueryTestCase.Builder testCase;
  private QueryTestCase prevCase;
  private QueryRun.Builder queryRun;

  public TestCaseAnalyzer(final TestSetSpec setSpec)
  {
    this.setSpec = setSpec;
  }

  public String sourceLabel()
  {
    return setSpec.source();
  }

  public List<QueryTestCase> analyze()
  {
    for (TestCaseSpec testSpec : setSpec.cases()) {
      testCases.add(analyzeTestCase(testSpec));
    }
    return testCases;
  }

  private QueryTestCase analyzeTestCase(TestCaseSpec spec)
  {
    testCase = new QueryTestCase.Builder(spec.label());
    for (SectionSpec sectionSpec : spec.sections()) {
      analyzeSection(sectionSpec);
    }
    prevCase = testCase.build();
    if (prevCase.sqlSection() == null) {
      throw new IAE(
          StringUtils.format(
              "[%s:%d]: missing sql section",
              sourceLabel(),
              spec.startLine()
          )
      );
    }
    testCase = null;
    queryRun = null;
    return prevCase;
  }

  private void analyzeSection(SectionSpec sectionSpec)
  {
    switch (StringUtils.toLowerCase(sectionSpec.name)) {
      case "case":
        analyzeCase(sectionSpec);
        return;
      case "run":
        analyzeRun(sectionSpec);
        return;
      default:
        break;
    }
    Section sectionKind = parseSectionName(sectionSpec);
    if ("copy".equalsIgnoreCase(sectionSpec.arg)) {
      copySection(sectionSpec, sectionKind);
    } else {
      addSection(analyzeSection(sectionSpec, sectionKind));
    }
  }

  private void analyzeCase(SectionSpec sectionSpec)
  {
    testCase.add(new CaseSection(sectionSpec.lines));
  }

  private TestSection analyzeSection(SectionSpec sectionSpec, Section sectionKind)
  {
    switch (sectionKind) {
      case SQL:
        return analyzeQuery(sectionSpec);
      case AST:
      case PLAN:
      case EXEC_PLAN:
      case EXPLAIN:
      case UNPARSED:
      case SCHEMA:
      case TARGET_SCHEMA:
      case NATIVE:
      case ERROR:
        return analyzePattern(sectionKind, sectionSpec);
      case RESOURCES:
        return analyzeResources(sectionSpec);
      case CONTEXT:
        return analyzeContext(sectionSpec);
      case EXCEPTION:
        return analyzeException(sectionSpec);
      case PARAMETERS:
        return analyzeParameters(sectionSpec);
      case RESULTS:
        return analyzeResults(sectionSpec);
      case OPTIONS:
        return analyzeOptions(sectionSpec);
      default:
        throw new IAE(
            StringUtils.format(
                "[%s:%d]: unknown section [%s]",
                sourceLabel(),
                sectionSpec.startLine,
                sectionSpec.name
            )
        );
    }
  }

  private Section parseSectionName(SectionSpec sectionSpec)
  {
    Section section = Section.forSection(sectionSpec.name);
    if (section == null) {
      throw new IAE(
          StringUtils.format(
              "[%s:%d]: unknown section [%s]",
              sourceLabel(),
              sectionSpec.startLine,
              sectionSpec.name
          )
      );
    }
    return section;
  }

  private void copySection(SectionSpec sectionSpec, Section sectionKind)
  {
    if (prevCase == null) {
      throw new IAE(
          StringUtils.format(
              "[%s:%d]: Section has \"copy\" option, but is the first case: %s",
              sourceLabel(),
              sectionSpec.startLine,
              sectionSpec.name
          )
      );
    }
    if (!sectionSpec.lines.isEmpty()) {
      throw new IAE(
          StringUtils.format(
              "[%s:%d]: %s section - \"copy\" option set, but section is not empty",
              sourceLabel(),
              sectionSpec.startLine,
              sectionSpec.name
          )
      );
    }
    if (sectionKind == Section.RESULTS && queryRun != null) {
      throw new IAE(
          StringUtils.format(
              "[%s:%d]: Cannot use \"copy\" option in run section",
              sourceLabel(),
              sectionSpec.startLine
          )
      );
    }
    TestSection copiedSection;
    if (sectionKind == Section.RESULTS) {
      if (prevCase.runs().size() != 1) {
        throw new IAE(
            StringUtils.format(
                "[%s:%d]: Can only copy results if previous test has only one run. Previous has %d",
                sourceLabel(),
                sectionSpec.startLine,
                prevCase.runs().size()
            )
        );
      }
      copiedSection = prevCase.runs().get(0).section(Section.RESULTS);
    } else {
      copiedSection = prevCase.copySection(sectionKind);
    }
    if (copiedSection == null) {
      throw new IAE(
          StringUtils.format(
              "[%s:%d]: %s section - \"copy\" option set, but previous test doesn't have that section",
              sourceLabel(),
              sectionSpec.startLine,
              sectionKind.sectionName()
          )
      );
    }
    addSection(copiedSection);
  }

  private void analyzeRun(SectionSpec sectionSpec)
  {
    String label = sectionSpec.toText().trim();
    queryRun = testCase.addRun(label, true);
  }

  private void addSection(TestSection section)
  {
    if (section == null) {
      return;
    }
    switch (section.section()) {
      case RESULTS:
        if (queryRun == null) {
          queryRun = testCase.addRun("", false);
        }
        queryRun.add(section);
        break;
      case ERROR:
      case EXCEPTION:
      case OPTIONS:
        if (queryRun == null) {
          testCase.add(section);
        } else {
          queryRun.add(section);
        }
        break;
      default:
        testCase.add(section);
    }
  }

  private TestSection analyzeQuery(SectionSpec sectionSpec)
  {
    String sql = sectionSpec.toText().trim();
    if (Strings.isNullOrEmpty(sql)) {
      throw new IAE(
          StringUtils.format(
              "[%s:%d]: SQL text is missing",
              sourceLabel(),
              sectionSpec.startLine
          )
      );
    }
    return new SqlSection("SQL", sql);
  }

  private TestSection analyzePattern(Section section, SectionSpec sectionSpec)
  {
    List<ExpectedLine> result = analyzeExpected(sectionSpec);
    return new PatternSection(section, sectionSpec.name, new ExpectedText(result));
  }

  private List<ExpectedLine> analyzeExpected(SectionSpec sectionSpec)
  {
    List<ExpectedLine> lines = new ArrayList<>();
    for (String line : sectionSpec.lines) {
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
    return lines;
  }

  private TestSection analyzeResources(SectionSpec sectionSpec)
  {
    List<ResourcesSection.Resource> resourceActions = new ArrayList<>();
    for (String entry : sectionSpec.lines) {
      String[] parts = entry.split("/");
      if (parts.length != 3) {
        throw new IAE(
            StringUtils.format(
                "[%s:%d]: Resources is not in type/name/action format: [%s]",
                sourceLabel(),
                sectionSpec.startLine,
                entry
            )
        );
      }
      Action action = Action.fromString(parts[2]);
      if (action == null) {
        throw new IAE(
          StringUtils.format(
              "[%s:%d]: Invalid action: [%s]",
              sourceLabel(),
              sectionSpec.startLine,
              parts[2]
          )
        );
      }
      resourceActions.add(new ResourcesSection.Resource(parts[0], parts[1], action));
    }
    return new ResourcesSection(resourceActions);
  }

  private TestSection analyzeContext(SectionSpec sectionSpec)
  {
    Properties props = new Properties();
    try {
      props.load(new StringReader(sectionSpec.toText()));
    }
    catch (IOException e) {
      throw new IAE(
          StringUtils.format(
              "[%s:%d]: failed to parse context: %s",
              sourceLabel(),
              sectionSpec.startLine,
              e.getMessage()
          )
      );
    }
    if (props.isEmpty()) {
      return null;
    }
    Map<String, Object> context = new HashMap<>();
    for (Entry<Object, Object> entry : props.entrySet()) {
      String key = entry.getKey().toString();
      context.put(
          key,
          QueryTestCases.definition(key).parse(
              entry.getValue().toString()));
    }
    return new ContextSection(context);
  }

  private TestSection analyzeException(SectionSpec sectionSpec)
  {
    return new ExceptionSection(sectionSpec.toText());
  }

  private TestSection analyzeParameters(SectionSpec sectionSpec)
  {
    List<SqlParameter> parameters = new ArrayList<>();
    for (String entry : sectionSpec.lines) {
      if ("null".equals(entry)) {
        parameters.add(null);
        continue;
      }
      int posn = entry.indexOf(':');
      if (posn == -1) {
        throw new IAE(
            StringUtils.format(
                "[%s:%d]: Parameter is not in type: value format: [%s]",
                sourceLabel(),
                sectionSpec.startLine,
                entry
            )
        );
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
                sourceLabel(),
                sectionSpec.startLine,
                entry,
                e.getMessage()
            )
        );
      }
    }
    return new ParametersSection(parameters);
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

  private TestSection analyzeOptions(SectionSpec sectionSpec)
  {
    Map<String, String> options = new HashMap<>();
    for (String line : sectionSpec.lines) {
      int posn = line.indexOf('=');
      if (posn == -1) {
        throw new IAE(
            StringUtils.format(
                "[%s:%d]: Option is not in key=value format: [%s]",
                sourceLabel(),
                sectionSpec.startLine,
                line
            )
        );
      }
      String key = line.substring(0, posn).trim();
      String value = QueryTestCases.unquote(line.substring(posn + 1).trim());
      options.put(key, value);
    }
    if (options.isEmpty()) {
      return null;
    } else {
      return new OptionsSection(options);
    }
  }

  private TestSection analyzeResults(SectionSpec sectionSpec)
  {
    return new ResultsSection(sectionSpec.lines);
  }
}
