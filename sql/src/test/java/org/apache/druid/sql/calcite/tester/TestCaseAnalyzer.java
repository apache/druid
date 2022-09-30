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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.calcite.avatica.SqlType;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.Action;
import org.apache.druid.sql.calcite.tester.ExpectedPattern.ExpectedLine;
import org.apache.druid.sql.calcite.tester.ExpectedPattern.ExpectedRegex;
import org.apache.druid.sql.calcite.tester.ExpectedPattern.ExpectedText;
import org.apache.druid.sql.calcite.tester.ExpectedPattern.SkipAny;
import org.apache.druid.sql.calcite.tester.LinesElement.CaseLabel;
import org.apache.druid.sql.calcite.tester.LinesElement.ExpectedResults;
import org.apache.druid.sql.calcite.tester.TestElement.ElementType;
import org.apache.druid.sql.calcite.tester.TestSetSpec.SectionSpec;
import org.apache.druid.sql.calcite.tester.TestSetSpec.TestCaseSpec;
import org.apache.druid.sql.calcite.tester.TextSection.ExceptionSection;
import org.apache.druid.sql.calcite.tester.TextSection.SqlSection;
import org.apache.druid.sql.http.SqlParameter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Analyzes a test test parse tree into the semantisized form used to
 * run a test.
 */
public class TestCaseAnalyzer
{
  private final TestSetSpec setSpec;
  private final List<QueryTestCase> testCases = new ArrayList<>();
  private final ObjectMapper jsonMapper = new ObjectMapper();
  private QueryTestCase.Builder testCase;
  private QueryTestCase prevCase;
  private QueryRun.Builder queryRun;

  public TestCaseAnalyzer(final TestSetSpec setSpec)
  {
    this.setSpec = setSpec;
    jsonMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
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
    ElementType sectionKind = parseSectionName(sectionSpec);
    if ("copy".equalsIgnoreCase(sectionSpec.arg)) {
      copySection(sectionSpec, sectionKind);
    } else {
      addElement(analyzeSection(sectionSpec, sectionKind));
    }
  }

  private void analyzeCase(SectionSpec sectionSpec)
  {
    testCase.add(new CaseLabel(sectionSpec.lines));
  }

  private TestElement analyzeSection(SectionSpec sectionSpec, ElementType sectionKind)
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

  private ElementType parseSectionName(SectionSpec sectionSpec)
  {
    ElementType section = ElementType.forElement(sectionSpec.name);
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

  private void copySection(SectionSpec sectionSpec, ElementType sectionKind)
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
    if (sectionKind == ElementType.RESULTS && queryRun != null) {
      throw new IAE(
          StringUtils.format(
              "[%s:%d]: Cannot use \"copy\" option in run section",
              sourceLabel(),
              sectionSpec.startLine
          )
      );
    }
    TestElement copiedSection;
    if (sectionKind == ElementType.RESULTS) {
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
      copiedSection = prevCase.runs().get(0).section(ElementType.RESULTS);
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
    addElement(copiedSection);
  }

  private void analyzeRun(SectionSpec sectionSpec)
  {
    String label = sectionSpec.toText().trim();
    queryRun = testCase.addRun(label, true);
  }

  private void addElement(TestElement section)
  {
    if (section == null) {
      return;
    }
    switch (section.type()) {
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

  private TestElement analyzeQuery(SectionSpec sectionSpec)
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

  private TestElement analyzePattern(ElementType section, SectionSpec sectionSpec)
  {
    List<ExpectedLine> result = analyzeExpected(sectionSpec);
    return new ExpectedPattern(section, sectionSpec.name, new ExpectedText(result));
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
      lines.add(new ExpectedPattern.ExpectedLiteral(line));
    }
    return lines;
  }

  private TestElement analyzeResources(SectionSpec sectionSpec)
  {
    List<Resources.Resource> resourceActions = new ArrayList<>();
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
      resourceActions.add(new Resources.Resource(parts[0], parts[1], action));
    }
    return new Resources(resourceActions);
  }

  private TestElement analyzeContext(SectionSpec sectionSpec)
  {
    Map<String, Object> context = convertMap(sectionSpec);
    return context.isEmpty() ? null : new Context(context);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> convertMap(SectionSpec sectionSpec)
  {
    String json = "{" +
        String.join(",\n", sectionSpec.lines) +
        "\n}";
    try {
      return jsonMapper.readValue(json, Map.class);
    }
    catch (JsonProcessingException e) {
      throw new IAE(
          "[%s:%d]: JSON parse failed: %s",
          sourceLabel(),
          sectionSpec.startLine,
          e.getMessage()
      );
    }
  }

  private TestElement analyzeException(SectionSpec sectionSpec)
  {
    return new ExceptionSection(sectionSpec.toText());
  }

  @SuppressWarnings("unchecked")
  private TestElement analyzeParameters(SectionSpec sectionSpec)
  {
    StringBuilder buf = new StringBuilder()
        .append("[");
    for (int i = 0; i < sectionSpec.lines.size(); i++) {
      if (i > 0) {
        buf.append(", ");
      }
      buf.append("[").append(sectionSpec.lines.get(i)).append("]");
    }
    String json = buf.append("]").toString();
    List<List<Object>> values;
    try {
      values = jsonMapper.readValue(json, List.class);
    }
    catch (JsonProcessingException e) {
      throw new IAE(
          "[%s:%d]: JSON parse failed: %s",
          sourceLabel(),
          sectionSpec.startLine,
          e.getMessage()
      );
    }
    List<SqlParameter> parameters = new ArrayList<>();
    for (List<Object> paramValue : values) {
      try {
        parameters.add(parseParameter(paramValue));
      }
      catch (Exception e) {
        throw new IAE("[%s:%d]: %s",
            sourceLabel(),
            sectionSpec.startLine,
            e.getMessage()
        );
      }
    }
    return new Parameters(parameters);
  }

  public SqlParameter parseParameter(List<Object> value)
  {
    if (value.size() != 2 || !(value.get(0) instanceof String)) {
      throw new IAE("Parameter not in \"<type>\": <value> form: [%s]", value);
    }
    String typeStr = StringUtils.toUpperCase((String) value.get(0));
    if ("INT".equals(typeStr)) {
      typeStr = SqlType.INTEGER.name();
    } else if ("STRING".equals(typeStr)) {
      typeStr = SqlType.VARCHAR.name();
    } else if ("LONG".equals(typeStr)) {
      typeStr = SqlType.BIGINT.name();
    }
    SqlType sqlType;
    try {
      sqlType = SqlType.valueOf(typeStr);
    }
    catch (IllegalArgumentException e) {
      throw new IAE("Parameter type [%s] is not a valid SQL type", value.get(0));
    }
    Object rawValue = value.get(1);
    if (rawValue == null) {
      return new SqlParameter(sqlType, null);
    }

    // Crude-but-effective way to coerce the type JSON chose to the
    // type specified for JSON.
    String strValue = rawValue.toString();
    Object sqlValue;
    switch (sqlType) {
      case INTEGER:
        sqlValue = Integer.parseInt(strValue);
        break;
      case BIGINT:
        sqlValue = Long.parseLong(strValue);
        break;
      case FLOAT:
      case REAL:
        sqlValue = Float.parseFloat(strValue);
        break;
      case DOUBLE:
        sqlValue = Double.parseDouble(strValue);
        break;
      case VARCHAR:
        sqlValue = QueryTestCases.unquote(strValue);
        break;
      case TIMESTAMP:
      case DATE:
        // Timestamps seem to appear as both quoted strings and numbers.
        sqlValue = QueryTestCases.unquote(strValue);
        break;
      default:
        throw new IAE("Unsupported SQL type: %s", sqlType);
    }
    return new SqlParameter(sqlType, sqlValue);
  }

  private TestElement analyzeOptions(SectionSpec sectionSpec)
  {
    Map<String, Object> options = convertMap(sectionSpec);
    return options.isEmpty() ? null : new TestOptions(options);
  }

  private TestElement analyzeResults(SectionSpec sectionSpec)
  {
    return new ExpectedResults(sectionSpec.lines);
  }
}
