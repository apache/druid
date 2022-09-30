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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.calcite.tester.LinesElement.ExpectedResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A query run consists of a set of results or an execution/error, along
 * with an optional set of query context values and execution options.
 * <p>
 * If a test case has more than one run, then there should be an options
 * or query context section to identify what changes between runs. The
 * typical case is that one run covers "classic" null handling, while another
 * covers SQL-compatible null handling. Options and query context "inherit"
 * values from the query test case, overridden by any values set in the run.
 */
public class QueryRun extends ElementContainer
{
  /**
   * Builder for a test case. Allows the test case itself to be
   * immutable.
   */
  public static class Builder
  {
    private final String label;
    protected boolean isExplicit;
    protected List<TestElement> sections = new ArrayList<>();
    protected String exception;

    public Builder(String label)
    {
      this.label = label;
    }

    public Builder explicit(boolean isExplicit)
    {
      this.isExplicit = isExplicit;
      return this;
    }

    public void add(TestElement section)
    {
      if (section != null) {
        sections.add(section);
      }
    }

    public QueryRun build(QueryTestCase testCase)
    {
      return new QueryRun(testCase, this);
    }
  }

  private final QueryTestCase testCase;
  /**
   * Whether the run section was explicitly included or was implied.
   * Used when writing cases to recreate the original format.
   */
  private final boolean isExplicit;
  /**
   * Order of the run within the test case. Used for generating a label
   * for a case when no label is provided in the source file.
   */
  private final int ordinal;

  public QueryRun(QueryTestCase testCase, Builder builder)
  {
    super(builder.label, builder.sections);
    this.testCase = testCase;
    this.ordinal = testCase.runs().size() + 1;
    this.isExplicit = builder.isExplicit;
  }

  public QueryRun(
      QueryTestCase testCase,
      String label,
      List<TestElement> sections,
      boolean isExplicit)
  {
    super(label, sections);
    this.testCase = testCase;
    this.ordinal = testCase.runs().size() + 1;
    this.isExplicit = isExplicit;
  }

  public QueryTestCase testCase()
  {
    return testCase;
  }

  public boolean isExplicit()
  {
    return isExplicit;
  }

  public String displayLabel()
  {
    String value = label();
    if (Strings.isNullOrEmpty(value)) {
      return "Run " + ordinal;
    } else {
      return value;
    }
  }

  public ExpectedResults resultsSection()
  {
    return (LinesElement.ExpectedResults) section(TestElement.ElementType.RESULTS);
  }

  public List<String> results()
  {
    ExpectedResults resultsSection = resultsSection();
    return resultsSection == null ? Collections.emptyList() : resultsSection.lines;
  }

  @Override
  public Map<String, Object> context()
  {
    Context section = contextSection();
    Context querySection = testCase.contextSection();
    if (querySection == null) {
      return section == null ? ImmutableMap.of() : section.context;
    }
    if (section == null) {
      return querySection == null ? ImmutableMap.of() : querySection.context;
    }
    Map<String, Object> merged = new HashMap<>();
    merged.putAll(querySection.context);
    merged.putAll(section.context);
    return merged;
  }

  public boolean shouldRunFail()
  {
    return failOnRun();
  }

  public boolean failOnRun()
  {
    return TestOptions.FAIL_AT_RUN.equalsIgnoreCase(option(TestOptions.FAILURE_OPTION));
  }

  @Override
  public Map<String, Object> options()
  {
    Map<String, Object> caseOptions = testCase.options();
    Map<String, Object> options = super.options();
    if (caseOptions.isEmpty()) {
      return options;
    }
    if (options.isEmpty()) {
      return caseOptions;
    }
    Map<String, Object> merged = new HashMap<>(caseOptions);
    merged.putAll(options);
    return merged;
  }

  @Override
  public boolean booleanOption(String key)
  {
    return QueryContexts.getAsBoolean(key, option(key), false);
  }

  @Override
  public String option(String key)
  {
    String value = super.option(key);
    if (value == null) {
      value = testCase.option(key);
    }
    return value;
  }

  public QueryRun copy(QueryTestCase testCase, boolean isExplicit)
  {
    return new QueryRun(testCase, label, fileOrder, isExplicit);
  }

  public QueryRun copy(QueryTestCase testCase)
  {
    return copy(testCase, isExplicit);
  }

  public void write(TestCaseWriter writer) throws IOException
  {
    if (isExplicit) {
      writer.emitSection("run");
      if (!Strings.isNullOrEmpty(label)) {
        writer.emitOptionalLine(label);
      }
    }
    for (TestElement section : fileOrder) {
      section.write(writer);
    }
  }
}
