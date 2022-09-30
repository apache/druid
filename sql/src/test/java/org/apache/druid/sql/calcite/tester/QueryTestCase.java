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
import org.apache.commons.text.StringEscapeUtils;
import org.apache.druid.sql.calcite.tester.LinesSection.CommentsSection;
import org.apache.druid.sql.calcite.tester.TestSection.Section;
import org.apache.druid.sql.http.SqlParameter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents one test case to exercise within the planner test
 * framework. A test must have a SQL statement and may have any number
 * of expected results sections. A case may include additional inputs
 * such as query context settings, parameters and test options.
 * <p>
 * A test case consists of a SQL statement, optional planner, optional
 * options and optional planner results. It also includes zero or more
 * runs of the query, each with optional results. Typically there are one
 * or two runs: one for each kind of null handling.
 */
public class QueryTestCase extends SectionContainer
{
  /**
   * Builder for a test case. Allows the test case itself to be
   * immutable.
   */
  public static class Builder
  {
    private final String label;
    protected List<TestSection> sections = new ArrayList<>();
    protected String exception;
    protected List<QueryRun.Builder> runBuilders = new ArrayList<>();

    public Builder(String label)
    {
      this.label = label;
    }

    public void add(TestSection section)
    {
      if (section != null) {
        sections.add(section);
      }
    }

    public QueryRun.Builder addRun(String label, boolean isExplicit)
    {
      QueryRun.Builder runBuilder = new QueryRun.Builder(label);
      runBuilder.explicit(isExplicit);
      runBuilders.add(runBuilder);
      return runBuilder;
    }

    public QueryTestCase build()
    {
      QueryTestCase testCase = new QueryTestCase(this);
      for (QueryRun.Builder runBuilder : runBuilders) {
        testCase.addRun(runBuilder.build(testCase));
      }
      return testCase;
    }
  }

  private List<QueryRun> runs = new ArrayList<>();

  public QueryTestCase(Builder builder)
  {
    super(builder.label, builder.sections);
  }

  protected void addRun(QueryRun run)
  {
    runs.add(run);
  }

  public void addRuns(List<QueryRun> runs)
  {
    this.runs.addAll(runs);
  }

  public TextSection.SqlSection sqlSection()
  {
    return (TextSection.SqlSection) section(TestSection.Section.SQL);
  }

  public String sql()
  {
    String sql = sqlSection().text();
    if (booleanOption(OptionsSection.UNICODE_ESCAPE_OPTION)) {
      sql = StringEscapeUtils.unescapeJava(sql);
    }
    return sql;
  }

  public String comment()
  {
    CommentsSection comments = (CommentsSection) section(TestSection.Section.COMMENTS);
    if (comments == null || comments.lines.isEmpty()) {
      return null;
    }
    if (comments.lines.size() == 1) {
      return comments.lines.get(0);
    }
    return String.join("\n", comments.lines);
  }

  public String user()
  {
    OptionsSection options = optionsSection();
    return options == null ? null : options.get(OptionsSection.USER_OPTION);
  }

  public PatternSection ast()
  {
    return (PatternSection) section(TestSection.Section.AST);
  }

  public PatternSection plan()
  {
    return (PatternSection) section(TestSection.Section.PLAN);
  }

  public PatternSection execPlan()
  {
    return (PatternSection) section(TestSection.Section.EXEC_PLAN);
  }

  @Override
  public Map<String, Object> context()
  {
    ContextSection section = contextSection();
    return section == null ? ImmutableMap.of() : section.context;
  }

  public PatternSection explain()
  {
    return (PatternSection) section(TestSection.Section.EXPLAIN);
  }

  public PatternSection unparsed()
  {
    return (PatternSection) section(TestSection.Section.UNPARSED);
  }

  public PatternSection schema()
  {
    return (PatternSection) section(TestSection.Section.SCHEMA);
  }

  public PatternSection targetSchema()
  {
    return (PatternSection) section(TestSection.Section.TARGET_SCHEMA);
  }

  public PatternSection nativeQuery()
  {
    return (PatternSection) section(TestSection.Section.NATIVE);
  }

  public ResourcesSection resourceActions()
  {
    return (ResourcesSection) section(TestSection.Section.RESOURCES);
  }

  public ParametersSection parametersSection()
  {
    return (ParametersSection) section(TestSection.Section.PARAMETERS);
  }

  public List<SqlParameter> parameters()
  {
    ParametersSection params = parametersSection();
    return params == null ? Collections.emptyList() : params.parameters();
  }

  /**
   * The Druid planner is designed to be configured once per run,
   * but tests want to be more flexible. If the test wants to change a
   * planner setting, we must reset the whole planner stack. Less than
   * idea, but it is what it is.
   */
  public boolean requiresCustomPlanner()
  {
    OptionsSection options = optionsSection();
    if (options == null) {
      return false;
    }
    for (String key : options.options.keySet()) {
      if (key.startsWith("planner.")) {
        return true;
      }
    }
    return false;
  }

  public List<QueryRun> runs()
  {
    return runs;
  }

  public boolean hasRuns()
  {
    return runs != null && !runs.isEmpty();
  }

  protected TestSection copySection(Section section)
  {
    TestSection thisSection = section(section);
    return thisSection == null ? null : thisSection.copy();
  }

  public void write(TestCaseWriter writer) throws IOException
  {
    for (TestSection section : fileOrder) {
      section.write(writer);
    }
    for (QueryRun run : runs) {
      run.write(writer);
    }
  }

  public boolean matches(QueryTestCase testCase)
  {
    return Objects.equals(options(), testCase.options())
        && Objects.equals(context(), testCase.context());
  }
}
