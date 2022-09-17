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

import org.apache.druid.java.util.common.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSetSpec
{
  public static class SectionSpec
  {
    public final String name;
    public final String arg;
    public final List<String> lines;
    public final int startLine;

    public SectionSpec(
        final String name,
        final String arg,
        final int startLine,
        final List<String> lines
    )
    {
      this.name = name;
      this.arg = arg;
      this.startLine = startLine;
      this.lines = lines;
    }

    public String toText()
    {
      // Preserve final newline
      return String.join("\n", lines) + "\n";
    }
  }

  public static class TestCaseSpec
  {
    public static class Builder
    {
      private String label;
      private int startLine;
      private List<String> comments;
      private final List<SectionSpec> sections = new ArrayList<>();

      public Builder label(final String label)
      {
        this.label = label;
        return this;
      }

      public Builder startLine(final int startLine)
      {
        this.startLine = startLine;
        return this;
      }

      public Builder comments(List<String> comments)
      {
        this.comments = comments;
        return this;
      }

      public Builder addSection(
          final String name,
          final String arg,
          final int startLine,
          final List<String> lines
      )
      {
        sections.add(new SectionSpec(name, arg, startLine, lines));
        return this;
      }

      public TestCaseSpec build()
      {
        return new TestCaseSpec(label, startLine, comments, sections);
      }
    }

    private final String label;
    private final int startLine;
    private final List<String> comments;
    private final Map<String, SectionSpec> sections = new HashMap<>();
    private final List<SectionSpec> fileOrder;

    public TestCaseSpec(
        final String label,
        final int startLine,
        final List<String> comments,
        final List<SectionSpec> sections
    )
    {
      this.label = label;
      this.startLine = startLine;
      this.comments = comments;
      this.fileOrder = sections;
      for (SectionSpec section : sections) {
        this.sections.put(StringUtils.toLowerCase(section.name), section);
      }
    }

    public String label()
    {
      return label;
    }

    public List<String> comments()
    {
      return comments;
    }

    public int startLine()
    {
      return startLine;
    }

    public List<SectionSpec> sections()
    {
      return fileOrder;
    }

    public SectionSpec section(String label)
    {
      return sections.get(label);
    }
  }

  public static class Builder
  {
    private final String source;
    private final List<TestCaseSpec> testCases = new ArrayList<>();

    public Builder(final String source)
    {
      this.source = source;
    }

    public String source()
    {
      return source;
    }

    public TestCaseSpec.Builder caseBuilder(int startLine)
    {
      return new TestCaseSpec.Builder().startLine(startLine);
    }

    public Builder add(TestCaseSpec.Builder builder)
    {
      testCases.add(builder.build());
      return this;
    }

    public TestSetSpec build()
    {
      return new TestSetSpec(source, testCases);
    }
  }

  private final String source;
  private final List<TestCaseSpec> cases;

  public TestSetSpec(final String source, List<TestCaseSpec> testCases)
  {
    this.source = source;
    this.cases = testCases;
  }

  public String source()
  {
    return source;
  }

  public List<TestCaseSpec> cases()
  {
    return cases;
  }
}
