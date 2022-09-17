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

import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.calcite.tester.TextSection.ExceptionSection;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Common parent for test cases and runs: things that have labels
 * and contain sections. Sections are kept in file order for writing,
 * and indexed for retrieval.
 */
public abstract class SectionContainer
{
  protected final String label;
  protected final Map<TestSection.Section, TestSection> sections = new HashMap<>();
  protected final List<TestSection> fileOrder;

  public SectionContainer(
      String label,
      List<TestSection> sections
  )
  {
    this.label = label;
    this.fileOrder = sections;
    for (TestSection section : sections) {
      this.sections.put(section.section(), section);
    }
  }

  public String label()
  {
    return label;
  }

  public List<TestSection> sections()
  {
    return fileOrder;
  }

  public TestSection section(TestSection.Section section)
  {
    return sections.get(section);
  }

  public OptionsSection optionsSection()
  {
    return (OptionsSection) section(TestSection.Section.OPTIONS);
  }

  public Map<String, String> options()
  {
    OptionsSection section = optionsSection();
    return section == null ? Collections.emptyMap() : section.options();
  }

  public String option(String key)
  {
    OptionsSection options = optionsSection();
    return options == null ? null : options.options.get(key);
  }

  public ContextSection contextSection()
  {
    return (ContextSection) section(TestSection.Section.CONTEXT);
  }

  public ExceptionSection exception()
  {
    return (TextSection.ExceptionSection) section(TestSection.Section.EXCEPTION);
  }

  public PatternSection error()
  {
    return (PatternSection) section(TestSection.Section.ERROR);
  }

  public boolean shouldFail()
  {
    return exception() != null || error() != null;
  }

  public boolean booleanOption(String key)
  {
    return QueryContexts.getAsBoolean(key, option(key), false);
  }

  public abstract Map<String, Object> context();
}
