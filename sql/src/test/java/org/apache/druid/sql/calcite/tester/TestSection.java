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

import java.io.IOException;

/**
 * One section of the test case.
 */
public abstract class TestSection
{
  /**
   * Enum which identifies the supported test sections.
   */
  public enum Section
  {
    CASE("case"),
    SQL("sql"),
    CONTEXT("context"),
    USER("user"),
    PARAMETERS("parameters"),
    OPTIONS("options"),
    AST("ast"),
    UNPARSED("unparsed"),
    PLAN("plan"),
    EXEC_PLAN("execplan"),
    SCHEMA("schema"),
    TARGET_SCHEMA("targetschema"),
    ERROR("error"),
    EXCEPTION("exception"),
    EXPLAIN("explain"),
    NATIVE("native"),
    RESOURCES("resources"),
    RESULTS("results"),
    COMMENTS("=");

    private final String name;

    Section(String name)
    {
      this.name = name;
    }

    public String sectionName()
    {
      return name;
    }

    public static Section forSection(String section)
    {
      section = StringUtils.toLowerCase(section);
      for (Section value : values()) {
        if (value.name.equals(section)) {
          return value;
        }
      }
      return null;
    }
  }

  protected final String name;
  protected final boolean copy;

  protected TestSection(String name, boolean copy)
  {
    this.name = name;
    this.copy = copy;
  }

  public abstract TestSection.Section section();
  public abstract TestSection copy();

  public String name()
  {
    return name;
  }

  public boolean isCopy()
  {
    return copy;
  }

  public void write(TestCaseWriter writer) throws IOException
  {
    if (copy) {
      writer.emitCopy(name);
    } else {
      writeSection(writer);
    }
  }

  protected abstract void writeSection(TestCaseWriter writer) throws IOException;
}
