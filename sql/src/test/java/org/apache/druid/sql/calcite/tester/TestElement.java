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
 * One element of the test case. Elements are either input to the test,
 * or expected results from the test.
 */
public abstract class TestElement
{
  /**
   * Enum which identifies the supported test elements.
   */
  public enum ElementType
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

    ElementType(String name)
    {
      this.name = name;
    }

    public String sectionName()
    {
      return name;
    }

    public static ElementType forElement(String section)
    {
      section = StringUtils.toLowerCase(section);
      for (ElementType value : values()) {
        if (value.name.equals(section)) {
          return value;
        }
      }
      return null;
    }
  }

  protected final String name;
  protected final boolean copy;

  protected TestElement(String name, boolean copy)
  {
    this.name = name;
    this.copy = copy;
  }

  public abstract ElementType type();
  public abstract TestElement copy();

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
      writeElement(writer);
    }
  }

  protected abstract void writeElement(TestCaseWriter writer) throws IOException;
}
