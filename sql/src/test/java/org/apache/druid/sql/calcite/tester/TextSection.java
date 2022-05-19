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
import java.util.Objects;

abstract class TextSection extends TestSection
{
  /**
   * The SQL test case section.
   */
  protected static class SqlSection extends TextSection
  {
    protected SqlSection(String name, String text)
    {
      this(name, text, false);
    }

    protected SqlSection(String name, String text, boolean copy)
    {
      super(name, copy, text);
    }

    @Override
    public TestSection.Section section()
    {
      return TestSection.Section.SQL;
    }

    @Override
    public TestSection copy()
    {
      return new SqlSection(name, text, true);
    }
  }

  /**
   * The exception test case section.
   */
  public static class ExceptionSection extends TextSection
  {
    protected ExceptionSection(String text)
    {
      this(text, false);
    }

    protected ExceptionSection(String text, boolean copy)
    {
      super(Section.EXCEPTION.sectionName(), copy, text);
    }

    @Override
    public TestSection.Section section()
    {
      return TestSection.Section.EXCEPTION;
    }

    @Override
    public TestSection copy()
    {
      return new ExceptionSection(text, true);
    }

    public boolean verify(Exception e, ActualResults.ErrorCollector errors)
    {
      Throwable cause = e;
      while (cause != null) {
        if (text.equals(cause.getClass().getSimpleName())) {
          return true;
        }
        cause = cause.getCause();
      }
      errors.setSection(section().sectionName());
      errors.add(StringUtils.format(
          "Expected exception [%s] but got [%s]: [%s]",
          text,
          e.getClass().getSimpleName(),
          e.getMessage()));
      return false;
    }
  }

  protected final String text;

  protected TextSection(String name, boolean copy, String text)
  {
    super(name, copy);
    this.text = text;
  }

  public String text()
  {
    return text;
  }

  @Override
  public void writeSection(TestCaseWriter writer) throws IOException
  {
    writer.emitSection(name, text);
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) {
      return true;
    }
    if (o == null || o.getClass() != getClass()) {
      return false;
    }
    TextSection other = (TextSection) o;
    return text.equals(other.text);
  }

  /**
   * Never used (doesn't make sense). But, needed to make static checks happy.
   */
  @Override
  public int hashCode()
  {
    return Objects.hash(text);
  }
}
