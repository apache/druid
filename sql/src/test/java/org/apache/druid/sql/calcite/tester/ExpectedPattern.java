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
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generic regex-based test case section.
 */
public class ExpectedPattern extends TestElement
{
  public interface ExpectedLine
  {
    boolean matches(String line);
    void write(TestCaseWriter writer) throws IOException;
  }

  /**
   * A single line of expected input. The line must match
   * exactly (ignoring leading and trailing whitespace.)
   */
  public static class ExpectedLiteral implements ExpectedLine
  {
    protected final String line;

    public ExpectedLiteral(String line)
    {
      this.line = line;
    }

    @Override
    public boolean matches(String actual)
    {
      return line.trim().equals(actual.trim());
    }

    @Override
    public String toString()
    {
      return line;
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
      ExpectedLiteral other = (ExpectedLiteral) o;
      return Objects.equals(line, other.line);
    }

    /**
     * Never used (doesn't make sense). But, needed to make static checks happy.
     */
    @Override
    public int hashCode()
    {
      return line.hashCode();
    }

    @Override
    public void write(TestCaseWriter writer) throws IOException
    {
      writer.emitLiteral(line);
    }
  }

  /**
   * Expected value for a single line when using regular expressions
   * to match the line. Normal Java regular expression rules apply.
   * Matches the expected and actual lines after stripping leading
   * and trailing whitespace.
   *
   */
  public static class ExpectedRegex implements ExpectedLine
  {
    protected final String line;

    public ExpectedRegex(String line)
    {
      this.line = line;
    }

    @Override
    public String toString()
    {
      return line;
    }

    @Override
    public boolean matches(String actual)
    {
      // Each line is used only once or twice: no advantage to caching.
      Pattern p = Pattern.compile(line.trim());
      Matcher m = p.matcher(actual.trim());
      return m.matches();
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
      ExpectedRegex other = (ExpectedRegex) o;
      return Objects.equals(line, other.line);
    }

    /**
     * Never used (doesn't make sense). But, needed to make static checks happy.
     */
    @Override
    public int hashCode()
    {
      return line.hashCode();
    }

    @Override
    public void write(TestCaseWriter writer) throws IOException
    {
      writer.emitPattern(line);
    }
  }

  /**
   * Matches any number of lines up to the first match of
   * the following pattern.
   */
  public static class SkipAny implements ExpectedLine
  {
    @Override
    public String toString()
    {
      return "<any>";
    }

    @Override
    public boolean matches(String actual)
    {
      return true;
    }

    @Override
    public boolean equals(Object o)
    {
      if (o == this) {
        return true;
      }
      return o != null && o.getClass() == getClass();
    }

    /**
     * Never used (doesn't make sense). But, needed to make static checks happy.
     */
    @Override
    public int hashCode()
    {
      return 1;
    }

    @Override
    public void write(TestCaseWriter writer) throws IOException
    {
      writer.emitOptionalLine("**");
    }
  }

  /**
   * Represents a block of expected lines: literals, regular
   * expressions or wild cards.
   */
  public static class ExpectedText
  {
    protected final List<ExpectedLine> lines;

    public ExpectedText(List<ExpectedLine> lines)
    {
      this.lines = lines;
    }

    public void verify(String actual, ActualResults.ErrorCollector errors)
    {
      if (actual == null) {
        errors.add("Actual value is null");
      } else {
        verify(actual.split("\n"), errors);
      }
    }

    public boolean verify(String[] lines, ActualResults.ErrorCollector errors)
    {
      int aPosn = 0;
      int ePosn = 0;
      while (aPosn < lines.length && ePosn < this.lines.size()) {
        ExpectedLine expected = this.lines.get(ePosn++);
        if (expected instanceof SkipAny) {
          if (ePosn == this.lines.size()) {
            return true;
          }
          expected = this.lines.get(ePosn);
          while (aPosn < lines.length) {
            if (expected.matches(lines[aPosn])) {
              aPosn++;
              ePosn++;
              break;
            }
            aPosn++;
          }
        } else {
          if (!expected.matches(lines[aPosn])) {
            errors.add(
                StringUtils.format("line %d: expected [%s], actual [%s]",
                    aPosn + 1,
                    expected,
                    lines[aPosn]));
            return false;
          }
          aPosn++;
        }
      }
      if (ePosn < this.lines.size()) {
        errors.add("Missing lines from actual result");
        return false;
      }
      // Ignore trailing newlines
      while (aPosn < lines.length && lines[aPosn].trim().length() == 0) {
        aPosn++;
      }
      if (aPosn < lines.length) {
        errors.add("Unexpected lines at line " + (aPosn + 1));
        return false;
      }
      return true;
    }

    public void write(TestCaseWriter writer) throws IOException
    {
      for (ExpectedLine line : lines) {
        line.write(writer);
      }
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
      ExpectedText other = (ExpectedText) o;
      return lines.equals(other.lines);
    }

    /**
     * Never used (doesn't make sense). But, needed to make static checks happy.
     */
    @Override
    public int hashCode()
    {
      return lines.hashCode();
    }
  }

  protected final TestElement.ElementType section;
  protected final ExpectedPattern.ExpectedText expected;

  protected ExpectedPattern(ElementType section, String name, ExpectedText expected)
  {
    this(section, name, expected, false);
  }

  protected ExpectedPattern(ElementType section, String name, ExpectedText expected, boolean copy)
  {
    super(name, copy);
    this.section = section;
    this.expected = expected;
  }

  public ExpectedPattern.ExpectedText expected()
  {
    return expected;
  }

  @Override
  public TestElement.ElementType type()
  {
    return section;
  }

  @Override
  public TestElement copy()
  {
    return new ExpectedPattern(section, name, expected, true);
  }

  public boolean verify(String actual, ActualResults.ErrorCollector errors)
  {
    String[] lines = actual == null ? null : actual.split("\n");
    return verify(lines, errors);
  }

  public boolean verify(String[] actual, ActualResults.ErrorCollector errors)
  {
    errors.setSection(type().sectionName());
    if (actual == null) {
      errors.add("Section " + section + " actual results are missing.");
      return false;
    } else {
      return expected.verify(actual, errors);
    }
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
    ExpectedPattern other = (ExpectedPattern) o;
    return expected.equals(other.expected);
  }

  /**
   * Never used (doesn't make sense). But, needed to make static checks happy.
   */
  @Override
  public int hashCode()
  {
    return Objects.hash(section, name, expected);
  }

  @Override
  public void writeElement(TestCaseWriter writer) throws IOException
  {
    writer.emitSection(name);
    expected.write(writer);
  }
}
