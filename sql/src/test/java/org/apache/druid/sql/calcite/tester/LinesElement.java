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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.sql.calcite.tester.ActualResults.ErrorCollector;
import org.junit.Assert;
import org.junit.internal.ComparisonCriteria;
import org.junit.internal.InexactComparisonCriteria;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A test case section that consists of a set of literal lines.
 */
public abstract class LinesElement extends TestElement
{
  /**
   * The case test case section. Contents is a single line
   * that gives the test case label.
   */
  public static class CaseLabel extends LinesElement
  {
    protected CaseLabel(List<String> lines)
    {
      super(ElementType.CASE.sectionName(), lines, false);
    }

    @Override
    public TestElement.ElementType type()
    {
      return TestElement.ElementType.CASE;
    }

    @Override
    public TestElement copy()
    {
      throw new UOE("CaseElement.copy()");
    }
  }

  /**
   * The (expected) results test case section.
   */
  public static class ExpectedResults extends LinesElement
  {
    protected ExpectedResults(List<String> lines)
    {
      this(lines, false);
    }

    protected ExpectedResults(List<String> lines, boolean copy)
    {
      super(ElementType.RESULTS.sectionName(), lines, copy);
    }

    @Override
    public ElementType type()
    {
      return ElementType.RESULTS;
    }

    @Override
    public TestElement copy()
    {
      return new ExpectedResults(lines, true);
    }

    /**
     * Verify results using a simple string compare. Works fine for all but
     * double and float types.
     */
    public boolean verify(List<String> actual, ErrorCollector errors)
    {
      if (!verifySize(actual.size(), errors)) {
        return false;
      }
      boolean ok = true;
      for (int i = 0; i < lines.size(); i++) {
        if (!actual.get(i).equals(lines.get(i))) {
          errors.add(
              StringUtils.format(
                  "Results differ at line %d",
                   i + 1));
          ok = false;
        }
      }
      return ok;
    }

    private static final TypeReference<Object[]> OBJECT_ARRAY_REFERENCE = new TypeReference<Object[]>()
    {
    };

    /**
     * JUnit-style comparison criteria for the case of an object deserialized
     * from JSON. JSON does not know the original types (the types used by
     * the query engine). Instead, it infers equivalent types from the data.
     * Thus, longs may be integers, floats may be doubles, etc. This class
     * works out the equivalences, and also compares doubles using an approximate
     * comparison.
     *
     * The result is generally useful, but a bit slow: use it only when there
     * are actual ambiguities.
     */
    public static class JsonComparsionCriteria extends InexactComparisonCriteria
    {
      public JsonComparsionCriteria(double delta)
      {
        super(delta);
      }

      @Override
      protected void assertElementsEqual(Object expected, Object actual)
      {
        // If both elements are a floating point type, convert both to double
        // and do an inexact compare.
        if (expected instanceof Float || expected instanceof Double &&
            actual instanceof Float || actual instanceof Double) {
          double eDouble = (expected instanceof Float) ? (Float) expected : (Double) expected;
          double aDouble = (actual instanceof Float) ? (Float) actual : (Double) actual;
          Assert.assertEquals(eDouble, aDouble, (double) fDelta);
          return;

        // If both types are integral, convert both to longs and do an exact
        // compare.
        } else if (expected instanceof Integer || expected instanceof Long &&
                   actual instanceof Integer || actual instanceof Long) {
          long eLong = (expected instanceof Integer) ? (Integer) expected : (Long) expected;
          long aLong = (actual instanceof Integer) ? (Integer) actual : (Long) actual;
          Assert.assertEquals(eLong, aLong);
          return;

        // Lists of objects? Lists are equivalent if they are of the same length,
        // all items in both sets are null (regardless of type, which JSON won't
        // know), or if the elements are equivalent as defined here.
        } else if (expected instanceof List && actual instanceof List) {
          List<?> eList = (List<?>) expected;
          List<?> aList = (List<?>) actual;
          if (eList.size() == aList.size()) {
            for (int i = 0; i < eList.size(); i++) {
              Object eItem = eList.get(i);
              Object aItem = aList.get(i);

              // Nulls of any type are equal.
              if (eItem == null && aItem == null) {
                continue;
              }
              assertElementsEqual(eItem, aItem);
            }
            return;
          }
        }

        // Not a special case, use a generic compare. This compare uses exact
        // semantics, so if it turns out that there are, say, embedded arrays,
        // we'd have to extend the above to handle that case.
        Assert.assertEquals(expected, actual);
      }
    }

    /**
     * Compare actual results, as Java objects, with the expected results,
     * parsed as JSON from string lines. Uses an inexact comparison that provides
     * a delta of 1% for float and double values.
     */
    public boolean verify(List<Object[]> actual, ObjectMapper mapper, ErrorCollector errors)
    {
      if (!verifySize(actual.size(), errors)) {
        return false;
      }
      ComparisonCriteria compare = new JsonComparsionCriteria(0.01);
      boolean ok = true;
      for (int i = 0; i < lines.size(); i++) {
        Object expectedRow;
        try {
          expectedRow = mapper.readValue(lines.get(i), OBJECT_ARRAY_REFERENCE);
        }
        catch (IOException e) {
          errors.add(
              StringUtils.format(
                  "Invalid JSON row object: on line %d: %s",
                   i + 1,
                   e.getMessage()));
          ok = false;
          continue;
        }
        try {
          compare.arrayEquals("", expectedRow, actual.get(i));
        }
        catch (Exception e) {
          errors.add(
              StringUtils.format(
                  "Results differ at line %d: %s",
                   i + 1,
                   e.getMessage()));
          ok = false;
        }
      }
      return ok;
    }

    private boolean verifySize(int actualSize, ErrorCollector errors)
    {
      if (actualSize != lines.size()) {
        errors.add(
            StringUtils.format(
                "Expected %d rows but got %d",
                lines.size(),
                actualSize));
        return false;
      }
      return true;
    }
  }

  /**
   * The comments test case section which precedes the
   * start of the test case.
   */
  public static class TestComments extends LinesElement
  {
    protected TestComments(List<String> lines)
    {
      super(ElementType.COMMENTS.sectionName(), lines, false);
    }

    @Override
    public ElementType type()
    {
      return ElementType.COMMENTS;
    }

    @Override
    public TestElement copy()
    {
      throw new UOE("CommentsSection.copy()");
    }

    @Override
    public void write(TestCaseWriter writer) throws IOException
    {
      writer.emitComment(lines);
    }
  }

  protected final List<String> lines;

  protected LinesElement(String name, List<String> lines, boolean copy)
  {
    super(name, copy);
    this.lines = lines;
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
    LinesElement other = (LinesElement) o;
    return lines.equals(other.lines);
  }

  /**
   * Never used (doesn't make sense). But, needed to make static checks happy.
   */
  @Override
  public int hashCode()
  {
    return Objects.hash(lines);
  }

  @Override
  public void writeElement(TestCaseWriter writer) throws IOException
  {
    writer.emitSection(name, lines);
  }
}
