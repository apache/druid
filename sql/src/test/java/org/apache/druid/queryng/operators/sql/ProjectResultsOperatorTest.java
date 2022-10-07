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

package org.apache.druid.queryng.operators.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.queryng.fragment.FragmentManager;
import org.apache.druid.queryng.fragment.Fragments;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operator.IterableOperator;
import org.apache.druid.queryng.operators.OperatorTest;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.ResultIterator;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test the SQL-type projection operator. This version tests two forms:
 * an interpreted version which is a literal copy/paste of the original
 * NativeQueryBuilder code, and a sped-up cached version.
 */
@Category(OperatorTest.class)
public class ProjectResultsOperatorTest
{
  private final ObjectMapper jsonMapper = new ObjectMapper();

  static {
    // Use SQL-compatible null handling here.
    NullHandling.initializeForTestsWithValues(false, true);
  }

  private static class MockResultsOperator implements IterableOperator<Object[]>
  {
    private final List<Object[]> results;
    private int posn = -1;

    public MockResultsOperator(List<Object[]> results)
    {
      this.results = results;
    }

    @Override
    public ResultIterator<Object[]> open()
    {
      return this;
    }

    @Override
    public Object[] next() throws ResultIterator.EofException
    {
      if (posn + 1 == results.size()) {
        throw Operators.eof();
      }
      return results.get(++posn);
    }

    @Override
    public void close(boolean cascade)
    {
    }
  }

  private class TestFixture
  {
    List<Object[]> input;
    List<Object[]> expected;
    int[] mapping;
    List<SqlTypeName> newTypes;
    DateTimeZone timeZone = DateTimeZone.UTC;
    boolean serializeComplexValues;
    boolean stringifyArrays;
    List<Object[]> output;

    TestFixture withInput(List<Object[]> input)
    {
      this.input = input;
      return this;
    }

    TestFixture toTypes(List<SqlTypeName> newTypes)
    {
      this.newTypes = newTypes;
      return this;
    }

    TestFixture expect(List<Object[]> expected)
    {
      this.expected = expected;
      return this;
    }

    TestFixture reorder(int[] mapping)
    {
      this.mapping = mapping;
      return this;
    }

    void run()
    {
      assertEquals(expected.size(), input.size());
      if (mapping == null) {
        if (input.isEmpty()) {
          mapping = new int[0];
        } else {
          int width = input.get(0).length;
          mapping = new int[width];
          for (int i = 0; i < mapping.length; i++) {
            mapping[i] = i;
          }
        }
      }
      FragmentManager fragment = Fragments.defaultFragment();
      MockResultsOperator inputOp = new MockResultsOperator(input);
      Operator<Object[]> op = new ProjectResultsOperator(
          fragment,
          inputOp,
          mapping,
          newTypes,
          jsonMapper,
          timeZone,
          serializeComplexValues,
          stringifyArrays
      );
      fragment.registerRoot(op);
      output = fragment.toList();
      verify();

      fragment = Fragments.defaultFragment();
      inputOp = new MockResultsOperator(input);
      op = new ProjectResultsOperatorEx(
          fragment,
          inputOp,
          mapping,
          newTypes,
          jsonMapper,
          timeZone,
          serializeComplexValues,
          stringifyArrays);
      fragment.registerRoot(op);
      output = fragment.toList();
      verify();
    }

    /**
     * Do-it-ourselves equality check since the JUnit one uses sameness
     * as the equality check for arrays.
     */
    private void verify()
    {
      assertEquals(expected.size(), output.size());
      for (int i = 0; i < output.size(); i++) {
        Object[] expectedRow = expected.get(i);
        Object[] actualRow = output.get(i);
        assertEquals(expectedRow.length, actualRow.length);
        for (int j = 0; j < actualRow.length; j++) {
          assertEquals(expectedRow[j], actualRow[j]);
        }
      }
    }
  }

  @Test
  public void testEmptySchema()
  {
    new TestFixture()
        .withInput(Collections.emptyList())
        .toTypes(Collections.emptyList())
        .expect(Collections.emptyList())
        .run();
  }

  @Test
  public void testSingleColumnNoConversion()
  {
    List<Object[]> data = Arrays.asList(new Object[] {"foo"}, new Object[] {"bar"});
    new TestFixture()
        .withInput(data)
        .toTypes(Collections.singletonList(SqlTypeName.VARCHAR))
        .expect(data)
        .run();
  }

  @Test
  public void testReorderNoConversion()
  {
    new TestFixture()
        .withInput(Arrays.asList(
            new Object[] {"foo", 10L},
            new Object[] {"bar", 20L}))
        .reorder(new int[] {1, 0})
        .toTypes(Arrays.asList(
            SqlTypeName.BIGINT,
            SqlTypeName.VARCHAR))
        .expect(Arrays.asList(
            new Object[] {10L, "foo"},
            new Object[] {20L, "bar"}))
        .run();
  }

  private List<Object[]> toRows(List<Object> values)
  {
    List<Object[]> rows = new ArrayList<>();
    for (Object value : values) {
      rows.add(new Object[] {value});
    }
    return rows;
  }

  private void testConversion(
      List<Object> input,
      SqlTypeName toType,
      List<Object> expected
  )
  {
    new TestFixture()
      .withInput(toRows(input))
      .toTypes(Collections.singletonList(toType))
      .expect(toRows(expected))
      .run();
  }

  @Test
  public void testCastToString()
  {
    testConversion(
        Arrays.asList(null, "foo", 10, 20L, 30.3F, 40.4D, Arrays.asList(10, 20)),
        SqlTypeName.VARCHAR,
        Arrays.asList(null, "foo", "10", "20", "30.3", "40.4", "[\"10\",\"20\"]"));
  }

  @Test
  public void testCastToBoolean()
  {
    testConversion(
        Arrays.asList(null, "true", "false", 0, 1, 20L, 0F, 30.3F, 0D, 40.4D),
        SqlTypeName.BOOLEAN,
        Arrays.asList(null, true, false, false, true, true, false, true, false, true));
  }

  @Test
  public void testCastToInteger()
  {
    testConversion(
        Arrays.asList(null, "foo", "5", "6.3", 0, 10, 20L, 30.3F, 40.6D),
        SqlTypeName.INTEGER,
        // Note the conversion from 6.3 is not valid
        // Note the conversion from 40.6 to 40: casting truncates, not rounds
        Arrays.asList(null, null, 5, null, 0, 10, 20, 30, 40));
  }

  @Test
  public void testCastToBigInt()
  {
    testConversion(
        Arrays.asList(null, "foo", "5", "6.3", 0, 10, 20L, 30.3F, 40.6D),
        SqlTypeName.BIGINT,
        // Note the conversion from 6.3 is not valid
        // Note the conversion from 40.6 to 40: casting truncates, not rounds
        Arrays.asList(null, null, 5L, null, 0L, 10L, 20L, 30L, 40L));
  }

  @Test
  public void testCastToFloat()
  {
    testConversion(
        Arrays.asList(null, "foo", "5", "6.3", 0, 10, 20L, 30.25F, 40.5D),
        SqlTypeName.FLOAT,
        Arrays.asList(null, null, 5F, 6.3F, 0F, 10F, 20F, 30.25F, 40.5F));
  }

  @Test
  public void testCastToDouble()
  {
    testConversion(
        Arrays.asList(null, "foo", "5", "6.3", 0, 10, 20L, 30.25F, 40.5D),
        SqlTypeName.DOUBLE,
        Arrays.asList(null, null, 5D, 6.3D, 0D, 10D, 20D, 30.25D, 40.5D));
  }

  @Test
  public void testArrayStringify()
  {
    TestFixture fixture = new TestFixture()
        .withInput(
            toRows(
                Arrays.asList(null, "foo", 10, 20L, 30.25F, 40.5D, new long[] {50, 60})))
        .toTypes(Collections.singletonList(SqlTypeName.ARRAY))
        .expect(
            toRows(
                Arrays.asList(null, "foo", "10", "20", "30.25", "40.5", "[50,60]")));
    fixture.stringifyArrays = true;
    fixture.run();
  }

  @Test
  public void testArrayWitoutStringify()
  {
    TestFixture fixture = new TestFixture()
        .withInput(
            toRows(
                Arrays.asList(
                    null,
                    Arrays.asList("foo", 10),
                    new String[] {"a1", "a2"},
                    new Long[] {10L, 20L},
                    new Double[] {10.25D, 20.5D},
                    new Object[] {"bar", 20})))
        .toTypes(Collections.singletonList(SqlTypeName.ARRAY))
        .expect(
            toRows(
                Arrays.asList(
                    null,
                    Arrays.asList("foo", 10),
                    Arrays.asList("a1", "a2"),
                    Arrays.asList(10L, 20L),
                    Arrays.asList(10.25D, 20.5D),
                    Arrays.asList("bar", 20))));
    fixture.stringifyArrays = false;
    fixture.run();
  }
}
