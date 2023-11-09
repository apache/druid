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

package org.apache.druid.segment.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.filter.ArrayContainsFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.Arrays;

@RunWith(Enclosed.class)
public class ArrayContainsFilterTests
{
  @RunWith(Parameterized.class)
  public static class ArrayContainsFilterTest extends BaseFilterTest
  {
    public ArrayContainsFilterTest(
        String testName,
        IndexBuilder indexBuilder,
        Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
        boolean cnf,
        boolean optimize
    )
    {
      super(testName, DEFAULT_ROWS, indexBuilder, finisher, cnf, optimize);
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
      BaseFilterTest.tearDown(ArrayContainsFilterTest.class.getName());
    }

    @Test
    public void testArrayStringColumn()
    {
      // only auto schema supports array columns... skip other segment types
      Assume.assumeTrue(isAutoSchema());
        /*
            dim0 .. arrayString
            "0", .. ["a", "b", "c"]
            "1", .. []
            "2", .. null
            "3", .. ["a", "b", "c"]
            "4", .. ["c", "d"]
            "5", .. [null]
         */

      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayString",
              ColumnType.STRING,
              "a",
              null
          ),
          ImmutableList.of("0", "3")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsFilter(
                  "arrayString",
                  ColumnType.STRING,
                  "a",
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("1", "4", "5")
          : ImmutableList.of("1", "2", "4", "5")
      );

      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayString",
              ColumnType.STRING,
              "c",
              null
          ),
          ImmutableList.of("0", "3", "4")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsFilter(
                  "arrayString",
                  ColumnType.STRING,
                  "c",
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("1", "5")
          : ImmutableList.of("1", "2", "5")
      );

      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayString",
              ColumnType.STRING,
              null,
              null
          ),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsFilter(
                  "arrayString",
                  ColumnType.STRING,
                  null,
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "1", "3", "4")
          : ImmutableList.of("0", "1", "2", "3", "4")
      );
    }

    @Test
    public void testArrayLongColumn()
    {
      // only auto schema supports array columns... skip other segment types
      Assume.assumeTrue(isAutoSchema());
        /*
            dim0 .. arrayLong
            "0", .. [1L, 2L, 3L]
            "1", .. []
            "2", .. [1L, 2L, 3L]
            "3", .. null
            "4", .. [null]
            "5", .. [123L, 345L]
         */
      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayLong",
              ColumnType.LONG,
              2L,
              null
          ),
          ImmutableList.of("0", "2")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsFilter(
                  "arrayLong",
                  ColumnType.LONG,
                  2L,
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("1", "4", "5")
          : ImmutableList.of("1", "3", "4", "5")
      );

      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayLong",
              ColumnType.LONG,
              null,
              null
          ),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsFilter(
                  "arrayLong",
                  ColumnType.LONG,
                  null,
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "1", "2", "5")
          : ImmutableList.of("0", "1", "2", "3", "5")
      );

      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayLong",
              ColumnType.DOUBLE,
              2.0,
              null
          ),
          ImmutableList.of("0", "2")
      );

      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayLong",
              ColumnType.STRING,
              "2",
              null
          ),
          ImmutableList.of("0", "2")
      );
    }

    @Test
    public void testArrayDoubleColumn()
    {
      // only auto schema supports array columns... skip other segment types
      Assume.assumeTrue(isAutoSchema());
        /*
            dim0 .. arrayDouble
            "0", .. [1.1, 2.2, 3.3]
            "1", .. [1.1, 2.2, 3.3]
            "2", .. [null]
            "3", .. []
            "4", .. [-1.1, -333.3]
            "5", .. null
         */

      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayDouble",
              ColumnType.DOUBLE,
              2.2,
              null
          ),
          ImmutableList.of("0", "1")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsFilter(
                  "arrayDouble",
                  ColumnType.DOUBLE,
                  2.2,
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("2", "3", "4")
          : ImmutableList.of("2", "3", "4", "5")
      );

      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayDouble",
              ColumnType.STRING,
              "2.2",
              null
          ),
          ImmutableList.of("0", "1")
      );

      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayDouble",
              ColumnType.DOUBLE,
              null,
              null
          ),
          ImmutableList.of("2")
      );
    }

    @Test
    public void testArrayStringColumnContainsArrays()
    {
      // only auto schema supports array columns... skip other segment types
      Assume.assumeTrue(isAutoSchema());
        /*
            dim0 .. arrayString
            "0", .. ["a", "b", "c"]
            "1", .. []
            "2", .. null
            "3", .. ["a", "b", "c"]
            "4", .. ["c", "d"]
            "5", .. [null]
         */

      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayString",
              ColumnType.STRING_ARRAY,
              ImmutableList.of("a", "b", "c"),
              null
          ),
          ImmutableList.of("0", "3")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsFilter(
                  "arrayString",
                  ColumnType.STRING_ARRAY,
                  ImmutableList.of("a", "b", "c"),
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("1", "4", "5")
          : ImmutableList.of("1", "2", "4", "5")
      );
      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayString",
              ColumnType.STRING_ARRAY,
              new Object[]{"a", "b", "c"},
              null
          ),
          ImmutableList.of("0", "3")
      );
      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayString",
              ColumnType.STRING_ARRAY,
              new Object[]{null},
              null
          ),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayString",
              ColumnType.STRING_ARRAY,
              new Object[]{null, null},
              null
          ),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsFilter(
                  "arrayString",
                  ColumnType.STRING_ARRAY,
                  new Object[]{null, null},
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "1", "3", "4")
          : ImmutableList.of("0", "1", "2", "3", "4")
      );
    }

    @Test
    public void testArrayLongColumnContainsArrays()
    {
      // only auto schema supports array columns... skip other segment types
      Assume.assumeTrue(isAutoSchema());

        /*
            dim0 .. arrayLong
            "0", .. [1L, 2L, 3L]
            "1", .. []
            "2", .. [1L, 2L, 3L]
            "3", .. null
            "4", .. [null]
            "5", .. [123L, 345L]
         */

      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayLong",
              ColumnType.LONG_ARRAY,
              ImmutableList.of(1L, 2L, 3L),
              null
          ),
          ImmutableList.of("0", "2")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsFilter(
                  "arrayLong",
                  ColumnType.LONG_ARRAY,
                  ImmutableList.of(1L, 2L, 3L),
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("1", "4", "5")
          : ImmutableList.of("1", "3", "4", "5")
      );
      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayLong",
              ColumnType.LONG_ARRAY,
              new Object[]{1L, 2L, 3L},
              null
          ),
          ImmutableList.of("0", "2")
      );
      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayLong",
              ColumnType.LONG_ARRAY,
              new Object[]{null},
              null
          ),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayLong",
              ColumnType.LONG_ARRAY,
              new Object[]{null, null},
              null
          ),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsFilter(
                  "arrayLong",
                  ColumnType.LONG_ARRAY,
                  new Object[]{null, null},
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "1", "2", "5")
          : ImmutableList.of("0", "1", "2", "3", "5")
      );

      // test loss of precision matching long arrays with double array match values
      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{1.0, 2.0, 3.0},
              null
          ),
          ImmutableList.of("0", "2")
      );
      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{1.1, 2.2, 3.3},
              null
          ),
          ImmutableList.of()
      );
      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{null},
              null
          ),
          ImmutableList.of("4")
      );
    }

    @Test
    public void testArrayDoubleColumnContainsArrays()
    {
      // only auto schema supports array columns... skip other segment types
      Assume.assumeTrue(isAutoSchema());
        /*
            dim0 .. arrayDouble
            "0", .. [1.1, 2.2, 3.3]
            "1", .. [1.1, 2.2, 3.3]
            "2", .. [null]
            "3", .. []
            "4", .. [-1.1, -333.3]
            "5", .. null
         */

      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayDouble",
              ColumnType.DOUBLE_ARRAY,
              ImmutableList.of(1.1, 2.2, 3.3),
              null
          ),
          ImmutableList.of("0", "1")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsFilter(
                  "arrayDouble",
                  ColumnType.DOUBLE_ARRAY,
                  ImmutableList.of(1.1, 2.2, 3.3),
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("2", "3", "4")
          : ImmutableList.of("2", "3", "4", "5")
      );
      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayDouble",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{1.1, 2.2, 3.3},
              null
          ),
          ImmutableList.of("0", "1")
      );
      assertFilterMatches(
          new ArrayContainsFilter(
              "arrayDouble",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{null},
              null
          ),
          ImmutableList.of("2")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsFilter(
                  "arrayDouble",
                  ColumnType.DOUBLE_ARRAY,
                  ImmutableList.of(1.1, 2.2, 3.4),
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "1", "2", "3", "4")
          : ImmutableList.of("0", "1", "2", "3", "4", "5")
      );
    }

    @Test
    public void testScalarColumnContains()
    {
      assertFilterMatches(
          new ArrayContainsFilter("s0", ColumnType.STRING, "a", null),
          ImmutableList.of("1", "5")
      );
      assertFilterMatches(
          new ArrayContainsFilter("s0", ColumnType.STRING, "b", null),
          ImmutableList.of("2")
      );
      assertFilterMatches(
          new ArrayContainsFilter("s0", ColumnType.STRING, "c", null),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new ArrayContainsFilter("s0", ColumnType.STRING, "noexist", null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new ArrayContainsFilter("s0", ColumnType.STRING_ARRAY, ImmutableList.of("c"), null),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new ArrayContainsFilter("s0", ColumnType.STRING_ARRAY, ImmutableList.of("a", "c"), null),
          ImmutableList.of()
      );

      assertFilterMatches(
          new ArrayContainsFilter("d0", ColumnType.DOUBLE, 10.1, null),
          ImmutableList.of("1")
      );
      assertFilterMatches(
          new ArrayContainsFilter("d0", ColumnType.DOUBLE, 120.0245, null),
          ImmutableList.of("3")
      );
      assertFilterMatches(
          new ArrayContainsFilter("d0", ColumnType.DOUBLE, 765.432, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          new ArrayContainsFilter("d0", ColumnType.DOUBLE, 765.431, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new ArrayContainsFilter("d0", ColumnType.DOUBLE_ARRAY, new Object[]{10.1}, null),
          ImmutableList.of("1")
      );
      assertFilterMatches(
          new ArrayContainsFilter("d0", ColumnType.DOUBLE_ARRAY, new Object[]{10.1, 120.0245}, null),
          ImmutableList.of()
      );

      assertFilterMatches(
          new ArrayContainsFilter("l0", ColumnType.LONG, 100L, null),
          ImmutableList.of("1")
      );
      assertFilterMatches(
          new ArrayContainsFilter("l0", ColumnType.LONG, 40L, null),
          ImmutableList.of("2")
      );
      assertFilterMatches(
          new ArrayContainsFilter("l0", ColumnType.LONG, 9001L, null),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new ArrayContainsFilter("l0", ColumnType.LONG, 9000L, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new ArrayContainsFilter("l0", ColumnType.LONG_ARRAY, ImmutableList.of(9001L), null),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new ArrayContainsFilter("l0", ColumnType.LONG_ARRAY, ImmutableList.of(40L, 9001L), null),
          ImmutableList.of()
      );
    }
  }

  public static class ArrayContainsFilterNonParameterizedTests extends InitializedNullHandlingTest
  {
    @Test
    public void testSerde() throws JsonProcessingException
    {
      ObjectMapper mapper = new DefaultObjectMapper();
      ArrayContainsFilter filter = new ArrayContainsFilter("x", ColumnType.STRING, "hello", null);
      String s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsFilter.class));

      filter = new ArrayContainsFilter("x", ColumnType.LONG, 1L, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsFilter.class));

      filter = new ArrayContainsFilter("x", ColumnType.LONG, 1, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsFilter.class));

      filter = new ArrayContainsFilter("x", ColumnType.DOUBLE, 111.111, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsFilter.class));

      filter = new ArrayContainsFilter("x", ColumnType.FLOAT, 1234.0f, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsFilter.class));

      filter = new ArrayContainsFilter("x", ColumnType.STRING_ARRAY, new Object[]{"a", "b", null, "c"}, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsFilter.class));

      filter = new ArrayContainsFilter("x", ColumnType.STRING_ARRAY, Arrays.asList("a", "b", null, "c"), null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsFilter.class));

      filter = new ArrayContainsFilter("x", ColumnType.LONG_ARRAY, new Object[]{1L, null, 2L, 3L}, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsFilter.class));

      filter = new ArrayContainsFilter("x", ColumnType.LONG_ARRAY, Arrays.asList(1L, null, 2L, 3L), null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsFilter.class));

      filter = new ArrayContainsFilter("x", ColumnType.DOUBLE_ARRAY, new Object[]{1.1, 2.1, null, 3.1}, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsFilter.class));

      filter = new ArrayContainsFilter("x", ColumnType.DOUBLE_ARRAY, Arrays.asList(1.1, 2.1, null, 3.1), null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsFilter.class));

      filter = new ArrayContainsFilter(
          "x",
          ColumnType.NESTED_DATA,
          ImmutableMap.of("x", ImmutableList.of(1, 2, 3)),
          null
      );
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsFilter.class));
    }

    @Test
    public void testRewrite()
    {
      ArrayContainsFilter filter = new ArrayContainsFilter("x", ColumnType.STRING, "hello", null);
      Filter rewrite = filter.rewriteRequiredColumns(ImmutableMap.of("x", "y"));
      ArrayContainsFilter expected = new ArrayContainsFilter("y", ColumnType.STRING, "hello", null);
      Assert.assertEquals(expected, rewrite);
    }

    @Test
    public void testGetCacheKey()
    {
      ArrayContainsFilter f1 = new ArrayContainsFilter("x", ColumnType.STRING, "hello", null);
      ArrayContainsFilter f1_2 = new ArrayContainsFilter("x", ColumnType.STRING, "hello", null);
      ArrayContainsFilter f2 = new ArrayContainsFilter("x", ColumnType.STRING, "world", null);
      ArrayContainsFilter f3 = new ArrayContainsFilter(
          "x",
          ColumnType.STRING,
          "hello",
          new FilterTuning(true, null, null)
      );
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new ArrayContainsFilter("x", ColumnType.LONG, 1L, null);
      f1_2 = new ArrayContainsFilter("x", ColumnType.LONG, 1, null);
      f2 = new ArrayContainsFilter("x", ColumnType.LONG, 2L, null);
      f3 = new ArrayContainsFilter("x", ColumnType.LONG, 1L, new FilterTuning(true, null, null));
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new ArrayContainsFilter("x", ColumnType.DOUBLE, 1.1, null);
      f1_2 = new ArrayContainsFilter("x", ColumnType.DOUBLE, 1.1, null);
      f2 = new ArrayContainsFilter("x", ColumnType.DOUBLE, 2.2, null);
      f3 = new ArrayContainsFilter("x", ColumnType.DOUBLE, 1.1, new FilterTuning(true, null, null));
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new ArrayContainsFilter("x", ColumnType.FLOAT, 1.1f, null);
      f1_2 = new ArrayContainsFilter("x", ColumnType.FLOAT, 1.1f, null);
      f2 = new ArrayContainsFilter("x", ColumnType.FLOAT, 2.2f, null);
      f3 = new ArrayContainsFilter("x", ColumnType.FLOAT, 1.1f, new FilterTuning(true, null, null));
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new ArrayContainsFilter("x", ColumnType.STRING_ARRAY, new Object[]{"a", "b", null, "c"}, null);
      f1_2 = new ArrayContainsFilter("x", ColumnType.STRING_ARRAY, Arrays.asList("a", "b", null, "c"), null);
      f2 = new ArrayContainsFilter("x", ColumnType.STRING_ARRAY, new Object[]{"a", "b", "c"}, null);
      f3 = new ArrayContainsFilter(
          "x",
          ColumnType.STRING_ARRAY,
          new Object[]{"a", "b", null, "c"},
          new FilterTuning(true, null, null)
      );
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new ArrayContainsFilter("x", ColumnType.LONG_ARRAY, new Object[]{100L, 200L, null, 300L}, null);
      f1_2 = new ArrayContainsFilter("x", ColumnType.LONG_ARRAY, Arrays.asList(100L, 200L, null, 300L), null);
      f2 = new ArrayContainsFilter("x", ColumnType.LONG_ARRAY, new Object[]{100L, null, 200L, 300L}, null);
      f3 = new ArrayContainsFilter(
          "x",
          ColumnType.LONG_ARRAY,
          new Object[]{100L, 200L, null, 300L},
          new FilterTuning(true, null, null)
      );
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new ArrayContainsFilter("x", ColumnType.DOUBLE_ARRAY, new Object[]{1.001, null, 20.0002, 300.0003}, null);
      f1_2 = new ArrayContainsFilter("x", ColumnType.DOUBLE_ARRAY, Arrays.asList(1.001, null, 20.0002, 300.0003), null);
      f2 = new ArrayContainsFilter("x", ColumnType.DOUBLE_ARRAY, new Object[]{1.001, 20.0002, 300.0003, null}, null);
      f3 = new ArrayContainsFilter(
          "x",
          ColumnType.DOUBLE_ARRAY,
          new Object[]{1.001, null, 20.0002, 300.0003},
          new FilterTuning(true, null, null)
      );
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      NestedDataModule.registerHandlersAndSerde();
      f1 = new ArrayContainsFilter("x", ColumnType.NESTED_DATA, ImmutableMap.of("x", ImmutableList.of(1, 2, 3)), null);
      f1_2 = new ArrayContainsFilter(
          "x",
          ColumnType.NESTED_DATA,
          ImmutableMap.of("x", ImmutableList.of(1, 2, 3)),
          null
      );
      f2 = new ArrayContainsFilter(
          "x",
          ColumnType.NESTED_DATA,
          ImmutableMap.of("x", ImmutableList.of(1, 2, 3, 4)),
          null
      );
      f3 = new ArrayContainsFilter(
          "x",
          ColumnType.NESTED_DATA,
          ImmutableMap.of("x", ImmutableList.of(1, 2, 3)),
          new FilterTuning(true, null, null)
      );
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());
    }

    @Test
    public void testInvalidParameters()
    {
      Throwable t = Assert.assertThrows(
          DruidException.class,
          () -> new ArrayContainsFilter(null, ColumnType.STRING, null, null)
      );
      Assert.assertEquals("Invalid array_contains filter, column cannot be null", t.getMessage());
      t = Assert.assertThrows(
          DruidException.class,
          () -> new ArrayContainsFilter("dim0", null, null, null)
      );
      Assert.assertEquals(
          "Invalid array_contains filter on column [dim0], elementMatchValueType cannot be null",
          t.getMessage()
      );
    }


    @Test
    public void test_equals()
    {
      EqualsVerifier.forClass(ArrayContainsFilter.class).usingGetClass()
                    .withNonnullFields(
                        "column",
                        "elementMatchValueType",
                        "elementMatchValueEval",
                        "elementMatchValue",
                        "predicateFactory",
                        "cachedOptimizedFilter"
                    )
                    .withPrefabValues(ColumnType.class, ColumnType.STRING, ColumnType.DOUBLE)
                    .withIgnoredFields("predicateFactory", "cachedOptimizedFilter", "elementMatchValue")
                    .verify();
    }
  }
}
