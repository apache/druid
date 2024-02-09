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
import org.apache.druid.query.filter.ArrayContainsElementFilter;
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
public class ArrayContainsElementFilterTests
{
  @RunWith(Parameterized.class)
  public static class ArrayContainsElementFilterTest extends BaseFilterTest
  {
    public ArrayContainsElementFilterTest(
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
      BaseFilterTest.tearDown(ArrayContainsElementFilterTest.class.getName());
    }

    @Test
    public void testArrayStringColumn()
    {
      Assume.assumeFalse(testName.contains("frame (columnar)") || testName.contains("rowBasedWithoutTypeSignature"));
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
          new ArrayContainsElementFilter(
              "arrayString",
              ColumnType.STRING,
              "a",
              null
          ),
          ImmutableList.of("0", "3")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
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
          new ArrayContainsElementFilter(
              "arrayString",
              ColumnType.STRING,
              "c",
              null
          ),
          ImmutableList.of("0", "3", "4")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
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
          new ArrayContainsElementFilter(
              "arrayString",
              ColumnType.STRING,
              null,
              null
          ),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
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
      Assume.assumeFalse(testName.contains("frame (columnar)") || testName.contains("rowBasedWithoutTypeSignature"));
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
          new ArrayContainsElementFilter(
              "arrayLong",
              ColumnType.LONG,
              2L,
              null
          ),
          ImmutableList.of("0", "2")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
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
          new ArrayContainsElementFilter(
              "arrayLong",
              ColumnType.LONG,
              null,
              null
          ),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
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
          new ArrayContainsElementFilter(
              "arrayLong",
              ColumnType.DOUBLE,
              2.0,
              null
          ),
          ImmutableList.of("0", "2")
      );

      assertFilterMatches(
          new ArrayContainsElementFilter(
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
      Assume.assumeFalse(testName.contains("frame (columnar)") || testName.contains("rowBasedWithoutTypeSignature"));
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
          new ArrayContainsElementFilter(
              "arrayDouble",
              ColumnType.DOUBLE,
              2.2,
              null
          ),
          ImmutableList.of("0", "1")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
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
          new ArrayContainsElementFilter(
              "arrayDouble",
              ColumnType.STRING,
              "2.2",
              null
          ),
          ImmutableList.of("0", "1")
      );

      assertFilterMatches(
          new ArrayContainsElementFilter(
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
      Assume.assumeFalse(testName.contains("frame (columnar)") || testName.contains("rowBasedWithoutTypeSignature"));
      // these are not nested arrays, expect no matches
      assertFilterMatches(
          new ArrayContainsElementFilter(
              "arrayString",
              ColumnType.STRING_ARRAY,
              ImmutableList.of("a", "b", "c"),
              null
          ),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
                  "arrayString",
                  ColumnType.STRING_ARRAY,
                  ImmutableList.of("a", "b", "c"),
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "1", "3", "4", "5")
          : ImmutableList.of("0", "1", "2", "3", "4", "5")
      );
    }

    @Test
    public void testArrayLongColumnContainsArrays()
    {
      Assume.assumeFalse(testName.contains("frame (columnar)") || testName.contains("rowBasedWithoutTypeSignature"));

      // these are not nested arrays, expect no matches
      assertFilterMatches(
          new ArrayContainsElementFilter(
              "arrayLong",
              ColumnType.LONG_ARRAY,
              ImmutableList.of(1L, 2L, 3L),
              null
          ),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
                  "arrayLong",
                  ColumnType.LONG_ARRAY,
                  ImmutableList.of(1L, 2L, 3L),
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "1", "2", "4", "5")
          : ImmutableList.of("0", "1", "2", "3", "4", "5")
      );
    }

    @Test
    public void testArrayDoubleColumnContainsArrays()
    {
      Assume.assumeFalse(testName.contains("frame (columnar)") || testName.contains("rowBasedWithoutTypeSignature"));
      // these are not nested arrays, expect no matches
      assertFilterMatches(
          new ArrayContainsElementFilter(
              "arrayDouble",
              ColumnType.DOUBLE_ARRAY,
              ImmutableList.of(1.1, 2.2, 3.3),
              null
          ),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
                  "arrayDouble",
                  ColumnType.DOUBLE_ARRAY,
                  ImmutableList.of(1.1, 2.2, 3.3),
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
          new ArrayContainsElementFilter("s0", ColumnType.STRING, "a", null),
          ImmutableList.of("1", "5")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("s0", ColumnType.STRING, "b", null),
          ImmutableList.of("2")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("s0", ColumnType.STRING, "c", null),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("s0", ColumnType.STRING, "noexist", null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("s0", ColumnType.STRING_ARRAY, ImmutableList.of("c"), null),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("s0", ColumnType.STRING_ARRAY, ImmutableList.of("a", "c"), null),
          ImmutableList.of()
      );

      assertFilterMatches(
          new ArrayContainsElementFilter("d0", ColumnType.DOUBLE, 10.1, null),
          ImmutableList.of("1")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("d0", ColumnType.DOUBLE, 120.0245, null),
          ImmutableList.of("3")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("d0", ColumnType.DOUBLE, 765.432, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("d0", ColumnType.DOUBLE, 765.431, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("d0", ColumnType.DOUBLE_ARRAY, new Object[]{10.1}, null),
          ImmutableList.of("1")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("d0", ColumnType.DOUBLE_ARRAY, new Object[]{10.1, 120.0245}, null),
          ImmutableList.of()
      );

      assertFilterMatches(
          new ArrayContainsElementFilter("l0", ColumnType.LONG, 100L, null),
          ImmutableList.of("1")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("l0", ColumnType.LONG, 40L, null),
          ImmutableList.of("2")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("l0", ColumnType.LONG, 9001L, null),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("l0", ColumnType.LONG, 9000L, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("l0", ColumnType.LONG_ARRAY, ImmutableList.of(9001L), null),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("l0", ColumnType.LONG_ARRAY, ImmutableList.of(40L, 9001L), null),
          ImmutableList.of()
      );
    }

    @Test
    public void testArrayContainsNestedArray()
    {
      // only auto schema supports array columns... skip other segment types
      Assume.assumeFalse(testName.contains("frame (columnar)") || testName.contains("rowBasedWithoutTypeSignature"));
      assertFilterMatchesSkipVectorize(
          new ArrayContainsElementFilter("nestedArrayLong", ColumnType.LONG_ARRAY, new Object[]{1L, 2L, 3L}, null),
          ImmutableList.of("0", "2")
      );

      assertFilterMatchesSkipVectorize(
          new ArrayContainsElementFilter("nestedArrayLong", ColumnType.LONG_ARRAY, new Object[]{1L, 2L}, null),
          ImmutableList.of()
      );
    }

    @Test
    public void testArrayContainsMvd()
    {
      assertFilterMatches(
          new ArrayContainsElementFilter("dim2", ColumnType.STRING, "a", null),
          ImmutableList.of("0", "3")
      );
      if (isAutoSchema()) {
        assertFilterMatches(
            NotDimFilter.of(new ArrayContainsElementFilter("dim2", ColumnType.STRING, "a", null)),
            NullHandling.sqlCompatible()
            ? ImmutableList.of("1", "2", "4")
            : ImmutableList.of("1", "2", "4", "5")
        );
        // [""] becomes [null] in default value mode
        assertFilterMatches(
            new ArrayContainsElementFilter("dim2", ColumnType.STRING, null, null),
            NullHandling.sqlCompatible() ? ImmutableList.of() : ImmutableList.of("2")
        );
      } else {
        // multi-value dimension treats [] as null, so in sql compatible mode row 1 ends up as not matching
        assertFilterMatches(
            NotDimFilter.of(new ArrayContainsElementFilter("dim2", ColumnType.STRING, "a", null)),
            NullHandling.sqlCompatible()
            ? ImmutableList.of("2", "4")
            : ImmutableList.of("1", "2", "4", "5")
        );
        assertFilterMatches(
            new ArrayContainsElementFilter("dim2", ColumnType.STRING, null, null),
            ImmutableList.of()
        );
      }
    }

    @Test
    public void testNestedArrayStringColumn()
    {
      // duplicate of testArrayStringColumn but targeting nested.arrayString
      Assume.assumeFalse(testName.contains("frame (columnar)") || testName.contains("rowBasedWithoutTypeSignature"));
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
          new ArrayContainsElementFilter(
              "nested.arrayString",
              ColumnType.STRING,
              "a",
              null
          ),
          ImmutableList.of("0", "3")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
                  "nested.arrayString",
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
          new ArrayContainsElementFilter(
              "nested.arrayString",
              ColumnType.STRING,
              "c",
              null
          ),
          ImmutableList.of("0", "3", "4")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
                  "nested.arrayString",
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
          new ArrayContainsElementFilter(
              "nested.arrayString",
              ColumnType.STRING,
              null,
              null
          ),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
                  "nested.arrayString",
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
    public void testNestedArrayLongColumn()
    {
      // duplicate of testArrayLongColumn but targeting nested.arrayLong
      Assume.assumeFalse(testName.contains("frame (columnar)") || testName.contains("rowBasedWithoutTypeSignature"));
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
          new ArrayContainsElementFilter(
              "nested.arrayLong",
              ColumnType.LONG,
              2L,
              null
          ),
          ImmutableList.of("0", "2")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
                  "nested.arrayLong",
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
          new ArrayContainsElementFilter(
              "nested.arrayLong",
              ColumnType.LONG,
              null,
              null
          ),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
                  "nested.arrayLong",
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
          new ArrayContainsElementFilter(
              "nested.arrayLong",
              ColumnType.DOUBLE,
              2.0,
              null
          ),
          ImmutableList.of("0", "2")
      );

      assertFilterMatches(
          new ArrayContainsElementFilter(
              "nested.arrayLong",
              ColumnType.STRING,
              "2",
              null
          ),
          ImmutableList.of("0", "2")
      );
    }

    @Test
    public void testNestedArrayDoubleColumn()
    {
      // duplicate of testArrayDoubleColumn but targeting nested.arrayDouble
      Assume.assumeTrue(canTestArrayColumns());
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
          new ArrayContainsElementFilter(
              "nested.arrayDouble",
              ColumnType.DOUBLE,
              2.2,
              null
          ),
          ImmutableList.of("0", "1")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
                  "nested.arrayDouble",
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
          new ArrayContainsElementFilter(
              "nested.arrayDouble",
              ColumnType.STRING,
              "2.2",
              null
          ),
          ImmutableList.of("0", "1")
      );

      assertFilterMatches(
          new ArrayContainsElementFilter(
              "nested.arrayDouble",
              ColumnType.DOUBLE,
              null,
              null
          ),
          ImmutableList.of("2")
      );
    }

    @Test
    public void testNestedArrayStringColumnContainsArrays()
    {
      // duplicate of testArrayStringColumnContainsArrays but targeting nested.arrayString
      Assume.assumeTrue(canTestArrayColumns());
      // these are not nested arrays, expect no matches
      assertFilterMatches(
          new ArrayContainsElementFilter(
              "nested.arrayString",
              ColumnType.STRING_ARRAY,
              ImmutableList.of("a", "b", "c"),
              null
          ),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
                  "nested.arrayString",
                  ColumnType.STRING_ARRAY,
                  ImmutableList.of("a", "b", "c"),
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "1", "3", "4", "5")
          : ImmutableList.of("0", "1", "2", "3", "4", "5")
      );
    }

    @Test
    public void testNestedArrayLongColumnContainsArrays()
    {
      // duplicate of testArrayLongColumnContainsArrays but targeting nested.arrayLong
      Assume.assumeTrue(canTestArrayColumns());

      // these are not nested arrays, expect no matches
      assertFilterMatches(
          new ArrayContainsElementFilter(
              "nested.arrayLong",
              ColumnType.LONG_ARRAY,
              ImmutableList.of(1L, 2L, 3L),
              null
          ),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
                  "nested.arrayLong",
                  ColumnType.LONG_ARRAY,
                  ImmutableList.of(1L, 2L, 3L),
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "1", "2", "4", "5")
          : ImmutableList.of("0", "1", "2", "3", "4", "5")
      );
    }

    @Test
    public void testNestedArrayDoubleColumnContainsArrays()
    {
      // duplicate of testArrayDoubleColumnContainsArrays but targeting nested.arrayDouble
      Assume.assumeTrue(canTestArrayColumns());
      // these are not nested arrays, expect no matches
      assertFilterMatches(
          new ArrayContainsElementFilter(
              "nested.arrayDouble",
              ColumnType.DOUBLE_ARRAY,
              ImmutableList.of(1.1, 2.2, 3.3),
              null
          ),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(
              new ArrayContainsElementFilter(
                  "nested.arrayDouble",
                  ColumnType.DOUBLE_ARRAY,
                  ImmutableList.of(1.1, 2.2, 3.3),
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "1", "2", "3", "4")
          : ImmutableList.of("0", "1", "2", "3", "4", "5")
      );
    }

    @Test
    public void testNestedScalarColumnContains()
    {
      Assume.assumeTrue(canTestArrayColumns());

      // duplicate of testScalarColumnContains but targeting nested columns
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.s0", ColumnType.STRING, "a", null),
          ImmutableList.of("1", "5")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.s0", ColumnType.STRING, "b", null),
          ImmutableList.of("2")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.s0", ColumnType.STRING, "c", null),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.s0", ColumnType.STRING, "noexist", null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.s0", ColumnType.STRING_ARRAY, ImmutableList.of("c"), null),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.s0", ColumnType.STRING_ARRAY, ImmutableList.of("a", "c"), null),
          ImmutableList.of()
      );

      assertFilterMatches(
          new ArrayContainsElementFilter("nested.d0", ColumnType.DOUBLE, 10.1, null),
          ImmutableList.of("1")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.d0", ColumnType.DOUBLE, 120.0245, null),
          ImmutableList.of("3")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.d0", ColumnType.DOUBLE, 765.432, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.d0", ColumnType.DOUBLE, 765.431, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.d0", ColumnType.DOUBLE_ARRAY, new Object[]{10.1}, null),
          ImmutableList.of("1")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.d0", ColumnType.DOUBLE_ARRAY, new Object[]{10.1, 120.0245}, null),
          ImmutableList.of()
      );

      assertFilterMatches(
          new ArrayContainsElementFilter("nested.l0", ColumnType.LONG, 100L, null),
          ImmutableList.of("1")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.l0", ColumnType.LONG, 40L, null),
          ImmutableList.of("2")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.l0", ColumnType.LONG, 9001L, null),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.l0", ColumnType.LONG, 9000L, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.l0", ColumnType.LONG_ARRAY, ImmutableList.of(9001L), null),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new ArrayContainsElementFilter("nested.l0", ColumnType.LONG_ARRAY, ImmutableList.of(40L, 9001L), null),
          ImmutableList.of()
      );
    }
  }

  public static class ArrayContainsElementFilterNonParameterizedTests extends InitializedNullHandlingTest
  {
    @Test
    public void testSerde() throws JsonProcessingException
    {
      ObjectMapper mapper = new DefaultObjectMapper();
      ArrayContainsElementFilter filter = new ArrayContainsElementFilter("x", ColumnType.STRING, "hello", null);
      String s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsElementFilter.class));

      filter = new ArrayContainsElementFilter("x", ColumnType.LONG, 1L, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsElementFilter.class));

      filter = new ArrayContainsElementFilter("x", ColumnType.LONG, 1, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsElementFilter.class));

      filter = new ArrayContainsElementFilter("x", ColumnType.DOUBLE, 111.111, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsElementFilter.class));

      filter = new ArrayContainsElementFilter("x", ColumnType.FLOAT, 1234.0f, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsElementFilter.class));

      filter = new ArrayContainsElementFilter("x", ColumnType.STRING_ARRAY, new Object[]{"a", "b", null, "c"}, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsElementFilter.class));

      filter = new ArrayContainsElementFilter("x", ColumnType.STRING_ARRAY, Arrays.asList("a", "b", null, "c"), null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsElementFilter.class));

      filter = new ArrayContainsElementFilter("x", ColumnType.LONG_ARRAY, new Object[]{1L, null, 2L, 3L}, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsElementFilter.class));

      filter = new ArrayContainsElementFilter("x", ColumnType.LONG_ARRAY, Arrays.asList(1L, null, 2L, 3L), null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsElementFilter.class));

      filter = new ArrayContainsElementFilter("x", ColumnType.DOUBLE_ARRAY, new Object[]{1.1, 2.1, null, 3.1}, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsElementFilter.class));

      filter = new ArrayContainsElementFilter("x", ColumnType.DOUBLE_ARRAY, Arrays.asList(1.1, 2.1, null, 3.1), null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsElementFilter.class));

      filter = new ArrayContainsElementFilter(
          "x",
          ColumnType.NESTED_DATA,
          ImmutableMap.of("x", ImmutableList.of(1, 2, 3)),
          null
      );
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, ArrayContainsElementFilter.class));
    }

    @Test
    public void testRewrite()
    {
      ArrayContainsElementFilter filter = new ArrayContainsElementFilter("x", ColumnType.STRING, "hello", null);
      Filter rewrite = filter.rewriteRequiredColumns(ImmutableMap.of("x", "y"));
      ArrayContainsElementFilter expected = new ArrayContainsElementFilter("y", ColumnType.STRING, "hello", null);
      Assert.assertEquals(expected, rewrite);
    }

    @Test
    public void testGetCacheKey()
    {
      ArrayContainsElementFilter f1 = new ArrayContainsElementFilter("x", ColumnType.STRING, "hello", null);
      ArrayContainsElementFilter f1_2 = new ArrayContainsElementFilter("x", ColumnType.STRING, "hello", null);
      ArrayContainsElementFilter f2 = new ArrayContainsElementFilter("x", ColumnType.STRING, "world", null);
      ArrayContainsElementFilter f3 = new ArrayContainsElementFilter(
          "x",
          ColumnType.STRING,
          "hello",
          new FilterTuning(true, null, null)
      );
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new ArrayContainsElementFilter("x", ColumnType.LONG, 1L, null);
      f1_2 = new ArrayContainsElementFilter("x", ColumnType.LONG, 1, null);
      f2 = new ArrayContainsElementFilter("x", ColumnType.LONG, 2L, null);
      f3 = new ArrayContainsElementFilter("x", ColumnType.LONG, 1L, new FilterTuning(true, null, null));
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new ArrayContainsElementFilter("x", ColumnType.DOUBLE, 1.1, null);
      f1_2 = new ArrayContainsElementFilter("x", ColumnType.DOUBLE, 1.1, null);
      f2 = new ArrayContainsElementFilter("x", ColumnType.DOUBLE, 2.2, null);
      f3 = new ArrayContainsElementFilter("x", ColumnType.DOUBLE, 1.1, new FilterTuning(true, null, null));
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new ArrayContainsElementFilter("x", ColumnType.FLOAT, 1.1f, null);
      f1_2 = new ArrayContainsElementFilter("x", ColumnType.FLOAT, 1.1f, null);
      f2 = new ArrayContainsElementFilter("x", ColumnType.FLOAT, 2.2f, null);
      f3 = new ArrayContainsElementFilter("x", ColumnType.FLOAT, 1.1f, new FilterTuning(true, null, null));
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new ArrayContainsElementFilter("x", ColumnType.STRING_ARRAY, new Object[]{"a", "b", null, "c"}, null);
      f1_2 = new ArrayContainsElementFilter("x", ColumnType.STRING_ARRAY, Arrays.asList("a", "b", null, "c"), null);
      f2 = new ArrayContainsElementFilter("x", ColumnType.STRING_ARRAY, new Object[]{"a", "b", "c"}, null);
      f3 = new ArrayContainsElementFilter(
          "x",
          ColumnType.STRING_ARRAY,
          new Object[]{"a", "b", null, "c"},
          new FilterTuning(true, null, null)
      );
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new ArrayContainsElementFilter("x", ColumnType.LONG_ARRAY, new Object[]{100L, 200L, null, 300L}, null);
      f1_2 = new ArrayContainsElementFilter("x", ColumnType.LONG_ARRAY, Arrays.asList(100L, 200L, null, 300L), null);
      f2 = new ArrayContainsElementFilter("x", ColumnType.LONG_ARRAY, new Object[]{100L, null, 200L, 300L}, null);
      f3 = new ArrayContainsElementFilter(
          "x",
          ColumnType.LONG_ARRAY,
          new Object[]{100L, 200L, null, 300L},
          new FilterTuning(true, null, null)
      );
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new ArrayContainsElementFilter(
          "x",
          ColumnType.DOUBLE_ARRAY,
          new Object[]{1.001, null, 20.0002, 300.0003},
          null
      );
      f1_2 = new ArrayContainsElementFilter(
          "x",
          ColumnType.DOUBLE_ARRAY,
          Arrays.asList(1.001, null, 20.0002, 300.0003),
          null
      );
      f2 = new ArrayContainsElementFilter(
          "x",
          ColumnType.DOUBLE_ARRAY,
          new Object[]{1.001, 20.0002, 300.0003, null},
          null
      );
      f3 = new ArrayContainsElementFilter(
          "x",
          ColumnType.DOUBLE_ARRAY,
          new Object[]{1.001, null, 20.0002, 300.0003},
          new FilterTuning(true, null, null)
      );
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      NestedDataModule.registerHandlersAndSerde();
      f1 = new ArrayContainsElementFilter(
          "x",
          ColumnType.NESTED_DATA,
          ImmutableMap.of("x", ImmutableList.of(1, 2, 3)),
          null
      );
      f1_2 = new ArrayContainsElementFilter(
          "x",
          ColumnType.NESTED_DATA,
          ImmutableMap.of("x", ImmutableList.of(1, 2, 3)),
          null
      );
      f2 = new ArrayContainsElementFilter(
          "x",
          ColumnType.NESTED_DATA,
          ImmutableMap.of("x", ImmutableList.of(1, 2, 3, 4)),
          null
      );
      f3 = new ArrayContainsElementFilter(
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
          () -> new ArrayContainsElementFilter(null, ColumnType.STRING, null, null)
      );
      Assert.assertEquals("Invalid array_contains filter, column cannot be null", t.getMessage());
      t = Assert.assertThrows(
          DruidException.class,
          () -> new ArrayContainsElementFilter("dim0", null, null, null)
      );
      Assert.assertEquals(
          "Invalid array_contains filter on column [dim0], elementMatchValueType cannot be null",
          t.getMessage()
      );
    }


    @Test
    public void test_equals()
    {
      EqualsVerifier.forClass(ArrayContainsElementFilter.class).usingGetClass()
                    .withNonnullFields(
                        "column",
                        "elementMatchValueType",
                        "elementMatchValueEval",
                        "elementMatchValue",
                        "predicateFactory",
                        "optimizedFilterIncludeUnknown",
                        "optimizedFilterNoIncludeUnknown"
                    )
                    .withPrefabValues(ColumnType.class, ColumnType.STRING, ColumnType.DOUBLE)
                    .withIgnoredFields(
                        "predicateFactory",
                        "optimizedFilterIncludeUnknown",
                        "optimizedFilterNoIncludeUnknown",
                        "elementMatchValue"
                    )
                    .verify();
    }
  }
}
