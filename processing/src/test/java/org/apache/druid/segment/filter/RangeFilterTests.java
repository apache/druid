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
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestHelper;
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
import java.util.Collections;
import java.util.List;

@RunWith(Enclosed.class)
public class RangeFilterTests
{
  @RunWith(Parameterized.class)
  public static class RangeFilterTest extends BaseFilterTest
  {
    private static final List<InputRow> ROWS =
        ImmutableList.<InputRow>builder()
                     .addAll(DEFAULT_ROWS)
                     .add(makeDefaultSchemaRow(
                         "6",
                         "-1000",
                         ImmutableList.of("a"),
                         null,
                         "d",
                         6.6,
                         null,
                         10L,
                         new Object[]{"x", "y"},
                         new Object[]{100, 200},
                         new Object[]{1.1, null, 3.3},
                         null,
                         TestHelper.makeMapWithExplicitNull(
                             "s0", "d",
                             "d0", 6.6,
                             "f0", null,
                             "l0", 10L,
                             "arrayString", new Object[]{"x", "y"},
                             "arrayLong", new Object[]{100, 200},
                             "arrayDouble", new Object[]{1.1, null, 3.3}
                         )
                     ))
                     .add(makeDefaultSchemaRow(
                         "7",
                         "-10.012",
                         ImmutableList.of("d"),
                         null,
                         "e",
                         null,
                         3.0f,
                         null,
                         new Object[]{null, "hello", "world"},
                         new Object[]{1234, 3456L, null},
                         new Object[]{1.23, 4.56, 6.78},
                         null,
                         TestHelper.makeMapWithExplicitNull(
                             "s0", "e",
                             "d0", null,
                             "f0", 3.0f,
                             "l0", null,
                             "arrayString", new Object[]{null, "hello", "world"},
                             "arrayLong", new Object[]{1234, 3456L, null},
                             "arrayDouble", new Object[]{1.23, 4.56, 6.78}
                         )
                     ))
                     .build();

    public RangeFilterTest(
        String testName,
        IndexBuilder indexBuilder,
        Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
        boolean cnf,
        boolean optimize
    )
    {
      super(testName, ROWS, indexBuilder, finisher, cnf, optimize);
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
      BaseFilterTest.tearDown(RangeFilterTest.class.getName());
    }

    @Test
    public void testLexicographicalMatch()
    {

      assertFilterMatches(
          new RangeFilter("dim0", ColumnType.STRING, null, "z", false, false, null),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter("vdim0", ColumnType.STRING, null, "z", false, false, null),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );

      if (NullHandling.sqlCompatible()) {
        assertFilterMatches(
            new RangeFilter("dim1", ColumnType.STRING, null, "z", false, false, null),
            ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
        );
        assertFilterMatches(
            new RangeFilter("vdim0", ColumnType.STRING, null, "z", false, false, null),
            ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
        );
        assertFilterMatches(
            NotDimFilter.of(new RangeFilter("dim1", ColumnType.STRING, null, "z", false, false, null)),
            ImmutableList.of()
        );
        assertFilterMatches(
            NotDimFilter.of(new RangeFilter("vdim0", ColumnType.STRING, null, "z", false, false, null)),
            ImmutableList.of()
        );
        assertFilterMatches(
            new RangeFilter("s0", ColumnType.STRING, null, "b", false, false, null),
            ImmutableList.of("0", "1", "2", "5")
        );
        assertFilterMatches(
            new RangeFilter("vs0", ColumnType.STRING, null, "b", false, false, null),
            ImmutableList.of("0", "1", "2", "5")
        );
        assertFilterMatches(
            NotDimFilter.of(new RangeFilter("s0", ColumnType.STRING, null, "b", false, false, null)),
            ImmutableList.of("4", "6", "7")
        );
        assertFilterMatches(
            NotDimFilter.of(new RangeFilter("vs0", ColumnType.STRING, null, "b", false, false, null)),
            ImmutableList.of("4", "6", "7")
        );
      } else {
        assertFilterMatches(
            new RangeFilter("dim1", ColumnType.STRING, null, "z", false, false, null),
            ImmutableList.of("1", "2", "3", "4", "5", "6", "7")
        );
        assertFilterMatches(
            new RangeFilter("vdim0", ColumnType.STRING, null, "z", false, false, null),
            ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
        );
        assertFilterMatches(
            NotDimFilter.of(new RangeFilter("dim1", ColumnType.STRING, null, "z", false, false, null)),
            NullHandling.sqlCompatible() ? ImmutableList.of() : ImmutableList.of("0")
        );
        assertFilterMatches(
            NotDimFilter.of(new RangeFilter("vdim0", ColumnType.STRING, null, "z", false, false, null)),
            NullHandling.sqlCompatible() ? ImmutableList.of("0") : ImmutableList.of()
        );
        assertFilterMatches(
            new RangeFilter("s0", ColumnType.STRING, null, "b", false, false, null),
            ImmutableList.of("1", "2", "5")
        );
        assertFilterMatches(
            new RangeFilter("vs0", ColumnType.STRING, null, "b", false, false, null),
            ImmutableList.of("1", "2", "5")
        );
        assertFilterMatches(
            NotDimFilter.of(new RangeFilter("s0", ColumnType.STRING, null, "b", false, false, null)),
            NullHandling.sqlCompatible()
            ? ImmutableList.of("4", "6", "7")
            : ImmutableList.of("0", "3", "4", "6", "7")
        );
        assertFilterMatches(
            NotDimFilter.of(new RangeFilter("vs0", ColumnType.STRING, null, "b", false, false, null)),
            NullHandling.sqlCompatible()
            ? ImmutableList.of("4", "6", "7")
            : ImmutableList.of("0", "3", "4", "6", "7")
        );
      }

      if (isAutoSchema()) {
        // auto schema ingests arrays instead of mvds.. this is covered in array tests
      } else {
        assertFilterMatches(
            new RangeFilter("dim2", ColumnType.STRING, null, "z", false, false, null),
            NullHandling.sqlCompatible()
            ? ImmutableList.of("0", "2", "3", "4", "6", "7")
            : ImmutableList.of("0", "3", "4", "6", "7")
        );
        // vdim2 does not exist...
        assertFilterMatches(
            new RangeFilter("dim3", ColumnType.STRING, null, "z", false, false, null),
            ImmutableList.of()
        );
      }
    }

    @Test
    public void testLexicographicMatchWithEmptyString()
    {
      if (NullHandling.sqlCompatible()) {
        assertFilterMatches(
            new RangeFilter("dim0", ColumnType.STRING, "", "z", false, false, null),
            ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
        );
        assertFilterMatches(
            new RangeFilter("dim1", ColumnType.STRING, "", "z", false, false, null),
            ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
        );
        if (!isAutoSchema()) {
          // auto schema ingests arrays which are currently incompatible with the range filter
          assertFilterMatches(
              new RangeFilter("dim2", ColumnType.STRING, "", "z", false, false, null),
              ImmutableList.of("0", "2", "3", "4", "6", "7")
          );
        }
        assertFilterMatches(
            new RangeFilter("dim3", ColumnType.STRING, "", "z", false, false, null),
            ImmutableList.of()
        );
      } else {
        assertFilterMatches(
            new RangeFilter("dim0", ColumnType.STRING, "", "z", false, false, null),
            ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
        );
        assertFilterMatches(
            new RangeFilter("dim1", ColumnType.STRING, "", "z", false, false, null),
            ImmutableList.of("1", "2", "3", "4", "5", "6", "7")
        );
        if (!isAutoSchema()) {
          // auto schema ingests arrays which are currently incompatible with the range filter
          assertFilterMatches(
              new RangeFilter("dim2", ColumnType.STRING, "", "z", false, false, null),
              ImmutableList.of("0", "3", "4", "6", "7")
          );
        }
        assertFilterMatches(
            new RangeFilter("dim3", ColumnType.STRING, "", "z", false, false, null),
            ImmutableList.of()
        );
      }
    }

    @Test
    public void testLexicographicMatchNull()
    {

      if (NullHandling.replaceWithDefault()) {
        // in default value mode this is null on both ends...
        Throwable t = Assert.assertThrows(
            DruidException.class,
            () -> assertFilterMatches(
                new RangeFilter("dim0", ColumnType.STRING, "", "", false, false, null),
                ImmutableList.of()
            )
        );
        Assert.assertEquals(
            "Invalid range filter on column [dim0], lower and upper cannot be null at the same time",
            t.getMessage()
        );
      } else {
        assertFilterMatches(
            new RangeFilter("dim0", ColumnType.STRING, "", "", false, false, null),
            ImmutableList.of()
        );
        assertFilterMatches(
            new RangeFilter("dim1", ColumnType.STRING, "", "", false, false, null),
            ImmutableList.of("0")
        );
        // still matches even with auto-schema because match-values are upcast to array types
        assertFilterMatches(
            new RangeFilter("dim2", ColumnType.STRING, "", "", false, false, null),
            ImmutableList.of("2")
        );
      }
    }

    @Test
    public void testLexicographicMatchMissingColumn()
    {
      assertFilterMatches(
          new RangeFilter("dim3", ColumnType.STRING, "", "z", false, false, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("dim3", ColumnType.STRING, "a", null, false, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("dim3", ColumnType.STRING, null, "z", false, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("dim3", ColumnType.STRING, "", "z", true, false, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("dim3", ColumnType.STRING, "", "z", false, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("dim3", ColumnType.STRING, null, "z", false, false, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("dim3", ColumnType.STRING, null, "z", false, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(new RangeFilter("dim3", ColumnType.STRING, null, "z", false, true, null)),
          NullHandling.sqlCompatible() ? ImmutableList.of() : ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
    }


    @Test
    public void testLexicographicMatchTooStrict()
    {
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.STRING, "abc", "abc", true, false, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.STRING, "abc", "abc", true, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.STRING, "abc", "abc", false, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(new RangeFilter("dim1", ColumnType.STRING, "abc", "abc", false, true, null)),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
    }

    @Test
    public void testLexicographicMatchExactlySingleValue()
    {
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.STRING, "abc", "abc", false, false, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          NotDimFilter.of(new RangeFilter("dim1", ColumnType.STRING, "abc", "abc", false, false, null)),
          ImmutableList.of("0", "1", "2", "3", "4", "6", "7")
      );
    }

    @Test
    public void testLexicographicMatchSurroundingSingleValue()
    {
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.STRING, "ab", "abd", true, true, null),
          ImmutableList.of("5")
      );
    }

    @Test
    public void testLexicographicMatchNoUpperLimit()
    {
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.STRING, "ab", null, true, true, null),
          ImmutableList.of("4", "5")
      );
    }

    @Test
    public void testLexicographicMatchNoLowerLimit()
    {
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.STRING, null, "abd", true, true, null),
          NullHandling.replaceWithDefault()
          ? ImmutableList.of("1", "2", "3", "5", "6", "7")
          : ImmutableList.of("0", "1", "2", "3", "5", "6", "7")
      );
    }

    @Test
    public void testLexicographicMatchNumbers()
    {
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.STRING, "1", "3", false, false, null),
          ImmutableList.of("1", "2", "3")
      );
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.STRING, "1", "3", true, true, null),
          ImmutableList.of("1", "2")
      );
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.STRING, "-1", "3", true, true, null),
          ImmutableList.of("1", "2", "3", "6", "7")
      );
    }

    @Test
    public void testNumericMatchTooStrict()
    {
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.LONG, 2L, 2L, true, false, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.LONG, 2L, 2L, true, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.LONG, 2L, 2L, false, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("d0", ColumnType.LONG, 2L, 3L, false, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("d0", ColumnType.DOUBLE, 2L, 3L, false, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(new RangeFilter("d0", ColumnType.DOUBLE, 2L, 3L, false, true, null)),
          NullHandling.replaceWithDefault()
          ? ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
          : ImmutableList.of("0", "1", "3", "4", "5", "6")
      );
      assertFilterMatches(
          new RangeFilter("f0", ColumnType.LONG, 2L, 3L, false, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("f0", ColumnType.DOUBLE, 2L, 3L, false, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(new RangeFilter("f0", ColumnType.DOUBLE, 2L, 3L, false, true, null)),
          NullHandling.replaceWithDefault()
          ? ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
          : ImmutableList.of("0", "1", "2", "3", "5", "7")
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.LONG, 2L, 3L, false, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 2L, 3L, false, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(new RangeFilter("l0", ColumnType.DOUBLE, 2L, 3L, false, true, null)),
          NullHandling.replaceWithDefault()
          ? ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
          : ImmutableList.of("0", "1", "2", "4", "5", "6")
      );
    }

    @Test
    public void testNumericMatchVirtualColumn()
    {
      assertFilterMatches(
          new RangeFilter("expr", ColumnType.LONG, 1L, 2L, false, false, null),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter("expr", ColumnType.DOUBLE, 1.1, 2.0, false, false, null),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter("expr", ColumnType.FLOAT, 1.1f, 2.0f, false, false, null),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );

      assertFilterMatches(
          new RangeFilter("expr", ColumnType.LONG, 2L, 3L, false, false, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("expr", ColumnType.DOUBLE, 2.0, 3.0, false, false, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("expr", ColumnType.FLOAT, 2.0f, 3.0f, false, false, null),
          ImmutableList.of()
      );
    }

    @Test
    public void testNumericMatchExactlySingleValue()
    {
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.LONG, 2L, 2L, false, false, null),
          ImmutableList.of("2")
      );

      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.DOUBLE, -10.012, -10.012, false, false, null),
          ImmutableList.of("7")
      );
      assertFilterMatches(
          new RangeFilter("d0", ColumnType.DOUBLE, 120.0245, 120.0245, false, false, null),
          ImmutableList.of("3")
      );
      assertFilterMatches(
          NotDimFilter.of(new RangeFilter("d0", ColumnType.DOUBLE, 120.0245, 120.0245, false, false, null)),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "1", "4", "5", "6")
          : ImmutableList.of("0", "1", "2", "4", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter("d0", ColumnType.FLOAT, 120.0245f, 120.0245f, false, false, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("d0", ColumnType.FLOAT, 60.0f, 60.0f, false, false, null),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new RangeFilter("f0", ColumnType.DOUBLE, 10.1, 10.1, false, false, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("f0", ColumnType.FLOAT, 10.1f, 10.1f, false, false, null),
          ImmutableList.of("1")
      );
      assertFilterMatches(
          NotDimFilter.of(new RangeFilter("f0", ColumnType.FLOAT, 10.1f, 10.1f, false, false, null)),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "2", "3", "5", "7")
          : ImmutableList.of("0", "2", "3", "4", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.LONG, 12345L, 12345L, false, false, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          NotDimFilter.of(new RangeFilter("l0", ColumnType.LONG, 12345L, 12345L, false, false, null)),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "1", "2", "4", "6")
          : ImmutableList.of("0", "1", "2", "3", "4", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 12345.0, 12345.0, false, false, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.FLOAT, 12345.0f, 12345.0f, false, false, null),
          ImmutableList.of("5")
      );
    }

    @Test
    public void testNumericMatchSurroundingSingleValue()
    {
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.LONG, 1L, 3L, true, true, null),
          ImmutableList.of("2")
      );

      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.LONG, -11L, -10L, false, false, null),
          ImmutableList.of("7")
      );
      assertFilterMatches(
          new RangeFilter("d0", ColumnType.DOUBLE, 120.0, 120.03, false, false, null),
          ImmutableList.of("3")
      );
      assertFilterMatches(
          new RangeFilter("d0", ColumnType.FLOAT, 120.02f, 120.03f, false, false, null),
          ImmutableList.of("3")
      );
      assertFilterMatches(
          new RangeFilter("d0", ColumnType.FLOAT, 59.5f, 60.01f, false, false, null),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new RangeFilter("f0", ColumnType.DOUBLE, 10.0, 10.2, false, false, null),
          ImmutableList.of("1")
      );
      assertFilterMatches(
          new RangeFilter("f0", ColumnType.FLOAT, 10.05f, 10.11f, false, false, null),
          ImmutableList.of("1")
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.LONG, 12344L, 12346L, false, false, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 12344.0, 12345.5, false, false, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.FLOAT, 12344.0f, 12345.5f, false, false, null),
          ImmutableList.of("5")
      );
    }

    @Test
    public void testNumericMatchNoUpperLimit()
    {
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.LONG, 1L, null, true, true, null),
          ImmutableList.of("1", "2")
      );
      assertFilterMatches(
          new RangeFilter("d0", ColumnType.DOUBLE, 1.0, null, true, true, null),
          ImmutableList.of("1", "3", "4", "5", "6")
      );
      assertFilterMatches(
          new RangeFilter("f0", ColumnType.FLOAT, 1.0f, null, true, true, null),
          ImmutableList.of("1", "2", "3", "5", "7")
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.LONG, 1L, null, true, true, null),
          ImmutableList.of("1", "2", "4", "5", "6")
      );
    }

    @Test
    public void testNumericMatchNoLowerLimit()
    {
      // strings are wierd...
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.LONG, null, 2L, false, true, null),
          NullHandling.replaceWithDefault()
          ? ImmutableList.of("3", "4", "5", "6", "7")
          : ImmutableList.of("0", "3", "4", "5", "6", "7")
      );
      // numbers are sane though
      assertFilterMatches(
          new RangeFilter("d0", ColumnType.DOUBLE, null, 10.0, false, true, null),
          canTestNumericNullsAsDefaultValues ? ImmutableList.of("0", "2", "6", "7") : ImmutableList.of("0", "6")
      );
      assertFilterMatches(
          new RangeFilter("f0", ColumnType.FLOAT, null, 50.5, false, true, null),
          canTestNumericNullsAsDefaultValues
          ? ImmutableList.of("0", "1", "2", "4", "6", "7")
          : ImmutableList.of("0", "1", "2", "7")
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.LONG, null, 100L, false, true, null),
          canTestNumericNullsAsDefaultValues
          ? ImmutableList.of("0", "2", "3", "6", "7")
          : ImmutableList.of("0", "2", "6")
      );
    }

    @Test
    public void testNumericMatchWithNegatives()
    {
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.LONG, -2000L, 3L, true, true, null),
          ImmutableList.of("2", "3", "6", "7")
      );
    }

    @Test
    public void testNumericMatchStringyBounds()
    {
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.STRING, "abc", null, true, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.STRING, "abc", "def", true, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.STRING, null, "abc", true, true, null),
          canTestNumericNullsAsDefaultValues
          ? ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
          : ImmutableList.of("0", "1", "2", "4", "5", "6")
      );
    }

    @Test
    public void testNumericMatchPrecisionLoss()
    {
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 12345.1, 12345.4, false, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 12344.1, 12345.4, false, false, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 12344.1, 12345.4, false, true, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 12344.1, 12345.4, true, false, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 12344.1, 12345.4, false, true, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 12345.0, 12345.1, false, true, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 12345.0, 12345.1, true, true, null),
          ImmutableList.of()
      );

      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 12344.0, 12344.9, false, false, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 12344.0, 12344.9, false, true, null),
          ImmutableList.of()
      );

      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 12344.5, null, true, true, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 12344.5, null, false, true, null),
          ImmutableList.of("5")
      );

      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 12345.5, null, true, true, null),
          ImmutableList.of()
      );
      assertFilterMatches(
          new RangeFilter("l0", ColumnType.DOUBLE, 12345.5, null, false, true, null),
          ImmutableList.of()
      );

      if (canTestNumericNullsAsDefaultValues) {
        assertFilterMatches(
            new RangeFilter("l0", ColumnType.DOUBLE, null, 12344.5, true, true, null),
            ImmutableList.of("0", "1", "2", "3", "4", "6", "7")
        );
        assertFilterMatches(
            new RangeFilter("l0", ColumnType.DOUBLE, null, 12344.5, false, true, null),
            ImmutableList.of("0", "1", "2", "3", "4", "6", "7")
        );

        assertFilterMatches(
            new RangeFilter("l0", ColumnType.DOUBLE, null, 12345.5, true, true, null),
            ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
        );
        assertFilterMatches(
            new RangeFilter("l0", ColumnType.DOUBLE, null, 12345.5, false, true, null),
            ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
        );
      } else {
        assertFilterMatches(
            new RangeFilter("l0", ColumnType.DOUBLE, null, 12344.5, true, true, null),
            ImmutableList.of("0", "1", "2", "4", "6")
        );
        assertFilterMatches(
            new RangeFilter("l0", ColumnType.DOUBLE, null, 12344.5, false, true, null),
            ImmutableList.of("0", "1", "2", "4", "6")
        );

        assertFilterMatches(
            new RangeFilter("l0", ColumnType.DOUBLE, null, 12345.5, true, true, null),
            ImmutableList.of("0", "1", "2", "4", "5", "6")
        );
        assertFilterMatches(
            new RangeFilter("l0", ColumnType.DOUBLE, null, 12345.5, false, true, null),
            ImmutableList.of("0", "1", "2", "4", "5", "6")
        );
      }
    }

    @Test
    public void testNumericNullsAndZeros()
    {
      assertFilterMatches(
          new RangeFilter(
              "d0",
              ColumnType.DOUBLE,
              0.0,
              1.1,
              false,
              false,
              null
          ),
          canTestNumericNullsAsDefaultValues ? ImmutableList.of("0", "2", "7") : ImmutableList.of("0")
      );

      assertFilterMatches(
          new RangeFilter(
              "f0",
              ColumnType.FLOAT,
              0.0,
              1.0,
              false,
              false,
              null
          ),
          canTestNumericNullsAsDefaultValues ? ImmutableList.of("0", "4", "6") : ImmutableList.of("0")
      );

      assertFilterMatches(
          new RangeFilter(
              "l0",
              ColumnType.LONG,
              0L,
              1L,
              false,
              false,
              null
          ),
          NullHandling.replaceWithDefault() && canTestNumericNullsAsDefaultValues
          ? ImmutableList.of("0", "3", "7")
          : ImmutableList.of("0")
      );
    }

    @Test
    public void testVirtualNumericNullsAndZeros()
    {
      assertFilterMatches(
          new RangeFilter(
              "vd0",
              ColumnType.DOUBLE,
              0.0,
              1.0,
              false,
              false,
              null
          ),
          canTestNumericNullsAsDefaultValues ? ImmutableList.of("0", "2", "7") : ImmutableList.of("0")
      );

      assertFilterMatches(
          new RangeFilter(
              "vf0",
              ColumnType.FLOAT,
              0.0,
              1.0,
              false,
              false,
              null
          ),
          canTestNumericNullsAsDefaultValues ? ImmutableList.of("0", "4", "6") : ImmutableList.of("0")
      );

      assertFilterMatches(
          new RangeFilter(
              "vl0",
              ColumnType.LONG,
              0L,
              1L,
              false,
              false,
              null
          ),
          NullHandling.replaceWithDefault() && canTestNumericNullsAsDefaultValues
          ? ImmutableList.of("0", "3", "7")
          : ImmutableList.of("0")
      );

      if (NullHandling.sqlCompatible() || canTestNumericNullsAsDefaultValues) {
        // these fail in default value mode that cannot be tested as numeric default values becuase of type
        // mismatch for subtract operation
        assertFilterMatches(
            new RangeFilter(
                "vd0-add-sub",
                ColumnType.DOUBLE,
                0.0,
                1.0,
                false,
                false,
                null
            ),
            NullHandling.replaceWithDefault() && canTestNumericNullsAsDefaultValues
            ? ImmutableList.of("0", "2", "7")
            : ImmutableList.of("0")
        );

        assertFilterMatches(
            new RangeFilter(
                "vf0-add-sub",
                ColumnType.FLOAT,
                0.0,
                1.0,
                false,
                false,
                null
            ),
            NullHandling.replaceWithDefault() && canTestNumericNullsAsDefaultValues
            ? ImmutableList.of("0", "4", "6")
            : ImmutableList.of("0")
        );

        assertFilterMatches(
            new RangeFilter(
                "vl0-add-sub",
                ColumnType.LONG,
                0L,
                1L,
                false,
                false,
                null
            ),
            NullHandling.replaceWithDefault() && canTestNumericNullsAsDefaultValues
            ? ImmutableList.of("0", "3", "7")
            : ImmutableList.of("0")
        );
      }
    }

    @Test
    public void testNumericNulls()
    {
      assertFilterMatches(
          new RangeFilter(
              "f0",
              ColumnType.FLOAT,
              1.0,
              null,
              false,
              false,
              null
          ),
          ImmutableList.of("1", "2", "3", "5", "7")
      );
      assertFilterMatches(
          new RangeFilter(
              "d0",
              ColumnType.DOUBLE,
              1.0,
              null,
              false,
              false,
              null
          ),
          ImmutableList.of("1", "3", "4", "5", "6")
      );
      assertFilterMatches(
          new RangeFilter(
              "l0",
              ColumnType.LONG,
              1L,
              null,
              false,
              false,
              null
          ),
          ImmutableList.of("1", "2", "4", "5", "6")
      );
    }

    @Test
    public void testListFilteredVirtualColumn()
    {
      assertFilterMatchesSkipVectorize(
          new RangeFilter("allow-dim0", ColumnType.STRING, "0", "2", false, false, null),
          ImmutableList.of()
      );
      assertFilterMatchesSkipVectorize(
          new RangeFilter("allow-dim0", ColumnType.STRING, "0", "6", false, false, null),
          ImmutableList.of("3", "4")
      );
      assertFilterMatchesSkipVectorize(
          new RangeFilter("allow-dim0", ColumnType.STRING, null, "6", false, false, null),
          ImmutableList.of("3", "4")
      );

      assertFilterMatchesSkipVectorize(
          new RangeFilter("deny-dim0", ColumnType.STRING, "0", "6", false, false, null),
          ImmutableList.of("0", "1", "2", "5", "6")
      );
      assertFilterMatchesSkipVectorize(
          new RangeFilter("deny-dim0", ColumnType.STRING, "3", "4", false, false, null),
          ImmutableList.of()
      );
      assertFilterMatchesSkipVectorize(
          new RangeFilter("deny-dim0", ColumnType.STRING, null, "6", false, false, null),
          ImmutableList.of("0", "1", "2", "5", "6")
      );

      // bail out, auto ingests arrays instead of mvds and this virtual column is for mvd stuff
      Assume.assumeFalse(isAutoSchema());

      assertFilterMatchesSkipVectorize(
          new RangeFilter("allow-dim2", ColumnType.STRING, "a", "c", false, false, null),
          ImmutableList.of("0", "3", "6")
      );
      assertFilterMatchesSkipVectorize(
          new RangeFilter("allow-dim2", ColumnType.STRING, "c", "z", false, false, null),
          ImmutableList.of()
      );
      assertFilterMatchesSkipVectorize(
          new RangeFilter("allow-dim2", ColumnType.STRING, null, "z", false, false, null),
          ImmutableList.of("0", "3", "6")
      );

      assertFilterMatchesSkipVectorize(
          new RangeFilter("deny-dim2", ColumnType.STRING, "a", "b", false, true, null),
          ImmutableList.of()
      );
      assertFilterMatchesSkipVectorize(
          new RangeFilter("deny-dim2", ColumnType.STRING, "c", "z", false, false, null),
          ImmutableList.of("4", "7")
      );

      assertFilterMatchesSkipVectorize(
          new RangeFilter("deny-dim2", ColumnType.STRING, null, "z", false, false, null),
          NullHandling.replaceWithDefault() ? ImmutableList.of("0", "4", "7") : ImmutableList.of("0", "2", "4", "7")
      );
    }


    @Test
    public void testArrayRanges()
    {
      // only auto schema supports array columns currently, this means the match value will need to be coerceable to
      // the column value type...
      Assume.assumeTrue(isAutoSchema());

      /*  dim0 .. arrayString               arrayLong             arrayDouble
          "0", .. ["a", "b", "c"],          [1L, 2L, 3L],         [1.1, 2.2, 3.3]
          "1", .. [],                       [],                   [1.1, 2.2, 3.3]
          "2", .. null,                     [1L, 2L, 3L],         [null]
          "3", .. ["a", "b", "c"],          null,                 []
          "4", .. ["c", "d"],               [null],               [-1.1, -333.3]
          "5", .. [null],                   [123L, 345L],         null
          "6", .. ["x", "y"],               [100, 200],           [1.1, null, 3.3]
          "7", .. [null, "hello", "world"], [1234, 3456L, null],  [1.23, 4.56, 6.78]
       */
      assertFilterMatches(
          new RangeFilter(
              "arrayString",
              ColumnType.STRING_ARRAY,
              new Object[]{"a", "b", "c"},
              new Object[]{"a", "b", "c"},
              false,
              false,
              null
          ),
          ImmutableList.of("0", "3")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new RangeFilter(
                  "arrayString",
                  ColumnType.STRING_ARRAY,
                  new Object[]{"a", "b", "c"},
                  new Object[]{"a", "b", "c"},
                  false,
                  false,
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("1", "4", "5", "6", "7")
          : ImmutableList.of("1", "2", "4", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter(
              "arrayString",
              ColumnType.STRING_ARRAY,
              null,
              new Object[]{"a", "b", "c"},
              false,
              false,
              null
          ),
          ImmutableList.of("0", "1", "3", "5", "7")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayString",
              ColumnType.STRING_ARRAY,
              new Object[]{"a", "b", "c"},
              null,
              true,
              false,
              null
          ),
          ImmutableList.of("4", "6")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayString",
              ColumnType.STRING_ARRAY,
              null,
              new Object[]{"a", "b", "c"},
              false,
              true,
              null
          ),
          ImmutableList.of("1", "5", "7")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayString",
              ColumnType.STRING_ARRAY,
              new Object[]{"a", "b"},
              new Object[]{"a", "b", "c", "d"},
              true,
              true,
              null
          ),
          ImmutableList.of("0", "3")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayString",
              ColumnType.STRING_ARRAY,
              new Object[]{"c", "d"},
              new Object[]{"c", "d", "e"},
              false,
              true,
              null
          ),
          ImmutableList.of("4")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayString",
              ColumnType.STRING_ARRAY,
              null,
              new Object[]{},
              false,
              false,
              null
          ),
          ImmutableList.of("1")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayString",
              ColumnType.STRING_ARRAY,
              null,
              new Object[]{null},
              false,
              false,
              null
          ),
          ImmutableList.of("1", "5")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.LONG_ARRAY,
              null,
              new Object[]{},
              false,
              false,
              null
          ),
          ImmutableList.of("1")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new RangeFilter(
                  "arrayLong",
                  ColumnType.LONG_ARRAY,
                  null,
                  new Object[]{},
                  false,
                  false,
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "2", "4", "5", "6", "7")
          : ImmutableList.of("0", "2", "3", "4", "5", "6", "7")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.LONG_ARRAY,
              new Object[]{},
              null,
              true,
              false,
              null
          ),
          ImmutableList.of("0", "2", "4", "5", "6", "7")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.LONG_ARRAY,
              null,
              new Object[]{null},
              false,
              false,
              null
          ),
          ImmutableList.of("1", "4")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.LONG_ARRAY,
              new Object[]{1L, 2L, 3L},
              new Object[]{1L, 2L, 3L},
              false,
              false,
              null
          ),
          ImmutableList.of("0", "2")
      );


      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.LONG_ARRAY,
              null,
              new Object[]{1L, 2L, 3L},
              false,
              true,
              null
          ),
          ImmutableList.of("1", "4")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.LONG_ARRAY,
              new Object[]{1L, 2L, 3L},
              null,
              true,
              false,
              null
          ),
          ImmutableList.of("5", "6", "7")
      );

      // empties and nulls still sort before numbers
      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.LONG_ARRAY,
              null,
              new Object[]{-1L},
              false,
              false,
              null
          ),
          ImmutableList.of("1", "4")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayDouble",
              ColumnType.DOUBLE_ARRAY,
              null,
              new Object[]{},
              false,
              false,
              null
          ),
          ImmutableList.of("3")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new RangeFilter(
                  "arrayDouble",
                  ColumnType.DOUBLE_ARRAY,
                  null,
                  new Object[]{},
                  false,
                  false,
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "1", "2", "4", "6", "7")
          : ImmutableList.of("0", "1", "2", "4", "5", "6", "7")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayDouble",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{},
              null,
              true,
              false,
              null
          ),
          ImmutableList.of("0", "1", "2", "4", "6", "7")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayDouble",
              ColumnType.DOUBLE_ARRAY,
              null,
              new Object[]{null},
              false,
              false,
              null
          ),
          ImmutableList.of("2", "3")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayDouble",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{1.1, 2.2, 3.3},
              new Object[]{1.1, 2.2, 3.3},
              false,
              false,
              null
          ),
          ImmutableList.of("0", "1")
      );
      assertFilterMatches(
          new RangeFilter(
              "arrayDouble",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{1.1, 2.2, 3.3},
              null,
              true,
              false,
              null
          ),
          ImmutableList.of("7")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayDouble",
              ColumnType.DOUBLE_ARRAY,
              null,
              new Object[]{1.1, 2.2, 3.3},
              true,
              false,
              null
          ),
          ImmutableList.of("0", "1", "2", "3", "4", "6")
      );

      // empties and nulls sort before numbers
      assertFilterMatches(
          new RangeFilter(
              "arrayDouble",
              ColumnType.DOUBLE_ARRAY,
              null,
              new Object[]{0.0},
              true,
              false,
              null
          ),
          ImmutableList.of("2", "3", "4")
      );
    }

    @Test
    public void testArrayRangesPrecisionLoss()
    {
      // only auto schema supports array columns currently, this means the match value will need to be coerceable to
      // the column value type...
      Assume.assumeTrue(isAutoSchema());

      /*  dim0 .. arrayLong
          "0", .. [1L, 2L, 3L],
          "1", .. [],
          "2", .. [1L, 2L, 3L],
          "3", .. null,
          "4", .. [null],
          "5", .. [123L, 345L],
          "6", .. [100, 200],
          "7", .. [1234, 3456L, null]
       */

      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              null,
              new Object[]{1.0, 2.0, 3.0},
              true,
              false,
              null
          ),
          ImmutableList.of("0", "1", "2", "4")
      );
      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              null,
              new Object[]{1.0, 2.0, 3.0},
              true,
              true,
              null
          ),
          ImmutableList.of("1", "4")
      );
      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              null,
              new Object[]{1.1, 2.1, 3.1},
              true,
              true,
              null
          ),
          ImmutableList.of("0", "1", "2", "4")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{1.0, 2.0, 3.0},
              null,
              false,
              false,
              null
          ),
          ImmutableList.of("0", "2", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{0.8, 1.8, 2.8},
              null,
              false,
              false,
              null
          ),
          ImmutableList.of("0", "2", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{0.8, 1.8, 2.8},
              null,
              true,
              false,
              null
          ),
          ImmutableList.of("0", "2", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{1.0, 2.0, 3.0},
              null,
              true,
              true,
              null
          ),
          ImmutableList.of("5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{1.1, 2.1, 3.1},
              null,
              false,
              true,
              null
          ),
          ImmutableList.of("5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{1.1, 2.1, 3.1},
              null,
              true,
              true,
              null
          ),
          ImmutableList.of("5", "6", "7")
      );

      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{0.8, 1.8, 2.8},
              new Object[]{1.1, 2.1, 3.1},
              true,
              true,
              null
          ),
          ImmutableList.of("0", "2")
      );
      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{0.8, 1.8, 2.8},
              new Object[]{1.1, 2.1, 3.1},
              false,
              true,
              null
          ),
          ImmutableList.of("0", "2")
      );
      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{0.8, 1.8, 2.8},
              new Object[]{1.1, 2.1, 3.1},
              true,
              false,
              null
          ),
          ImmutableList.of("0", "2")
      );
      assertFilterMatches(
          new RangeFilter(
              "arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{0.8, 1.8, 2.8},
              new Object[]{1.1, 2.1, 3.1},
              false,
              false,
              null
          ),
          ImmutableList.of("0", "2")
      );
    }

    @Test
    public void testVariant()
    {
      /*
      dim0 .. variant
      "0", .. "abc"
      "1", .. 100L
      "2", .. "100"
      "3", .. [1.1, 2.2, 3.3]
      "4", .. 12.34
      "5", .. [100, 200, 300]
      "6", .. null
      "7", .. null
       */
      Assume.assumeTrue(isAutoSchema());
      assertFilterMatches(
          new RangeFilter(
              "variant",
              ColumnType.LONG,
              100L,
              null,
              false,
              false,
              null
          ),
          ImmutableList.of("1", "2", "5")
      );
      assertFilterMatches(
          NotDimFilter.of(
              new RangeFilter(
                  "variant",
                  ColumnType.LONG,
                  100L,
                  null,
                  false,
                  false,
                  null
              )
          ),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "3", "4")
          : ImmutableList.of("0", "3", "4", "6", "7")
      );
      // lexicographical comparison
      assertFilterMatches(
          new RangeFilter(
              "variant",
              ColumnType.STRING,
              "100",
              null,
              false,
              false,
              null
          ),
          ImmutableList.of("0", "1", "2", "4", "5")
      );
      assertFilterMatches(
          new RangeFilter(
              "variant",
              ColumnType.LONG_ARRAY,
              Collections.singletonList(100L),
              Arrays.asList(100L, 200L, 300L),
              false,
              false,
              null
          ),
          ImmutableList.of("1", "2", "5")
      );
    }

    @Test
    public void testNested()
    {
      // nested column mirrors the top level columns, so these cases are copied from other tests
      Assume.assumeTrue(canTestArrayColumns());
      assertFilterMatches(
          new RangeFilter("nested.d0", ColumnType.DOUBLE, 120.0, 120.03, false, false, null),
          ImmutableList.of("3")
      );
      assertFilterMatches(
          new RangeFilter("nested.d0", ColumnType.FLOAT, 120.02f, 120.03f, false, false, null),
          ImmutableList.of("3")
      );
      assertFilterMatches(
          new RangeFilter("nested.d0", ColumnType.FLOAT, 59.5f, 60.01f, false, false, null),
          ImmutableList.of("4")
      );
      assertFilterMatches(
          new RangeFilter("nested.l0", ColumnType.LONG, 12344L, 12346L, false, false, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          new RangeFilter("nested.l0", ColumnType.DOUBLE, 12344.0, 12345.5, false, false, null),
          ImmutableList.of("5")
      );
      assertFilterMatches(
          new RangeFilter("nested.l0", ColumnType.FLOAT, 12344.0f, 12345.5f, false, false, null),
          ImmutableList.of("5")
      );

      assertFilterMatches(
          new RangeFilter(
              "nested.arrayLong",
              ColumnType.DOUBLE_ARRAY,
              null,
              new Object[]{1.0, 2.0, 3.0},
              true,
              false,
              null
          ),
          ImmutableList.of("0", "1", "2", "4")
      );
      assertFilterMatches(
          new RangeFilter(
              "nested.arrayLong",
              ColumnType.DOUBLE_ARRAY,
              null,
              new Object[]{1.0, 2.0, 3.0},
              true,
              true,
              null
          ),
          ImmutableList.of("1", "4")
      );
      assertFilterMatches(
          new RangeFilter(
              "nested.arrayLong",
              ColumnType.DOUBLE_ARRAY,
              null,
              new Object[]{1.1, 2.1, 3.1},
              true,
              true,
              null
          ),
          ImmutableList.of("0", "1", "2", "4")
      );

      assertFilterMatches(
          new RangeFilter(
              "nested.arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{1.0, 2.0, 3.0},
              null,
              false,
              false,
              null
          ),
          ImmutableList.of("0", "2", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter(
              "nested.arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{0.8, 1.8, 2.8},
              null,
              false,
              false,
              null
          ),
          ImmutableList.of("0", "2", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter(
              "nested.arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{0.8, 1.8, 2.8},
              null,
              true,
              false,
              null
          ),
          ImmutableList.of("0", "2", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter(
              "nested.arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{1.0, 2.0, 3.0},
              null,
              true,
              true,
              null
          ),
          ImmutableList.of("5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter(
              "nested.arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{1.1, 2.1, 3.1},
              null,
              false,
              true,
              null
          ),
          ImmutableList.of("5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter(
              "nested.arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{1.1, 2.1, 3.1},
              null,
              true,
              true,
              null
          ),
          ImmutableList.of("5", "6", "7")
      );

      assertFilterMatches(
          new RangeFilter(
              "nested.arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{0.8, 1.8, 2.8},
              new Object[]{1.1, 2.1, 3.1},
              true,
              true,
              null
          ),
          ImmutableList.of("0", "2")
      );
      assertFilterMatches(
          new RangeFilter(
              "nested.arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{0.8, 1.8, 2.8},
              new Object[]{1.1, 2.1, 3.1},
              false,
              true,
              null
          ),
          ImmutableList.of("0", "2")
      );
      assertFilterMatches(
          new RangeFilter(
              "nested.arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{0.8, 1.8, 2.8},
              new Object[]{1.1, 2.1, 3.1},
              true,
              false,
              null
          ),
          ImmutableList.of("0", "2")
      );
      assertFilterMatches(
          new RangeFilter(
              "nested.arrayLong",
              ColumnType.DOUBLE_ARRAY,
              new Object[]{0.8, 1.8, 2.8},
              new Object[]{1.1, 2.1, 3.1},
              false,
              false,
              null
          ),
          ImmutableList.of("0", "2")
      );
    }
  }

  public static class RangeFilterNonParameterizedTests extends InitializedNullHandlingTest
  {
    @Test
    public void testSerde() throws JsonProcessingException
    {
      ObjectMapper mapper = new DefaultObjectMapper();
      RangeFilter filter = new RangeFilter("x", ColumnType.STRING, "abc", "xyz", true, true, null);
      String s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, RangeFilter.class));

      filter = new RangeFilter("x", ColumnType.STRING, "abc", "xyz", false, false, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, RangeFilter.class));

      filter = new RangeFilter("x", ColumnType.LONG, 100L, null, true, false, null);
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, RangeFilter.class));

      filter = new RangeFilter(
          "x",
          ColumnType.STRING_ARRAY,
          new Object[]{"a", "b"},
          new Object[]{"x", null, "z"},
          true,
          true,
          null
      );
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, RangeFilter.class));

      filter = new RangeFilter(
          "x",
          ColumnType.LONG_ARRAY,
          new Object[]{101L, 1L},
          new Object[]{101L, 100L},
          true,
          true,
          null
      );
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, RangeFilter.class));

      filter = new RangeFilter(
          "x",
          ColumnType.DOUBLE_ARRAY,
          new Object[]{1.1, null, 1.2},
          new Object[]{2.02, 3.03},
          true,
          true,
          null
      );
      s = mapper.writeValueAsString(filter);
      Assert.assertEquals(filter, mapper.readValue(s, RangeFilter.class));
    }

    @Test
    public void testGetCacheKey()
    {
      RangeFilter f1 = new RangeFilter("x", ColumnType.STRING, "abc", "xyz", true, true, null);
      RangeFilter f1_2 = new RangeFilter("x", ColumnType.STRING, "abc", "xyz", true, true, null);
      RangeFilter f2 = new RangeFilter("x", ColumnType.STRING, "abc", "xyz", false, true, null);
      RangeFilter f2_2 = new RangeFilter("x", ColumnType.STRING, "abc", "xyzz", true, true, null);
      RangeFilter f3 = new RangeFilter(
          "x",
          ColumnType.STRING,
          "abc",
          "xyz",
          true,
          true,
          new FilterTuning(true, null, null)
      );
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2_2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new RangeFilter("x", ColumnType.LONG, 100L, 300L, true, true, null);
      f1_2 = new RangeFilter("x", ColumnType.LONG, 100, 300, true, true, null);
      f2 = new RangeFilter("x", ColumnType.LONG, 100L, 300L, false, true, null);
      f2_2 = new RangeFilter("x", ColumnType.LONG, 101L, 300L, true, true, null);
      f3 = new RangeFilter("x", ColumnType.LONG, 100L, 300L, true, true, new FilterTuning(true, null, null));
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2_2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new RangeFilter("x", ColumnType.DOUBLE, -1.1, 1.1, true, true, null);
      f1_2 = new RangeFilter("x", ColumnType.DOUBLE, -1.1, 1.1, true, true, null);
      f2 = new RangeFilter("x", ColumnType.DOUBLE, -1.1, 1.1, false, true, null);
      f2_2 = new RangeFilter("x", ColumnType.DOUBLE, -1.1000000001, 1.1, true, true, null);
      f3 = new RangeFilter("x", ColumnType.DOUBLE, -1.1, 1.1, true, true, new FilterTuning(true, null, null));
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2_2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new RangeFilter(
          "x",
          ColumnType.STRING_ARRAY,
          new Object[]{"a", "b"},
          new Object[]{"x", null, "z"},
          true,
          true,
          null
      );
      f1_2 = new RangeFilter(
          "x",
          ColumnType.STRING_ARRAY,
          Arrays.asList("a", "b"),
          Arrays.asList("x", null, "z"),
          true,
          true,
          null
      );
      f2 = new RangeFilter(
          "x",
          ColumnType.STRING_ARRAY,
          new Object[]{"a", "b"},
          new Object[]{"x", null, "z"},
          false,
          true,
          null
      );
      f2_2 = new RangeFilter(
          "x",
          ColumnType.STRING_ARRAY,
          new Object[]{"a", "b"},
          new Object[]{"x", "z"},
          true,
          true,
          null
      );
      f3 = new RangeFilter(
          "x",
          ColumnType.STRING_ARRAY,
          new Object[]{"a", "b"},
          new Object[]{"x", null, "z"},
          true,
          true,
          new FilterTuning(true, null, null)
      );
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2_2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new RangeFilter(
          "x",
          ColumnType.LONG_ARRAY,
          new Object[]{101L, 1L},
          new Object[]{101L, 100L},
          true,
          true,
          null
      );
      f1_2 = new RangeFilter(
          "x",
          ColumnType.LONG_ARRAY,
          Arrays.asList(101L, 1L),
          Arrays.asList(101L, 100L),
          true,
          true,
          null
      );
      f2 = new RangeFilter(
          "x",
          ColumnType.LONG_ARRAY,
          new Object[]{101L, 1L},
          new Object[]{101L, 100L},
          false,
          true,
          null
      );
      f2_2 = new RangeFilter(
          "x",
          ColumnType.LONG_ARRAY,
          new Object[]{101L, 1L},
          new Object[]{101L, 99L},
          true,
          true,
          null
      );
      f3 = new RangeFilter(
          "x",
          ColumnType.LONG_ARRAY,
          new Object[]{101L, 1L},
          new Object[]{101L, 100L},
          true,
          true,
          new FilterTuning(true, null, null)
      );
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2_2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());

      f1 = new RangeFilter(
          "x",
          ColumnType.DOUBLE_ARRAY,
          new Object[]{1.1, null, 1.2},
          new Object[]{2.02, 3.03},
          true,
          true,
          null
      );
      f1_2 = new RangeFilter(
          "x",
          ColumnType.DOUBLE_ARRAY,
          Arrays.asList(1.1, null, 1.2),
          Arrays.asList(2.02, 3.03),
          true,
          true,
          null
      );
      f2 = new RangeFilter(
          "x",
          ColumnType.DOUBLE_ARRAY,
          new Object[]{1.1, null, 1.2},
          new Object[]{2.02, 3.03},
          false,
          true,
          null
      );
      f2_2 = new RangeFilter(
          "x",
          ColumnType.DOUBLE_ARRAY,
          new Object[]{1.1, 1.2},
          new Object[]{2.02, 3.03},
          true,
          true,
          null
      );
      f3 = new RangeFilter(
          "x",
          ColumnType.DOUBLE_ARRAY,
          new Object[]{1.1, null, 1.2},
          new Object[]{2.02, 3.03},
          true,
          true,
          new FilterTuning(true, null, null)
      );
      Assert.assertArrayEquals(f1.getCacheKey(), f1_2.getCacheKey());
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2.getCacheKey()));
      Assert.assertFalse(Arrays.equals(f1.getCacheKey(), f2_2.getCacheKey()));
      Assert.assertArrayEquals(f1.getCacheKey(), f3.getCacheKey());
    }

    @Test
    public void testRequiredColumnRewrite()
    {
      RangeFilter filter = new RangeFilter("dim0", ColumnType.STRING, "abc", "def", false, false, null);
      RangeFilter filter2 = new RangeFilter("dim1", ColumnType.STRING, "abc", "def", false, false, null);
      Assert.assertTrue(filter.supportsRequiredColumnRewrite());
      Assert.assertTrue(filter2.supportsRequiredColumnRewrite());

      Filter rewrittenFilter = filter.rewriteRequiredColumns(ImmutableMap.of("dim0", "dim1"));
      Assert.assertEquals(filter2, rewrittenFilter);

      Throwable t = Assert.assertThrows(
          IAE.class,
          () -> filter.rewriteRequiredColumns(ImmutableMap.of("invalidName", "dim1"))
      );

      Assert.assertEquals(
          "Received a non-applicable rewrite: {invalidName=dim1}, filter's dimension: dim0",
          t.getMessage()
      );
    }

    @Test
    public void testNumericMatchBadParameters()
    {
      Throwable t = Assert.assertThrows(
          DruidException.class,
          () -> new RangeFilter(null, ColumnType.DOUBLE, "1234", "", false, false, null)
      );
      Assert.assertEquals(
          "Invalid range filter, column cannot be null",
          t.getMessage()
      );
      t = Assert.assertThrows(
          DruidException.class,
          () -> new RangeFilter("dim0", null, "1234", "", false, false, null)
      );
      Assert.assertEquals(
          "Invalid range filter on column [dim0], matchValueType cannot be null",
          t.getMessage()
      );
      t = Assert.assertThrows(
          DruidException.class,
          () -> new RangeFilter("dim0", ColumnType.DOUBLE, null, null, false, false, null)
      );
      Assert.assertEquals(
          "Invalid range filter on column [dim0], lower and upper cannot be null at the same time",
          t.getMessage()
      );

      t = Assert.assertThrows(
          DruidException.class,
          () -> new RangeFilter("dim0", ColumnType.DOUBLE, "1234", "", false, false, null)
      );
      Assert.assertEquals(
          "Invalid range filter on column [dim0], upper bound [] cannot be parsed as specified match value type [DOUBLE]",
          t.getMessage()
      );

      t = Assert.assertThrows(
          DruidException.class,
          () -> new RangeFilter("dim0", ColumnType.DOUBLE, "abc", "1234", false, false, null)
      );
      Assert.assertEquals(
          "Invalid range filter on column [dim0], lower bound [abc] cannot be parsed as specified match value type [DOUBLE]",
          t.getMessage()
      );
    }


    @Test
    public void testGetDimensionRangeSet()
    {
      RangeFilter filter = new RangeFilter("x", ColumnType.STRING, "abc", "xyz", true, true, null);

      RangeSet<String> set = TreeRangeSet.create();
      set.add(Range.range("abc", BoundType.OPEN, "xyz", BoundType.OPEN));
      Assert.assertEquals(set, filter.getDimensionRangeSet("x"));
      Assert.assertNull(filter.getDimensionRangeSet("y"));

      ExprEval<?> evalLower = ExprEval.ofType(ExpressionType.STRING_ARRAY, new Object[]{"abc", "def"});
      filter = new RangeFilter("x", ColumnType.STRING_ARRAY, evalLower.value(), null, true, false, null);
      set = TreeRangeSet.create();
      set.add(Range.greaterThan(Arrays.deepToString(evalLower.asArray())));
      Assert.assertEquals(set, filter.getDimensionRangeSet("x"));
      Assert.assertNull(filter.getDimensionRangeSet("y"));
    }

    @Test
    public void test_equals()
    {
      EqualsVerifier.forClass(RangeFilter.class)
                    .withNonnullFields("column", "matchValueType", "lowerEval", "upperEval")
                    .withIgnoredFields(
                        "lower",
                        "upper",
                        "optimizedFilterIncludeUnknown",
                        "optimizedFilterNoIncludeUnknown",
                        "stringPredicateSupplier",
                        "longPredicateSupplier",
                        "floatPredicateSupplier",
                        "doublePredicateSupplier",
                        "arrayPredicates",
                        "typeDetectingArrayPredicateSupplier"
                    )
                    .withPrefabValues(ColumnType.class, ColumnType.STRING, ColumnType.DOUBLE)
                    .usingGetClass()
                    .verify();
    }
  }
}
