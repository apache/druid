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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.List;

@RunWith(Parameterized.class)
public class RangeFilterTest extends BaseFilterTest
{
  private static final List<InputRow> ROWS =
      ImmutableList.<InputRow>builder()
                   .addAll(DEFAULT_ROWS)
                   .add(
                       makeDefaultSchemaRow("6", "-1000", ImmutableList.of("a"), null, 6.6, null, 10L)
                   )
                   .add(
                       makeDefaultSchemaRow("7", "-10.012", ImmutableList.of("d"), null, null, 3.0f, null)
                   )
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
        new RangeFilter("dim0", ColumnType.STRING, null, "z", false, false, null, null),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );
    assertFilterMatches(
        new RangeFilter("vdim0", ColumnType.STRING, null, "z", false, false, null, null),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    if (NullHandling.sqlCompatible()) {
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.STRING, null, "z", false, false, null, null),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter("vdim0", ColumnType.STRING, null, "z", false, false, null, null),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
    } else {
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.STRING, null, "z", false, false, null, null),
          ImmutableList.of("1", "2", "3", "4", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter("vdim0", ColumnType.STRING, null, "z", false, false, null, null),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
    }

    if (isAutoSchema()) {
      // auto schema ingests arrays instead of mvds.. this filter doesn't currently support arrays
    } else {
      assertFilterMatches(
          new RangeFilter("dim2", ColumnType.STRING, null, "z", false, false, null, null),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("0", "2", "3", "4", "6", "7")
          : ImmutableList.of("0", "3", "4", "6", "7")
      );
      // vdim2 does not exist...
      assertFilterMatches(
          new RangeFilter("dim3", ColumnType.STRING, null, "z", false, false, null, null),
          ImmutableList.of()
      );
    }
  }

  @Test
  public void testLexicographicMatchWithEmptyString()
  {
    if (NullHandling.sqlCompatible()) {
      assertFilterMatches(
          new RangeFilter("dim0", ColumnType.STRING, "", "z", false, false, null, null),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.STRING, "", "z", false, false, null, null),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
      if (!isAutoSchema()) {
        // auto schema ingests arrays which are currently incompatible with the range filter
        assertFilterMatches(
            new RangeFilter("dim2", ColumnType.STRING, "", "z", false, false, null, null),
            ImmutableList.of("0", "2", "3", "4", "6", "7")
        );
      }
      assertFilterMatches(
          new RangeFilter("dim3", ColumnType.STRING, "", "z", false, false, null, null),
          ImmutableList.of()
      );
    } else {
      assertFilterMatches(
          new RangeFilter("dim0", ColumnType.STRING, "", "z", false, false, null, null),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
      assertFilterMatches(
          new RangeFilter("dim1", ColumnType.STRING, "", "z", false, false, null, null),
          ImmutableList.of("1", "2", "3", "4", "5", "6", "7")
      );
      if (!isAutoSchema()) {
        // auto schema ingests arrays which are currently incompatible with the range filter
        assertFilterMatches(
            new RangeFilter("dim2", ColumnType.STRING, "", "z", false, false, null, null),
            ImmutableList.of("0", "3", "4", "6", "7")
        );
      }
      assertFilterMatches(
          new RangeFilter("dim3", ColumnType.STRING, "", "z", false, false, null, null),
          ImmutableList.of()
      );
    }
  }

  @Test
  public void testLexicographicMatchNull()
  {
    assertFilterMatches(
        new RangeFilter("dim0", ColumnType.STRING, "", "", false, false, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.STRING, "", "", false, false, null, null),
        NullHandling.replaceWithDefault() ? ImmutableList.of() : ImmutableList.of("0")
    );
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatchesSkipArrays(
          new RangeFilter("dim2", ColumnType.STRING, "", "", false, false, null, null),
          ImmutableList.of()
      );
    } else {
      assertFilterMatchesSkipArrays(
          new RangeFilter("dim2", ColumnType.STRING, "", "", false, false, null, null),
          isAutoSchema() ? ImmutableList.of() : ImmutableList.of("2")
      );
    }
  }

  @Test
  public void testLexicographicMatchMissingColumn()
  {
    assertFilterMatches(
        new RangeFilter("dim3", ColumnType.STRING, "", "", false, false, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("dim3", ColumnType.STRING, "", null, false, true, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("dim3", ColumnType.STRING, null, "", false, true, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("dim3", ColumnType.STRING, "", "", true, false, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("dim3", ColumnType.STRING, "", "", false, true, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("dim3", ColumnType.STRING, null, "", false, false, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("dim3", ColumnType.STRING, null, "", false, true, null, null),
        ImmutableList.of()
    );
  }


  @Test
  public void testLexicographicMatchTooStrict()
  {
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.STRING, "abc", "abc", true, false, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.STRING, "abc", "abc", true, true, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.STRING, "abc", "abc", false, true, null, null),
        ImmutableList.of()
    );
  }

  @Test
  public void testLexicographicMatchExactlySingleValue()
  {
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.STRING, "abc", "abc", false, false, null, null),
        ImmutableList.of("5")
    );
  }

  @Test
  public void testLexicographicMatchSurroundingSingleValue()
  {
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.STRING, "ab", "abd", true, true, null, null),
        ImmutableList.of("5")
    );
  }

  @Test
  public void testLexicographicMatchNoUpperLimit()
  {
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.STRING, "ab", null, true, true, null, null),
        ImmutableList.of("4", "5")
    );
  }

  @Test
  public void testLexicographicMatchNoLowerLimit()
  {
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.STRING, null, "abd", true, true, null, null),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of("1", "2", "3", "5", "6", "7")
        : ImmutableList.of("0", "1", "2", "3", "5", "6", "7")
    );
  }

  @Test
  public void testLexicographicMatchNumbers()
  {
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.STRING, "1", "3", false, false, null, null),
        ImmutableList.of("1", "2", "3")
    );
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.STRING, "1", "3", true, true, null, null),
        ImmutableList.of("1", "2")
    );
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.STRING, "-1", "3", true, true, null, null),
        ImmutableList.of("1", "2", "3", "6", "7")
    );
  }


  @Test
  public void testNumericMatchBadParameters()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> assertFilterMatches(
            new RangeFilter("dim0", ColumnType.DOUBLE, "1234", "", false, false, null, null),
            ImmutableList.of()
        )
    );
    Assert.assertEquals(
        "Invalid range filter on column [dim0], upper bound [] cannot be parsed as specified match value type [DOUBLE]",
        t.getMessage()
    );

    t = Assert.assertThrows(
        DruidException.class,
        () -> assertFilterMatches(
            new RangeFilter("dim0", ColumnType.DOUBLE, "abc", "1234", false, false, null, null),
            ImmutableList.of()
        )
    );
    Assert.assertEquals(
        "Invalid range filter on column [dim0], lower bound [abc] cannot be parsed as specified match value type [DOUBLE]",
        t.getMessage()
    );
  }

  @Test
  public void testNumericMatchTooStrict()
  {
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.LONG, 2L, 2L, true, false, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.LONG, 2L, 2L, true, true, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.LONG, 2L, 2L, false, true, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("d0", ColumnType.LONG, 2L, 3L, false, true, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("d0", ColumnType.DOUBLE, 2L, 3L, false, true, null, null),
        ImmutableList.of()
    );

    assertFilterMatches(
        new RangeFilter("f0", ColumnType.LONG, 2L, 3L, false, true, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("f0", ColumnType.DOUBLE, 2L, 3L, false, true, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("l0", ColumnType.LONG, 2L, 3L, false, true, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("l0", ColumnType.DOUBLE, 2L, 3L, false, true, null, null),
        ImmutableList.of()
    );
  }

  @Test
  public void testNumericMatchVirtualColumn()
  {
    assertFilterMatches(
        new RangeFilter("expr", ColumnType.LONG, 1L, 2L, false, false, null, null),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );
    assertFilterMatches(
        new RangeFilter("expr", ColumnType.DOUBLE, 1.1, 2.0, false, false, null, null),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );
    assertFilterMatches(
        new RangeFilter("expr", ColumnType.FLOAT, 1.1f, 2.0f, false, false, null, null),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    assertFilterMatches(
        new RangeFilter("expr", ColumnType.LONG, 2L, 3L, false, false, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("expr", ColumnType.DOUBLE, 2.0, 3.0, false, false, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("expr", ColumnType.FLOAT, 2.0f, 3.0f, false, false, null, null),
        ImmutableList.of()
    );
  }

  @Test
  public void testNumericMatchExactlySingleValue()
  {
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.LONG, 2L, 2L, false, false, null, null),
        ImmutableList.of("2")
    );

    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.DOUBLE, -10.012, -10.012, false, false, null, null),
        ImmutableList.of("7")
    );
    assertFilterMatches(
        new RangeFilter("d0", ColumnType.DOUBLE, 120.0245, 120.0245, false, false, null, null),
        ImmutableList.of("3")
    );
    assertFilterMatches(
        new RangeFilter("d0", ColumnType.FLOAT, 120.0245f, 120.0245f, false, false, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("d0", ColumnType.FLOAT, 60.0f, 60.0f, false, false, null, null),
        ImmutableList.of("4")
    );
    assertFilterMatches(
        new RangeFilter("f0", ColumnType.DOUBLE, 10.1, 10.1, false, false, null, null),
        ImmutableList.of()
    );
    assertFilterMatches(
        new RangeFilter("f0", ColumnType.FLOAT, 10.1f, 10.1f, false, false, null, null),
        ImmutableList.of("1")
    );
    assertFilterMatches(
        new RangeFilter("l0", ColumnType.LONG, 12345L, 12345L, false, false, null, null),
        ImmutableList.of("5")
    );
    assertFilterMatches(
        new RangeFilter("l0", ColumnType.DOUBLE, 12345.0, 12345.0, false, false, null, null),
        ImmutableList.of("5")
    );
    assertFilterMatches(
        new RangeFilter("l0", ColumnType.FLOAT, 12345.0f, 12345.0f, false, false, null, null),
        ImmutableList.of("5")
    );
  }

  @Test
  public void testNumericMatchSurroundingSingleValue()
  {
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.LONG, 1L, 3L, true, true, null, null),
        ImmutableList.of("2")
    );

    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.LONG, -11L, -10L, false, false, null, null),
        ImmutableList.of("7")
    );
    assertFilterMatches(
        new RangeFilter("d0", ColumnType.DOUBLE, 120.0, 120.03, false, false, null, null),
        ImmutableList.of("3")
    );
    assertFilterMatches(
        new RangeFilter("d0", ColumnType.FLOAT, 120.02f, 120.03f, false, false, null, null),
        ImmutableList.of("3")
    );
    assertFilterMatches(
        new RangeFilter("d0", ColumnType.FLOAT, 59.5f, 60.01f, false, false, null, null),
        ImmutableList.of("4")
    );
    assertFilterMatches(
        new RangeFilter("f0", ColumnType.DOUBLE, 10.0, 10.2, false, false, null, null),
        ImmutableList.of("1")
    );
    assertFilterMatches(
        new RangeFilter("f0", ColumnType.FLOAT, 10.05f, 10.11f, false, false, null, null),
        ImmutableList.of("1")
    );
    assertFilterMatches(
        new RangeFilter("l0", ColumnType.LONG, 12344L, 12346L, false, false, null, null),
        ImmutableList.of("5")
    );
    assertFilterMatches(
        new RangeFilter("l0", ColumnType.DOUBLE, 12344.0, 12345.5, false, false, null, null),
        ImmutableList.of("5")
    );
    assertFilterMatches(
        new RangeFilter("l0", ColumnType.FLOAT, 12344.0f, 12345.5f, false, false, null, null),
        ImmutableList.of("5")
    );
  }

  @Test
  public void testNumericMatchNoUpperLimit()
  {
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.LONG, 1L, null, true, true, null, null),
        ImmutableList.of("1", "2")
    );
    assertFilterMatches(
        new RangeFilter("d0", ColumnType.DOUBLE, 1.0, null, true, true, null, null),
        ImmutableList.of("1", "3", "4", "5", "6")
    );
    assertFilterMatches(
        new RangeFilter("f0", ColumnType.FLOAT, 1.0f, null, true, true, null, null),
        ImmutableList.of("1", "2", "3", "5", "7")
    );
    assertFilterMatches(
        new RangeFilter("l0", ColumnType.LONG, 1L, null, true, true, null, null),
        ImmutableList.of("1", "2", "4", "5", "6")
    );
  }

  @Test
  public void testNumericMatchNoLowerLimit()
  {
    // strings are wierd...
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.LONG, null, 2L, false, true, null, null),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of("3", "4", "5", "6", "7")
        : ImmutableList.of("0", "3", "4", "5", "6", "7")
    );
    // numbers are sane though
    assertFilterMatches(
        new RangeFilter("d0", ColumnType.DOUBLE, null, 10.0, false, true, null, null),
        canTestNumericNullsAsDefaultValues ? ImmutableList.of("0", "2", "6", "7") : ImmutableList.of("0", "6")
    );
    assertFilterMatches(
        new RangeFilter("f0", ColumnType.FLOAT, null, 50.5, false, true, null, null),
        canTestNumericNullsAsDefaultValues
        ? ImmutableList.of("0", "1", "2", "4", "6", "7")
        : ImmutableList.of("0", "1", "2", "7")
    );
    assertFilterMatches(
        new RangeFilter("l0", ColumnType.LONG, null, 100L, false, true, null, null),
        canTestNumericNullsAsDefaultValues ? ImmutableList.of("0", "2", "3", "6", "7") : ImmutableList.of("0", "2", "6")
    );
  }

  @Test
  public void testNumericMatchWithNegatives()
  {
    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.LONG, -2000L, 3L, true, true, null, null),
        ImmutableList.of("2", "3", "6", "7")
    );
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
            null,
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
            null,
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
            null,
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
            null,
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
            null,
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
            null,
            null
        ),
        NullHandling.replaceWithDefault() && canTestNumericNullsAsDefaultValues
        ? ImmutableList.of("0", "3", "7")
        : ImmutableList.of("0")
    );
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
            null,
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
            null,
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
            null,
            null
        ),
        ImmutableList.of("1", "2", "4", "5", "6")
    );
  }

  @Test
  public void testMatchWithExtractionFn()
  {
    String extractionJsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn superFn = new JavaScriptExtractionFn(extractionJsFn, false, JavaScriptConfig.getEnabledInstance());

    String nullJsFn = "function(str) { return null; }";
    ExtractionFn makeNullFn = new JavaScriptExtractionFn(nullJsFn, false, JavaScriptConfig.getEnabledInstance());

    assertFilterMatches(
        new RangeFilter("dim0", ColumnType.STRING, "", "", false, false, makeNullFn, null),
        ImmutableList.of()
    );

    assertFilterMatches(
        new RangeFilter(
            "dim1",
            ColumnType.STRING,
            "super-ab",
            "super-abd",
            true,
            true,
            superFn,
            null
        ),
        ImmutableList.of("5")
    );

    assertFilterMatches(
        new RangeFilter("dim1", ColumnType.STRING, "super-0", "super-10", false, false, superFn, null),
        ImmutableList.of("1", "3")
    );

    assertFilterMatchesSkipArrays(
        new RangeFilter(
            "dim2",
            ColumnType.STRING,
            "super-",
            "super-zzzzzz",
            false,
            false,
            superFn,
            null
        ),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatchesSkipArrays(
          new RangeFilter(
              "dim2",
              ColumnType.STRING,
              "super-null",
              "super-null",
              false,
              false,
              superFn,
              null
          ),
          ImmutableList.of("1", "2", "5")
      );
      assertFilterMatchesSkipArrays(
          new RangeFilter(
              "dim2",
              ColumnType.STRING,
              "super-null",
              "super-null",
              false,
              false,
              superFn,
              null
          ),
          ImmutableList.of("1", "2", "5")
      );
    } else {
      assertFilterMatchesSkipArrays(
          new RangeFilter(
              "dim2",
              ColumnType.STRING,
              "super-null",
              "super-null",
              false,
              false,
              superFn,
              null
          ),
          ImmutableList.of("1", "5")
      );
      assertFilterMatchesSkipArrays(
          new RangeFilter("dim2", ColumnType.STRING, "super-", "super-", false, false, superFn, null),
          ImmutableList.of("2")
      );
      assertFilterMatchesSkipArrays(
          new RangeFilter(
              "dim2",
              ColumnType.STRING,
              "super-null",
              "super-null",
              false,
              false,
              superFn,
              null
          ),
          ImmutableList.of("1", "5")
      );
      assertFilterMatchesSkipArrays(
          new RangeFilter("dim2", ColumnType.STRING, "super-", "super-", false, false, superFn, null),
          ImmutableList.of("2")
      );
    }

    assertFilterMatches(
        new RangeFilter(
            "dim3",
            ColumnType.STRING,
            "super-null",
            "super-null",
            false,
            false,
            superFn,
            null
        ),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    assertFilterMatches(
        new RangeFilter(
            "dim4",
            ColumnType.STRING,
            "super-null",
            "super-null",
            false,
            false,
            superFn,
            null
        ),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    assertFilterMatches(
        new RangeFilter("dim4", ColumnType.STRING, "super-null", "super-null", false, false, superFn, null),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );
  }

  @Test
  public void testListFilteredVirtualColumn()
  {
    assertFilterMatchesSkipVectorize(
        new RangeFilter("allow-dim0", ColumnType.STRING, "0", "2", false, false, null, null),
        ImmutableList.of()
    );
    assertFilterMatchesSkipVectorize(
        new RangeFilter("allow-dim0", ColumnType.STRING, "0", "6", false, false, null, null),
        ImmutableList.of("3", "4")
    );
    assertFilterMatchesSkipVectorize(
        new RangeFilter("allow-dim0", ColumnType.STRING, null, "6", false, false, null, null),
        ImmutableList.of("3", "4")
    );

    assertFilterMatchesSkipVectorize(
        new RangeFilter("deny-dim0", ColumnType.STRING, "0", "6", false, false, null, null),
        ImmutableList.of("0", "1", "2", "5", "6")
    );
    assertFilterMatchesSkipVectorize(
        new RangeFilter("deny-dim0", ColumnType.STRING, "3", "4", false, false, null, null),
        ImmutableList.of()
    );
    assertFilterMatchesSkipVectorize(
        new RangeFilter("deny-dim0", ColumnType.STRING, null, "6", false, false, null, null),
        ImmutableList.of("0", "1", "2", "5", "6")
    );

    if (isAutoSchema()) {
      // bail out, auto ingests arrays instead of mvds and this virtual column is for mvd stuff
      return;
    }


    /*
      makeDefaultSchemaRow("0", "", ImmutableList.of("a", "b"), "2017-07-25", 0.0, 0.0f, 0L),
      makeDefaultSchemaRow("1", "10", ImmutableList.of(), "2017-07-25", 10.1, 10.1f, 100L),
      makeDefaultSchemaRow("2", "2", ImmutableList.of(""), "2017-05-25", null, 5.5f, 40L),
      makeDefaultSchemaRow("3", "1", ImmutableList.of("a"), "2020-01-25", 120.0245, 110.0f, null),
      makeDefaultSchemaRow("4", "abdef", ImmutableList.of("c"), null, 60.0, null, 9001L),
      makeDefaultSchemaRow("5", "abc", null, "2020-01-25", 765.432, 123.45f, 12345L)
      makeDefaultSchemaRow("6", "-1000", ImmutableList.of("a"), null, 6.6, null, 10L)
      makeDefaultSchemaRow("7", "-10.012", ImmutableList.of("d"), null, null, 3.0f, null)

      // allow 'a'
      // deny 'a'
     */
    assertFilterMatchesSkipVectorize(
        new RangeFilter("allow-dim2", ColumnType.STRING, "a", "c", false, false, null, null),
        ImmutableList.of("0", "3", "6")
    );
    assertFilterMatchesSkipVectorize(
        new RangeFilter("allow-dim2", ColumnType.STRING, "c", "z", false, false, null, null),
        ImmutableList.of()
    );
    assertFilterMatchesSkipVectorize(
        new RangeFilter("allow-dim2", ColumnType.STRING, null, "z", false, false, null, null),
        ImmutableList.of("0", "3", "6")
    );

    assertFilterMatchesSkipVectorize(
        new RangeFilter("deny-dim2", ColumnType.STRING, "a", "b", false, true, null, null),
        ImmutableList.of()
    );
    assertFilterMatchesSkipVectorize(
        new RangeFilter("deny-dim2", ColumnType.STRING, "c", "z", false, false, null, null),
        ImmutableList.of("4", "7")
    );

    assertFilterMatchesSkipVectorize(
        new RangeFilter("deny-dim2", ColumnType.STRING, null, "z", false, false, null, null),
        NullHandling.replaceWithDefault() ? ImmutableList.of("0", "4", "7") : ImmutableList.of("0", "2", "4", "7")
    );
  }

  @Test
  public void testRequiredColumnRewrite()
  {
    RangeFilter filter = new RangeFilter("dim0", ColumnType.STRING, "", "", false, false, null, null);
    RangeFilter filter2 = new RangeFilter("dim1", ColumnType.STRING, "", "", false, false, null, null);
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
  public void test_equals()
  {
    EqualsVerifier.forClass(RangeFilter.class)
                  .withNonnullFields("column", "matchValueType")
                  .withIgnoredFields(
                      "lowerEval",
                      "upperEval",
                      "cachedOptimizedFilter",
                      "stringPredicateSupplier",
                      "longPredicateSupplier",
                      "floatPredicateSupplier",
                      "doublePredicateSupplier"
                  )
                  .withPrefabValues(ColumnType.class, ColumnType.STRING, ColumnType.DOUBLE)
                  .usingGetClass()
                  .verify();
  }
}
