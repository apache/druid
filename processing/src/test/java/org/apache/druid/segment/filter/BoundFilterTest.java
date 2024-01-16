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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.List;

@RunWith(Parameterized.class)
public class BoundFilterTest extends BaseFilterTest
{
  private static final List<InputRow> ROWS = ImmutableList.<InputRow>builder()
      .addAll(DEFAULT_ROWS)
      .add(makeDefaultSchemaRow("6", "-1000", ImmutableList.of("a"), null, null, 6.6, null, 10L))
      .add(makeDefaultSchemaRow("7", "-10.012", ImmutableList.of("d"), null, "e", null, 3.0f, null))
      .build();

  public BoundFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(testName, ROWS, indexBuilder, finisher, cnf, optimize);
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @AfterClass
  public static void tearDown() throws Exception
  {
    BaseFilterTest.tearDown(BoundFilterTest.class.getName());
  }

  @Test
  public void testLexicographicMatchEverything()
  {
    final List<BoundDimFilter> filters = ImmutableList.of(
        new BoundDimFilter("dim0", null, "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        new BoundDimFilter("vdim0", null, "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        new BoundDimFilter("dim1", null, "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        new BoundDimFilter("vdim1", null, "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        new BoundDimFilter("dim2", null, "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        new BoundDimFilter("vdim2", null, "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        new BoundDimFilter("dim3", null, "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        new BoundDimFilter("vdim3", null, "z", false, false, false, null, StringComparators.LEXICOGRAPHIC)
    );

    for (BoundDimFilter filter : filters) {
      if (filter.getDimension().equals("dim2")) {
        assertFilterMatchesSkipArrays(filter, ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7"));
      } else {
        assertFilterMatches(filter, ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7"));
      }
    }
  }

  @Test
  public void testLexicographicMatchWithEmptyString()
  {
    final List<BoundDimFilter> filters = ImmutableList.of(
        new BoundDimFilter("dim0", "", "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        new BoundDimFilter("dim1", "", "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        new BoundDimFilter("dim2", "", "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        new BoundDimFilter("dim3", "", "z", false, false, false, null, StringComparators.LEXICOGRAPHIC)
    );
    if (NullHandling.replaceWithDefault()) {
      for (BoundDimFilter filter : filters) {
        if (filter.getDimension().equals("dim2")) {
          assertFilterMatchesSkipArrays(filter, ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7"));
        } else {
          assertFilterMatches(filter, ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7"));
        }
      }
    } else {
      assertFilterMatches(filters.get(0), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7"));
      assertFilterMatches(filters.get(1), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7"));
      assertFilterMatchesSkipArrays(filters.get(2), ImmutableList.of("0", "2", "3", "4", "6", "7"));
      assertFilterMatches(filters.get(3), ImmutableList.of());
    }
  }

  @Test
  public void testLexicographicMatchNull()
  {
    assertFilterMatches(
        new BoundDimFilter("dim0", "", "", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "", "", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0")
    );
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatchesSkipArrays(
          new BoundDimFilter("dim2", "", "", false, false, false, null, StringComparators.LEXICOGRAPHIC),
          ImmutableList.of("1", "2", "5")
      );
    } else {
      assertFilterMatchesSkipArrays(
          new BoundDimFilter("dim2", "", "", false, false, false, null, StringComparators.LEXICOGRAPHIC),
          ImmutableList.of("2")
      );
    }
  }

  @Test
  public void testLexicographicMatchMissingColumn()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new BoundDimFilter("dim3", "", "", false, false, false, null, StringComparators.LEXICOGRAPHIC),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
      assertFilterMatches(
          new BoundDimFilter("dim3", "", null, false, true, false, null, StringComparators.LEXICOGRAPHIC),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
      assertFilterMatches(
          new BoundDimFilter("dim3", null, "", false, true, false, null, StringComparators.LEXICOGRAPHIC),
          ImmutableList.of()
      );
    } else {
      assertFilterMatches(
          new BoundDimFilter("dim3", "", "", false, false, false, null, StringComparators.LEXICOGRAPHIC),
          ImmutableList.of()
      );
      assertFilterMatches(
          new BoundDimFilter("dim3", "", null, false, true, false, null, StringComparators.LEXICOGRAPHIC),
          ImmutableList.of()
      );
      assertFilterMatches(
          new BoundDimFilter("dim3", null, "", false, true, false, null, StringComparators.LEXICOGRAPHIC),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
    }
    assertFilterMatches(
        new BoundDimFilter("dim3", "", "", true, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim3", "", "", false, true, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of()
    );

    assertFilterMatches(
        new BoundDimFilter("dim3", null, "", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );
    if (NullHandling.sqlCompatible()) {
      assertFilterMatches(
          new BoundDimFilter("dim3", null, "", false, true, false, null, StringComparators.LEXICOGRAPHIC),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
    } else {
      assertFilterMatches(
          new BoundDimFilter("dim3", null, "", false, true, false, null, StringComparators.LEXICOGRAPHIC),
          ImmutableList.of()
      );
    }
  }


  @Test
  public void testLexicographicMatchTooStrict()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "abc", "abc", true, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "abc", "abc", true, true, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "abc", "abc", false, true, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of()
    );
  }

  @Test
  public void testLexicographicMatchExactlySingleValue()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "abc", "abc", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("5")
    );
  }

  @Test
  public void testLexicographicMatchSurroundingSingleValue()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "ab", "abd", true, true, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("5")
    );
  }

  @Test
  public void testLexicographicMatchNoUpperLimit()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "ab", null, true, true, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("4", "5")
    );
  }

  @Test
  public void testLexicographicMatchNoLowerLimit()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", null, "abd", true, true, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "3", "5", "6", "7")
    );
  }

  @Test
  public void testLexicographicMatchNumbers()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "1", "3", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("1", "2", "3")
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "1", "3", true, true, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("1", "2")
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "-1", "3", true, true, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("1", "2", "3", "6", "7")
    );
  }

  @Test
  public void testAlphaNumericMatchNull()
  {
    assertFilterMatches(
        new BoundDimFilter("dim0", "", "", false, false, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "", "", false, false, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of("0")
    );
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatchesSkipArrays(
          new BoundDimFilter("dim2", "", "", false, false, true, null, StringComparators.ALPHANUMERIC),
          ImmutableList.of("1", "2", "5")
      );
      assertFilterMatches(
          new BoundDimFilter("dim3", "", "", false, false, true, null, StringComparators.ALPHANUMERIC),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
    } else {
      assertFilterMatchesSkipArrays(
          new BoundDimFilter("dim2", "", "", false, false, true, null, StringComparators.ALPHANUMERIC),
          ImmutableList.of("2")
      );
      assertFilterMatches(
          new BoundDimFilter("dim3", "", "", false, false, true, null, StringComparators.ALPHANUMERIC),
          ImmutableList.of()
      );
    }
  }

  @Test
  public void testAlphaNumericMatchTooStrict()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", true, false, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", true, true, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", false, true, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of()
    );
  }

  @Test
  public void testAlphaNumericMatchExactlySingleValue()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", false, false, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of("2")
    );
  }

  @Test
  public void testAlphaNumericMatchSurroundingSingleValue()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "1", "3", true, true, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of("2")
    );
  }

  @Test
  public void testAlphaNumericMatchNoUpperLimit()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "1", null, true, true, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of("1", "2", "4", "5", "6", "7")
    );

    assertFilterMatches(
        new BoundDimFilter("dim1", "-1", null, true, true, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of("4", "5", "6", "7")
    );
  }

  @Test
  public void testAlphaNumericMatchNoLowerLimit()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", null, "2", true, true, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of("0", "3")
    );

    assertFilterMatches(
        new BoundDimFilter("dim1", null, "ZZZZZ", true, true, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );
  }

  @Test
  public void testAlphaNumericMatchWithNegatives()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "-2000", "3", true, true, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of()
    );

    assertFilterMatches(
        new BoundDimFilter("dim1", "3", "-2000", true, true, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of("1", "6", "7")
    );
  }

  @Test
  public void testNumericMatchNull()
  {
    assertFilterMatches(
        new BoundDimFilter("dim0", "", "", false, false, false, null, StringComparators.NUMERIC),
        ImmutableList.of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "", "", false, false, false, null, StringComparators.NUMERIC),
        ImmutableList.of("0")
    );
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatchesSkipArrays(
          new BoundDimFilter("dim2", "", "", false, false, false, null, StringComparators.NUMERIC),
          ImmutableList.of("1", "2", "5")
      );
      assertFilterMatches(
          new BoundDimFilter("dim3", "", "", false, false, false, null, StringComparators.NUMERIC),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
    } else {
      assertFilterMatchesSkipArrays(
          new BoundDimFilter("dim2", "", "", false, false, false, null, StringComparators.NUMERIC),
          ImmutableList.of("2")
      );
      assertFilterMatches(
          new BoundDimFilter("dim3", "", "", false, false, false, null, StringComparators.NUMERIC),
          ImmutableList.of()
      );
    }

  }

  @Test
  public void testNumericMatchTooStrict()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", true, false, false, null, StringComparators.NUMERIC),
        ImmutableList.of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", true, true, false, null, StringComparators.NUMERIC),
        ImmutableList.of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", false, true, false, null, StringComparators.NUMERIC),
        ImmutableList.of()
    );
  }

  @Test
  public void testNumericMatchVirtualColumn()
  {
    assertFilterMatches(
        new BoundDimFilter("expr", "1", "2", false, false, false, null, StringComparators.NUMERIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    assertFilterMatches(
        new BoundDimFilter("expr", "2", "3", false, false, false, null, StringComparators.NUMERIC),
        ImmutableList.of()
    );
  }

  @Test
  public void testListFilteredVirtualColumn()
  {
    assertFilterMatchesSkipVectorize(
        new BoundDimFilter("allow-dim0", "0", "2", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of()
    );
    assertFilterMatchesSkipVectorize(
        new BoundDimFilter("allow-dim0", "0", "6", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("3", "4")
    );
    // the bound filter matches null, so it is what it is...
    assertFilterMatchesSkipVectorize(
        new BoundDimFilter("allow-dim0", null, "6", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    assertFilterMatchesSkipVectorize(
        new BoundDimFilter("deny-dim0", "0", "6", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "5", "6")
    );
    assertFilterMatchesSkipVectorize(
        new BoundDimFilter("deny-dim0", "3", "4", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of()
    );
    // the bound filter matches null, so it is what it is...
    assertFilterMatchesSkipVectorize(
        new BoundDimFilter("deny-dim0", null, "6", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6")
    );

    if (isAutoSchema()) {
      // bail out, auto ingests arrays instead of mvds and this virtual column is for mvd stuff
      return;
    }
    assertFilterMatchesSkipVectorize(
        new BoundDimFilter("allow-dim2", "a", "c", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "3", "6")
    );
    assertFilterMatchesSkipVectorize(
        new BoundDimFilter("allow-dim2", "c", "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of()
    );
    // the bound filter matches null, so it is what it is...
    assertFilterMatchesSkipVectorize(
        new BoundDimFilter("allow-dim2", null, "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    assertFilterMatchesSkipVectorize(
        new BoundDimFilter("deny-dim2", "a", "b", false, true, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of()
    );
    assertFilterMatchesSkipVectorize(
        new BoundDimFilter("deny-dim2", "c", "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("4", "7")
    );
    // the bound filter matches null, so it is what it is...
    assertFilterMatchesSkipVectorize(
        new BoundDimFilter("deny-dim2", null, "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );
  }

  @Test
  public void testNumericMatchExactlySingleValue()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", false, false, false, null, StringComparators.NUMERIC),
        ImmutableList.of("2")
    );

    assertFilterMatches(
        new BoundDimFilter("dim1", "-10.012", "-10.012", false, false, false, null, StringComparators.NUMERIC),
        ImmutableList.of("7")
    );
  }

  @Test
  public void testNumericMatchSurroundingSingleValue()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "1", "3", true, true, false, null, StringComparators.NUMERIC),
        ImmutableList.of("2")
    );

    assertFilterMatches(
        new BoundDimFilter("dim1", "-11", "-10", false, false, false, null, StringComparators.NUMERIC),
        ImmutableList.of("7")
    );
  }

  @Test
  public void testNumericMatchNoUpperLimit()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "1", null, true, true, false, null, StringComparators.NUMERIC),
        ImmutableList.of("1", "2")
    );
  }

  @Test
  public void testNumericMatchNoLowerLimit()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", null, "2", true, true, false, null, StringComparators.NUMERIC),
        ImmutableList.of("0", "3", "4", "5", "6", "7")
    );
  }

  @Test
  public void testNumericMatchWithNegatives()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "-2000", "3", true, true, false, null, StringComparators.NUMERIC),
        ImmutableList.of("2", "3", "6", "7")
    );
  }

  @Test
  public void testMatchWithExtractionFn()
  {
    String extractionJsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn superFn = new JavaScriptExtractionFn(extractionJsFn, false, JavaScriptConfig.getEnabledInstance());

    String nullJsFn = "function(str) { return null; }";
    ExtractionFn makeNullFn = new JavaScriptExtractionFn(nullJsFn, false, JavaScriptConfig.getEnabledInstance());

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new BoundDimFilter("dim0", "", "", false, false, false, makeNullFn, StringComparators.LEXICOGRAPHIC),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
    } else {
      assertFilterMatches(
          new BoundDimFilter("dim0", "", "", false, false, false, makeNullFn, StringComparators.LEXICOGRAPHIC),
          ImmutableList.of()
      );
    }

    assertFilterMatches(
        new BoundDimFilter(
            "dim1",
            "super-ab",
            "super-abd",
            true,
            true,
            false,
            superFn,
            StringComparators.LEXICOGRAPHIC
        ),
        ImmutableList.of("5")
    );

    assertFilterMatches(
        new BoundDimFilter("dim1", "super-0", "super-10", false, false, true, superFn, StringComparators.ALPHANUMERIC),
        ImmutableList.of("1", "2", "3")
    );

    assertFilterMatchesSkipArrays(
        new BoundDimFilter(
            "dim2",
            "super-",
            "super-zzzzzz",
            false,
            false,
            false,
            superFn,
            StringComparators.LEXICOGRAPHIC
        ),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatchesSkipArrays(
          new BoundDimFilter(
              "dim2",
              "super-null",
              "super-null",
              false,
              false,
              false,
              superFn,
              StringComparators.LEXICOGRAPHIC
          ),
          ImmutableList.of("1", "2", "5")
      );
      assertFilterMatchesSkipArrays(
          new BoundDimFilter(
              "dim2",
              "super-null",
              "super-null",
              false,
              false,
              false,
              superFn,
              StringComparators.NUMERIC
          ),
          ImmutableList.of("1", "2", "5")
      );
    } else {
      assertFilterMatchesSkipArrays(
          new BoundDimFilter(
              "dim2",
              "super-null",
              "super-null",
              false,
              false,
              false,
              superFn,
              StringComparators.LEXICOGRAPHIC
          ),
          ImmutableList.of("1", "5")
      );
      assertFilterMatchesSkipArrays(
          new BoundDimFilter("dim2", "super-", "super-", false, false, false, superFn, StringComparators.NUMERIC),
          ImmutableList.of("2")
      );
      assertFilterMatchesSkipArrays(
          new BoundDimFilter(
              "dim2",
              "super-null",
              "super-null",
              false,
              false,
              false,
              superFn,
              StringComparators.LEXICOGRAPHIC
          ),
          ImmutableList.of("1", "5")
      );
      assertFilterMatchesSkipArrays(
          new BoundDimFilter("dim2", "super-", "super-", false, false, false, superFn, StringComparators.NUMERIC),
          ImmutableList.of("2")
      );
    }

    assertFilterMatches(
        new BoundDimFilter(
            "dim3",
            "super-null",
            "super-null",
            false,
            false,
            false,
            superFn,
            StringComparators.LEXICOGRAPHIC
        ),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    assertFilterMatches(
        new BoundDimFilter(
            "dim4",
            "super-null",
            "super-null",
            false,
            false,
            false,
            superFn,
            StringComparators.LEXICOGRAPHIC
        ),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    assertFilterMatches(
        new BoundDimFilter("dim4", "super-null", "super-null", false, false, false, superFn, StringComparators.NUMERIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );
  }

  @Test
  public void testNumericNullsAndZeros()
  {
    assertFilterMatches(
        new BoundDimFilter(
            "d0",
            "0.0",
            "1.0",
            false,
            false,
            false,
            null,
            StringComparators.NUMERIC
        ),
        canTestNumericNullsAsDefaultValues ? ImmutableList.of("0", "2", "7") : ImmutableList.of("0")
    );

    assertFilterMatches(
        new BoundDimFilter(
            "f0",
            "0.0",
            "1.0",
            false,
            false,
            false,
            null,
            StringComparators.NUMERIC
        ),
        canTestNumericNullsAsDefaultValues ? ImmutableList.of("0", "4", "6") : ImmutableList.of("0")
    );

    assertFilterMatches(
        new BoundDimFilter(
            "l0",
            "0.0",
            "1.0",
            false,
            false,
            false,
            null,
            StringComparators.NUMERIC
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
        new BoundDimFilter(
            "vd0",
            "0.0",
            "1.0",
            false,
            false,
            false,
            null,
            StringComparators.NUMERIC
        ),
        canTestNumericNullsAsDefaultValues ? ImmutableList.of("0", "2", "7") : ImmutableList.of("0")
    );

    assertFilterMatches(
        new BoundDimFilter(
            "vf0",
            "0.0",
            "1.0",
            false,
            false,
            false,
            null,
            StringComparators.NUMERIC
        ),
        canTestNumericNullsAsDefaultValues ? ImmutableList.of("0", "4", "6") : ImmutableList.of("0")
    );

    assertFilterMatches(
        new BoundDimFilter(
            "vl0",
            "0.0",
            "1.0",
            false,
            false,
            false,
            null,
            StringComparators.NUMERIC
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
        new BoundDimFilter(
            "f0",
            "1.0",
            null,
            false,
            false,
            false,
            null,
            StringComparators.NUMERIC
        ),
        ImmutableList.of("1", "2", "3", "5", "7")
    );
    assertFilterMatches(
        new BoundDimFilter(
            "d0",
            "1",
            null,
            false,
            false,
            false,
            null,
            StringComparators.NUMERIC
        ),
        ImmutableList.of("1", "3", "4", "5", "6")
    );
    assertFilterMatches(
        new BoundDimFilter(
            "l0",
            "1",
            null,
            false,
            false,
            false,
            null,
            StringComparators.NUMERIC
        ),
        ImmutableList.of("1", "2", "4", "5", "6")
    );
  }

  @Test
  public void testRequiredColumnRewrite()
  {
    BoundFilter filter = new BoundFilter(
        new BoundDimFilter("dim0", "", "", false, false, true, null, StringComparators.ALPHANUMERIC)
    );
    BoundFilter filter2 = new BoundFilter(
        new BoundDimFilter("dim1", "", "", false, false, true, null, StringComparators.ALPHANUMERIC)
    );
    Assert.assertTrue(filter.supportsRequiredColumnRewrite());
    Assert.assertTrue(filter2.supportsRequiredColumnRewrite());

    Filter rewrittenFilter = filter.rewriteRequiredColumns(ImmutableMap.of("dim0", "dim1"));
    Assert.assertEquals(filter2, rewrittenFilter);

    expectedException.expect(IAE.class);
    expectedException.expectMessage("Received a non-applicable rewrite: {invalidName=dim1}, filter's dimension: dim0");
    filter.rewriteRequiredColumns(ImmutableMap.of("invalidName", "dim1"));
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(BoundFilter.class)
                  .usingGetClass()
                  .withNonnullFields("boundDimFilter")
                  .verify();
  }

  @Test
  public void test_equals_boundDimFilterDruidPredicateFactory()
  {
    EqualsVerifier.forClass(BoundFilter.BoundDimFilterDruidPredicateFactory.class)
                  .usingGetClass()
                  .withIgnoredFields(
                      "longPredicateSupplier",
                      "floatPredicateSupplier",
                      "doublePredicateSupplier"
                  )
                  .verify();
  }
}
