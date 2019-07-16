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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class BoundFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", DateTimes.of("2000")),
          new DimensionsSpec(null, null, null)
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parseBatch(ImmutableMap.of("dim0", "0", "dim1", "", "dim2", ImmutableList.of("a", "b"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "1", "dim1", "10", "dim2", ImmutableList.<String>of())).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "2", "dim1", "2", "dim2", ImmutableList.of(""))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "3", "dim1", "1", "dim2", ImmutableList.of("a"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "4", "dim1", "def", "dim2", ImmutableList.of("c"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "5", "dim1", "abc")).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "6", "dim1", "-1000", "dim2", ImmutableList.of("a"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "7", "dim1", "-10.012", "dim2", ImmutableList.of("d"))).get(0)
  );

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
        new BoundDimFilter("dim1", null, "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        new BoundDimFilter("dim2", null, "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        new BoundDimFilter("dim3", null, "z", false, false, false, null, StringComparators.LEXICOGRAPHIC)
    );

    for (BoundDimFilter filter : filters) {
      assertFilterMatches(filter, ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7"));
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
        assertFilterMatches(filter, ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7"));
      }
    } else {
      assertFilterMatches(filters.get(0), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7"));
      assertFilterMatches(filters.get(1), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7"));
      assertFilterMatches(filters.get(2), ImmutableList.of("0", "2", "3", "4", "6", "7"));
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
      assertFilterMatches(
          new BoundDimFilter("dim2", "", "", false, false, false, null, StringComparators.LEXICOGRAPHIC),
          ImmutableList.of("1", "2", "5")
      );
    } else {
      assertFilterMatches(
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
      assertFilterMatches(
          new BoundDimFilter("dim2", "", "", false, false, true, null, StringComparators.ALPHANUMERIC),
          ImmutableList.of("1", "2", "5")
      );
      assertFilterMatches(
          new BoundDimFilter("dim3", "", "", false, false, true, null, StringComparators.ALPHANUMERIC),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
    } else {
      assertFilterMatches(
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
      assertFilterMatches(
          new BoundDimFilter("dim2", "", "", false, false, false, null, StringComparators.NUMERIC),
          ImmutableList.of("1", "2", "5")
      );
      assertFilterMatches(
          new BoundDimFilter("dim3", "", "", false, false, false, null, StringComparators.NUMERIC),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
      );
    } else {
      assertFilterMatches(
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
    assertFilterMatchesSkipVectorize(
        new BoundDimFilter("expr", "1", "2", false, false, false, null, StringComparators.NUMERIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    assertFilterMatchesSkipVectorize(
        new BoundDimFilter("expr", "2", "3", false, false, false, null, StringComparators.NUMERIC),
        ImmutableList.of()
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
        new BoundDimFilter("dim1", "super-ab", "super-abd", true, true, false, superFn, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("5")
    );

    assertFilterMatches(
        new BoundDimFilter("dim1", "super-0", "super-10", false, false, true, superFn, StringComparators.ALPHANUMERIC),
        ImmutableList.of("1", "2", "3")
    );

    assertFilterMatches(
        new BoundDimFilter("dim2", "super-", "super-zzzzzz", false, false, false, superFn, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
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
      assertFilterMatches(
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
      assertFilterMatches(
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
      assertFilterMatches(
          new BoundDimFilter("dim2", "super-", "super-", false, false, false, superFn, StringComparators.NUMERIC),
          ImmutableList.of("2")
      );
      assertFilterMatches(
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
      assertFilterMatches(
          new BoundDimFilter("dim2", "super-", "super-", false, false, false, superFn, StringComparators.NUMERIC),
          ImmutableList.of("2")
      );
    }

    assertFilterMatches(
        new BoundDimFilter("dim3", "super-null", "super-null", false, false, false, superFn, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    assertFilterMatches(
        new BoundDimFilter("dim4", "super-null", "super-null", false, false, false, superFn, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    assertFilterMatches(
        new BoundDimFilter("dim4", "super-null", "super-null", false, false, false, superFn, StringComparators.NUMERIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );
  }
}
