/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.Pair;
import io.druid.js.JavaScriptConfig;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.JavaScriptExtractionFn;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.IndexBuilder;
import io.druid.segment.StorageAdapter;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
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
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", new DateTime("2000")),
          new DimensionsSpec(null, null, null)
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "0", "dim1", "", "dim2", ImmutableList.of("a", "b"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "1", "dim1", "10", "dim2", ImmutableList.<String>of())),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "2", "dim1", "2", "dim2", ImmutableList.of(""))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "3", "dim1", "1", "dim2", ImmutableList.of("a"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "4", "dim1", "def", "dim2", ImmutableList.of("c"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "5", "dim1", "abc")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "6", "dim1", "-1000", "dim2", ImmutableList.of("a"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "7", "dim1", "-10.012", "dim2", ImmutableList.of("d")))
  );

  public BoundFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean optimize
  )
  {
    super(testName, ROWS, indexBuilder, finisher, optimize);
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
        new BoundDimFilter("dim0", "", "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        new BoundDimFilter("dim1", "", "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        new BoundDimFilter("dim2", "", "z", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        new BoundDimFilter("dim3", "", "z", false, false, false, null, StringComparators.LEXICOGRAPHIC)
    );

    for (BoundDimFilter filter : filters) {
      assertFilterMatches(filter, ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7"));
    }
  }

  @Test
  public void testLexicographicMatchNull()
  {
    assertFilterMatches(
        new BoundDimFilter("dim0", "", "", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "", "", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0")
    );
    assertFilterMatches(
        new BoundDimFilter("dim2", "", "", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("1", "2", "5")
    );
  }

  @Test
  public void testLexicographicMatchMissingColumn()
  {
    assertFilterMatches(
        new BoundDimFilter("dim3", "", "", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );
    assertFilterMatches(
        new BoundDimFilter("dim3", "", "", true, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim3", "", "", false, true, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim3", "", null, false, true, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );
    assertFilterMatches(
        new BoundDimFilter("dim3", null, "", false, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );
    assertFilterMatches(
        new BoundDimFilter("dim3", null, "", false, true, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.<String>of()
    );
  }


  @Test
  public void testLexicographicMatchTooStrict()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "abc", "abc", true, false, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "abc", "abc", true, true, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "abc", "abc", false, true, false, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.<String>of()
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
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "", "", false, false, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of("0")
    );
    assertFilterMatches(
        new BoundDimFilter("dim2", "", "", false, false, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of("1", "2", "5")
    );
    assertFilterMatches(
        new BoundDimFilter("dim3", "", "", false, false, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );
  }

  @Test
  public void testAlphaNumericMatchTooStrict()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", true, false, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", true, true, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", false, true, true, null, StringComparators.ALPHANUMERIC),
        ImmutableList.<String>of()
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
        ImmutableList.<String>of()
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
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "", "", false, false, false, null, StringComparators.NUMERIC),
        ImmutableList.of("0")
    );
    assertFilterMatches(
        new BoundDimFilter("dim2", "", "", false, false, false, null, StringComparators.NUMERIC),
        ImmutableList.of("1", "2", "5")
    );
    assertFilterMatches(
        new BoundDimFilter("dim3", "", "", false, false, false, null, StringComparators.NUMERIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );
  }

  @Test
  public void testNumericMatchTooStrict()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", true, false, false, null, StringComparators.NUMERIC),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", true, true, false, null, StringComparators.NUMERIC),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", false, true, false, null, StringComparators.NUMERIC),
        ImmutableList.<String>of()
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
    ExtractionFn superFn = new JavaScriptExtractionFn(extractionJsFn, false, JavaScriptConfig.getDefault());

    String nullJsFn = "function(str) { return null; }";
    ExtractionFn makeNullFn = new JavaScriptExtractionFn(nullJsFn, false, JavaScriptConfig.getDefault());

    assertFilterMatches(
        new BoundDimFilter("dim0", "", "", false, false, false, makeNullFn, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

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

    assertFilterMatches(
        new BoundDimFilter("dim2", "super-null", "super-null", false, false, false, superFn, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("1", "2", "5")
    );

    assertFilterMatches(
        new BoundDimFilter("dim3", "super-null", "super-null", false, false, false, superFn, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    assertFilterMatches(
        new BoundDimFilter("dim4", "super-null", "super-null", false, false, false, superFn, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );

    assertFilterMatches(
        new BoundDimFilter("dim2", "super-null", "super-null", false, false, false, superFn, StringComparators.NUMERIC),
        ImmutableList.of("1", "2", "5")
    );

    assertFilterMatches(
        new BoundDimFilter("dim4", "super-null", "super-null", false, false, false, superFn, StringComparators.NUMERIC),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7")
    );
  }

  private void assertFilterMatches(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    Assert.assertEquals(filter.toString(), expectedRows, selectColumnValuesMatchingFilter(filter, "dim0"));
    Assert.assertEquals(filter.toString(), expectedRows.size(), selectCountUsingFilteredAggregator(filter));
  }
}
