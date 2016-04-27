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
import com.metamx.common.Pair;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.js.JavaScriptConfig;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.JavaScriptExtractionFn;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.segment.IndexBuilder;
import io.druid.segment.StorageAdapter;
import org.joda.time.DateTime;
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
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "5", "dim1", "abc"))
  );

  public BoundFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean optimize
  )
  {
    super(ROWS, indexBuilder, finisher, optimize);
  }

  @Test
  public void testLexicographicMatchEverything()
  {
    final List<BoundDimFilter> filters = ImmutableList.of(
        new BoundDimFilter("dim0", "", "z", false, false, false, null),
        new BoundDimFilter("dim1", "", "z", false, false, false, null),
        new BoundDimFilter("dim2", "", "z", false, false, false, null),
        new BoundDimFilter("dim3", "", "z", false, false, false, null)
    );

    for (BoundDimFilter filter : filters) {
      assertFilterMatches(filter, ImmutableList.of("0", "1", "2", "3", "4", "5"));
    }
  }

  @Test
  public void testLexicographicMatchNull()
  {
    assertFilterMatches(
        new BoundDimFilter("dim0", "", "", false, false, false, null),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "", "", false, false, false, null),
        ImmutableList.of("0")
    );
    assertFilterMatches(
        new BoundDimFilter("dim2", "", "", false, false, false, null),
        ImmutableList.of("1", "2", "5")
    );
  }

  @Test
  public void testLexicographicMatchMissingColumn()
  {
    assertFilterMatches(
        new BoundDimFilter("dim3", "", "", false, false, false, null),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
    assertFilterMatches(
        new BoundDimFilter("dim3", "", "", true, false, false, null),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim3", "", "", false, true, false, null),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim3", "", null, false, true, false, null),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
    assertFilterMatches(
        new BoundDimFilter("dim3", null, "", false, false, false, null),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
    assertFilterMatches(
        new BoundDimFilter("dim3", null, "", false, true, false, null),
        ImmutableList.<String>of()
    );
  }

  @Test
  public void testLexicographicMatchTooStrict()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "abc", "abc", true, false, false, null),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "abc", "abc", true, true, false, null),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "abc", "abc", false, true, false, null),
        ImmutableList.<String>of()
    );
  }

  @Test
  public void testLexicographicMatchExactlySingleValue()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "abc", "abc", false, false, false, null),
        ImmutableList.of("5")
    );
  }

  @Test
  public void testLexicographicMatchSurroundingSingleValue()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "ab", "abd", true, true, false, null),
        ImmutableList.of("5")
    );
  }

  @Test
  public void testLexicographicMatchNoUpperLimit()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "ab", null, true, true, false, null),
        ImmutableList.of("4", "5")
    );
  }

  @Test
  public void testLexicographicMatchNoLowerLimit()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", null, "abd", true, true, false, null),
        ImmutableList.of("0", "1", "2", "3", "5")
    );
  }

  @Test
  public void testLexicographicMatchNumbers()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "1", "3", false, false, false, null),
        ImmutableList.of("1", "2", "3")
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "1", "3", true, true, false, null),
        ImmutableList.of("1", "2")
    );
  }

  @Test
  public void testAlphaNumericMatchNull()
  {
    assertFilterMatches(
        new BoundDimFilter("dim0", "", "", false, false, true, null),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "", "", false, false, true, null),
        ImmutableList.of("0")
    );
    assertFilterMatches(
        new BoundDimFilter("dim2", "", "", false, false, true, null),
        ImmutableList.of("1", "2", "5")
    );
    assertFilterMatches(
        new BoundDimFilter("dim3", "", "", false, false, true, null),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
  }

  @Test
  public void testAlphaNumericMatchTooStrict()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", true, false, true, null),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", true, true, true, null),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", false, true, true, null),
        ImmutableList.<String>of()
    );
  }

  @Test
  public void testAlphaNumericMatchExactlySingleValue()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "2", "2", false, false, true, null),
        ImmutableList.of("2")
    );
  }

  @Test
  public void testAlphaNumericMatchSurroundingSingleValue()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "1", "3", true, true, true, null),
        ImmutableList.of("2")
    );
  }

  @Test
  public void testAlphaNumericMatchNoUpperLimit()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", "1", null, true, true, true, null),
        ImmutableList.of("1", "2", "4", "5")
    );
  }

  @Test
  public void testAlphaNumericMatchNoLowerLimit()
  {
    assertFilterMatches(
        new BoundDimFilter("dim1", null, "2", true, true, true, null),
        ImmutableList.of("0", "3")
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
        new BoundDimFilter("dim0", "", "", false, false, false, makeNullFn),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );

    assertFilterMatches(
        new BoundDimFilter("dim1", "super-ab", "super-abd", true, true, false, superFn),
        ImmutableList.of("5")
    );

    assertFilterMatches(
        new BoundDimFilter("dim1", "super-0", "super-10", false, false, true, superFn),
        ImmutableList.of("1", "2", "3")
    );

    assertFilterMatches(
        new BoundDimFilter("dim2", "super-", "super-zzzzzz", false, false, false, superFn),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );

    assertFilterMatches(
        new BoundDimFilter("dim2", "super-null", "super-null", false, false, false, superFn),
        ImmutableList.of("1", "2", "5")
    );

    assertFilterMatches(
        new BoundDimFilter("dim3", "super-null", "super-null", false, false, false, superFn),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );

    assertFilterMatches(
        new BoundDimFilter("dim4", "super-null", "super-null", false, false, false, superFn),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
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
