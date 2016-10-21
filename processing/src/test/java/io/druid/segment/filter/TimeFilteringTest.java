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
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.extraction.TimeFormatExtractionFn;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.IntervalDimFilter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.filter.SearchQueryDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.lookup.LookupExtractor;
import io.druid.query.ordering.StringComparators;
import io.druid.query.search.search.ContainsSearchQuerySpec;
import io.druid.segment.IndexBuilder;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class TimeFilteringTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "ts";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "millis", new DateTime("2000")),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1", "dim2", "dim3")),
              null,
              null
          )
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 0L, "dim0", "0", "dim1", "", "dim2", ImmutableList.of("a", "b"))),
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 1L, "dim0", "1", "dim1", "10", "dim2", ImmutableList.of())),
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 2L, "dim0", "2", "dim1", "2", "dim2", ImmutableList.of(""))),
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 3L, "dim0", "3", "dim1", "1", "dim2", ImmutableList.of("a"))),
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 4L, "dim0", "4", "dim1", "def", "dim2", ImmutableList.of("c"))),
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 5L, "dim0", "5", "dim1", "abc"))
  );

  public TimeFilteringTest(
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
    BaseFilterTest.tearDown(TimeFilteringTest.class.getName());
  }

  @Test
  public void testTimeFilterAsLong()
  {
    assertFilterMatches(
        new SelectorDimFilter(Column.TIME_COLUMN_NAME, "0", null),
        ImmutableList.<String>of("0")
    );
    assertFilterMatches(
        new SelectorDimFilter(Column.TIME_COLUMN_NAME, "9000", null),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new BoundDimFilter(Column.TIME_COLUMN_NAME, "0", "4", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of("0", "1", "2", "3", "4")
    );
    assertFilterMatches(
        new BoundDimFilter(Column.TIME_COLUMN_NAME, "0", "4", true, true, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of("1", "2", "3")
    );

    assertFilterMatches(
        new InDimFilter(Column.TIME_COLUMN_NAME, Arrays.asList("2", "4", "8"), null),
        ImmutableList.<String>of("2", "4")
    );

    // cross the hashing threshold to test hashset implementation, filter on even values
    List<String> infilterValues = new ArrayList<>(InDimFilter.LONG_HASHING_THRESHOLD * 2);
    for (int i = 0; i < InDimFilter.LONG_HASHING_THRESHOLD * 2; i++) {
      infilterValues.add(String.valueOf(i*2));
    }
    assertFilterMatches(
        new InDimFilter(Column.TIME_COLUMN_NAME, infilterValues, null),
        ImmutableList.<String>of("0", "2", "4")
    );

    String jsFn = "function(x) { return(x === 3 || x === 5) }";
    assertFilterMatches(
        new JavaScriptDimFilter(Column.TIME_COLUMN_NAME, jsFn, null, JavaScriptConfig.getDefault()),
        ImmutableList.<String>of("3", "5")
    );

    assertFilterMatches(
        new RegexDimFilter(Column.TIME_COLUMN_NAME, "4", null),
        ImmutableList.<String>of("4")
    );

    assertFilterMatches(
        new SearchQueryDimFilter(Column.TIME_COLUMN_NAME, new ContainsSearchQuerySpec("2", true), null),
        ImmutableList.<String>of("2")
    );
  }

  @Test
  public void testTimeFilterWithExtractionFn()
  {
    final Map<String, String> stringMap = new HashMap<>();
    stringMap.put("0", "Monday");
    stringMap.put("1", "Tuesday");
    stringMap.put("2", "Wednesday");
    stringMap.put("3", "Thursday");
    stringMap.put("4", "Friday");
    stringMap.put("5", "Saturday");
    LookupExtractor mapExtractor = new MapLookupExtractor(stringMap, false);
    LookupExtractionFn exfn = new LookupExtractionFn(mapExtractor, false, "UNKNOWN", false, true);

    assertFilterMatches(
        new SelectorDimFilter(Column.TIME_COLUMN_NAME, "Monday", exfn),
        ImmutableList.<String>of("0")
    );
    assertFilterMatches(
        new SelectorDimFilter(Column.TIME_COLUMN_NAME, "Notaday", exfn),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new BoundDimFilter(Column.TIME_COLUMN_NAME, "Fridax", "Fridaz", false, false, null, exfn, StringComparators.ALPHANUMERIC),
        ImmutableList.<String>of("4")
    );
    assertFilterMatches(
        new BoundDimFilter(Column.TIME_COLUMN_NAME, "Friday", "Friday", true, true, null, exfn, StringComparators.ALPHANUMERIC),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new InDimFilter(Column.TIME_COLUMN_NAME, Arrays.asList("Caturday", "Saturday", "Tuesday"), exfn),
        ImmutableList.<String>of("1", "5")
    );

    // test InFilter HashSet implementation
    List<String> bigList = Arrays.asList(
        "Saturday", "Tuesday",
        "Caturday", "Xanaday", "Vojuday", "Gribaday", "Kipoday", "Dheferday", "Fakeday", "Qeearaday",
        "Hello", "World", "1", "2", "3", "4", "5", "6", "7"
    );
    assertFilterMatches(
        new InDimFilter(Column.TIME_COLUMN_NAME, bigList, exfn),
        ImmutableList.<String>of("1", "5")
    );

    String jsFn = "function(x) { return(x === 'Wednesday' || x === 'Thursday') }";
    assertFilterMatches(
        new JavaScriptDimFilter(Column.TIME_COLUMN_NAME, jsFn, exfn, JavaScriptConfig.getDefault()),
        ImmutableList.<String>of("2", "3")
    );

    assertFilterMatches(
        new RegexDimFilter(Column.TIME_COLUMN_NAME, ".*day", exfn),
        ImmutableList.<String>of("0", "1", "2", "3", "4", "5")
    );

    assertFilterMatches(
        new SearchQueryDimFilter(Column.TIME_COLUMN_NAME, new ContainsSearchQuerySpec("s", true), exfn),
        ImmutableList.<String>of("1", "2", "3")
    );
  }

  @Test
  public void testTimeFilterWithTimeFormatExtractionFn()
  {
    ExtractionFn exfn = new TimeFormatExtractionFn("EEEE", DateTimeZone.forID("America/New_York"), "en", null);
    assertFilterMatches(
        new SelectorDimFilter(Column.TIME_COLUMN_NAME, "Wednesday", exfn),
        ImmutableList.<String>of("0", "1", "2", "3", "4", "5")
    );
  }

  @Test
  public void testIntervalFilter()
  {
    assertFilterMatches(
        new IntervalDimFilter(
            Column.TIME_COLUMN_NAME,
            Arrays.asList(Interval.parse("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.005Z")),
            null
        ),
        ImmutableList.<String>of("1", "2", "3", "4")
    );

    assertFilterMatches(
        new IntervalDimFilter(
            Column.TIME_COLUMN_NAME,
            Arrays.asList(
                Interval.parse("1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.003Z"),
                Interval.parse("1970-01-01T00:00:00.004Z/1970-01-01T00:00:00.006Z")
            ),
            null
        ),
        ImmutableList.<String>of("0", "1", "2", "4", "5")
    );

    assertFilterMatches(
        new IntervalDimFilter(
            Column.TIME_COLUMN_NAME,
            Arrays.asList(
                Interval.parse("1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.001Z"),
                Interval.parse("1970-01-01T00:00:00.003Z/1970-01-01T00:00:00.006Z"),
                Interval.parse("1970-01-01T00:00:00.002Z/1970-01-01T00:00:00.005Z")
            ),
            null
        ),
        ImmutableList.<String>of("0", "2", "3", "4", "5")
    );

    // increment timestamp by 2 hours
    String timeBoosterJsFn = "function(x) { return(x + 7200000) }";
    ExtractionFn exFn = new JavaScriptExtractionFn(timeBoosterJsFn, true, JavaScriptConfig.getDefault());
    assertFilterMatches(
        new IntervalDimFilter(
            Column.TIME_COLUMN_NAME,
            Arrays.asList(Interval.parse("1970-01-01T02:00:00.001Z/1970-01-01T02:00:00.005Z")),
            exFn
        ),
        ImmutableList.<String>of("1", "2", "3", "4")
    );
  }

  @Test
  public void testIntervalFilterOnStringDimension()
  {
    assertFilterMatches(
        new IntervalDimFilter(
            "dim0",
            Arrays.asList(Interval.parse("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.005Z")),
            null
        ),
        ImmutableList.<String>of("1", "2", "3", "4")
    );

    assertFilterMatches(
        new IntervalDimFilter(
            "dim0",
            Arrays.asList(
                Interval.parse("1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.003Z"),
                Interval.parse("1970-01-01T00:00:00.004Z/1970-01-01T00:00:00.006Z")
            ),
            null
        ),
        ImmutableList.<String>of("0", "1", "2", "4", "5")
    );

    assertFilterMatches(
        new IntervalDimFilter(
            "dim0",
            Arrays.asList(
                Interval.parse("1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.001Z"),
                Interval.parse("1970-01-01T00:00:00.003Z/1970-01-01T00:00:00.006Z"),
                Interval.parse("1970-01-01T00:00:00.002Z/1970-01-01T00:00:00.005Z")
            ),
            null
        ),
        ImmutableList.<String>of("0", "2", "3", "4", "5")
    );

    assertFilterMatches(
        new IntervalDimFilter(
            "dim1",
            Arrays.asList(Interval.parse("1970-01-01T00:00:00.002Z/1970-01-01T00:00:00.011Z")),
            null
        ),
        ImmutableList.<String>of("1", "2")
    );

    // increment timestamp by 2 hours
    String timeBoosterJsFn = "function(x) { return(Number(x) + 7200000) }";
    ExtractionFn exFn = new JavaScriptExtractionFn(timeBoosterJsFn, true, JavaScriptConfig.getDefault());
    assertFilterMatches(
        new IntervalDimFilter(
            "dim0",
            Arrays.asList(Interval.parse("1970-01-01T02:00:00.001Z/1970-01-01T02:00:00.005Z")),
            exFn
        ),
        ImmutableList.<String>of("1", "2", "3", "4")
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
