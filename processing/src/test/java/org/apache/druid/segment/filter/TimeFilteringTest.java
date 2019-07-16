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
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.extraction.TimeFormatExtractionFn;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.IntervalDimFilter;
import org.apache.druid.query.filter.JavaScriptDimFilter;
import org.apache.druid.query.filter.RegexDimFilter;
import org.apache.druid.query.filter.SearchQueryDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.search.ContainsSearchQuerySpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnHolder;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class TimeFilteringTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "ts";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "millis", DateTimes.of("2000")),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1", "dim2", "dim3")),
              null,
              null
          )
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parseBatch(ImmutableMap.of("ts", 0L, "dim0", "0", "dim1", "", "dim2", ImmutableList.of("a", "b"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("ts", 1L, "dim0", "1", "dim1", "10", "dim2", ImmutableList.of())).get(0),
      PARSER.parseBatch(ImmutableMap.of("ts", 2L, "dim0", "2", "dim1", "2", "dim2", ImmutableList.of(""))).get(0),
      PARSER.parseBatch(ImmutableMap.of("ts", 3L, "dim0", "3", "dim1", "1", "dim2", ImmutableList.of("a"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("ts", 4L, "dim0", "4", "dim1", "def", "dim2", ImmutableList.of("c"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("ts", 5L, "dim0", "5", "dim1", "abc")).get(0)
  );

  public TimeFilteringTest(
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
    BaseFilterTest.tearDown(TimeFilteringTest.class.getName());
  }

  @Test
  public void testTimeFilterAsLong()
  {
    assertFilterMatches(
        new SelectorDimFilter(ColumnHolder.TIME_COLUMN_NAME, "0", null),
        ImmutableList.of("0")
    );
    assertFilterMatches(
        new SelectorDimFilter(ColumnHolder.TIME_COLUMN_NAME, "9000", null),
        ImmutableList.of()
    );

    assertFilterMatches(
        new BoundDimFilter(ColumnHolder.TIME_COLUMN_NAME, "0", "4", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.of("0", "1", "2", "3", "4")
    );
    assertFilterMatches(
        new BoundDimFilter(ColumnHolder.TIME_COLUMN_NAME, "0", "4", true, true, null, null, StringComparators.NUMERIC),
        ImmutableList.of("1", "2", "3")
    );

    assertFilterMatches(
        new InDimFilter(ColumnHolder.TIME_COLUMN_NAME, Arrays.asList("2", "4", "8"), null),
        ImmutableList.of("2", "4")
    );

    // cross the hashing threshold to test hashset implementation, filter on even values
    List<String> infilterValues = new ArrayList<>(InDimFilter.NUMERIC_HASHING_THRESHOLD * 2);
    for (int i = 0; i < InDimFilter.NUMERIC_HASHING_THRESHOLD * 2; i++) {
      infilterValues.add(String.valueOf(i * 2));
    }
    assertFilterMatches(
        new InDimFilter(ColumnHolder.TIME_COLUMN_NAME, infilterValues, null),
        ImmutableList.of("0", "2", "4")
    );

    String jsFn = "function(x) { return(x === 3 || x === 5) }";
    assertFilterMatchesSkipVectorize(
        new JavaScriptDimFilter(ColumnHolder.TIME_COLUMN_NAME, jsFn, null, JavaScriptConfig.getEnabledInstance()),
        ImmutableList.of("3", "5")
    );

    assertFilterMatches(
        new RegexDimFilter(ColumnHolder.TIME_COLUMN_NAME, "4", null),
        ImmutableList.of("4")
    );

    assertFilterMatches(
        new SearchQueryDimFilter(ColumnHolder.TIME_COLUMN_NAME, new ContainsSearchQuerySpec("2", true), null),
        ImmutableList.of("2")
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
        new SelectorDimFilter(ColumnHolder.TIME_COLUMN_NAME, "Monday", exfn),
        ImmutableList.of("0")
    );
    assertFilterMatches(
        new SelectorDimFilter(ColumnHolder.TIME_COLUMN_NAME, "Notaday", exfn),
        ImmutableList.of()
    );

    assertFilterMatches(
        new BoundDimFilter(ColumnHolder.TIME_COLUMN_NAME, "Fridax", "Fridaz", false, false, null, exfn, StringComparators.ALPHANUMERIC),
        ImmutableList.of("4")
    );
    assertFilterMatches(
        new BoundDimFilter(ColumnHolder.TIME_COLUMN_NAME, "Friday", "Friday", true, true, null, exfn, StringComparators.ALPHANUMERIC),
        ImmutableList.of()
    );

    assertFilterMatches(
        new InDimFilter(ColumnHolder.TIME_COLUMN_NAME, Arrays.asList("Caturday", "Saturday", "Tuesday"), exfn),
        ImmutableList.of("1", "5")
    );

    // test InFilter HashSet implementation
    List<String> bigList = Arrays.asList(
        "Saturday", "Tuesday",
        "Caturday", "Xanaday", "Vojuday", "Gribaday", "Kipoday", "Dheferday", "Fakeday", "Qeearaday",
        "Hello", "World", "1", "2", "3", "4", "5", "6", "7"
    );
    assertFilterMatches(
        new InDimFilter(ColumnHolder.TIME_COLUMN_NAME, bigList, exfn),
        ImmutableList.of("1", "5")
    );

    String jsFn = "function(x) { return(x === 'Wednesday' || x === 'Thursday') }";
    assertFilterMatchesSkipVectorize(
        new JavaScriptDimFilter(ColumnHolder.TIME_COLUMN_NAME, jsFn, exfn, JavaScriptConfig.getEnabledInstance()),
        ImmutableList.of("2", "3")
    );

    assertFilterMatches(
        new RegexDimFilter(ColumnHolder.TIME_COLUMN_NAME, ".*day", exfn),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );

    assertFilterMatches(
        new SearchQueryDimFilter(ColumnHolder.TIME_COLUMN_NAME, new ContainsSearchQuerySpec("s", true), exfn),
        ImmutableList.of("1", "2", "3")
    );
  }

  @Test
  public void testTimeFilterWithTimeFormatExtractionFn()
  {
    ExtractionFn exfn = new TimeFormatExtractionFn(
        "EEEE",
        DateTimes.inferTzFromString("America/New_York"),
        "en",
        null,
        false
    );
    assertFilterMatches(
        new SelectorDimFilter(ColumnHolder.TIME_COLUMN_NAME, "Wednesday", exfn),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
  }

  @Test
  public void testIntervalFilter()
  {
    assertFilterMatches(
        new IntervalDimFilter(
            ColumnHolder.TIME_COLUMN_NAME,
            Collections.singletonList(Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.005Z")),
            null
        ),
        ImmutableList.of("1", "2", "3", "4")
    );

    assertFilterMatches(
        new IntervalDimFilter(
            ColumnHolder.TIME_COLUMN_NAME,
            Arrays.asList(
                Intervals.of("1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.003Z"),
                Intervals.of("1970-01-01T00:00:00.004Z/1970-01-01T00:00:00.006Z")
            ),
            null
        ),
        ImmutableList.of("0", "1", "2", "4", "5")
    );

    assertFilterMatches(
        new IntervalDimFilter(
            ColumnHolder.TIME_COLUMN_NAME,
            Arrays.asList(
                Intervals.of("1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.001Z"),
                Intervals.of("1970-01-01T00:00:00.003Z/1970-01-01T00:00:00.006Z"),
                Intervals.of("1970-01-01T00:00:00.002Z/1970-01-01T00:00:00.005Z")
            ),
            null
        ),
        ImmutableList.of("0", "2", "3", "4", "5")
    );

    // increment timestamp by 2 hours
    String timeBoosterJsFn = "function(x) { return(x + 7200000) }";
    ExtractionFn exFn = new JavaScriptExtractionFn(timeBoosterJsFn, true, JavaScriptConfig.getEnabledInstance());
    assertFilterMatches(
        new IntervalDimFilter(
            ColumnHolder.TIME_COLUMN_NAME,
            Collections.singletonList(Intervals.of("1970-01-01T02:00:00.001Z/1970-01-01T02:00:00.005Z")),
            exFn
        ),
        ImmutableList.of("1", "2", "3", "4")
    );
  }

  @Test
  public void testIntervalFilterOnStringDimension()
  {
    assertFilterMatches(
        new IntervalDimFilter(
            "dim0",
            Collections.singletonList(Intervals.of("1970-01-01T00:00:00.001Z/1970-01-01T00:00:00.005Z")),
            null
        ),
        ImmutableList.of("1", "2", "3", "4")
    );

    assertFilterMatches(
        new IntervalDimFilter(
            "dim0",
            Arrays.asList(
                Intervals.of("1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.003Z"),
                Intervals.of("1970-01-01T00:00:00.004Z/1970-01-01T00:00:00.006Z")
            ),
            null
        ),
        ImmutableList.of("0", "1", "2", "4", "5")
    );

    assertFilterMatches(
        new IntervalDimFilter(
            "dim0",
            Arrays.asList(
                Intervals.of("1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.001Z"),
                Intervals.of("1970-01-01T00:00:00.003Z/1970-01-01T00:00:00.006Z"),
                Intervals.of("1970-01-01T00:00:00.002Z/1970-01-01T00:00:00.005Z")
            ),
            null
        ),
        ImmutableList.of("0", "2", "3", "4", "5")
    );

    assertFilterMatches(
        new IntervalDimFilter(
            "dim1",
            Collections.singletonList(Intervals.of("1970-01-01T00:00:00.002Z/1970-01-01T00:00:00.011Z")),
            null
        ),
        ImmutableList.of("1", "2")
    );

    // increment timestamp by 2 hours
    String timeBoosterJsFn = "function(x) { return(Number(x) + 7200000) }";
    ExtractionFn exFn = new JavaScriptExtractionFn(timeBoosterJsFn, true, JavaScriptConfig.getEnabledInstance());
    assertFilterMatchesSkipVectorize(
        new IntervalDimFilter(
            "dim0",
            Collections.singletonList(Intervals.of("1970-01-01T02:00:00.001Z/1970-01-01T02:00:00.005Z")),
            exFn
        ),
        ImmutableList.of("1", "2", "3", "4")
    );
  }
}
