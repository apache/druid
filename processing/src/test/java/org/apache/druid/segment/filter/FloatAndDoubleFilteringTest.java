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
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.JavaScriptDimFilter;
import org.apache.druid.query.filter.RegexDimFilter;
import org.apache.druid.query.filter.SearchQueryDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.search.ContainsSearchQuerySpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.junit.AfterClass;
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
public class FloatAndDoubleFilteringTest extends BaseFilterTest
{
  private static final String FLOAT_COLUMN = "flt";
  private static final String DOUBLE_COLUMN = "dbl";
  private static final String TIMESTAMP_COLUMN = "ts";
  private static final int NUM_FILTER_VALUES = 32;

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "millis", DateTimes.of("2000")),
          new DimensionsSpec(
              ImmutableList.of(
                  new StringDimensionSchema("dim0"),
                  new FloatDimensionSchema("flt"),
                  new DoubleDimensionSchema("dbl")
              )
          )
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parseBatch(ImmutableMap.of("ts", 1L, "dim0", "1", "flt", 1.0f, "dbl", 1.0d)).get(0),
      PARSER.parseBatch(ImmutableMap.of("ts", 2L, "dim0", "2", "flt", 2.0f, "dbl", 2.0d)).get(0),
      PARSER.parseBatch(ImmutableMap.of("ts", 3L, "dim0", "3", "flt", 3.0f, "dbl", 3.0d)).get(0),
      PARSER.parseBatch(ImmutableMap.of("ts", 4L, "dim0", "4", "flt", 4.0f, "dbl", 4.0d)).get(0),
      PARSER.parseBatch(ImmutableMap.of("ts", 5L, "dim0", "5", "flt", 5.0f, "dbl", 5.0d)).get(0),
      PARSER.parseBatch(ImmutableMap.of("ts", 6L, "dim0", "6", "flt", 6.0f, "dbl", 6.0d)).get(0)
  );

  public FloatAndDoubleFilteringTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<CursorFactory, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(
        testName,
        ROWS,
        indexBuilder.schema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(PARSER.getParseSpec().getDimensionsSpec())
                .build()
        ),
        finisher,
        cnf,
        optimize
    );
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    BaseFilterTest.tearDown(FloatAndDoubleFilteringTest.class.getName());
  }

  @Test
  public void testFloatColumnFiltering()
  {
    doTestFloatColumnFiltering(FLOAT_COLUMN);
    doTestFloatColumnFiltering(DOUBLE_COLUMN);
  }

  @Test
  public void testFloatColumnFilteringWithNonNumbers()
  {
    doTestFloatColumnFilteringWithNonNumbers(FLOAT_COLUMN);
    doTestFloatColumnFilteringWithNonNumbers(DOUBLE_COLUMN);
  }

  @Test
  public void testFloatFilterWithExtractionFn()
  {
    doTestFloatFilterWithExtractionFn(FLOAT_COLUMN);
    doTestFloatFilterWithExtractionFn(DOUBLE_COLUMN);
  }

  private void doTestFloatColumnFiltering(final String columnName)
  {
    assertFilterMatches(
        new SelectorDimFilter(columnName, "3", null),
        ImmutableList.of("3")
    );

    assertFilterMatches(
        new SelectorDimFilter(columnName, "3.0", null),
        ImmutableList.of("3")
    );

    assertFilterMatches(
        new BoundDimFilter(columnName, "2", "5", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.of("2", "3", "4", "5")
    );

    assertFilterMatches(
        new BoundDimFilter(columnName, "2.0", "5.0", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.of("2", "3", "4", "5")
    );

    assertFilterMatches(
        new BoundDimFilter(columnName, "1", "4", true, true, null, null, StringComparators.NUMERIC),
        ImmutableList.of("2", "3")
    );

    assertFilterMatches(
        new BoundDimFilter(columnName, "1.0", "4.0", true, true, null, null, StringComparators.NUMERIC),
        ImmutableList.of("2", "3")
    );

    assertFilterMatches(
        new InDimFilter(columnName, Arrays.asList("2", "4", "8"), null),
        ImmutableList.of("2", "4")
    );

    assertFilterMatches(
        new InDimFilter(columnName, Arrays.asList("2.0", "4.0", "8.0"), null),
        ImmutableList.of("2", "4")
    );

    // cross the hashing threshold to test hashset implementation, filter on even values
    List<String> infilterValues = new ArrayList<>(NUM_FILTER_VALUES);
    for (int i = 0; i < NUM_FILTER_VALUES; i++) {
      infilterValues.add(String.valueOf(i * 2));
    }
    assertFilterMatches(
        new InDimFilter(columnName, infilterValues, null),
        ImmutableList.of("2", "4", "6")
    );


    String jsFn = "function(x) { return(x === 3 || x === 5) }";
    assertFilterMatchesSkipVectorize(
        new JavaScriptDimFilter(columnName, jsFn, null, JavaScriptConfig.getEnabledInstance()),
        ImmutableList.of("3", "5")
    );

    String jsFn2 = "function(x) { return(x === 3.0 || x === 5.0) }";
    assertFilterMatchesSkipVectorize(
        new JavaScriptDimFilter(columnName, jsFn2, null, JavaScriptConfig.getEnabledInstance()),
        ImmutableList.of("3", "5")
    );

    assertFilterMatches(
        new RegexDimFilter(columnName, "4", null),
        ImmutableList.of("4")
    );

    assertFilterMatches(
        new RegexDimFilter(columnName, "4.0", null),
        ImmutableList.of("4")
    );

    assertFilterMatches(
        new SearchQueryDimFilter(columnName, new ContainsSearchQuerySpec("2", true), null),
        ImmutableList.of("2")
    );

    assertFilterMatches(
        new SearchQueryDimFilter(columnName, new ContainsSearchQuerySpec("2", true), null),
        ImmutableList.of("2")
    );
  }

  private void doTestFloatColumnFilteringWithNonNumbers(final String columnName)
  {
    assertFilterMatches(
        new SelectorDimFilter(columnName, "", null),
        ImmutableList.of()
    );

    assertFilterMatches(
        new SelectorDimFilter(columnName, null, null),
        ImmutableList.of()
    );

    assertFilterMatches(
        new SelectorDimFilter(columnName, "abc", null),
        ImmutableList.of()
    );

    assertFilterMatches(
        new BoundDimFilter(columnName, "a", "b", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.of()
    );

    assertFilterMatches(
        new BoundDimFilter(columnName, " ", "4", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.of("1", "2", "3", "4")
    );

    assertFilterMatches(
        new BoundDimFilter(columnName, " ", "4", false, false, null, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("1", "2", "3")
    );

    assertFilterMatches(
        new BoundDimFilter(columnName, " ", "4.0", false, false, null, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("1", "2", "3", "4")
    );

    assertFilterMatches(
        new BoundDimFilter(columnName, " ", "A", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.of()
    );

    assertFilterMatches(
        new BoundDimFilter(columnName, " ", "A", false, false, null, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.of("1", "2", "3", "4", "5", "6")
    );
  }

  private void doTestFloatFilterWithExtractionFn(final String columnName)
  {
    final Map<String, String> stringMap = new HashMap<>();
    stringMap.put("1.0", "Monday");
    stringMap.put("2.0", "Tuesday");
    stringMap.put("3.0", "Wednesday");
    stringMap.put("4.0", "Thursday");
    stringMap.put("5.0", "Friday");
    stringMap.put("6.0", "Saturday");
    LookupExtractor mapExtractor = new MapLookupExtractor(stringMap, false);
    LookupExtractionFn exfn = new LookupExtractionFn(mapExtractor, false, "UNKNOWN", false, true);

    assertFilterMatches(
        new SelectorDimFilter(columnName, "Monday", exfn),
        ImmutableList.of("1")
    );
    assertFilterMatches(
        new SelectorDimFilter(columnName, "Notaday", exfn),
        ImmutableList.of()
    );

    assertFilterMatches(
        new BoundDimFilter(columnName, "Fridax", "Fridaz", false, false, null, exfn, StringComparators.ALPHANUMERIC),
        ImmutableList.of("5")
    );
    assertFilterMatches(
        new BoundDimFilter(columnName, "Friday", "Friday", true, true, null, exfn, StringComparators.ALPHANUMERIC),
        ImmutableList.of()
    );

    assertFilterMatches(
        new InDimFilter(columnName, Arrays.asList("Caturday", "Saturday", "Tuesday"), exfn),
        ImmutableList.of("2", "6")
    );

    // test InFilter HashSet implementation
    List<String> bigList = Arrays.asList(
        "Saturday", "Tuesday",
        "Caturday", "Xanaday", "Vojuday", "Gribaday", "Kipoday", "Dheferday", "Fakeday", "Qeearaday",
        "Hello", "World", "1", "2", "3", "4", "5", "6", "7"
    );
    assertFilterMatches(
        new InDimFilter(columnName, bigList, exfn),
        ImmutableList.of("2", "6")
    );

    String jsFn = "function(x) { return(x === 'Wednesday' || x === 'Thursday') }";
    assertFilterMatchesSkipVectorize(
        new JavaScriptDimFilter(columnName, jsFn, exfn, JavaScriptConfig.getEnabledInstance()),
        ImmutableList.of("3", "4")
    );

    assertFilterMatches(
        new RegexDimFilter(columnName, ".*day", exfn),
        ImmutableList.of("1", "2", "3", "4", "5", "6")
    );

    assertFilterMatches(
        new SearchQueryDimFilter(columnName, new ContainsSearchQuerySpec("s", true), exfn),
        ImmutableList.of("2", "3", "4")
    );
  }
}
