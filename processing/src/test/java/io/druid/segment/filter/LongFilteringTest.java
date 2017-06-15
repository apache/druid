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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.Pair;
import io.druid.js.JavaScriptConfig;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
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
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.joda.time.DateTime;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class LongFilteringTest extends BaseFilterTest
{
  private static final String LONG_COLUMN = "lng";
  private static final String TIMESTAMP_COLUMN = "ts";
  private static int EXECUTOR_NUM_THREADS = 16;
  private static int EXECUTOR_NUM_TASKS = 2000;

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
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 1L, "dim0", "1", "lng", 1L, "dim1", "", "dim2", ImmutableList.of("a", "b"))),
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 2L, "dim0", "2", "lng", 2L, "dim1", "10", "dim2", ImmutableList.of())),
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 3L, "dim0", "3", "lng", 3L, "dim1", "2", "dim2", ImmutableList.of(""))),
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 4L, "dim0", "4", "lng", 4L, "dim1", "1", "dim2", ImmutableList.of("a"))),
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 5L, "dim0", "5", "lng", 5L, "dim1", "def", "dim2", ImmutableList.of("c"))),
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 6L, "dim0", "6", "lng", 6L, "dim1", "abc")),
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 7L, "dim0", "7", "lng", 100000000L, "dim1", "xyz")),
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 8L, "dim0", "8", "lng", 100000001L, "dim1", "xyz")),
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 9L, "dim0", "9", "lng", -25L, "dim1", "ghi")),
      PARSER.parse(ImmutableMap.<String, Object>of("ts", 10L, "dim0", "10", "lng", -100000001L, "dim1", "qqq"))
  );

  public LongFilteringTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(
        testName,
        ROWS,
        indexBuilder.schema(
            new IncrementalIndexSchema.Builder()
                .withMetrics(new LongSumAggregatorFactory(LONG_COLUMN, LONG_COLUMN))
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
    BaseFilterTest.tearDown(LongFilteringTest.class.getName());
  }

  @Test
  public void testLongColumnFiltering()
  {
    assertFilterMatches(
        new SelectorDimFilter(LONG_COLUMN, "0", null),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new SelectorDimFilter(LONG_COLUMN, "3", null),
        ImmutableList.<String>of("3")
    );

    assertFilterMatches(
        new SelectorDimFilter(LONG_COLUMN, "3.0", null),
        ImmutableList.<String>of("3")
    );

    assertFilterMatches(
        new SelectorDimFilter(LONG_COLUMN, "3.00000000000000000000001", null),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new SelectorDimFilter(LONG_COLUMN, "100000001.0", null),
        ImmutableList.<String>of("8")
    );

    assertFilterMatches(
        new SelectorDimFilter(LONG_COLUMN, "-100000001.0", null),
        ImmutableList.<String>of("10")
    );

    assertFilterMatches(
        new SelectorDimFilter(LONG_COLUMN, "111119223372036854775807.674398674398", null),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "2", "5", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of("2", "3", "4", "5")
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "1", "4", true, true, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of("2", "3")
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "2.0", "5.0", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of("2", "3", "4", "5")
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "2.0", "5.0", true, true, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of("3", "4")
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "1.9", "5.9", true, true, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of("2", "3", "4", "5")
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "2.1", "5.9", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of("3", "4", "5")
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "111119223372036854775807.67", "5.9", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "-111119223372036854775807.67", "5.9", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of("1", "2", "3", "4", "5", "9", "10")
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "2.1", "111119223372036854775807.67", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of("3", "4", "5", "6", "7", "8")
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "2.1", "-111119223372036854775807.67", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "100000000.0", "100000001.0", true, true, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "100000000.0", "100000001.0", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of("7", "8")
    );

    assertFilterMatches(
        new InDimFilter(LONG_COLUMN, Arrays.asList("2", "4", "8"), null),
        ImmutableList.<String>of("2", "4")
    );

    assertFilterMatches(
        new InDimFilter(LONG_COLUMN, Arrays.asList("1.999999999999999999", "4.00000000000000000000001"), null),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new InDimFilter(LONG_COLUMN, Arrays.asList("100000001.0", "99999999.999999999"), null),
        ImmutableList.<String>of("8")
    );

    assertFilterMatches(
        new InDimFilter(LONG_COLUMN, Arrays.asList("-25.0", "-99999999.999999999"), null),
        ImmutableList.<String>of("9")
    );

    // cross the hashing threshold to test hashset implementation, filter on even values
    List<String> infilterValues = new ArrayList<>(InDimFilter.NUMERIC_HASHING_THRESHOLD * 2);
    for (int i = 0; i < InDimFilter.NUMERIC_HASHING_THRESHOLD * 2; i++) {
      infilterValues.add(String.valueOf(i * 2));
    }
    assertFilterMatches(
        new InDimFilter(LONG_COLUMN, infilterValues, null),
        ImmutableList.<String>of("2", "4", "6")
    );

    String jsFn = "function(x) { return(x === 3 || x === 5) }";
    assertFilterMatches(
        new JavaScriptDimFilter(LONG_COLUMN, jsFn, null, JavaScriptConfig.getEnabledInstance()),
        ImmutableList.<String>of("3", "5")
    );

    assertFilterMatches(
        new RegexDimFilter(LONG_COLUMN, "4", null),
        ImmutableList.<String>of("4")
    );

    assertFilterMatches(
        new SearchQueryDimFilter(LONG_COLUMN, new ContainsSearchQuerySpec("2", true), null),
        ImmutableList.<String>of("2", "9")
    );
  }

  @Test
  public void testLongColumnFilteringWithNonNumbers()
  {
    assertFilterMatches(
        new SelectorDimFilter(LONG_COLUMN, "", null),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new SelectorDimFilter(LONG_COLUMN, null, null),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new SelectorDimFilter(LONG_COLUMN, "abc", null),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "a", "b", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, " ", "4", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of("1", "2", "3", "4", "9", "10")
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, " ", "4", false, false, null, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.<String>of("1", "2", "3", "4", "7", "8", "9", "10")
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, " ", "A", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, " ", "A", false, false, null, null, StringComparators.LEXICOGRAPHIC),
        ImmutableList.<String>of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
    );
  }

  @Test
  public void testLongFilterWithExtractionFn()
  {
    final Map<String, String> stringMap = new HashMap<>();
    stringMap.put("1", "Monday");
    stringMap.put("2", "Tuesday");
    stringMap.put("3", "Wednesday");
    stringMap.put("4", "Thursday");
    stringMap.put("5", "Friday");
    stringMap.put("6", "Saturday");
    LookupExtractor mapExtractor = new MapLookupExtractor(stringMap, false);
    LookupExtractionFn exfn = new LookupExtractionFn(mapExtractor, false, "UNKNOWN", false, true);

    assertFilterMatches(
        new SelectorDimFilter(LONG_COLUMN, "Monday", exfn),
        ImmutableList.<String>of("1")
    );
    assertFilterMatches(
        new SelectorDimFilter(LONG_COLUMN, "Notaday", exfn),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "Fridax", "Fridaz", false, false, null, exfn, StringComparators.ALPHANUMERIC),
        ImmutableList.<String>of("5")
    );
    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "Friday", "Friday", true, true, null, exfn, StringComparators.ALPHANUMERIC),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new InDimFilter(LONG_COLUMN, Arrays.asList("Caturday", "Saturday", "Tuesday"), exfn),
        ImmutableList.<String>of("2", "6")
    );

    // test InFilter HashSet implementation
    List<String> bigList = Arrays.asList(
        "Saturday", "Tuesday",
        "Caturday", "Xanaday", "Vojuday", "Gribaday", "Kipoday", "Dheferday", "Fakeday", "Qeearaday",
        "Hello", "World", "1", "2", "3", "4", "5", "6", "7"
    );
    assertFilterMatches(
        new InDimFilter(LONG_COLUMN, bigList, exfn),
        ImmutableList.<String>of("2", "6")
    );

    String jsFn = "function(x) { return(x === 'Wednesday' || x === 'Thursday') }";
    assertFilterMatches(
        new JavaScriptDimFilter(LONG_COLUMN, jsFn, exfn, JavaScriptConfig.getEnabledInstance()),
        ImmutableList.<String>of("3", "4")
    );

    assertFilterMatches(
        new RegexDimFilter(LONG_COLUMN, ".*day", exfn),
        ImmutableList.<String>of("1", "2", "3", "4", "5", "6")
    );

    assertFilterMatches(
        new SearchQueryDimFilter(LONG_COLUMN, new ContainsSearchQuerySpec("s", true), exfn),
        ImmutableList.<String>of("2", "3", "4")
    );
  }

  @Test
  public void testMultithreaded()
  {
    assertFilterMatchesMultithreaded(
        new SelectorDimFilter(LONG_COLUMN, "3", null),
        ImmutableList.<String>of("3")
    );

    assertFilterMatchesMultithreaded(
        new InDimFilter(LONG_COLUMN, Arrays.asList("2", "4", "8"), null),
        ImmutableList.<String>of("2", "4")
    );

    // cross the hashing threshold to test hashset implementation, filter on even values
    List<String> infilterValues = new ArrayList<>(InDimFilter.NUMERIC_HASHING_THRESHOLD * 2);
    for (int i = 0; i < InDimFilter.NUMERIC_HASHING_THRESHOLD * 2; i++) {
      infilterValues.add(String.valueOf(i * 2));
    }
    assertFilterMatchesMultithreaded(
        new InDimFilter(LONG_COLUMN, infilterValues, null),
        ImmutableList.<String>of("2", "4", "6")
    );

    assertFilterMatches(
        new BoundDimFilter(LONG_COLUMN, "2", "5", false, false, null, null, StringComparators.NUMERIC),
        ImmutableList.<String>of("2", "3", "4", "5")
    );
  }

  private void assertFilterMatchesMultithreaded(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    testWithExecutor(filter, expectedRows);
  }

  private Runnable makeFilterRunner(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        assertFilterMatches(filter, expectedRows);
      }
    };
  }

  private void testWithExecutor(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(EXECUTOR_NUM_THREADS));

    List<ListenableFuture<?>> futures = new ArrayList<>();

    for (int i = 0; i < EXECUTOR_NUM_TASKS; i++) {
      Runnable runnable = makeFilterRunner(filter, expectedRows);
      ListenableFuture fut = executor.submit(runnable);
      futures.add(fut);
    }

    try {
      Futures.allAsList(futures).get(60, TimeUnit.SECONDS);
    }
    catch (Exception ex) {
      Assert.fail(ex.getMessage());
    }

    executor.shutdown();
  }
}
