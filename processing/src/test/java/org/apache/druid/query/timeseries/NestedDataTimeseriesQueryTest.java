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

package org.apache.druid.query.timeseries;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Druids;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

@RunWith(Parameterized.class)
public class NestedDataTimeseriesQueryTest extends InitializedNullHandlingTest
{
  @Parameterized.Parameters(name = "{0}:vectorize={1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<BiFunction<TemporaryFolder, Closer, List<Segment>>> segmentsGenerators =
        NestedDataTestUtils.getSegmentGenerators(NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE);

    return QueryRunnerTestHelper.cartesian(
        // runners
        segmentsGenerators,
        // vectorize?
        ImmutableList.of("false", "force")
    );
  }

  private static <T> void assertExpectedResults(Iterable<Result<T>> expectedResults, Iterable<Result<T>> results)
  {
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final Closer closer;
  private final AggregationTestHelper helper;

  private final BiFunction<TemporaryFolder, Closer, List<Segment>> segmentsGenerator;
  private final QueryContexts.Vectorize vectorize;
  private final String segmentsName;

  public NestedDataTimeseriesQueryTest(
      BiFunction<TemporaryFolder, Closer, List<Segment>> segmentsGenerator,
      String vectorize
  )
  {
    this.helper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelper(
        NestedDataModule.getJacksonModulesList(),
        tempFolder
    );
    this.segmentsGenerator = segmentsGenerator;
    this.segmentsName = segmentsGenerator.toString();
    this.vectorize = QueryContexts.Vectorize.fromString(vectorize);
    this.closer = Closer.create();
  }

  public Map<String, Object> getContext()
  {
    return ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, vectorize.toString(),
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize.toString()
    );
  }

  @Test
  public void testCount()
  {
    // this doesn't really have anything to do with nested columns
    // just a smoke test to make sure everything else is sane
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_datasource")
                                  .intervals(Collections.singletonList(Intervals.ETERNITY))
                                  .aggregators(new CountAggregatorFactory("count"))
                                  .context(getContext())
                                  .build();
    runResults(
        query,
        ImmutableList.of(
            new Result<>(
                DateTimes.of("2023-01-01T00:00:00.000Z"),
                new TimeseriesResultValue(
                    ImmutableMap.of("count", 14L)
                )
            )
        )
    );
  }

  @Test
  public void testSums()
  {
    /*
      "long":1,     "double":1.0,     "obj":{"a": 100, "b": {"x": "a", "y": 1.1, "z": [1, 2, 3, 4]}},
      "long":2,                       "obj":{"a": 200, "b": {"x": "b", "y": 1.1, "z": [2, 4, 6]}},
      "long":3,     "double":2.0,     "obj":{"a": 300},
      "long":4,     "double":3.3,     "obj":{"a": 400, "b": {"x": "d", "y": 1.1, "z": [3, 4]}},
      "long": null, "double":4.4,     "obj":{"a": 500, "b": {"x": "e", "z": [1, 2, 3, 4]}},
      "long":5,     "double":5.9,     "obj":{"a": 600, "b": {"x": "f", "y": 1.1, "z": [6, 7, 8, 9]}},
                    "double":null,    "obj":{"a": 700, "b": {"x": "g", "y": 1.1, "z": [9, null, 9, 9]}},
     */
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_datasource")
                                  .intervals(Collections.singletonList(Intervals.ETERNITY))
                                  .aggregators(
                                      new CountAggregatorFactory("count"),
                                      new LongSumAggregatorFactory("sumLong", "long"),
                                      new DoubleSumAggregatorFactory("sumDouble", "double"),
                                      new LongSumAggregatorFactory("sumNestedLong", "v0"),
                                      new DoubleSumAggregatorFactory("sumNestedDouble", "v1"),
                                      new DoubleSumAggregatorFactory("sumNestedLongFromArray", "v2")
                                  )
                                  .virtualColumns(
                                      new NestedFieldVirtualColumn("obj", "$.a", "v0", ColumnType.LONG),
                                      new NestedFieldVirtualColumn("obj", "$.b.y", "v1", ColumnType.DOUBLE),
                                      new NestedFieldVirtualColumn("obj", "$.b.z[0]", "v2", ColumnType.LONG)
                                  )
                                  .context(getContext())
                                  .build();
    runResults(
        query,
        ImmutableList.of(
            new Result<>(
                DateTimes.of("2023-01-01T00:00:00.000Z"),
                new TimeseriesResultValue(
                    ImmutableMap.<String, Object>builder()
                                .put("count", 14L)
                                .put("sumLong", 30L)
                                .put("sumDouble", 33.2)
                                .put("sumNestedLong", 5600L)
                                .put("sumNestedDouble", 11.0)
                                .put("sumNestedLongFromArray", 44.0)
                                .build()
                )
            )
        )
    );
  }

  @Test
  public void testSumsNoVectorize()
  {
    if (QueryContexts.Vectorize.FORCE.equals(vectorize)) {
      // variant types cannot vectorize aggregators
      return;
    }
    /*
    "variant": 1,       "variantWithArrays": 1,
    "variant": "b",     "variantWithArrays": "b",
    "variant": 3.0,     "variantWithArrays": 3.0,
    "variant": "1",     "variantWithArrays": "1",
    "variant": "hello", "variantWithArrays": "hello",

    "variant": 51,      "variantWithArrays": [51, -35],
     */
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_datasource")
                                  .intervals(Collections.singletonList(Intervals.ETERNITY))
                                  .aggregators(
                                      new CountAggregatorFactory("count"),
                                      new LongSumAggregatorFactory("sumVariantLong", "variant"),
                                      new DoubleSumAggregatorFactory("sumVariantDouble", "variant"),
                                      new LongSumAggregatorFactory("sumVariantArraysLong", "variantWithArrays"),
                                      new DoubleSumAggregatorFactory("sumVariantArraysDouble", "variantWithArrays")
                                  )
                                  .context(getContext())
                                  .build();
    runResults(
        query,
        ImmutableList.of(
            new Result<>(
                DateTimes.of("2023-01-01T00:00:00.000Z"),
                new TimeseriesResultValue(
                    ImmutableMap.<String, Object>builder()
                                .put("count", 14L)
                                .put("sumVariantLong", 112L)
                                .put("sumVariantDouble", 112.0)
                                .put("sumVariantArraysLong", 10L)
                                .put("sumVariantArraysDouble", 10.0)
                                .build()
                )
            )
        )
    );
  }

  @Test
  public void testFilterLong()
  {
    // this doesn't really have anything to do with nested columns
    // just a smoke test to make sure everything else is sane
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_datasource")
                                  .intervals(Collections.singletonList(Intervals.ETERNITY))
                                  .filters(
                                      new AndDimFilter(
                                          new EqualityFilter("long", ColumnType.LONG, 2L, null),
                                          new EqualityFilter("v0", ColumnType.LONG, 2L, null)
                                      )
                                  )
                                  .virtualColumns(
                                      new NestedFieldVirtualColumn(
                                          "long",
                                          "$.",
                                          "v0",
                                          ColumnType.LONG
                                      )
                                  )
                                  .aggregators(new CountAggregatorFactory("count"))
                                  .context(getContext())
                                  .build();
    runResults(
        query,
        ImmutableList.of(
            new Result<>(
                DateTimes.of("2023-01-01T00:00:00.000Z"),
                new TimeseriesResultValue(
                    ImmutableMap.of("count", 2L)
                )
            )
        )
    );
  }

  @Test
  public void testFilterVariantAsString()
  {
    // this doesn't really have anything to do with nested columns
    // just a smoke test to make sure everything else is sane
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_datasource")
                                  .intervals(Collections.singletonList(Intervals.ETERNITY))
                                  .filters(
                                      new AndDimFilter(
                                          new EqualityFilter("variant", ColumnType.STRING, "hello", null),
                                          new EqualityFilter("v0", ColumnType.STRING, "hello", null)
                                      )
                                  )
                                  .virtualColumns(
                                      new NestedFieldVirtualColumn(
                                          "variant",
                                          "$.",
                                          "v0",
                                          ColumnType.STRING
                                      )
                                  )
                                  .aggregators(new CountAggregatorFactory("count"))
                                  .context(getContext())
                                  .build();
    runResults(
        query,
        ImmutableList.of(
            new Result<>(
                DateTimes.of("2023-01-01T00:00:00.000Z"),
                new TimeseriesResultValue(
                    ImmutableMap.of("count", 2L)
                )
            )
        )
    );
  }

  @Test
  public void testFilterVariantAsStringNoIndexes()
  {
    // this doesn't really have anything to do with nested columns
    // just a smoke test to make sure everything else is sane
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_datasource")
                                  .intervals(Collections.singletonList(Intervals.ETERNITY))
                                  .filters(
                                      new AndDimFilter(
                                          new EqualityFilter("variant", ColumnType.STRING, "hello", new FilterTuning(false, null, null)),
                                          new EqualityFilter("v0", ColumnType.STRING, "hello", new FilterTuning(false, null, null))
                                      )
                                  )
                                  .virtualColumns(
                                      new NestedFieldVirtualColumn(
                                          "variant",
                                          "$.",
                                          "v0",
                                          ColumnType.STRING
                                      )
                                  )
                                  .aggregators(new CountAggregatorFactory("count"))
                                  .context(getContext())
                                  .build();
    runResults(
        query,
        ImmutableList.of(
            new Result<>(
                DateTimes.of("2023-01-01T00:00:00.000Z"),
                new TimeseriesResultValue(
                    ImmutableMap.of("count", 2L)
                )
            )
        )
    );
  }

  @Test
  public void testFilterVariantAsLong()
  {
    // this doesn't really have anything to do with nested columns
    // just a smoke test to make sure everything else is sane
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_datasource")
                                  .intervals(Collections.singletonList(Intervals.ETERNITY))
                                  .filters(
                                      new AndDimFilter(
                                          new EqualityFilter("variant", ColumnType.LONG, 51L, null),
                                          new EqualityFilter("v0", ColumnType.LONG, 51L, null)
                                      )
                                  )
                                  .virtualColumns(
                                      new NestedFieldVirtualColumn(
                                          "variant",
                                          "$.",
                                          "v0",
                                          ColumnType.LONG
                                      )
                                  )
                                  .aggregators(new CountAggregatorFactory("count"))
                                  .context(getContext())
                                  .build();
    runResults(
        query,
        ImmutableList.of(
            new Result<>(
                DateTimes.of("2023-01-01T00:00:00.000Z"),
                new TimeseriesResultValue(
                    ImmutableMap.of("count", 2L)
                )
            )
        )
    );
  }

  @Test
  public void testFilterVariantAsLongNoIndexes()
  {
    // this doesn't really have anything to do with nested columns
    // just a smoke test to make sure everything else is sane
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_datasource")
                                  .intervals(Collections.singletonList(Intervals.ETERNITY))
                                  .filters(
                                      new AndDimFilter(
                                          new EqualityFilter("variant", ColumnType.LONG, 51L, new FilterTuning(false, null, null)),
                                          new EqualityFilter("v0", ColumnType.LONG, 51L, new FilterTuning(false, null, null))
                                      )
                                  )
                                  .virtualColumns(
                                      new NestedFieldVirtualColumn(
                                          "variant",
                                          "$.",
                                          "v0",
                                          ColumnType.LONG
                                      )
                                  )
                                  .aggregators(new CountAggregatorFactory("count"))
                                  .context(getContext())
                                  .build();
    runResults(
        query,
        ImmutableList.of(
            new Result<>(
                DateTimes.of("2023-01-01T00:00:00.000Z"),
                new TimeseriesResultValue(
                    ImmutableMap.of("count", 2L)
                )
            )
        )
    );
  }

  @Test
  public void testFilterVariantArrayAsString()
  {
    // this doesn't really have anything to do with nested columns
    // just a smoke test to make sure everything else is sane
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_datasource")
                                  .intervals(Collections.singletonList(Intervals.ETERNITY))
                                  .filters(
                                      new AndDimFilter(
                                          new EqualityFilter("variantWithArrays", ColumnType.STRING, "1", null),
                                          new EqualityFilter("v0", ColumnType.STRING, "1", null)
                                      )
                                  )
                                  .virtualColumns(
                                      new NestedFieldVirtualColumn(
                                          "variantWithArrays",
                                          "$.",
                                          "v0",
                                          ColumnType.STRING
                                      )
                                  )
                                  .aggregators(new CountAggregatorFactory("count"))
                                  .context(getContext())
                                  .build();
    runResults(
        query,
        ImmutableList.of(
            new Result<>(
                DateTimes.of("2023-01-01T00:00:00.000Z"),
                new TimeseriesResultValue(
                    ImmutableMap.of("count", 4L)
                )
            )
        )
    );
  }

  @Test
  public void testFilterVariantArrayAsDouble()
  {
    // this doesn't really have anything to do with nested columns
    // just a smoke test to make sure everything else is sane
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_datasource")
                                  .intervals(Collections.singletonList(Intervals.ETERNITY))
                                  .filters(
                                      new AndDimFilter(
                                          new EqualityFilter("variantWithArrays", ColumnType.DOUBLE, 3.0, null),
                                          new EqualityFilter("v0", ColumnType.DOUBLE, 3.0, null)
                                      )
                                  )
                                  .virtualColumns(
                                      new NestedFieldVirtualColumn(
                                          "variantWithArrays",
                                          "$.",
                                          "v0",
                                          ColumnType.STRING
                                      )
                                  )
                                  .aggregators(new CountAggregatorFactory("count"))
                                  .context(getContext())
                                  .build();
    runResults(
        query,
        ImmutableList.of(
            new Result<>(
                DateTimes.of("2023-01-01T00:00:00.000Z"),
                new TimeseriesResultValue(
                    ImmutableMap.of("count", 2L)
                )
            )
        )
    );
  }

  @Test
  public void testFilterVariantArrayAsArray()
  {
    // this doesn't really have anything to do with nested columns
    // just a smoke test to make sure everything else is sane
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_datasource")
                                  .intervals(Collections.singletonList(Intervals.ETERNITY))
                                  .filters(
                                      new AndDimFilter(
                                          new EqualityFilter("variantWithArrays", ColumnType.LONG_ARRAY, Arrays.asList(51, -35), null),
                                          new EqualityFilter("v0", ColumnType.LONG_ARRAY, Arrays.asList(51, -35), null)
                                      )
                                  )
                                  .virtualColumns(
                                      new NestedFieldVirtualColumn(
                                          "variantWithArrays",
                                          "$.",
                                          "v0",
                                          ColumnType.STRING_ARRAY
                                      )
                                  )
                                  .aggregators(new CountAggregatorFactory("count"))
                                  .context(getContext())
                                  .build();
    runResults(
        query,
        ImmutableList.of(
            new Result<>(
                DateTimes.of("2023-01-01T00:00:00.000Z"),
                new TimeseriesResultValue(
                    ImmutableMap.of("count", 2L)
                )
            )
        )
    );
  }

  @Test
  public void testFilterVariantArrayStringArray()
  {
    // this doesn't really have anything to do with nested columns
    // just a smoke test to make sure everything else is sane
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_datasource")
                                  .intervals(Collections.singletonList(Intervals.ETERNITY))
                                  .filters(
                                      new EqualityFilter("variantWithArrays", ColumnType.STRING_ARRAY, Collections.singletonList("hello"), null)
                                  )
                                  .virtualColumns(
                                      new NestedFieldVirtualColumn(
                                          "long",
                                          "$.",
                                          "v0",
                                          ColumnType.LONG
                                      )
                                  )
                                  .aggregators(new CountAggregatorFactory("count"))
                                  .context(getContext())
                                  .build();
    runResults(
        query,
        ImmutableList.of(
            new Result<>(
                DateTimes.of("2023-01-01T00:00:00.000Z"),
                new TimeseriesResultValue(
                    ImmutableMap.of("count", 2L)
                )
            )
        )
    );
  }

  @Test
  public void testFilterVariantArrayStringArrayNoIndexes()
  {
    // this doesn't really have anything to do with nested columns
    // just a smoke test to make sure everything else is sane
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test_datasource")
                                  .intervals(Collections.singletonList(Intervals.ETERNITY))
                                  .filters(
                                      new EqualityFilter("variantWithArrays", ColumnType.STRING_ARRAY, Collections.singletonList("hello"), new FilterTuning(false, null, null))
                                  )
                                  .virtualColumns(
                                      new NestedFieldVirtualColumn(
                                          "long",
                                          "$.",
                                          "v0",
                                          ColumnType.LONG
                                      )
                                  )
                                  .aggregators(new CountAggregatorFactory("count"))
                                  .context(getContext())
                                  .build();
    runResults(
        query,
        ImmutableList.of(
            new Result<>(
                DateTimes.of("2023-01-01T00:00:00.000Z"),
                new TimeseriesResultValue(
                    ImmutableMap.of("count", 2L)
                )
            )
        )
    );
  }


  private void runResults(
      TimeseriesQuery timeseriesQuery,
      List<Result<TimeseriesResultValue>> expectedResults
  )
  {
    List<Segment> segments = segmentsGenerator.apply(tempFolder, closer);
    Supplier<List<Result<TimeseriesResultValue>>> runner =
        () -> helper.runQueryOnSegmentsObjs(segments, timeseriesQuery).toList();
    Filter filter = timeseriesQuery.getFilter() == null ? null : timeseriesQuery.getFilter().toFilter();
    boolean allCanVectorize = segments.stream()
                                      .allMatch(
                                          s -> s.asStorageAdapter()
                                                .canVectorize(
                                                    filter,
                                                    timeseriesQuery.getVirtualColumns(),
                                                    timeseriesQuery.isDescending()
                                                )
                                      );

    Assert.assertEquals(NestedDataTestUtils.expectSegmentGeneratorCanVectorize(segmentsName), allCanVectorize);
    if (!allCanVectorize) {
      if (vectorize == QueryContexts.Vectorize.FORCE) {
        Throwable t = Assert.assertThrows(RuntimeException.class, runner::get);
        Assert.assertEquals(
            "org.apache.druid.java.util.common.ISE: Cannot vectorize!",
            t.getMessage()
        );
        return;
      }
    }

    List<Result<TimeseriesResultValue>> results = runner.get();
    assertExpectedResults(
        expectedResults,
        results
    );
  }
}
