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

package org.apache.druid.query.select;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
@RunWith(Parameterized.class)
public class SelectQueryRunnerTest
{
  // copied from druid.sample.numeric.tsv
  public static final String[] V_0112 = {
      "2011-01-12T00:00:00.000Z\tspot\tautomotive\t1000\t10000.0\t100000\tpreferred\tapreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\tbusiness\t1100\t11000.0\t110000\tpreferred\tbpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\tentertainment\t1200\t12000.0\t120000\tpreferred\tepreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\thealth\t1300\t13000.0\t130000\tpreferred\thpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\tmezzanine\t1400\t14000.0\t140000\tpreferred\tmpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\tnews\t1500\t15000.0\t150000\tpreferred\tnpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\tpremium\t1600\t16000.0\t160000\tpreferred\tppreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\ttechnology\t1700\t17000.0\t170000\tpreferred\ttpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\ttravel\t1800\t18000.0\t180000\tpreferred\ttpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\ttotal_market\tmezzanine\t1400\t14000.0\t140000\tpreferred\tmpreferred\t1000.000000",
      "2011-01-12T00:00:00.000Z\ttotal_market\tpremium\t1600\t16000.0\t160000\tpreferred\tppreferred\t1000.000000",
      "2011-01-12T00:00:00.000Z\tupfront\tmezzanine\t1400\t14000.0\t140000\tpreferred\tmpreferred\t800.000000\tvalue",
      "2011-01-12T00:00:00.000Z\tupfront\tpremium\t1600\t16000.0\t160000\tpreferred\tppreferred\t800.000000\tvalue"
  };
  public static final String[] V_0113 = {
      "2011-01-13T00:00:00.000Z\tspot\tautomotive\t1000\t10000.0\t100000\tpreferred\tapreferred\t94.874713",
      "2011-01-13T00:00:00.000Z\tspot\tbusiness\t1100\t11000.0\t110000\tpreferred\tbpreferred\t103.629399",
      "2011-01-13T00:00:00.000Z\tspot\tentertainment\t1200\t12000.0\t120000\tpreferred\tepreferred\t110.087299",
      "2011-01-13T00:00:00.000Z\tspot\thealth\t1300\t13000.0\t130000\tpreferred\thpreferred\t114.947403",
      "2011-01-13T00:00:00.000Z\tspot\tmezzanine\t1400\t14000.0\t140000\tpreferred\tmpreferred\t104.465767",
      "2011-01-13T00:00:00.000Z\tspot\tnews\t1500\t15000.0\t150000\tpreferred\tnpreferred\t102.851683",
      "2011-01-13T00:00:00.000Z\tspot\tpremium\t1600\t16000.0\t160000\tpreferred\tppreferred\t108.863011",
      "2011-01-13T00:00:00.000Z\tspot\ttechnology\t1700\t17000.0\t170000\tpreferred\ttpreferred\t111.356672",
      "2011-01-13T00:00:00.000Z\tspot\ttravel\t1800\t18000.0\t180000\tpreferred\ttpreferred\t106.236928",
      "2011-01-13T00:00:00.000Z\ttotal_market\tmezzanine\t1400\t14000.0\t140000\tpreferred\tmpreferred\t1040.945505",
      "2011-01-13T00:00:00.000Z\ttotal_market\tpremium\t1600\t16000.0\t160000\tpreferred\tppreferred\t1689.012875",
      "2011-01-13T00:00:00.000Z\tupfront\tmezzanine\t1400\t14000.0\t140000\tpreferred\tmpreferred\t826.060182\tvalue",
      "2011-01-13T00:00:00.000Z\tupfront\tpremium\t1600\t16000.0\t160000\tpreferred\tppreferred\t1564.617729\tvalue"
  };

  private static final Interval I_0112_0114 = Intervals.of("2011-01-12/2011-01-14");
  public static final QuerySegmentSpec I_0112_0114_SPEC = new LegacySegmentSpec(I_0112_0114);
  private static final SegmentId SEGMENT_ID_I_0112_0114 = QueryRunnerTestHelper.SEGMENT_ID.withInterval(I_0112_0114);

  private static final String SEGMENT_ID_STRING = SEGMENT_ID_I_0112_0114.toString();

  public static final String[] V_0112_0114 = ObjectArrays.concat(V_0112, V_0113, String.class);

  private static final boolean DEFAULT_FROM_NEXT = true;
  private static final SelectQueryConfig CONFIG = new SelectQueryConfig(true);

  static {
    CONFIG.setEnableFromNextDefault(DEFAULT_FROM_NEXT);
  }

  private static final Supplier<SelectQueryConfig> CONFIG_SUPPLIER = Suppliers.ofInstance(CONFIG);

  private static final SelectQueryQueryToolChest TOOL_CHEST = new SelectQueryQueryToolChest(
      new DefaultObjectMapper(),
      QueryRunnerTestHelper.noopIntervalChunkingQueryRunnerDecorator()
  );

  @Parameterized.Parameters(name = "{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.cartesian(
        QueryRunnerTestHelper.makeQueryRunners(
            new SelectQueryRunnerFactory(
                TOOL_CHEST,
                new SelectQueryEngine(),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        ),
        Arrays.asList(false, true)
    );
  }
  private final QueryRunner runner;
  private final boolean descending;

  public SelectQueryRunnerTest(QueryRunner runner, boolean descending)
  {
    this.runner = runner;
    this.descending = descending;
  }

  private Druids.SelectQueryBuilder newTestQuery()
  {
    return Druids.newSelectQueryBuilder()
                 .dataSource(new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE))
                 .dimensionSpecs(DefaultDimensionSpec.toSpec(Collections.emptyList()))
                 .metrics(Collections.emptyList())
                 .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                 .granularity(QueryRunnerTestHelper.ALL_GRAN)
                 .pagingSpec(PagingSpec.newSpec(3))
                 .descending(descending);
  }

  @Test
  public void testFullOnSelect()
  {
    SelectQuery query = newTestQuery()
        .intervals(I_0112_0114_SPEC)
        .build();

    Iterable<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    PagingOffset offset = query.getPagingOffset(SEGMENT_ID_STRING);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        SEGMENT_ID_STRING,
        toFullEvents(V_0112_0114),
        Lists.newArrayList(
            "market",
            "quality",
            "qualityLong",
            "qualityFloat",
            "qualityDouble",
            "qualityNumericString",
            "placement",
            "placementish",
            "partial_null_column",
            "null_column"
        ),
        Lists.newArrayList(
            "index",
            "quality_uniques",
            "indexMin",
            "indexMaxPlusTen",
            "indexFloat",
            "indexMaxFloat",
            "indexMinFloat"
        ),
        offset.startOffset(),
        offset.threshold()
    );
    verify(expectedResults, populateNullColumnAtLastForQueryableIndexCase(results, "null_column"));
  }

  @Test
  public void testSequentialPaging()
  {
    int[] asc = {2, 5, 8, 11, 14, 17, 20, 23, 25};
    int[] dsc = {-3, -6, -9, -12, -15, -18, -21, -24, -26};
    int[] expected = descending ? dsc : asc;

    SelectQuery query = newTestQuery().intervals(I_0112_0114_SPEC).build();
    for (int offset : expected) {
      List<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

      Assert.assertEquals(1, results.size());

      SelectResultValue result = results.get(0).getValue();
      Map<String, Integer> pagingIdentifiers = result.getPagingIdentifiers();
      Assert.assertEquals(offset, pagingIdentifiers.get(SEGMENT_ID_I_0112_0114.toString()).intValue());

      Map<String, Integer> next = PagingSpec.next(pagingIdentifiers, descending);
      query = query.withPagingSpec(new PagingSpec(next, 3, false));
    }

    query = newTestQuery().intervals(I_0112_0114_SPEC).build();
    for (int offset : expected) {
      List<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

      Assert.assertEquals(1, results.size());

      SelectResultValue result = results.get(0).getValue();
      Map<String, Integer> pagingIdentifiers = result.getPagingIdentifiers();
      Assert.assertEquals(offset, pagingIdentifiers.get(SEGMENT_ID_I_0112_0114.toString()).intValue());

      // use identifier as-is but with fromNext=true
      query = query.withPagingSpec(new PagingSpec(pagingIdentifiers, 3, true));
    }
  }

  @Test
  public void testFullOnSelectWithDimensionSpec()
  {
    Map<String, String> map = new HashMap<>();
    map.put("automotive", "automotive0");
    map.put("business", "business0");
    map.put("entertainment", "entertainment0");
    map.put("health", "health0");
    map.put("mezzanine", "mezzanine0");
    map.put("news", "news0");
    map.put("premium", "premium0");
    map.put("technology", "technology0");
    map.put("travel", "travel0");

    SelectQuery query = newTestQuery()
        .dimensionSpecs(
            Arrays.asList(
                new DefaultDimensionSpec(QueryRunnerTestHelper.MARKET_DIMENSION, "mar"),
                new ExtractionDimensionSpec(
                    QueryRunnerTestHelper.QUALITY_DIMENSION,
                    "qual",
                    new LookupExtractionFn(new MapLookupExtractor(map, true), false, null, true, false)
                ),
                new DefaultDimensionSpec(QueryRunnerTestHelper.PLACEMENT_DIMENSION, "place")
            )
        )
        .build();

    Iterable<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    String segmentIdInThisQuery = QueryRunnerTestHelper.SEGMENT_ID.toString();

    List<Result<SelectResultValue>> expectedResultsAsc = Collections.singletonList(
        new Result<SelectResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.of(segmentIdInThisQuery, 2),
                Sets.newHashSet("mar", "qual", "place"),
                Sets.newHashSet("index", "quality_uniques", "indexMin", "indexMaxPlusTen", "indexMinFloat",
                    "indexFloat", "indexMaxFloat"
                ),
                Arrays.asList(
                    new EventHolder(
                        segmentIdInThisQuery,
                        0,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-01-12T00:00:00.000Z"))
                            .put("mar", "spot")
                            .put("qual", "automotive0")
                            .put("place", "preferred")
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 100.000000F)
                            .build()
                    ),
                    new EventHolder(
                        segmentIdInThisQuery,
                        1,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-01-12T00:00:00.000Z"))
                            .put("mar", "spot")
                            .put("qual", "business0")
                            .put("place", "preferred")
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 100.000000F)
                            .build()
                    ),
                    new EventHolder(
                        segmentIdInThisQuery,
                        2,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-01-12T00:00:00.000Z"))
                            .put("mar", "spot")
                            .put("qual", "entertainment0")
                            .put("place", "preferred")
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 100.000000F)
                            .build()
                    )
                )
            )
        )
    );

    List<Result<SelectResultValue>> expectedResultsDsc = Collections.singletonList(
        new Result<SelectResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.of(segmentIdInThisQuery, -3),
                Sets.newHashSet("mar", "qual", "place"),
                Sets.newHashSet("index", "quality_uniques", "indexMin", "indexMaxPlusTen", "indexMinFloat",
                    "indexFloat", "indexMaxFloat"
                ),
                Arrays.asList(
                    new EventHolder(
                        segmentIdInThisQuery,
                        -1,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-04-15T00:00:00.000Z"))
                            .put("mar", "upfront")
                            .put("qual", "premium0")
                            .put("place", "preferred")
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 780.27197265625F)
                            .build()
                    ),
                    new EventHolder(
                        segmentIdInThisQuery,
                        -2,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-04-15T00:00:00.000Z"))
                            .put("mar", "upfront")
                            .put("qual", "mezzanine0")
                            .put("place", "preferred")
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 962.731201171875F)
                            .build()
                    ),
                    new EventHolder(
                        segmentIdInThisQuery,
                        -3,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-04-15T00:00:00.000Z"))
                            .put("mar", "total_market")
                            .put("qual", "premium0")
                            .put("place", "preferred")
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 1029.0570068359375F)
                            .build()
                    )
                )
            )
        )
    );

    verify(descending ? expectedResultsDsc : expectedResultsAsc, results);
  }

  @Test
  public void testSelectWithDimsAndMets()
  {
    SelectQuery query = newTestQuery()
        .intervals(I_0112_0114_SPEC)
        .dimensionSpecs(DefaultDimensionSpec.toSpec(QueryRunnerTestHelper.MARKET_DIMENSION))
        .metrics(Collections.singletonList(QueryRunnerTestHelper.INDEX_METRIC))
        .build();

    Iterable<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    PagingOffset offset = query.getPagingOffset(SEGMENT_ID_STRING);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        SEGMENT_ID_STRING,
        toEvents(
            new String[]{
                EventHolder.TIMESTAMP_KEY + ":TIME",
                QueryRunnerTestHelper.MARKET_DIMENSION + ":STRING",
                null,
                null,
                null,
                null,
                null,
                null,
                QueryRunnerTestHelper.INDEX_METRIC + ":FLOAT"
            },
            V_0112_0114
        ),
        Collections.singletonList("market"),
        Collections.singletonList("index"),
        offset.startOffset(),
        offset.threshold()
    );
    verify(expectedResults, results);
  }

  @Test
  public void testSelectPagination()
  {
    SelectQuery query = newTestQuery()
        .intervals(I_0112_0114_SPEC)
        .dimensionSpecs(DefaultDimensionSpec.toSpec(QueryRunnerTestHelper.QUALITY_DIMENSION))
        .metrics(Collections.singletonList(QueryRunnerTestHelper.INDEX_METRIC))
        .pagingSpec(new PagingSpec(toPagingIdentifier(3, descending), 3))
        .build();

    Iterable<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    PagingOffset offset = query.getPagingOffset(SEGMENT_ID_STRING);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        SEGMENT_ID_STRING,
        toEvents(
            new String[]{
                EventHolder.TIMESTAMP_KEY + ":TIME",
                "foo:NULL",
                "foo2:NULL"
            },
            V_0112_0114
        ),
        Collections.singletonList("quality"),
        Collections.singletonList("index"),
        offset.startOffset(),
        offset.threshold()
    );
    verify(expectedResults, results);
  }

  @Test
  public void testFullOnSelectWithFilter()
  {
    // startDelta + threshold pairs
    for (int[] param : new int[][]{{3, 3}, {0, 1}, {5, 5}, {2, 7}, {3, 0}}) {
      SelectQuery query = newTestQuery()
          .intervals(I_0112_0114_SPEC)
          .filters(new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null))
          .granularity(QueryRunnerTestHelper.DAY_GRAN)
          .dimensionSpecs(DefaultDimensionSpec.toSpec(QueryRunnerTestHelper.QUALITY_DIMENSION))
          .metrics(Collections.singletonList(QueryRunnerTestHelper.INDEX_METRIC))
          .pagingSpec(new PagingSpec(toPagingIdentifier(param[0], descending), param[1]))
          .build();

      Iterable<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

      final List<List<Map<String, Object>>> events = toEvents(
          new String[]{
              EventHolder.TIMESTAMP_KEY + ":TIME",
              null,
              QueryRunnerTestHelper.QUALITY_DIMENSION + ":STRING",
              null,
              null,
              QueryRunnerTestHelper.INDEX_METRIC + ":FLOAT"
          },
          // filtered values with day granularity
          new String[]{
              "2011-01-12T00:00:00.000Z\tspot\tautomotive\tpreferred\tapreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\tbusiness\tpreferred\tbpreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\tentertainment\tpreferred\tepreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\thealth\tpreferred\thpreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\tmezzanine\tpreferred\tmpreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\tnews\tpreferred\tnpreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\tpremium\tpreferred\tppreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\ttechnology\tpreferred\ttpreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\ttravel\tpreferred\ttpreferred\t100.000000"
          },
          new String[]{
              "2011-01-13T00:00:00.000Z\tspot\tautomotive\tpreferred\tapreferred\t94.874713",
              "2011-01-13T00:00:00.000Z\tspot\tbusiness\tpreferred\tbpreferred\t103.629399",
              "2011-01-13T00:00:00.000Z\tspot\tentertainment\tpreferred\tepreferred\t110.087299",
              "2011-01-13T00:00:00.000Z\tspot\thealth\tpreferred\thpreferred\t114.947403",
              "2011-01-13T00:00:00.000Z\tspot\tmezzanine\tpreferred\tmpreferred\t104.465767",
              "2011-01-13T00:00:00.000Z\tspot\tnews\tpreferred\tnpreferred\t102.851683",
              "2011-01-13T00:00:00.000Z\tspot\tpremium\tpreferred\tppreferred\t108.863011",
              "2011-01-13T00:00:00.000Z\tspot\ttechnology\tpreferred\ttpreferred\t111.356672",
              "2011-01-13T00:00:00.000Z\tspot\ttravel\tpreferred\ttpreferred\t106.236928"
          }
      );

      PagingOffset offset = query.getPagingOffset(SEGMENT_ID_STRING);
      List<Result<SelectResultValue>> expectedResults = toExpected(
          SEGMENT_ID_STRING,
          events,
          Collections.singletonList("quality"),
          Collections.singletonList("index"),
          offset.startOffset(),
          offset.threshold()
      );
      verify(expectedResults, results);
    }
  }

  @Test
  public void testFullOnSelectWithFilterOnVirtualColumn()
  {
    Interval interval = Intervals.of("2011-01-13/2011-01-14");
    SelectQuery query = newTestQuery()
        .intervals(new LegacySegmentSpec(interval))
        .filters(
            new AndDimFilter(
                Arrays.asList(
                    new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null),
                    new BoundDimFilter("expr", "11.1", null, false, false, null, null, StringComparators.NUMERIC)
                )
            )
        )
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimensionSpecs(DefaultDimensionSpec.toSpec(QueryRunnerTestHelper.QUALITY_DIMENSION))
        .metrics(Collections.singletonList(QueryRunnerTestHelper.INDEX_METRIC))
        .pagingSpec(new PagingSpec(null, 10, true))
        .virtualColumns(
            new ExpressionVirtualColumn("expr", "index / 10.0", ValueType.FLOAT, TestExprMacroTable.INSTANCE)
        )
        .build();

    Iterable<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    final List<List<Map<String, Object>>> events = toEvents(
        new String[]{
            EventHolder.TIMESTAMP_KEY + ":TIME",
            null,
            QueryRunnerTestHelper.QUALITY_DIMENSION + ":STRING",
            null,
            null,
            QueryRunnerTestHelper.INDEX_METRIC + ":FLOAT"
        },
        // filtered values with all granularity
        new String[]{
            "2011-01-13T00:00:00.000Z\tspot\thealth\tpreferred\thpreferred\t114.947403",
            "2011-01-13T00:00:00.000Z\tspot\ttechnology\tpreferred\ttpreferred\t111.356672"
        }
    );

    String segmentIdInThisQuery = QueryRunnerTestHelper.SEGMENT_ID.withInterval(interval).toString();
    PagingOffset offset = query.getPagingOffset(segmentIdInThisQuery);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        segmentIdInThisQuery,
        events,
        Collections.singletonList("quality"),
        Collections.singletonList("index"),
        offset.startOffset(),
        offset.threshold()
    );
    verify(expectedResults, results);
  }

  @Test
  public void testSelectWithFilterLookupExtractionFn()
  {

    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("total_market", "replaced");
    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, true);
    SelectQuery query = newTestQuery()
        .intervals(I_0112_0114_SPEC)
        .filters(new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "replaced", lookupExtractionFn))
        .granularity(QueryRunnerTestHelper.DAY_GRAN)
        .dimensionSpecs(DefaultDimensionSpec.toSpec(QueryRunnerTestHelper.QUALITY_DIMENSION))
        .metrics(Collections.singletonList(QueryRunnerTestHelper.INDEX_METRIC))
        .build();

    Iterable<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    Iterable<Result<SelectResultValue>> resultsOptimize = TOOL_CHEST
        .postMergeQueryDecoration(TOOL_CHEST.mergeResults(TOOL_CHEST.preMergeQueryDecoration(runner)))
        .run(QueryPlus.wrap(query))
        .toList();

    final List<List<Map<String, Object>>> events = toEvents(
        new String[]{
            EventHolder.TIMESTAMP_KEY + ":TIME",
            null,
            QueryRunnerTestHelper.QUALITY_DIMENSION + ":STRING",
            null,
            null,
            QueryRunnerTestHelper.INDEX_METRIC + ":FLOAT"
        },
        // filtered values with day granularity
        new String[]{
            "2011-01-12T00:00:00.000Z\ttotal_market\tmezzanine\tpreferred\tmpreferred\t1000.000000",
            "2011-01-12T00:00:00.000Z\ttotal_market\tpremium\tpreferred\tppreferred\t1000.000000"
        },
        new String[]{
            "2011-01-13T00:00:00.000Z\ttotal_market\tmezzanine\tpreferred\tmpreferred\t1040.945505",
            "2011-01-13T00:00:00.000Z\ttotal_market\tpremium\tpreferred\tppreferred\t1689.012875"
        }
    );

    PagingOffset offset = query.getPagingOffset(SEGMENT_ID_STRING);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        SEGMENT_ID_STRING,
        events,
        Collections.singletonList(QueryRunnerTestHelper.QUALITY_DIMENSION),
        Collections.singletonList(QueryRunnerTestHelper.INDEX_METRIC),
        offset.startOffset(),
        offset.threshold()
    );

    verify(expectedResults, results);
    verify(expectedResults, resultsOptimize);
  }

  @Test
  public void testFullSelectNoResults()
  {
    SelectQuery query = newTestQuery()
        .intervals(I_0112_0114_SPEC)
        .filters(
            new AndDimFilter(
                Arrays.asList(
                    new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null),
                    new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "foo", null)
                )
            )
        )
        .build();

    Iterable<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    List<Result<SelectResultValue>> expectedResults = Collections.singletonList(
        new Result<SelectResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.of(),
                Sets.newHashSet(
                    "market",
                    "quality",
                    "qualityLong",
                    "qualityFloat",
                    "qualityDouble",
                    "qualityNumericString",
                    "placement",
                    "placementish",
                    "partial_null_column",
                    "null_column"
                ),
                Sets.newHashSet(
                    "index",
                    "quality_uniques",
                    "indexMin",
                    "indexMaxPlusTen",
                    "indexMinFloat",
                    "indexFloat",
                    "indexMaxFloat"
                ),
                new ArrayList<>()
            )
        )
    );

    verify(expectedResults, populateNullColumnAtLastForQueryableIndexCase(results, "null_column"));
  }

  @Test
  public void testFullSelectNoDimensionAndMetric()
  {
    SelectQuery query = newTestQuery()
        .intervals(I_0112_0114_SPEC)
        .dimensionSpecs(DefaultDimensionSpec.toSpec("foo"))
        .metrics(Collections.singletonList("foo2"))
        .build();

    Iterable<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    final List<List<Map<String, Object>>> events = toEvents(
        new String[]{
            EventHolder.TIMESTAMP_KEY + ":TIME",
            "foo:NULL",
            "foo2:NULL"
        },
        V_0112_0114
    );

    PagingOffset offset = query.getPagingOffset(SEGMENT_ID_STRING);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        SEGMENT_ID_STRING,
        events,
        Collections.singletonList("foo"),
        Collections.singletonList("foo2"),
        offset.startOffset(),
        offset.threshold()
    );
    verify(expectedResults, results);
  }

  @Test
  public void testFullOnSelectWithLongAndFloat()
  {
    List<DimensionSpec> dimSpecs = Arrays.asList(
        new DefaultDimensionSpec(QueryRunnerTestHelper.INDEX_METRIC, "floatIndex", ValueType.FLOAT),
        new DefaultDimensionSpec(ColumnHolder.TIME_COLUMN_NAME, "longTime", ValueType.LONG)
    );

    SelectQuery query = newTestQuery()
        .dimensionSpecs(dimSpecs)
        .metrics(Arrays.asList(ColumnHolder.TIME_COLUMN_NAME, "index"))
        .intervals(I_0112_0114_SPEC)
        .build();

    Iterable<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    List<Result<SelectResultValue>> expectedResultsAsc = Collections.singletonList(
        new Result<SelectResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.of(SEGMENT_ID_STRING, 2),
                Sets.newHashSet("null_column", "floatIndex", "longTime"),
                Sets.newHashSet("__time", "index"),
                Arrays.asList(
                    new EventHolder(
                        SEGMENT_ID_STRING,
                        0,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-01-12T00:00:00.000Z"))
                            .put("longTime", 1294790400000L)
                            .put("floatIndex", 100.0f)
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 100.000000F)
                            .put(ColumnHolder.TIME_COLUMN_NAME, 1294790400000L)
                            .build()
                    ),
                    new EventHolder(
                        SEGMENT_ID_STRING,
                        1,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-01-12T00:00:00.000Z"))
                            .put("longTime", 1294790400000L)
                            .put("floatIndex", 100.0f)
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 100.000000F)
                            .put(ColumnHolder.TIME_COLUMN_NAME, 1294790400000L)
                            .build()
                    ),
                    new EventHolder(
                        SEGMENT_ID_STRING,
                        2,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-01-12T00:00:00.000Z"))
                            .put("longTime", 1294790400000L)
                            .put("floatIndex", 100.0f)
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 100.000000F)
                            .put(ColumnHolder.TIME_COLUMN_NAME, 1294790400000L)
                            .build()
                    )
                )
            )
        )
    );

    List<Result<SelectResultValue>> expectedResultsDsc = Collections.singletonList(
        new Result<SelectResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.of(SEGMENT_ID_STRING, -3),
                Sets.newHashSet("null_column", "floatIndex", "longTime"),
                Sets.newHashSet("__time", "index"),
                Arrays.asList(
                    new EventHolder(
                        SEGMENT_ID_STRING,
                        -1,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-01-13T00:00:00.000Z"))
                            .put("longTime", 1294876800000L)
                            .put("floatIndex", 1564.6177f)
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 1564.6177f)
                            .put(ColumnHolder.TIME_COLUMN_NAME, 1294876800000L)
                            .build()
                    ),
                    new EventHolder(
                        SEGMENT_ID_STRING,
                        -2,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-01-13T00:00:00.000Z"))
                            .put("longTime", 1294876800000L)
                            .put("floatIndex", 826.0602f)
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 826.0602f)
                            .put(ColumnHolder.TIME_COLUMN_NAME, 1294876800000L)
                            .build()
                    ),
                    new EventHolder(
                        SEGMENT_ID_STRING,
                        -3,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-01-13T00:00:00.000Z"))
                            .put("longTime", 1294876800000L)
                            .put("floatIndex", 1689.0128f)
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 1689.0128f)
                            .put(ColumnHolder.TIME_COLUMN_NAME, 1294876800000L)
                            .build()
                    )
                )
            )
        )
    );

    verify(descending ? expectedResultsDsc : expectedResultsAsc, populateNullColumnAtLastForQueryableIndexCase(results, "null_column"));
  }

  @Test
  public void testFullOnSelectWithLongAndFloatWithExFn()
  {
    String jsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn jsExtractionFn = new JavaScriptExtractionFn(jsFn, false, JavaScriptConfig.getEnabledInstance());

    List<DimensionSpec> dimSpecs = Arrays.asList(
        new ExtractionDimensionSpec(QueryRunnerTestHelper.INDEX_METRIC, "floatIndex", jsExtractionFn),
        new ExtractionDimensionSpec(ColumnHolder.TIME_COLUMN_NAME, "longTime", jsExtractionFn)
    );

    SelectQuery query = newTestQuery()
        .dimensionSpecs(dimSpecs)
        .metrics(Arrays.asList(ColumnHolder.TIME_COLUMN_NAME, "index"))
        .intervals(I_0112_0114_SPEC)
        .build();

    Iterable<Result<SelectResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    List<Result<SelectResultValue>> expectedResultsAsc = Collections.singletonList(
        new Result<SelectResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.of(SEGMENT_ID_STRING, 2),
                Sets.newHashSet("null_column", "floatIndex", "longTime"),
                Sets.newHashSet("__time", "index"),
                Arrays.asList(
                    new EventHolder(
                        SEGMENT_ID_STRING,
                        0,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-01-12T00:00:00.000Z"))
                            .put("longTime", "super-1294790400000")
                            .put("floatIndex", "super-100")
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 100.000000F)
                            .put(ColumnHolder.TIME_COLUMN_NAME, 1294790400000L)
                            .build()
                    ),
                    new EventHolder(
                        SEGMENT_ID_STRING,
                        1,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-01-12T00:00:00.000Z"))
                            .put("longTime", "super-1294790400000")
                            .put("floatIndex", "super-100")
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 100.000000F)
                            .put(ColumnHolder.TIME_COLUMN_NAME, 1294790400000L)
                            .build()
                    ),
                    new EventHolder(
                        SEGMENT_ID_STRING,
                        2,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-01-12T00:00:00.000Z"))
                            .put("longTime", "super-1294790400000")
                            .put("floatIndex", "super-100")
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 100.000000F)
                            .put(ColumnHolder.TIME_COLUMN_NAME, 1294790400000L)
                            .build()
                    )
                )
            )
        )
    );

    List<Result<SelectResultValue>> expectedResultsDsc = Collections.singletonList(
        new Result<SelectResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.of(SEGMENT_ID_STRING, -3),
                Sets.newHashSet("null_column", "floatIndex", "longTime"),
                Sets.newHashSet("__time", "index"),
                Arrays.asList(
                    new EventHolder(
                        SEGMENT_ID_STRING,
                        -1,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-01-13T00:00:00.000Z"))
                            .put("longTime", "super-1294876800000")
                            .put("floatIndex", "super-1564.617729")
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 1564.6177f)
                            .put(ColumnHolder.TIME_COLUMN_NAME, 1294876800000L)
                            .build()
                    ),
                    new EventHolder(
                        SEGMENT_ID_STRING,
                        -2,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-01-13T00:00:00.000Z"))
                            .put("longTime", "super-1294876800000")
                            .put("floatIndex", "super-826.060182")
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 826.0602f)
                            .put(ColumnHolder.TIME_COLUMN_NAME, 1294876800000L)
                            .build()
                    ),
                    new EventHolder(
                        SEGMENT_ID_STRING,
                        -3,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.TIMESTAMP_KEY, DateTimes.of("2011-01-13T00:00:00.000Z"))
                            .put("longTime", "super-1294876800000")
                            .put("floatIndex", "super-1689.012875")
                            .put(QueryRunnerTestHelper.INDEX_METRIC, 1689.0128f)
                            .put(ColumnHolder.TIME_COLUMN_NAME, 1294876800000L)
                            .build()
                    )
                )
            )
        )
    );

    verify(descending ? expectedResultsDsc : expectedResultsAsc, populateNullColumnAtLastForQueryableIndexCase(results, "null_column"));
  }

  private Map<String, Integer> toPagingIdentifier(int startDelta, boolean descending)
  {
    return ImmutableMap.of(SEGMENT_ID_STRING, PagingOffset.toOffset(startDelta, descending));
  }

  private List<List<Map<String, Object>>> toFullEvents(final String[]... valueSet)
  {
    return toEvents(new String[]{EventHolder.TIMESTAMP_KEY + ":TIME",
                                 QueryRunnerTestHelper.MARKET_DIMENSION + ":STRING",
                                 QueryRunnerTestHelper.QUALITY_DIMENSION + ":STRING",
                                 "qualityLong" + ":LONG",
                                 "qualityFloat" + ":FLOAT",
                                 "qualityNumericString" + ":STRING",
                                 QueryRunnerTestHelper.PLACEMENT_DIMENSION + ":STRING",
                                 QueryRunnerTestHelper.PLACEMENTISH_DIMENSION + ":STRINGS",
                                 QueryRunnerTestHelper.INDEX_METRIC + ":FLOAT",
                                 QueryRunnerTestHelper.PARTIAL_NULL_DIMENSION + ":STRING"},
                    valueSet);
  }

  private List<List<Map<String, Object>>> toEvents(final String[] dimSpecs, final String[]... valueSet)
  {
    List<List<Map<String, Object>>> events = new ArrayList<>();
    for (String[] values : valueSet) {
      events.add(
          Lists.newArrayList(
              Iterables.transform(
                  Arrays.asList(values), new Function<String, Map<String, Object>>()
                  {
                    @Override
                    public Map<String, Object> apply(String input)
                    {
                      Map<String, Object> event = new HashMap<>();
                      String[] values = input.split("\\t");
                      for (int i = 0; i < dimSpecs.length; i++) {
                        if (dimSpecs[i] == null || i >= dimSpecs.length || i >= values.length) {
                          continue;
                        }
                        String[] specs = dimSpecs[i].split(":");
                        event.put(
                            specs[0],
                            specs.length == 1 || specs[1].equals("STRING") ? values[i] :
                            specs[1].equals("TIME") ? DateTimes.of(values[i]) :
                            specs[1].equals("FLOAT") ? Float.valueOf(values[i]) :
                            specs[1].equals("DOUBLE") ? Double.valueOf(values[i]) :
                            specs[1].equals("LONG") ? Long.valueOf(values[i]) :
                            specs[1].equals("NULL") ? null :
                            specs[1].equals("STRINGS") ? Arrays.asList(values[i].split("\u0001")) :
                            values[i]
                        );
                      }
                      return event;
                    }
                  }
              )
          )
      );
    }
    return events;
  }

  private List<Result<SelectResultValue>> toExpected(
      String segmentId,
      List<List<Map<String, Object>>> targets,
      List<String> dimensions,
      List<String> metrics,
      final int offset,
      final int threshold
  )
  {
    if (offset < 0) {
      targets = Lists.reverse(targets);
    }
    List<Result<SelectResultValue>> expected = Lists.newArrayListWithExpectedSize(targets.size());
    for (List<Map<String, Object>> group : targets) {
      List<EventHolder> holders = Lists.newArrayListWithExpectedSize(threshold);
      int newOffset = offset;
      if (offset < 0) {
        int start = group.size() + offset;
        int end = Math.max(-1, start - threshold);
        for (int i = start; i > end; i--) {
          holders.add(new EventHolder(segmentId, newOffset--, group.get(i)));
        }
      } else {
        int end = Math.min(group.size(), offset + threshold);
        for (int i = offset; i < end; i++) {
          holders.add(new EventHolder(segmentId, newOffset++, group.get(i)));
        }
      }
      int lastOffset = holders.isEmpty() ? offset : holders.get(holders.size() - 1).getOffset();
      expected.add(
          new Result(
              new DateTime(group.get(0).get(EventHolder.TIMESTAMP_KEY), ISOChronology.getInstanceUTC()),
              new SelectResultValue(
                  ImmutableMap.of(segmentId, lastOffset),
                  Sets.newHashSet(dimensions),
                  Sets.newHashSet(metrics),
                  holders)
          )
      );
    }
    return expected;
  }

  private static void verify(
      Iterable<Result<SelectResultValue>> expectedResults,
      Iterable<Result<SelectResultValue>> actualResults
  )
  {
    Iterator<Result<SelectResultValue>> expectedIter = expectedResults.iterator();
    Iterator<Result<SelectResultValue>> actualIter = actualResults.iterator();

    while (expectedIter.hasNext()) {
      Result<SelectResultValue> expected = expectedIter.next();
      Result<SelectResultValue> actual = actualIter.next();

      Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());

      for (Map.Entry<String, Integer> entry : expected.getValue().getPagingIdentifiers().entrySet()) {
        Assert.assertEquals(entry.getValue(), actual.getValue().getPagingIdentifiers().get(entry.getKey()));
      }

      Assert.assertEquals(expected.getValue().getDimensions(), actual.getValue().getDimensions());
      Assert.assertEquals(expected.getValue().getMetrics(), actual.getValue().getMetrics());

      Iterator<EventHolder> expectedEvts = expected.getValue().getEvents().iterator();
      Iterator<EventHolder> actualEvts = actual.getValue().getEvents().iterator();

      while (expectedEvts.hasNext()) {
        EventHolder exHolder = expectedEvts.next();
        EventHolder acHolder = actualEvts.next();

        Assert.assertEquals(exHolder.getTimestamp(), acHolder.getTimestamp());
        Assert.assertEquals(exHolder.getOffset(), acHolder.getOffset());

        for (Map.Entry<String, Object> ex : exHolder.getEvent().entrySet()) {
          Object actVal = acHolder.getEvent().get(ex.getKey());

          // work around for current II limitations
          if (acHolder.getEvent().get(ex.getKey()) instanceof Double) {
            actVal = ((Double) actVal).floatValue();
          }
          Assert.assertEquals("invalid value for " + ex.getKey(), ex.getValue(), actVal);
        }
      }

      if (actualEvts.hasNext()) {
        throw new ISE("This event iterator should be exhausted!");
      }
    }

    if (actualIter.hasNext()) {
      throw new ISE("This iterator should be exhausted!");
    }
  }

  private static Iterable<Result<SelectResultValue>> populateNullColumnAtLastForQueryableIndexCase(Iterable<Result<SelectResultValue>> results, String columnName)
  {
    // A Queryable index does not have the null column when it has loaded a index.
    for (Result<SelectResultValue> value : results) {
      Set<String> dimensions = value.getValue().getDimensions();
      if (dimensions.contains(columnName)) {
        break;
      }
      dimensions.add(columnName);
    }

    return results;
  }

}
