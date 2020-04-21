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

package org.apache.druid.query.scan;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.io.CharSource;
import com.google.common.io.LineProcessor;
import org.apache.commons.lang.ArrayUtils;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
@RunWith(Parameterized.class)
public class ScanQueryRunnerTest extends InitializedNullHandlingTest
{
  private static final VirtualColumn EXPR_COLUMN =
      new ExpressionVirtualColumn("expr", "index * 2", ValueType.LONG, TestExprMacroTable.INSTANCE);

  // Read the first set of 12 lines from the sample data, which covers the day 2011-01-12T00:00:00.000Z
  public static final String[] V_0112 = readLinesFromSample(0, 13).toArray(new String[0]);

  // Read the second set of 12 lines from the sample data, which covers the day 2011-01-13T00:00:00.000Z
  public static final String[] V_0113 = readLinesFromSample(13, 26).toArray(new String[0]);

  private static List<String> readLinesFromSample(
      int startLineNum,
      int endLineNum
  )
  {
    CharSource sampleData = TestIndex.getResourceCharSource("druid.sample.numeric.tsv");
    List<String> lines = new ArrayList<>();
    try {
      sampleData.readLines(
          new LineProcessor<Object>()
          {
            int count = 0;

            @Override
            public boolean processLine(String line)
            {
              if (count >= startLineNum && count < endLineNum) {
                lines.add(line);
              }
              count++;
              return count < endLineNum;
            }

            @Override
            public Object getResult()
            {
              return null;
            }
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    return lines;
  }


  public static final QuerySegmentSpec I_0112_0114 = new LegacySegmentSpec(
      Intervals.of("2011-01-12T00:00:00.000Z/2011-01-14T00:00:00.000Z")
  );
  public static final String[] V_0112_0114 = ObjectArrays.concat(V_0112, V_0113, String.class);

  private static final ScanQueryQueryToolChest TOOL_CHEST = new ScanQueryQueryToolChest(
      new ScanQueryConfig(),
      DefaultGenericQueryMetricsFactory.instance()
  );

  @Parameterized.Parameters(name = "{0}, legacy = {1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.cartesian(
        QueryRunnerTestHelper.makeQueryRunners(
            new ScanQueryRunnerFactory(
                TOOL_CHEST,
                new ScanQueryEngine(),
                new ScanQueryConfig()
            )
        ),
        ImmutableList.of(false, true)
    );
  }

  private final QueryRunner runner;
  private final boolean legacy;

  public ScanQueryRunnerTest(final QueryRunner runner, final boolean legacy)
  {
    this.runner = runner;
    this.legacy = legacy;
  }

  private Druids.ScanQueryBuilder newTestQuery()
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE))
                 .columns(Collections.emptyList())
                 .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                 .limit(3)
                 .legacy(legacy);
  }

  @Test
  public void testFullOnSelect()
  {
    List<String> columns = Lists.newArrayList(
        getTimestampName(),
        "expr",
        "market",
        "quality",
        "qualityLong",
        "qualityFloat",
        "qualityDouble",
        "qualityNumericString",
        "longNumericNull",
        "floatNumericNull",
        "doubleNumericNull",
        "placement",
        "placementish",
        "partial_null_column",
        "null_column",
        "index",
        "indexMin",
        "indexMaxPlusTen",
        "quality_uniques",
        "indexFloat",
        "indexMaxFloat",
        "indexMinFloat"
    );
    ScanQuery query = newTestQuery()
        .intervals(I_0112_0114)
        .virtualColumns(EXPR_COLUMN)
        .build();

    Iterable<ScanResultValue> results = runner.run(QueryPlus.wrap(query)).toList();

    List<ScanResultValue> expectedResults = toExpected(
        toFullEvents(V_0112_0114),
        columns,
        0,
        3
    );
    verify(expectedResults, populateNullColumnAtLastForQueryableIndexCase(results, "null_column"));
  }

  @Test
  public void testFullOnSelectAsCompactedList()
  {
    final List<String> columns = Lists.newArrayList(
        getTimestampName(),
        "expr",
        "market",
        "quality",
        "qualityLong",
        "qualityFloat",
        "qualityDouble",
        "qualityNumericString",
        "longNumericNull",
        "floatNumericNull",
        "doubleNumericNull",
        "placement",
        "placementish",
        "partial_null_column",
        "null_column",
        "index",
        "indexMin",
        "indexMaxPlusTen",
        "quality_uniques",
        "indexFloat",
        "indexMaxFloat",
        "indexMinFloat"
    );
    ScanQuery query = newTestQuery()
        .intervals(I_0112_0114)
        .virtualColumns(EXPR_COLUMN)
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .build();

    Iterable<ScanResultValue> results = runner.run(QueryPlus.wrap(query)).toList();

    List<ScanResultValue> expectedResults = toExpected(
        toFullEvents(V_0112_0114),
        columns,
        0,
        3
    );
    verify(expectedResults, populateNullColumnAtLastForQueryableIndexCase(compactedListToRow(results), "null_column"));
  }

  @Test
  public void testSelectWithUnderscoreUnderscoreTime()
  {
    ScanQuery query = newTestQuery()
        .intervals(I_0112_0114)
        .columns(
            ColumnHolder.TIME_COLUMN_NAME,
            QueryRunnerTestHelper.MARKET_DIMENSION,
            QueryRunnerTestHelper.INDEX_METRIC
        )
        .build();

    Iterable<ScanResultValue> results = runner.run(QueryPlus.wrap(query)).toList();

    final List<List<Map<String, Object>>> expectedEvents = toEvents(
        new String[]{
            getTimestampName() + ":TIME",
            QueryRunnerTestHelper.MARKET_DIMENSION + ":STRING",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            QueryRunnerTestHelper.INDEX_METRIC + ":DOUBLE"
        },
        V_0112_0114
    );

    // Add "__time" to all the expected events in legacy mode
    if (legacy) {
      for (List<Map<String, Object>> batch : expectedEvents) {
        for (Map<String, Object> event : batch) {
          event.put("__time", ((DateTime) event.get("timestamp")).getMillis());
        }
      }
    }

    List<ScanResultValue> expectedResults = toExpected(
        expectedEvents,
        legacy
        ? Lists.newArrayList(getTimestampName(), "__time", "market", "index")
        : Lists.newArrayList("__time", "market", "index"),
        0,
        3
    );
    verify(expectedResults, results);
  }

  @Test
  public void testSelectWithDimsAndMets()
  {
    ScanQuery query = newTestQuery()
        .intervals(I_0112_0114)
        .columns(QueryRunnerTestHelper.MARKET_DIMENSION, QueryRunnerTestHelper.INDEX_METRIC)
        .build();

    Iterable<ScanResultValue> results = runner.run(QueryPlus.wrap(query)).toList();

    List<ScanResultValue> expectedResults = toExpected(
        toEvents(
            new String[]{
                legacy ? getTimestampName() + ":TIME" : null,
                QueryRunnerTestHelper.MARKET_DIMENSION + ":STRING",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                QueryRunnerTestHelper.INDEX_METRIC + ":DOUBLE"
            },
            V_0112_0114
        ),
        legacy ? Lists.newArrayList(getTimestampName(), "market", "index") : Lists.newArrayList("market", "index"),
        0,
        3
    );
    verify(expectedResults, results);
  }

  @Test
  public void testSelectWithDimsAndMetsAsCompactedList()
  {
    ScanQuery query = newTestQuery()
        .intervals(I_0112_0114)
        .columns(QueryRunnerTestHelper.MARKET_DIMENSION, QueryRunnerTestHelper.INDEX_METRIC)
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .build();

    Iterable<ScanResultValue> results = runner.run(QueryPlus.wrap(query)).toList();

    List<ScanResultValue> expectedResults = toExpected(
        toEvents(
            new String[]{
                legacy ? getTimestampName() + ":TIME" : null,
                QueryRunnerTestHelper.MARKET_DIMENSION + ":STRING",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                QueryRunnerTestHelper.INDEX_METRIC + ":DOUBLE"
            },
            V_0112_0114
        ),
        legacy ? Lists.newArrayList(getTimestampName(), "market", "index") : Lists.newArrayList("market", "index"),
        0,
        3
    );
    verify(expectedResults, compactedListToRow(results));
  }

  @Test
  public void testFullOnSelectWithFilterAndLimit()
  {
    // limits
    for (int limit : new int[]{3, 1, 5, 7, 0}) {
      ScanQuery query = newTestQuery()
          .intervals(I_0112_0114)
          .filters(new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null))
          .columns(QueryRunnerTestHelper.QUALITY_DIMENSION, QueryRunnerTestHelper.INDEX_METRIC)
          .limit(limit)
          .build();

      Iterable<ScanResultValue> results = runner.run(QueryPlus.wrap(query)).toList();

      final List<List<Map<String, Object>>> events = toEvents(
          new String[]{
              legacy ? getTimestampName() + ":TIME" : null,
              null,
              QueryRunnerTestHelper.QUALITY_DIMENSION + ":STRING",
              null,
              null,
              QueryRunnerTestHelper.INDEX_METRIC + ":DOUBLE"
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

      List<ScanResultValue> expectedResults = toExpected(
          events,
          legacy ? Lists.newArrayList(getTimestampName(), "quality", "index") : Lists.newArrayList("quality", "index"),
          0,
          limit
      );
      verify(expectedResults, results);
    }
  }

  @Test
  public void testSelectWithFilterLookupExtractionFn()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("total_market", "replaced");
    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, true);
    ScanQuery query = newTestQuery()
        .intervals(I_0112_0114)
        .filters(new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "replaced", lookupExtractionFn))
        .columns(QueryRunnerTestHelper.QUALITY_DIMENSION, QueryRunnerTestHelper.INDEX_METRIC)
        .build();

    Iterable<ScanResultValue> results = runner.run(QueryPlus.wrap(query)).toList();
    Iterable<ScanResultValue> resultsOptimize = TOOL_CHEST
        .postMergeQueryDecoration(TOOL_CHEST.mergeResults(TOOL_CHEST.preMergeQueryDecoration(runner)))
        .run(QueryPlus.wrap(query))
        .toList();

    final List<List<Map<String, Object>>> events = toEvents(
        new String[]{
            legacy ? getTimestampName() + ":TIME" : null,
            null,
            QueryRunnerTestHelper.QUALITY_DIMENSION + ":STRING",
            null,
            null,
            QueryRunnerTestHelper.INDEX_METRIC + ":DOUBLE"
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

    List<ScanResultValue> expectedResults = toExpected(
        events,
        legacy ? Lists.newArrayList(
            getTimestampName(),
            QueryRunnerTestHelper.QUALITY_DIMENSION,
            QueryRunnerTestHelper.INDEX_METRIC
        ) : Lists.newArrayList(
            QueryRunnerTestHelper.QUALITY_DIMENSION,
            QueryRunnerTestHelper.INDEX_METRIC
        ),
        0,
        3
    );

    verify(expectedResults, results);
    verify(expectedResults, resultsOptimize);
  }

  @Test
  public void testFullSelectNoResults()
  {
    ScanQuery query = newTestQuery()
        .intervals(I_0112_0114)
        .filters(
            new AndDimFilter(
                Arrays.asList(
                    new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null),
                    new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "foo", null)
                )
            )
        )
        .build();

    Iterable<ScanResultValue> results = runner.run(QueryPlus.wrap(query)).toList();

    List<ScanResultValue> expectedResults = Collections.emptyList();

    verify(expectedResults, populateNullColumnAtLastForQueryableIndexCase(results, "null_column"));
  }

  @Test
  public void testFullSelectNoDimensionAndMetric()
  {
    ScanQuery query = newTestQuery()
        .intervals(I_0112_0114)
        .columns("foo", "foo2")
        .build();

    Iterable<ScanResultValue> results = runner.run(QueryPlus.wrap(query)).toList();

    final List<List<Map<String, Object>>> events = toEvents(
        legacy ? new String[]{getTimestampName() + ":TIME"} : new String[0],
        V_0112_0114
    );

    List<ScanResultValue> expectedResults = toExpected(
        events,
        legacy ? Lists.newArrayList(getTimestampName(), "foo", "foo2") : Lists.newArrayList("foo", "foo2"),
        0,
        3
    );

    verify(expectedResults, results);
  }

  @Test
  public void testFullOnSelectWithFilterLimitAndAscendingTimeOrderingListFormat()
  {
    // limits shouldn't matter -> all rows should be returned if time-ordering on the broker is occurring
    for (int limit : new int[]{3, 1, 5, 7, 0}) {
      ScanQuery query = newTestQuery()
          .intervals(I_0112_0114)
          .filters(new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null))
          .columns(
              QueryRunnerTestHelper.TIME_DIMENSION,
              QueryRunnerTestHelper.QUALITY_DIMENSION,
              QueryRunnerTestHelper.INDEX_METRIC
          )
          .limit(limit)
          .order(ScanQuery.Order.ASCENDING)
          .context(ImmutableMap.of(ScanQuery.CTX_KEY_OUTERMOST, false))
          .build();

      Iterable<ScanResultValue> results = runner.run(QueryPlus.wrap(query)).toList();
      String[] seg1Results = new String[]{
          "2011-01-12T00:00:00.000Z\tspot\tautomotive\tpreferred\tapreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\tbusiness\tpreferred\tbpreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\tentertainment\tpreferred\tepreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\thealth\tpreferred\thpreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\tmezzanine\tpreferred\tmpreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\tnews\tpreferred\tnpreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\tpremium\tpreferred\tppreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\ttechnology\tpreferred\ttpreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\ttravel\tpreferred\ttpreferred\t100.000000"
      };
      String[] seg2Results = new String[]{
          "2011-01-13T00:00:00.000Z\tspot\tautomotive\tpreferred\tapreferred\t94.874713",
          "2011-01-13T00:00:00.000Z\tspot\tbusiness\tpreferred\tbpreferred\t103.629399",
          "2011-01-13T00:00:00.000Z\tspot\tentertainment\tpreferred\tepreferred\t110.087299",
          "2011-01-13T00:00:00.000Z\tspot\thealth\tpreferred\thpreferred\t114.947403",
          "2011-01-13T00:00:00.000Z\tspot\tmezzanine\tpreferred\tmpreferred\t104.465767",
          "2011-01-13T00:00:00.000Z\tspot\tnews\tpreferred\tnpreferred\t102.851683",
          "2011-01-13T00:00:00.000Z\tspot\tpremium\tpreferred\tppreferred\t108.863011",
          "2011-01-13T00:00:00.000Z\tspot\ttechnology\tpreferred\ttpreferred\t111.356672",
          "2011-01-13T00:00:00.000Z\tspot\ttravel\tpreferred\ttpreferred\t106.236928"
      };
      final List<List<Map<String, Object>>> ascendingEvents = toEvents(
          new String[]{
              legacy ? getTimestampName() + ":TIME" : ColumnHolder.TIME_COLUMN_NAME,
              null,
              QueryRunnerTestHelper.QUALITY_DIMENSION + ":STRING",
              null,
              null,
              QueryRunnerTestHelper.INDEX_METRIC + ":DOUBLE"
          },
          (String[]) ArrayUtils.addAll(seg1Results, seg2Results)
      );

      if (legacy) {
        for (List<Map<String, Object>> batch : ascendingEvents) {
          for (Map<String, Object> event : batch) {
            event.put("__time", ((DateTime) event.get("timestamp")).getMillis());
          }
        }
      } else {
        for (List<Map<String, Object>> batch : ascendingEvents) {
          for (Map<String, Object> event : batch) {
            event.put("__time", (DateTimes.of((String) event.get("__time"))).getMillis());
          }
        }
      }

      List<ScanResultValue> ascendingExpectedResults = toExpected(
          ascendingEvents,
          legacy ?
          Lists.newArrayList(
              QueryRunnerTestHelper.TIME_DIMENSION,
              getTimestampName(),
              "quality",
              "index"
          ) :
          Lists.newArrayList(
              QueryRunnerTestHelper.TIME_DIMENSION,
              "quality",
              "index"
          ),
          0,
          limit
      );
      verify(ascendingExpectedResults, results);
    }
  }

  @Test
  public void testFullOnSelectWithFilterLimitAndDescendingTimeOrderingListFormat()
  {
    // limits shouldn't matter -> all rows should be returned if time-ordering on the broker is occurring
    for (int limit : new int[]{3, 1, 5, 7, 0}) {
      ScanQuery query = newTestQuery()
          .intervals(I_0112_0114)
          .filters(new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null))
          .columns(
              QueryRunnerTestHelper.TIME_DIMENSION,
              QueryRunnerTestHelper.QUALITY_DIMENSION,
              QueryRunnerTestHelper.INDEX_METRIC
          )
          .limit(limit)
          .order(ScanQuery.Order.DESCENDING)
          .build();

      Iterable<ScanResultValue> results = runner.run(QueryPlus.wrap(query)).toList();
      String[] seg1Results = new String[]{
          "2011-01-12T00:00:00.000Z\tspot\tautomotive\tpreferred\tapreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\tbusiness\tpreferred\tbpreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\tentertainment\tpreferred\tepreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\thealth\tpreferred\thpreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\tmezzanine\tpreferred\tmpreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\tnews\tpreferred\tnpreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\tpremium\tpreferred\tppreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\ttechnology\tpreferred\ttpreferred\t100.000000",
          "2011-01-12T00:00:00.000Z\tspot\ttravel\tpreferred\ttpreferred\t100.000000"
      };
      String[] seg2Results = new String[]{
          "2011-01-13T00:00:00.000Z\tspot\tautomotive\tpreferred\tapreferred\t94.874713",
          "2011-01-13T00:00:00.000Z\tspot\tbusiness\tpreferred\tbpreferred\t103.629399",
          "2011-01-13T00:00:00.000Z\tspot\tentertainment\tpreferred\tepreferred\t110.087299",
          "2011-01-13T00:00:00.000Z\tspot\thealth\tpreferred\thpreferred\t114.947403",
          "2011-01-13T00:00:00.000Z\tspot\tmezzanine\tpreferred\tmpreferred\t104.465767",
          "2011-01-13T00:00:00.000Z\tspot\tnews\tpreferred\tnpreferred\t102.851683",
          "2011-01-13T00:00:00.000Z\tspot\tpremium\tpreferred\tppreferred\t108.863011",
          "2011-01-13T00:00:00.000Z\tspot\ttechnology\tpreferred\ttpreferred\t111.356672",
          "2011-01-13T00:00:00.000Z\tspot\ttravel\tpreferred\ttpreferred\t106.236928"
      };
      String[] expectedRet = (String[]) ArrayUtils.addAll(seg1Results, seg2Results);
      ArrayUtils.reverse(expectedRet);
      final List<List<Map<String, Object>>> descendingEvents = toEvents(
          new String[]{
              legacy ? getTimestampName() + ":TIME" : ColumnHolder.TIME_COLUMN_NAME,
              null,
              QueryRunnerTestHelper.QUALITY_DIMENSION + ":STRING",
              null,
              null,
              QueryRunnerTestHelper.INDEX_METRIC + ":DOUBLE"
          },
          expectedRet
      );
      if (legacy) {
        for (List<Map<String, Object>> batch : descendingEvents) {
          for (Map<String, Object> event : batch) {
            event.put("__time", ((DateTime) event.get("timestamp")).getMillis());
          }
        }
      } else {
        for (List<Map<String, Object>> batch : descendingEvents) {
          for (Map<String, Object> event : batch) {
            event.put("__time", (DateTimes.of((String) event.get("__time"))).getMillis());
          }
        }
      }
      List<ScanResultValue> descendingExpectedResults = toExpected(
          descendingEvents,
          legacy ?
          Lists.newArrayList(
              QueryRunnerTestHelper.TIME_DIMENSION,
              getTimestampName(),
              // getTimestampName() always returns the legacy timestamp when legacy is true
              "quality",
              "index"
          ) :
          Lists.newArrayList(
              QueryRunnerTestHelper.TIME_DIMENSION,
              "quality",
              "index"
          ),
          0,
          limit
      );
      verify(descendingExpectedResults, results);
    }
  }

  @Test
  public void testFullOnSelectWithFilterLimitAndAscendingTimeOrderingCompactedListFormat()
  {
    String[] seg1Results = new String[]{
        "2011-01-12T00:00:00.000Z\tspot\tautomotive\tpreferred\tapreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\tbusiness\tpreferred\tbpreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\tentertainment\tpreferred\tepreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\thealth\tpreferred\thpreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\tmezzanine\tpreferred\tmpreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\tnews\tpreferred\tnpreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\tpremium\tpreferred\tppreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\ttechnology\tpreferred\ttpreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\ttravel\tpreferred\ttpreferred\t100.000000"
    };
    String[] seg2Results = new String[]{
        "2011-01-13T00:00:00.000Z\tspot\tautomotive\tpreferred\tapreferred\t94.874713",
        "2011-01-13T00:00:00.000Z\tspot\tbusiness\tpreferred\tbpreferred\t103.629399",
        "2011-01-13T00:00:00.000Z\tspot\tentertainment\tpreferred\tepreferred\t110.087299",
        "2011-01-13T00:00:00.000Z\tspot\thealth\tpreferred\thpreferred\t114.947403",
        "2011-01-13T00:00:00.000Z\tspot\tmezzanine\tpreferred\tmpreferred\t104.465767",
        "2011-01-13T00:00:00.000Z\tspot\tnews\tpreferred\tnpreferred\t102.851683",
        "2011-01-13T00:00:00.000Z\tspot\tpremium\tpreferred\tppreferred\t108.863011",
        "2011-01-13T00:00:00.000Z\tspot\ttechnology\tpreferred\ttpreferred\t111.356672",
        "2011-01-13T00:00:00.000Z\tspot\ttravel\tpreferred\ttpreferred\t106.236928"
    };
    // limits shouldn't matter -> all rows should be returned if time-ordering on the broker is occurring
    for (int limit : new int[]{3, 0}) {
      /* Ascending */
      ScanQuery query = newTestQuery()
          .intervals(I_0112_0114)
          .filters(new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null))
          .columns(
              QueryRunnerTestHelper.TIME_DIMENSION,
              QueryRunnerTestHelper.QUALITY_DIMENSION,
              QueryRunnerTestHelper.INDEX_METRIC
          )
          .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
          .order(ScanQuery.Order.ASCENDING)
          .limit(limit)
          .build();

      Iterable<ScanResultValue> results = runner.run(QueryPlus.wrap(query)).toList();
      final List<List<Map<String, Object>>> ascendingEvents = toEvents(
          new String[]{
              legacy ? getTimestampName() + ":TIME" : ColumnHolder.TIME_COLUMN_NAME,
              null,
              QueryRunnerTestHelper.QUALITY_DIMENSION + ":STRING",
              null,
              null,
              QueryRunnerTestHelper.INDEX_METRIC + ":DOUBLE"
          },
          (String[]) ArrayUtils.addAll(seg1Results, seg2Results)
      );
      if (legacy) {
        for (List<Map<String, Object>> batch : ascendingEvents) {
          for (Map<String, Object> event : batch) {
            event.put("__time", ((DateTime) event.get("timestamp")).getMillis());
          }
        }
      } else {
        for (List<Map<String, Object>> batch : ascendingEvents) {
          for (Map<String, Object> event : batch) {
            event.put("__time", ((DateTimes.of((String) event.get("__time"))).getMillis()));
          }
        }
      }
      List<ScanResultValue> ascendingExpectedResults = toExpected(
          ascendingEvents,
          legacy ?
          Lists.newArrayList(
              QueryRunnerTestHelper.TIME_DIMENSION,
              getTimestampName(),
              // getTimestampName() always returns the legacy timestamp when legacy is true
              "quality",
              "index"
          ) :
          Lists.newArrayList(
              QueryRunnerTestHelper.TIME_DIMENSION,
              "quality",
              "index"
          ),
          0,
          limit
      );
      results = compactedListToRow(results);
      verify(ascendingExpectedResults, results);
    }
  }

  @Test
  public void testFullOnSelectWithFilterLimitAndDescendingTimeOrderingCompactedListFormat()
  {
    String[] seg1Results = new String[]{
        "2011-01-12T00:00:00.000Z\tspot\tautomotive\tpreferred\tapreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\tbusiness\tpreferred\tbpreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\tentertainment\tpreferred\tepreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\thealth\tpreferred\thpreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\tmezzanine\tpreferred\tmpreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\tnews\tpreferred\tnpreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\tpremium\tpreferred\tppreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\ttechnology\tpreferred\ttpreferred\t100.000000",
        "2011-01-12T00:00:00.000Z\tspot\ttravel\tpreferred\ttpreferred\t100.000000"
    };
    String[] seg2Results = new String[]{
        "2011-01-13T00:00:00.000Z\tspot\tautomotive\tpreferred\tapreferred\t94.874713",
        "2011-01-13T00:00:00.000Z\tspot\tbusiness\tpreferred\tbpreferred\t103.629399",
        "2011-01-13T00:00:00.000Z\tspot\tentertainment\tpreferred\tepreferred\t110.087299",
        "2011-01-13T00:00:00.000Z\tspot\thealth\tpreferred\thpreferred\t114.947403",
        "2011-01-13T00:00:00.000Z\tspot\tmezzanine\tpreferred\tmpreferred\t104.465767",
        "2011-01-13T00:00:00.000Z\tspot\tnews\tpreferred\tnpreferred\t102.851683",
        "2011-01-13T00:00:00.000Z\tspot\tpremium\tpreferred\tppreferred\t108.863011",
        "2011-01-13T00:00:00.000Z\tspot\ttechnology\tpreferred\ttpreferred\t111.356672",
        "2011-01-13T00:00:00.000Z\tspot\ttravel\tpreferred\ttpreferred\t106.236928"
    };
    // limits shouldn't matter -> all rows should be returned if time-ordering on the broker is occurring
    for (int limit : new int[]{3, 1}) {
      /* Descending */
      ScanQuery query = newTestQuery()
          .intervals(I_0112_0114)
          .filters(new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null))
          .columns(
              QueryRunnerTestHelper.TIME_DIMENSION,
              QueryRunnerTestHelper.QUALITY_DIMENSION,
              QueryRunnerTestHelper.INDEX_METRIC
          )
          .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
          .order(ScanQuery.Order.DESCENDING)
          .context(ImmutableMap.of(ScanQuery.CTX_KEY_OUTERMOST, false))
          .limit(limit)
          .build();

      Iterable<ScanResultValue> results = runner.run(QueryPlus.wrap(query)).toList();
      String[] expectedRet = (String[]) ArrayUtils.addAll(seg1Results, seg2Results);
      ArrayUtils.reverse(expectedRet);
      final List<List<Map<String, Object>>> descendingEvents = toEvents(
          new String[]{
              legacy ? getTimestampName() + ":TIME" : ColumnHolder.TIME_COLUMN_NAME,
              null,
              QueryRunnerTestHelper.QUALITY_DIMENSION + ":STRING",
              null,
              null,
              QueryRunnerTestHelper.INDEX_METRIC + ":DOUBLE"
          },
          expectedRet //segments in reverse order from above
      );
      if (legacy) {
        for (List<Map<String, Object>> batch : descendingEvents) {
          for (Map<String, Object> event : batch) {
            event.put("__time", ((DateTime) event.get("timestamp")).getMillis());
          }
        }
      } else {
        for (List<Map<String, Object>> batch : descendingEvents) {
          for (Map<String, Object> event : batch) {
            event.put("__time", ((DateTimes.of((String) event.get("__time"))).getMillis()));
          }
        }
      }
      List<ScanResultValue> descendingExpectedResults = toExpected(
          descendingEvents,
          legacy ?
          Lists.newArrayList(
              QueryRunnerTestHelper.TIME_DIMENSION,
              getTimestampName(),
              // getTimestampName() always returns the legacy timestamp when legacy is true
              "quality",
              "index"
          ) :
          Lists.newArrayList(
              QueryRunnerTestHelper.TIME_DIMENSION,
              "quality",
              "index"
          ),
          0,
          limit
      );
      results = compactedListToRow(results);
      verify(descendingExpectedResults, results);
    }
  }

  private List<List<Map<String, Object>>> toFullEvents(final String[]... valueSet)
  {
    return toEvents(
        new String[]{
            getTimestampName() + ":TIME",
            QueryRunnerTestHelper.MARKET_DIMENSION + ":STRING",
            QueryRunnerTestHelper.QUALITY_DIMENSION + ":STRING",
            "qualityLong" + ":LONG",
            "qualityFloat" + ":FLOAT",
            "qualityDouble" + ":DOUBLE",
            "qualityNumericString" + ":STRING",
            "longNumericNull" + ":LONG",
            "floatNumericNull" + ":FLOAT",
            "doubleNumericNull" + ":DOUBLE",
            QueryRunnerTestHelper.PLACEMENT_DIMENSION + ":STRING",
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION + ":STRINGS",
            QueryRunnerTestHelper.INDEX_METRIC + ":DOUBLE",
            QueryRunnerTestHelper.PARTIAL_NULL_DIMENSION + ":STRING",
            "expr",
            "indexMin",
            "indexFloat",
            "indexMaxPlusTen",
            "indexMinFloat",
            "indexMaxFloat",
            "quality_uniques"
        },
        valueSet
    );
  }

  private List<List<Map<String, Object>>> toEvents(final String[] dimSpecs, final String[]... valueSet)
  {
    List<String> values = new ArrayList<>();
    for (String[] vSet : valueSet) {
      values.addAll(Arrays.asList(vSet));
    }
    List<List<Map<String, Object>>> events = new ArrayList<>();
    events.add(
        Lists.newArrayList(
            Iterables.transform(
                values,
                input -> {
                  Map<String, Object> event = new HashMap<>();
                  String[] values1 = input.split("\\t");
                  for (int i = 0; i < dimSpecs.length; i++) {
                    if (dimSpecs[i] == null || i >= dimSpecs.length) {
                      continue;
                    }

                    // For testing metrics and virtual columns we have some special handling here, since
                    // they don't appear in the source data.
                    if (dimSpecs[i].equals(EXPR_COLUMN.getOutputName())) {
                      event.put(
                          EXPR_COLUMN.getOutputName(),
                          (double) event.get(QueryRunnerTestHelper.INDEX_METRIC) * 2
                      );
                      continue;
                    } else if (dimSpecs[i].equals("indexMin")) {
                      event.put("indexMin", (double) event.get(QueryRunnerTestHelper.INDEX_METRIC));
                      continue;
                    } else if (dimSpecs[i].equals("indexFloat")) {
                      event.put("indexFloat", (float) (double) event.get(QueryRunnerTestHelper.INDEX_METRIC));
                      continue;
                    } else if (dimSpecs[i].equals("indexMaxPlusTen")) {
                      event.put("indexMaxPlusTen", (double) event.get(QueryRunnerTestHelper.INDEX_METRIC) + 10);
                      continue;
                    } else if (dimSpecs[i].equals("indexMinFloat")) {
                      event.put("indexMinFloat", (float) (double) event.get(QueryRunnerTestHelper.INDEX_METRIC));
                      continue;
                    } else if (dimSpecs[i].equals("indexMaxFloat")) {
                      event.put("indexMaxFloat", (float) (double) event.get(QueryRunnerTestHelper.INDEX_METRIC));
                      continue;
                    } else if (dimSpecs[i].equals("quality_uniques")) {
                      final HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
                      collector.add(
                          Hashing.murmur3_128()
                                 .hashBytes(StringUtils.toUtf8((String) event.get("quality")))
                                 .asBytes()
                      );
                      event.put("quality_uniques", collector);
                    }

                    if (i >= values1.length) {
                      continue;
                    }

                    String[] specs = dimSpecs[i].split(":");

                    Object eventVal;
                    if (specs.length == 1 || specs[1].equals("STRING")) {
                      eventVal = values1[i];
                    } else if (specs[1].equals("TIME")) {
                      eventVal = toTimestamp(values1[i]);
                    } else if (specs[1].equals("FLOAT")) {
                      eventVal = values1[i].isEmpty() ? NullHandling.defaultFloatValue() : Float.valueOf(values1[i]);
                    } else if (specs[1].equals("DOUBLE")) {
                      eventVal = values1[i].isEmpty() ? NullHandling.defaultDoubleValue() : Double.valueOf(values1[i]);
                    } else if (specs[1].equals("LONG")) {
                      eventVal = values1[i].isEmpty() ? NullHandling.defaultLongValue() : Long.valueOf(values1[i]);
                    } else if (specs[1].equals(("NULL"))) {
                      eventVal = null;
                    } else if (specs[1].equals("STRINGS")) {
                      eventVal = Arrays.asList(values1[i].split("\u0001"));
                    } else {
                      eventVal = values1[i];
                    }

                    event.put(specs[0], eventVal);
                  }
                  return event;
                }
            )
        )
    );
    return events;
  }

  private Object toTimestamp(final String value)
  {
    if (legacy) {
      return DateTimes.of(value);
    } else {
      return DateTimes.of(value).getMillis();
    }
  }

  private String getTimestampName()
  {
    return legacy ? "timestamp" : ColumnHolder.TIME_COLUMN_NAME;
  }

  private List<ScanResultValue> toExpected(
      List<List<Map<String, Object>>> targets,
      List<String> columns,
      final int offset,
      final int limit
  )
  {
    List<ScanResultValue> expected = Lists.newArrayListWithExpectedSize(targets.size());
    for (List<Map<String, Object>> group : targets) {
      List<Map<String, Object>> events = Lists.newArrayListWithExpectedSize(limit);
      int end = Math.min(group.size(), offset + limit);
      if (end == 0) {
        end = group.size();
      }
      events.addAll(group.subList(offset, end));
      expected.add(new ScanResultValue(QueryRunnerTestHelper.SEGMENT_ID.toString(), columns, events));
    }
    return expected;
  }

  public static void verify(
      Iterable<ScanResultValue> expectedResults,
      Iterable<ScanResultValue> actualResults
  )
  {
    Iterator<ScanResultValue> expectedIter = expectedResults.iterator();
    Iterator<ScanResultValue> actualIter = actualResults.iterator();

    while (expectedIter.hasNext()) {
      ScanResultValue expected = expectedIter.next();
      ScanResultValue actual = actualIter.next();

      Assert.assertEquals(expected.getSegmentId(), actual.getSegmentId());

      Set exColumns = Sets.newTreeSet(expected.getColumns());
      Set acColumns = Sets.newTreeSet(actual.getColumns());
      Assert.assertEquals(exColumns, acColumns);

      Iterator<Map<String, Object>> expectedEvts = ((List<Map<String, Object>>) expected.getEvents()).iterator();
      Iterator<Map<String, Object>> actualEvts = ((List<Map<String, Object>>) actual.getEvents()).iterator();

      while (expectedEvts.hasNext()) {
        Map<String, Object> exHolder = expectedEvts.next();
        Map<String, Object> acHolder = actualEvts.next();

        for (Map.Entry<String, Object> ex : exHolder.entrySet()) {
          Object actVal = acHolder.get(ex.getKey());

          if (actVal instanceof String[]) {
            actVal = Arrays.asList((String[]) actVal);
          }
          Object exValue = ex.getValue();
          if (exValue instanceof Double || exValue instanceof Float) {
            final double expectedDoubleValue = ((Number) exValue).doubleValue();
            Assert.assertEquals(
                "invalid value for " + ex.getKey(),
                expectedDoubleValue,
                ((Number) actVal).doubleValue(),
                expectedDoubleValue * 1e-6
            );
          } else {
            Assert.assertEquals("invalid value for " + ex.getKey(), ex.getValue(), actVal);
          }
        }

        for (Map.Entry<String, Object> ac : acHolder.entrySet()) {
          Object exVal = exHolder.get(ac.getKey());
          Object actVal = ac.getValue();

          if (actVal instanceof String[]) {
            actVal = Arrays.asList((String[]) actVal);
          }

          if (exVal instanceof Double || exVal instanceof Float) {
            final double exDoubleValue = ((Number) exVal).doubleValue();
            Assert.assertEquals(
                "invalid value for " + ac.getKey(),
                exDoubleValue,
                ((Number) actVal).doubleValue(),
                exDoubleValue * 1e-6
            );
          } else {
            Assert.assertEquals("invalid value for " + ac.getKey(), exVal, actVal);
          }
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

  private static Iterable<ScanResultValue> populateNullColumnAtLastForQueryableIndexCase(
      Iterable<ScanResultValue> results,
      String columnName
  )
  {
    // A Queryable index does not have the null column when it has loaded a index.
    for (ScanResultValue value : results) {
      List<String> columns = value.getColumns();
      if (columns.contains(columnName)) {
        break;
      }
      columns.add(columnName);
    }

    return results;
  }

  private Iterable<ScanResultValue> compactedListToRow(Iterable<ScanResultValue> results)
  {
    return Lists.newArrayList(Iterables.transform(results, new Function<ScanResultValue, ScanResultValue>()
    {
      @Override
      public ScanResultValue apply(ScanResultValue input)
      {
        List<Map<String, Object>> mapEvents = new ArrayList<>();
        List<?> events = ((List<?>) input.getEvents());
        for (Object event : events) {
          Iterator<?> compactedEventIter = ((List<?>) event).iterator();
          Map<String, Object> mapEvent = new LinkedHashMap<>();
          for (String column : input.getColumns()) {
            mapEvent.put(column, compactedEventIter.next());
          }
          mapEvents.add(mapEvent);
        }
        return new ScanResultValue(input.getSegmentId(), input.getColumns(), mapEvents);
      }
    }));
  }
}
