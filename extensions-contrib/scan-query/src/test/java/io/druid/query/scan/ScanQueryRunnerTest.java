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

package io.druid.query.scan;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.DefaultGenericQueryMetricsFactory;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TableDataSource;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
@RunWith(Parameterized.class)
public class ScanQueryRunnerTest
{
  // copied from druid.sample.numeric.tsv
  public static final String[] V_0112 = {
      "2011-01-12T00:00:00.000Z\tspot\tautomotive\t1000\t10000.0\t10000.0\t100000\tpreferred\tapreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\tbusiness\t1100\t11000.0\t11000.0\t110000\tpreferred\tbpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\tentertainment\t1200\t12000.0\t12000.0\t120000\tpreferred\tepreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\thealth\t1300\t13000.0\t13000.0\t130000\tpreferred\thpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\tnews\t1500\t15000.0\t15000.0\t150000\tpreferred\tnpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\ttechnology\t1700\t17000.0\t17000.0\t170000\tpreferred\ttpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\ttravel\t1800\t18000.0\t18000.0\t180000\tpreferred\ttpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\ttotal_market\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t1000.000000",
      "2011-01-12T00:00:00.000Z\ttotal_market\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t1000.000000",
      "2011-01-12T00:00:00.000Z\tupfront\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t800.000000\tvalue",
      "2011-01-12T00:00:00.000Z\tupfront\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t800.000000\tvalue"
  };
  public static final String[] V_0113 = {
      "2011-01-13T00:00:00.000Z\tspot\tautomotive\t1000\t10000.0\t10000.0\t100000\tpreferred\tapreferred\t94.874713",
      "2011-01-13T00:00:00.000Z\tspot\tbusiness\t1100\t11000.0\t11000.0\t110000\tpreferred\tbpreferred\t103.629399",
      "2011-01-13T00:00:00.000Z\tspot\tentertainment\t1200\t12000.0\t12000.0\t120000\tpreferred\tepreferred\t110.087299",
      "2011-01-13T00:00:00.000Z\tspot\thealth\t1300\t13000.0\t13000.0\t130000\tpreferred\thpreferred\t114.947403",
      "2011-01-13T00:00:00.000Z\tspot\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t104.465767",
      "2011-01-13T00:00:00.000Z\tspot\tnews\t1500\t15000.0\t15000.0\t150000\tpreferred\tnpreferred\t102.851683",
      "2011-01-13T00:00:00.000Z\tspot\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t108.863011",
      "2011-01-13T00:00:00.000Z\tspot\ttechnology\t1700\t17000.0\t17000.0\t170000\tpreferred\ttpreferred\t111.356672",
      "2011-01-13T00:00:00.000Z\tspot\ttravel\t1800\t18000.0\t18000.0\t180000\tpreferred\ttpreferred\t106.236928",
      "2011-01-13T00:00:00.000Z\ttotal_market\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t1040.945505",
      "2011-01-13T00:00:00.000Z\ttotal_market\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t1689.012875",
      "2011-01-13T00:00:00.000Z\tupfront\tmezzanine\t1400\t14000.0\t14000.0\t140000\tpreferred\tmpreferred\t826.060182\tvalue",
      "2011-01-13T00:00:00.000Z\tupfront\tpremium\t1600\t16000.0\t16000.0\t160000\tpreferred\tppreferred\t1564.617729\tvalue"
  };

  public static final QuerySegmentSpec I_0112_0114 = new LegacySegmentSpec(
      new Interval("2011-01-12T00:00:00.000Z/2011-01-14T00:00:00.000Z")
  );
  public static final String[] V_0112_0114 = ObjectArrays.concat(V_0112, V_0113, String.class);

  private static final ScanQueryQueryToolChest toolChest = new ScanQueryQueryToolChest(
      DefaultGenericQueryMetricsFactory.instance()
  );

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.cartesian(
        QueryRunnerTestHelper.makeQueryRunners(
            new ScanQueryRunnerFactory(
                toolChest,
                new ScanQueryEngine()
            )
        )
    );
  }

  private final QueryRunner runner;

  public ScanQueryRunnerTest(QueryRunner runner)
  {
    this.runner = runner;
  }

  private ScanQuery.ScanQueryBuilder newTestQuery()
  {
    return ScanQuery.newScanQueryBuilder()
                    .dataSource(new TableDataSource(QueryRunnerTestHelper.dataSource))
                    .columns(Arrays.<String>asList())
                    .intervals(QueryRunnerTestHelper.fullOnInterval)
                    .limit(3);
  }

  @Test
  public void testFullOnSelect()
  {
    List<String> columns = Lists.newArrayList(
        ScanResultValue.timestampKey,
        "market",
        "quality",
        "qualityLong",
        "qualityFloat",
        "qualityDouble",
        "qualityNumericString",
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
        .build();

    HashMap<String, Object> context = new HashMap<String, Object>();
    Iterable<ScanResultValue> results = Sequences.toList(
        runner.run(query, context),
        Lists.<ScanResultValue>newArrayList()
    );

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
        ScanResultValue.timestampKey,
        "market",
        "quality",
        "qualityLong",
        "qualityFloat",
        "qualityDouble",
        "qualityNumericString",
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
        .resultFormat(ScanQuery.RESULT_FORMAT_COMPACTED_LIST)
        .build();

    HashMap<String, Object> context = new HashMap<String, Object>();
    Iterable<ScanResultValue> results = Sequences.toList(
        runner.run(query, context),
        Lists.<ScanResultValue>newArrayList()
    );

    List<ScanResultValue> expectedResults = toExpected(
        toFullEvents(V_0112_0114),
        columns,
        0,
        3
    );
    verify(expectedResults, populateNullColumnAtLastForQueryableIndexCase(compactedListToRow(results), "null_column"));
  }

  @Test
  public void testSelectWithDimsAndMets()
  {
    ScanQuery query = newTestQuery()
        .intervals(I_0112_0114)
        .columns(QueryRunnerTestHelper.marketDimension, QueryRunnerTestHelper.indexMetric)
        .build();

    HashMap<String, Object> context = new HashMap<String, Object>();
    Iterable<ScanResultValue> results = Sequences.toList(
        runner.run(query, context),
        Lists.<ScanResultValue>newArrayList()
    );

    List<ScanResultValue> expectedResults = toExpected(
        toEvents(
            new String[]{
                ScanResultValue.timestampKey + ":TIME",
                QueryRunnerTestHelper.marketDimension + ":STRING",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                QueryRunnerTestHelper.indexMetric + ":DOUBLE"
            },
            V_0112_0114
        ),
        Lists.newArrayList(ScanResultValue.timestampKey, "market", "index"),
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
        .columns(QueryRunnerTestHelper.marketDimension, QueryRunnerTestHelper.indexMetric)
        .resultFormat(ScanQuery.RESULT_FORMAT_COMPACTED_LIST)
        .build();

    HashMap<String, Object> context = new HashMap<String, Object>();
    Iterable<ScanResultValue> results = Sequences.toList(
        runner.run(query, context),
        Lists.<ScanResultValue>newArrayList()
    );

    List<ScanResultValue> expectedResults = toExpected(
        toEvents(
            new String[]{
                ScanResultValue.timestampKey + ":TIME",
                QueryRunnerTestHelper.marketDimension + ":STRING",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                QueryRunnerTestHelper.indexMetric + ":DOUBLE"
            },
            V_0112_0114
        ),
        Lists.newArrayList(ScanResultValue.timestampKey, "market", "index"),
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
          .filters(new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot", null))
          .columns(QueryRunnerTestHelper.qualityDimension, QueryRunnerTestHelper.indexMetric)
          .limit(limit)
          .build();

      HashMap<String, Object> context = new HashMap<String, Object>();
      Iterable<ScanResultValue> results = Sequences.toList(
          runner.run(query, context),
          Lists.<ScanResultValue>newArrayList()
      );

      final List<List<Map<String, Object>>> events = toEvents(
          new String[]{
              ScanResultValue.timestampKey + ":TIME",
              null,
              QueryRunnerTestHelper.qualityDimension + ":STRING",
              null,
              null,
              QueryRunnerTestHelper.indexMetric + ":DOUBLE"
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
          Lists.newArrayList(ScanResultValue.timestampKey, "quality", "index"),
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
        .filters(new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "replaced", lookupExtractionFn))
        .columns(QueryRunnerTestHelper.qualityDimension, QueryRunnerTestHelper.indexMetric)
        .build();

    Iterable<ScanResultValue> results = Sequences.toList(
        runner.run(query, Maps.newHashMap()),
        Lists.<ScanResultValue>newArrayList()
    );
    Iterable<ScanResultValue> resultsOptimize = Sequences.toList(
        toolChest.postMergeQueryDecoration(toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner))).
            run(query, Maps.<String, Object>newHashMap()), Lists.<ScanResultValue>newArrayList()
    );

    final List<List<Map<String, Object>>> events = toEvents(
        new String[]{
            ScanResultValue.timestampKey + ":TIME",
            null,
            QueryRunnerTestHelper.qualityDimension + ":STRING",
            null,
            null,
            QueryRunnerTestHelper.indexMetric + ":DOUBLE"
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
        Lists.newArrayList(ScanResultValue.timestampKey, QueryRunnerTestHelper.qualityDimension, QueryRunnerTestHelper.indexMetric),
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
                Arrays.<DimFilter>asList(
                    new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot", null),
                    new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "foo", null)
                )
            )
        )
        .build();

    Iterable<ScanResultValue> results = Sequences.toList(
        runner.run(query, Maps.newHashMap()),
        Lists.<ScanResultValue>newArrayList()
    );

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

    Iterable<ScanResultValue> results = Sequences.toList(
        runner.run(query, Maps.newHashMap()),
        Lists.<ScanResultValue>newArrayList()
    );

    final List<List<Map<String, Object>>> events = toEvents(
        new String[]{
            ScanResultValue.timestampKey + ":TIME"
        },
        V_0112_0114
    );

    List<ScanResultValue> expectedResults = toExpected(
        events,
        Lists.<String>newArrayList(ScanResultValue.timestampKey, "foo", "foo2"),
        0,
        3
    );
    verify(expectedResults, results);
  }

  private List<List<Map<String, Object>>> toFullEvents(final String[]... valueSet)
  {
    return toEvents(
        new String[]{
            ScanResultValue.timestampKey + ":TIME",
            QueryRunnerTestHelper.marketDimension + ":STRING",
            QueryRunnerTestHelper.qualityDimension + ":STRING",
            "qualityLong" + ":LONG",
            "qualityFloat" + ":FLOAT",
            "qualityDouble" + ":DOUBLE",
            "qualityNumericString" + ":STRING",
            QueryRunnerTestHelper.placementDimension + ":STRING",
            QueryRunnerTestHelper.placementishDimension + ":STRINGS",
            QueryRunnerTestHelper.indexMetric + ":DOUBLE",
            QueryRunnerTestHelper.partialNullDimension + ":STRING"
        },
        valueSet
    );
  }

  private List<List<Map<String, Object>>> toEvents(final String[] dimSpecs, final String[]... valueSet)
  {
    List<String> values = Lists.newArrayList();
    for (String[] vSet : valueSet) {
      values.addAll(Arrays.asList(vSet));
    }
    List<List<Map<String, Object>>> events = Lists.newArrayList();
    events.add(
        Lists.newArrayList(
            Iterables.transform(
                values, new Function<String, Map<String, Object>>()
                {
                  @Override
                  public Map<String, Object> apply(String input)
                  {
                    Map<String, Object> event = Maps.newHashMap();
                    String[] values = input.split("\\t");
                    for (int i = 0; i < dimSpecs.length; i++) {
                      if (dimSpecs[i] == null || i >= dimSpecs.length || i >= values.length) {
                        continue;
                      }
                      String[] specs = dimSpecs[i].split(":");
                      event.put(
                          specs[0],
                          specs.length == 1 || specs[1].equals("STRING") ? values[i] :
                          specs[1].equals("TIME") ? new DateTime(values[i]) :
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
    return events;
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
      expected.add(
          new ScanResultValue(
              QueryRunnerTestHelper.segmentId,
              columns,
              events
          )
      );
    }
    return expected;
  }

  private static void verify(
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

          // work around for current II limitations
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
    return Iterables.transform(results, new Function<ScanResultValue, ScanResultValue>()
    {
      @Override
      public ScanResultValue apply(ScanResultValue input)
      {
        List mapEvents = Lists.newLinkedList();
        List events = ((List) input.getEvents());
        for (int i = 0; i < events.size(); i++) {
          Iterator compactedEventIter = ((List) events.get(i)).iterator();
          Map mapEvent = new LinkedHashMap();
          for (String column : input.getColumns()) {
            mapEvent.put(column, compactedEventIter.next());
          }
          mapEvents.add(mapEvent);
        }
        return new ScanResultValue(input.getSegmentId(), input.getColumns(), mapEvents);
      }
    });
  }
}
