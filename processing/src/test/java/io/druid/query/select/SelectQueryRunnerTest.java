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

package io.druid.query.select;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequences;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.SelectorDimFilter;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class SelectQueryRunnerTest
{
  // copied from druid.sample.tsv
  public static final String[] EVENTS_0112 = {
      "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\tautomotive\t1111\t111.111\tpreferred\tapreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\tbusiness\t2222\t222.222\tpreferred\tbpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\tentertainment\t3333\t333.333\tpreferred\tepreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\thealth\t4444\t444.444\tpreferred\thpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\tmezzanine\t5555\t555.555\tpreferred\tmpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\tnews\t6666\t666.666\tpreferred\tnpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\tpremium\t7777\t777.777\tpreferred\tppreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\ttechnology\t8888\t888.888\tpreferred\ttpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\ttravel\t8899\t889.999\tpreferred\ttpreferred\t100.000000",
      "2011-01-12T00:00:00.000Z\ttotal_market\t2222\t222.222\tmezzanine\t5555\t555.555\tpreferred\tmpreferred\t1000.000000",
      "2011-01-12T00:00:00.000Z\ttotal_market\t2222\t222.222\tpremium\t7777\t777.777\tpreferred\tppreferred\t1000.000000",
      "2011-01-12T00:00:00.000Z\tupfront\t3333\t333.333\tmezzanine\t5555\t555.555\tpreferred\tmpreferred\t800.000000\tvalue",
      "2011-01-12T00:00:00.000Z\tupfront\t3333\t333.333\tpremium\t7777\t777.777\tpreferred\tppreferred\t800.000000\tvalue"
  };
  public static final String[] EVENTS_0113 = {
      "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\tautomotive\t1111\t111.111\tpreferred\tapreferred\t94.874713",
      "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\tbusiness\t2222\t222.222\tpreferred\tbpreferred\t103.629399",
      "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\tentertainment\t3333\t333.333\tpreferred\tepreferred\t110.087299",
      "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\thealth\t4444\t444.444\tpreferred\thpreferred\t114.947403",
      "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\tmezzanine\t5555\t555.555\tpreferred\tmpreferred\t104.465767",
      "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\tnews\t6666\t666.666\tpreferred\tnpreferred\t102.851683",
      "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\tpremium\t7777\t777.777\tpreferred\tppreferred\t108.863011",
      "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\ttechnology\t8888\t888.888\tpreferred\ttpreferred\t111.356672",
      "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\ttravel\t8899\t889.999\tpreferred\ttpreferred\t106.236928",
      "2011-01-13T00:00:00.000Z\ttotal_market\t2222\t222.222\tmezzanine\t5555\t555.555\tpreferred\tmpreferred\t1040.945505",
      "2011-01-13T00:00:00.000Z\ttotal_market\t2222\t222.222\tpremium\t7777\t777.777\tpreferred\tppreferred\t1689.012875",
      "2011-01-13T00:00:00.000Z\tupfront\t3333\t333.333\tmezzanine\t5555\t555.555\tpreferred\tmpreferred\t826.060182\tvalue",
      "2011-01-13T00:00:00.000Z\tupfront\t3333\t333.333\tpremium\t7777\t777.777\tpreferred\tppreferred\t1564.617729\tvalue"
  };

  public static final QuerySegmentSpec INTERVAL_0112_0114 = new LegacySegmentSpec(
      new Interval("2011-01-12/2011-01-14")
  );
  public static final String[] EVENTS_0112_0114 = ObjectArrays.concat(EVENTS_0112, EVENTS_0113, String.class);

  @Parameterized.Parameters(name = "{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.cartesian(
        QueryRunnerTestHelper.makeQueryRunners(
            new SelectQueryRunnerFactory(
                new SelectQueryQueryToolChest(
                    new DefaultObjectMapper(),
                    QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                ),
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

  @Test
  public void testFullOnSelect()
  {
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        INTERVAL_0112_0114,
        descending,
        null,
        QueryRunnerTestHelper.allGran,
        DefaultDimensionSpec.toSpec(Arrays.<String>asList()),
        Arrays.<String>asList(),
        new PagingSpec(null, 3),
        null
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    PagingOffset offset = query.getPagingOffset(QueryRunnerTestHelper.segmentId);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        toEvents(new String[]{EventHolder.timestampKey + ":TIME"}, EVENTS_0112_0114),
        offset.startOffset(),
        offset.threshold()
    );
    verify(expectedResults, results);
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

    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        QueryRunnerTestHelper.fullOnInterval,
        descending,
        null,
        QueryRunnerTestHelper.allGran,
        Arrays.<DimensionSpec>asList(
            new DefaultDimensionSpec(QueryRunnerTestHelper.marketDimension, "mar"),
            new DefaultDimensionSpec("market_long", null),
            new DefaultDimensionSpec("market_float", null),
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.qualityDimension,
                "qual",
                new LookupExtractionFn(new MapLookupExtractor(map, true), false, null, true, false)
            ),
            new DefaultDimensionSpec("quality_long", null),
            new DefaultDimensionSpec("quality_float", null),
            new DefaultDimensionSpec(QueryRunnerTestHelper.placementDimension, "place")
        ), Lists.<String>newArrayList(), new PagingSpec(null, 3),
        null
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    List<Result<SelectResultValue>> expectedResultsAsc = Arrays.asList(
        new Result<SelectResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.of(QueryRunnerTestHelper.segmentId, 2),
                Arrays.asList(
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        0,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-12T00:00:00.000Z"))
                            .put("mar", "spot")
                            .put("qual", "automotive0")
                            .put("place", "preferred")
                            .put(QueryRunnerTestHelper.indexMetric, 100.000000F)
                            .build()
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        1,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-12T00:00:00.000Z"))
                            .put("mar", "spot")
                            .put("qual", "business0")
                            .put("place", "preferred")
                            .put(QueryRunnerTestHelper.indexMetric, 100.000000F)
                            .build()
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        2,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-01-12T00:00:00.000Z"))
                            .put("mar", "spot")
                            .put("qual", "entertainment0")
                            .put("place", "preferred")
                            .put(QueryRunnerTestHelper.indexMetric, 100.000000F)
                            .build()
                    )
                )
            )
        )
    );

    List<Result<SelectResultValue>> expectedResultsDsc = Arrays.asList(
        new Result<SelectResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.of(QueryRunnerTestHelper.segmentId, -3),
                Arrays.asList(
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        -1,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-04-15T00:00:00.000Z"))
                            .put("mar", "upfront")
                            .put("qual", "premium0")
                            .put("place", "preferred")
                            .put(QueryRunnerTestHelper.indexMetric, 780.27197265625F)
                            .build()
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        -2,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-04-15T00:00:00.000Z"))
                            .put("mar", "upfront")
                            .put("qual", "mezzanine0")
                            .put("place", "preferred")
                            .put(QueryRunnerTestHelper.indexMetric, 962.731201171875F)
                            .build()
                    ),
                    new EventHolder(
                        QueryRunnerTestHelper.segmentId,
                        -3,
                        new ImmutableMap.Builder<String, Object>()
                            .put(EventHolder.timestampKey, new DateTime("2011-04-15T00:00:00.000Z"))
                            .put("mar", "total_market")
                            .put("qual", "premium0")
                            .put("place", "preferred")
                            .put(QueryRunnerTestHelper.indexMetric, 1029.0570068359375F)
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
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        INTERVAL_0112_0114,
        descending,
        null,
        QueryRunnerTestHelper.allGran,
        DefaultDimensionSpec.toSpec(Arrays.asList(QueryRunnerTestHelper.marketDimension, "market_long", "market_float",
                                                  QueryRunnerTestHelper.qualityDimension, "quality_long", "quality_float")),
        Arrays.asList(QueryRunnerTestHelper.indexMetric),
        new PagingSpec(null, 3),
        null
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    PagingOffset offset = query.getPagingOffset(QueryRunnerTestHelper.segmentId);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        toEvents(
            new String[]{
                EventHolder.timestampKey + ":TIME",
                QueryRunnerTestHelper.marketDimension + ":STRING",
                "market_long" + ":LONG",
                "market_float" + ":FLOAT",
                QueryRunnerTestHelper.qualityDimension + ":STRING",
                "quality_long" + ":LONG",
                "quality_float" + ":FLOAT",
                null,
                null,
                QueryRunnerTestHelper.indexMetric + ":FLOAT"
            },
            EVENTS_0112_0114
        ),
        offset.startOffset(),
        offset.threshold()
    );
    verify(expectedResults, results);
  }

  @Test
  public void testSelectPagination()
  {
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        INTERVAL_0112_0114,
        descending,
        null,
        QueryRunnerTestHelper.allGran,
        DefaultDimensionSpec.toSpec(Arrays.asList(QueryRunnerTestHelper.qualityDimension)),
        Arrays.asList(QueryRunnerTestHelper.indexMetric),
        new PagingSpec(toPagingIdentifier(3, descending), 3),
        null
    );

    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, Maps.newHashMap()),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    PagingOffset offset = query.getPagingOffset(QueryRunnerTestHelper.segmentId);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        toEvents(
            new String[]{
                EventHolder.timestampKey + ":TIME",
                "foo:NULL",
                "foo2:NULL"
            },
            EVENTS_0112_0114
        ),
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
      SelectQuery query = new SelectQuery(
          new TableDataSource(QueryRunnerTestHelper.dataSource),
          INTERVAL_0112_0114,
          descending,
          new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot"),
          QueryRunnerTestHelper.dayGran,
          DefaultDimensionSpec.toSpec(Lists.<String>newArrayList(
              QueryRunnerTestHelper.marketDimension,
              "market_long",
              "market_float",
              QueryRunnerTestHelper.qualityDimension,
              "quality_long",
              "quality_float")
          ),
          Lists.<String>newArrayList(QueryRunnerTestHelper.indexMetric),
          new PagingSpec(toPagingIdentifier(param[0], descending), param[1]),
          null
      );
      HashMap<String, Object> context = new HashMap<String, Object>();
      Iterable<Result<SelectResultValue>> results = Sequences.toList(
          runner.run(query, context),
          Lists.<Result<SelectResultValue>>newArrayList()
      );

      final List<List<Map<String, Object>>> events = toEvents(
          new String[]{
              EventHolder.timestampKey + ":TIME",
              QueryRunnerTestHelper.marketDimension + ":STRING",
              "market_long" + ":LONG",
              "market_float" + ":FLOAT",
              QueryRunnerTestHelper.qualityDimension + ":STRING",
              "quality_long" + ":LONG",
              "quality_float" + ":FLOAT",
              null,
              null,
              QueryRunnerTestHelper.indexMetric + ":FLOAT"
          },
          // filtered values with day granularity
          new String[]{
              "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\tautomotive\t1111\t111.111\tpreferred\tapreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\tbusiness\t2222\t222.222\tpreferred\tbpreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\tentertainment\t3333\t333.333\tpreferred\tepreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\thealth\t4444\t444.444\tpreferred\thpreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\tmezzanine\t5555\t555.555\tpreferred\tmpreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\tnews\t6666\t666.666\tpreferred\tnpreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\tpremium\t7777\t777.777\tpreferred\tppreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\ttechnology\t8888\t888.888\tpreferred\ttpreferred\t100.000000",
              "2011-01-12T00:00:00.000Z\tspot\t1111\t111.111\ttravel\t8899\t889.999\tpreferred\ttpreferred\t100.000000"
          },
          new String[]{
              "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\tautomotive\t1111\t111.111\tpreferred\tapreferred\t94.874713",
              "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\tbusiness\t2222\t222.222\tpreferred\tbpreferred\t103.629399",
              "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\tentertainment\t3333\t333.333\tpreferred\tepreferred\t110.087299",
              "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\thealth\t4444\t444.444\tpreferred\thpreferred\t114.947403",
              "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\tmezzanine\t5555\t555.555\tpreferred\tmpreferred\t104.465767",
              "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\tnews\t6666\t666.666\tpreferred\tnpreferred\t102.851683",
              "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\tpremium\t7777\t777.777\tpreferred\tppreferred\t108.863011",
              "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\ttechnology\t8888\t888.888\tpreferred\ttpreferred\t111.356672",
              "2011-01-13T00:00:00.000Z\tspot\t1111\t111.111\ttravel\t8899\t889.999\tpreferred\ttpreferred\t106.236928"
          }
      );

      PagingOffset offset = query.getPagingOffset(QueryRunnerTestHelper.segmentId);
      List<Result<SelectResultValue>> expectedResults = toExpected(
          events,
          offset.startOffset(),
          offset.threshold()
      );
      verify(expectedResults, results);
    }
  }

  @Test
  public void testFullSelectNoResults()
  {
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        INTERVAL_0112_0114,
        descending,
        new AndDimFilter(
                Arrays.<DimFilter>asList(
                    new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot"),
                    new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "foo"),
                    new SelectorDimFilter("market_long", "1111"),
                    new SelectorDimFilter("market_float", "111.111")
                )
            ),
        QueryRunnerTestHelper.allGran,
        DefaultDimensionSpec.toSpec(Lists.<String>newArrayList()), Lists.<String>newArrayList(), new PagingSpec(null, 3),
        null
    );

    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, Maps.newHashMap()),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    List<Result<SelectResultValue>> expectedResults = Arrays.asList(
        new Result<SelectResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.<String, Integer>of(),
                Lists.<EventHolder>newArrayList()
            )
        )
    );

    verify(expectedResults, results);
  }

  @Test
  public void testFullSelectNoDimensionAndMetric()
  {
    SelectQuery query = new SelectQuery(
        new TableDataSource(QueryRunnerTestHelper.dataSource),
        INTERVAL_0112_0114,
        descending,
        null,
        QueryRunnerTestHelper.allGran,
        DefaultDimensionSpec.toSpec(Lists.<String>newArrayList("foo")),
        Lists.<String>newArrayList("foo2"),
        new PagingSpec(null, 3),
        null
    );

    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, Maps.newHashMap()),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    final List<List<Map<String, Object>>> events = toEvents(
        new String[]{
            EventHolder.timestampKey + ":TIME",
            "foo:NULL",
            "foo2:NULL"
        },
        EVENTS_0112_0114
    );

    PagingOffset offset = query.getPagingOffset(QueryRunnerTestHelper.segmentId);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        events,
        offset.startOffset(),
        offset.threshold()
    );
    verify(expectedResults, results);
  }

  private LinkedHashMap<String, Integer> toPagingIdentifier(int startDelta, boolean descending)
  {
    return Maps.newLinkedHashMap(
        ImmutableMap.of(
            QueryRunnerTestHelper.segmentId,
            PagingOffset.toOffset(startDelta, descending)
        )
    );
  }

  private List<List<Map<String, Object>>> toEvents(final String[] dimSpecs, final String[]... valueSet)
  {
    List<List<Map<String, Object>>> events = Lists.newArrayList();
    for (String[] values : valueSet) {
      events.add(
          Lists.newArrayList(
              Iterables.transform(
                  Arrays.asList(values), new Function<String, Map<String, Object>>()
                  {
                    @Override
                    public Map<String, Object> apply(String input)
                    {
                      Map<String, Object> event = Maps.newHashMap();
                      String[] values = input.split("\\t");
                      for (int i = 0; i < dimSpecs.length; i++) {
                        if (dimSpecs[i] == null || i >= dimSpecs.length) {
                          continue;
                        }
                        String[] specs = dimSpecs[i].split(":");
                        event.put(
                            specs[0],
                            specs[1].equals("TIME") ? new DateTime(values[i]) :
                            specs[1].equals("FLOAT") ? Float.valueOf(values[i]) :
                            specs[1].equals("DOUBLE") ? Double.valueOf(values[i]) :
                            specs[1].equals("LONG") ? Long.valueOf(values[i]) :
                            specs[1].equals("NULL") ? null :
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
      List<List<Map<String, Object>>> targets,
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
          holders.add(new EventHolder(QueryRunnerTestHelper.segmentId, newOffset--, group.get(i)));
        }
      } else {
        int end = Math.min(group.size(), offset + threshold);
        for (int i = offset; i < end; i++) {
          holders.add(new EventHolder(QueryRunnerTestHelper.segmentId, newOffset++, group.get(i)));
        }
      }
      int lastOffset = holders.isEmpty() ? offset : holders.get(holders.size() - 1).getOffset();
      expected.add(
          new Result(
              new DateTime(group.get(0).get(EventHolder.timestampKey)),
              new SelectResultValue(ImmutableMap.of(QueryRunnerTestHelper.segmentId, lastOffset), holders)
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

      String resultsStr = "\nExpected: " + expected + "\nActual: " + actual + "\nField: ";
      Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());

      for (Map.Entry<String, Integer> entry : expected.getValue().getPagingIdentifiers().entrySet()) {
        Assert.assertEquals(entry.getValue(), actual.getValue().getPagingIdentifiers().get(entry.getKey()));
      }

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
          Assert.assertEquals(resultsStr + ex.getKey(), ex.getValue(), actVal);
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
}
