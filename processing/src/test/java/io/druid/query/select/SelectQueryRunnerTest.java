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
import com.google.common.collect.Sets;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
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
  // copied from druid.sample.tsv
  public static final String[] V_0112 = {
      "2011-01-12T00:00:00.000Z	spot	automotive	preferred	apreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	business	preferred	bpreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	entertainment	preferred	epreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	health	preferred	hpreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	mezzanine	preferred	mpreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	news	preferred	npreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	premium	preferred	ppreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	technology	preferred	tpreferred	100.000000",
      "2011-01-12T00:00:00.000Z	spot	travel	preferred	tpreferred	100.000000",
      "2011-01-12T00:00:00.000Z	total_market	mezzanine	preferred	mpreferred	1000.000000",
      "2011-01-12T00:00:00.000Z	total_market	premium	preferred	ppreferred	1000.000000",
      "2011-01-12T00:00:00.000Z	upfront	mezzanine	preferred	mpreferred	800.000000	value",
      "2011-01-12T00:00:00.000Z	upfront	premium	preferred	ppreferred	800.000000	value"
  };
  public static final String[] V_0113 = {
      "2011-01-13T00:00:00.000Z	spot	automotive	preferred	apreferred	94.874713",
      "2011-01-13T00:00:00.000Z	spot	business	preferred	bpreferred	103.629399",
      "2011-01-13T00:00:00.000Z	spot	entertainment	preferred	epreferred	110.087299",
      "2011-01-13T00:00:00.000Z	spot	health	preferred	hpreferred	114.947403",
      "2011-01-13T00:00:00.000Z	spot	mezzanine	preferred	mpreferred	104.465767",
      "2011-01-13T00:00:00.000Z	spot	news	preferred	npreferred	102.851683",
      "2011-01-13T00:00:00.000Z	spot	premium	preferred	ppreferred	108.863011",
      "2011-01-13T00:00:00.000Z	spot	technology	preferred	tpreferred	111.356672",
      "2011-01-13T00:00:00.000Z	spot	travel	preferred	tpreferred	106.236928",
      "2011-01-13T00:00:00.000Z	total_market	mezzanine	preferred	mpreferred	1040.945505",
      "2011-01-13T00:00:00.000Z	total_market	premium	preferred	ppreferred	1689.012875",
      "2011-01-13T00:00:00.000Z	upfront	mezzanine	preferred	mpreferred	826.060182	value",
      "2011-01-13T00:00:00.000Z	upfront	premium	preferred	ppreferred	1564.617729	value"
  };

  public static final QuerySegmentSpec I_0112_0114 = new LegacySegmentSpec(
      new Interval("2011-01-12/2011-01-14")
  );
  public static final String[] V_0112_0114 = ObjectArrays.concat(V_0112, V_0113, String.class);

  private static final SelectQueryQueryToolChest toolChest = new SelectQueryQueryToolChest(
      new DefaultObjectMapper(),
      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
  );

  @Parameterized.Parameters(name = "{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.cartesian(
        QueryRunnerTestHelper.makeQueryRunners(
            new SelectQueryRunnerFactory(
                toolChest,
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

  private Druids.SelectQueryBuilder newTestQuery() {
    return Druids.newSelectQueryBuilder()
                 .dataSource(new TableDataSource(QueryRunnerTestHelper.dataSource))
                 .dimensionSpecs(DefaultDimensionSpec.toSpec(Arrays.<String>asList()))
                 .metrics(Arrays.<String>asList())
                 .intervals(QueryRunnerTestHelper.fullOnInterval)
                 .granularity(QueryRunnerTestHelper.allGran)
                 .pagingSpec(PagingSpec.newSpec(3))
                 .descending(descending);
  }

  @Test
  public void testFullOnSelect()
  {
    SelectQuery query = newTestQuery()
        .intervals(I_0112_0114)
        .build();

    HashMap<String, Object> context = new HashMap<String, Object>();
    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, context),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    PagingOffset offset = query.getPagingOffset(QueryRunnerTestHelper.segmentId);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        toFullEvents(V_0112_0114),
        Lists.newArrayList("market", "quality", "placement", "placementish", "partial_null_column", "null_column"),
        Lists.newArrayList("index", "quality_uniques", "indexMin", "indexMaxPlusTen"),
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

    SelectQuery query = newTestQuery().intervals(I_0112_0114).build();
    for (int offset : expected) {
      List<Result<SelectResultValue>> results = Sequences.toList(
          runner.run(query, ImmutableMap.of()),
          Lists.<Result<SelectResultValue>>newArrayList()
      );

      Assert.assertEquals(1, results.size());

      SelectResultValue result = results.get(0).getValue();
      Map<String, Integer> pagingIdentifiers = result.getPagingIdentifiers();
      Assert.assertEquals(offset, pagingIdentifiers.get(QueryRunnerTestHelper.segmentId).intValue());

      Map<String, Integer> next = PagingSpec.next(pagingIdentifiers, descending);
      query = query.withPagingSpec(new PagingSpec(next, 3));
    }

    query = newTestQuery().intervals(I_0112_0114).build();
    for (int offset : expected) {
      List<Result<SelectResultValue>> results = Sequences.toList(
          runner.run(query, ImmutableMap.of()),
          Lists.<Result<SelectResultValue>>newArrayList()
      );

      Assert.assertEquals(1, results.size());

      SelectResultValue result = results.get(0).getValue();
      Map<String, Integer> pagingIdentifiers = result.getPagingIdentifiers();
      Assert.assertEquals(offset, pagingIdentifiers.get(QueryRunnerTestHelper.segmentId).intValue());

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
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(QueryRunnerTestHelper.marketDimension, "mar"),
                new ExtractionDimensionSpec(
                    QueryRunnerTestHelper.qualityDimension,
                    "qual",
                    new LookupExtractionFn(new MapLookupExtractor(map, true), false, null, true, false)
                ),
                new DefaultDimensionSpec(QueryRunnerTestHelper.placementDimension, "place")
            )
        )
        .build();

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
                Sets.newHashSet("mar", "qual", "place"),
                Sets.newHashSet("index", "quality_uniques", "indexMin", "indexMaxPlusTen"),
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
                Sets.newHashSet("mar", "qual", "place"),
                Sets.newHashSet("index", "quality_uniques", "indexMin", "indexMaxPlusTen"),
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
    SelectQuery query = newTestQuery()
        .intervals(I_0112_0114)
        .dimensionSpecs(DefaultDimensionSpec.toSpec(QueryRunnerTestHelper.marketDimension))
        .metrics(Arrays.asList(QueryRunnerTestHelper.indexMetric))
        .build();

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
                null,
                null,
                null,
                QueryRunnerTestHelper.indexMetric + ":FLOAT"
            },
            V_0112_0114
        ),
        Lists.newArrayList("market"),
        Lists.<String>newArrayList("index"),
        offset.startOffset(),
        offset.threshold()
    );
    verify(expectedResults, results);
  }

  @Test
  public void testSelectPagination()
  {
    SelectQuery query = newTestQuery()
        .intervals(I_0112_0114)
        .dimensionSpecs(DefaultDimensionSpec.toSpec(QueryRunnerTestHelper.qualityDimension))
        .metrics(Arrays.asList(QueryRunnerTestHelper.indexMetric))
        .pagingSpec(new PagingSpec(toPagingIdentifier(3, descending), 3))
        .build();

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
            V_0112_0114
        ),
        Lists.newArrayList("quality"),
        Lists.<String>newArrayList("index"),
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
          .intervals(I_0112_0114)
          .filters(new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot", null))
          .granularity(QueryRunnerTestHelper.dayGran)
          .dimensionSpecs(DefaultDimensionSpec.toSpec(QueryRunnerTestHelper.qualityDimension))
          .metrics(Lists.<String>newArrayList(QueryRunnerTestHelper.indexMetric))
          .pagingSpec(new PagingSpec(toPagingIdentifier(param[0], descending), param[1]))
          .build();

      HashMap<String, Object> context = new HashMap<String, Object>();
      Iterable<Result<SelectResultValue>> results = Sequences.toList(
          runner.run(query, context),
          Lists.<Result<SelectResultValue>>newArrayList()
      );

      final List<List<Map<String, Object>>> events = toEvents(
          new String[]{
              EventHolder.timestampKey + ":TIME",
              null,
              QueryRunnerTestHelper.qualityDimension + ":STRING",
              null,
              null,
              QueryRunnerTestHelper.indexMetric + ":FLOAT"
          },
          // filtered values with day granularity
          new String[]{
              "2011-01-12T00:00:00.000Z	spot	automotive	preferred	apreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	business	preferred	bpreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	entertainment	preferred	epreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	health	preferred	hpreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	mezzanine	preferred	mpreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	news	preferred	npreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	premium	preferred	ppreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	technology	preferred	tpreferred	100.000000",
              "2011-01-12T00:00:00.000Z	spot	travel	preferred	tpreferred	100.000000"
          },
          new String[]{
              "2011-01-13T00:00:00.000Z	spot	automotive	preferred	apreferred	94.874713",
              "2011-01-13T00:00:00.000Z	spot	business	preferred	bpreferred	103.629399",
              "2011-01-13T00:00:00.000Z	spot	entertainment	preferred	epreferred	110.087299",
              "2011-01-13T00:00:00.000Z	spot	health	preferred	hpreferred	114.947403",
              "2011-01-13T00:00:00.000Z	spot	mezzanine	preferred	mpreferred	104.465767",
              "2011-01-13T00:00:00.000Z	spot	news	preferred	npreferred	102.851683",
              "2011-01-13T00:00:00.000Z	spot	premium	preferred	ppreferred	108.863011",
              "2011-01-13T00:00:00.000Z	spot	technology	preferred	tpreferred	111.356672",
              "2011-01-13T00:00:00.000Z	spot	travel	preferred	tpreferred	106.236928"
          }
      );

      PagingOffset offset = query.getPagingOffset(QueryRunnerTestHelper.segmentId);
      List<Result<SelectResultValue>> expectedResults = toExpected(
          events,
          Lists.newArrayList("quality"),
          Lists.<String>newArrayList("index"),
          offset.startOffset(),
          offset.threshold()
      );
      verify(expectedResults, results);
    }
  }

  @Test
  public void testSelectWithFilterLookupExtractionFn () {

    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("total_market","replaced");
    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, true);
    SelectQuery query = newTestQuery()
        .intervals(I_0112_0114)
        .filters(new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "replaced", lookupExtractionFn))
        .granularity(QueryRunnerTestHelper.dayGran)
        .dimensionSpecs(DefaultDimensionSpec.toSpec(QueryRunnerTestHelper.qualityDimension))
        .metrics(Lists.<String>newArrayList(QueryRunnerTestHelper.indexMetric))
        .build();

    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, Maps.newHashMap()),
        Lists.<Result<SelectResultValue>>newArrayList()
    );
    Iterable<Result<SelectResultValue>> resultsOptimize = Sequences.toList(
        toolChest.postMergeQueryDecoration(toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner))).
                run(query, Maps.<String, Object>newHashMap()), Lists.<Result<SelectResultValue>>newArrayList()
    );

    final List<List<Map<String, Object>>> events = toEvents(
        new String[]{
            EventHolder.timestampKey + ":TIME",
            null,
            QueryRunnerTestHelper.qualityDimension + ":STRING",
            null,
            null,
            QueryRunnerTestHelper.indexMetric + ":FLOAT"
        },
        // filtered values with day granularity
        new String[]{
            "2011-01-12T00:00:00.000Z	total_market	mezzanine	preferred	mpreferred	1000.000000",
            "2011-01-12T00:00:00.000Z	total_market	premium	preferred	ppreferred	1000.000000"
        },
        new String[]{
            "2011-01-13T00:00:00.000Z	total_market	mezzanine	preferred	mpreferred	1040.945505",
            "2011-01-13T00:00:00.000Z	total_market	premium	preferred	ppreferred	1689.012875"
        }
    );

    PagingOffset offset = query.getPagingOffset(QueryRunnerTestHelper.segmentId);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        events,
        Lists.newArrayList(QueryRunnerTestHelper.qualityDimension),
        Lists.<String>newArrayList(QueryRunnerTestHelper.indexMetric),
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

    Iterable<Result<SelectResultValue>> results = Sequences.toList(
        runner.run(query, Maps.newHashMap()),
        Lists.<Result<SelectResultValue>>newArrayList()
    );

    List<Result<SelectResultValue>> expectedResults = Arrays.asList(
        new Result<SelectResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SelectResultValue(
                ImmutableMap.<String, Integer>of(),
                Sets.newHashSet("market", "quality", "placement", "placementish", "partial_null_column", "null_column"),
                Sets.newHashSet("index", "quality_uniques", "indexMin", "indexMaxPlusTen"),
                Lists.<EventHolder>newArrayList()
            )
        )
    );

    verify(expectedResults, populateNullColumnAtLastForQueryableIndexCase(results, "null_column"));
  }

  @Test
  public void testFullSelectNoDimensionAndMetric()
  {
    SelectQuery query = newTestQuery()
        .intervals(I_0112_0114)
        .dimensionSpecs(DefaultDimensionSpec.toSpec("foo"))
        .metrics(Lists.<String>newArrayList("foo2"))
        .build();

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
        V_0112_0114
    );

    PagingOffset offset = query.getPagingOffset(QueryRunnerTestHelper.segmentId);
    List<Result<SelectResultValue>> expectedResults = toExpected(
        events,
        Lists.newArrayList("foo"),
        Lists.<String>newArrayList("foo2"),
        offset.startOffset(),
        offset.threshold()
    );
    verify(expectedResults, results);
  }

  private Map<String, Integer> toPagingIdentifier(int startDelta, boolean descending)
  {
    return ImmutableMap.of(
        QueryRunnerTestHelper.segmentId,
        PagingOffset.toOffset(startDelta, descending)
    );
  }

  private List<List<Map<String, Object>>> toFullEvents(final String[]... valueSet)
  {
    return toEvents(new String[]{EventHolder.timestampKey + ":TIME",
                                 QueryRunnerTestHelper.marketDimension + ":STRING",
                                 QueryRunnerTestHelper.qualityDimension + ":STRING",
                                 QueryRunnerTestHelper.placementDimension + ":STRING",
                                 QueryRunnerTestHelper.placementishDimension + ":STRINGS",
                                 QueryRunnerTestHelper.indexMetric + ":FLOAT",
                                 QueryRunnerTestHelper.partialNullDimension + ":STRING"},
                    valueSet);
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
    }
    return events;
  }

  private List<Result<SelectResultValue>> toExpected(
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
              new SelectResultValue(
                  ImmutableMap.of(QueryRunnerTestHelper.segmentId, lastOffset),
                  Sets.<String>newHashSet(dimensions),
                  Sets.<String>newHashSet(metrics),
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
