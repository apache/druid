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

package org.apache.druid.query.search;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.extraction.TimeFormatExtractionFn;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ExtractionDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class SearchQueryRunnerTest
{
  private static final Logger LOG = new Logger(SearchQueryRunnerTest.class);
  private static final SearchQueryConfig config = new SearchQueryConfig();
  private static final SearchQueryQueryToolChest toolChest = new SearchQueryQueryToolChest(
      config,
      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
  );
  private static final SearchStrategySelector selector = new SearchStrategySelector(Suppliers.ofInstance(config));

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(
        QueryRunnerTestHelper.makeQueryRunners(
            new SearchQueryRunnerFactory(
                selector,
                toolChest,
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
    );
  }

  private final QueryRunner runner;
  private final QueryRunner decoratedRunner;

  public SearchQueryRunnerTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
    this.decoratedRunner = toolChest.postMergeQueryDecoration(
        toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner)));
  }

  @Test
  public void testSearchHitSerDe() throws Exception
  {
    for (SearchHit hit : Arrays.asList(new SearchHit("dim1", "val1"), new SearchHit("dim2", "val2", 3))) {
      SearchHit read = TestHelper.makeJsonMapper().readValue(
          TestHelper.makeJsonMapper().writeValueAsString(hit),
          SearchHit.class
      );
      Assert.assertEquals(hit, read);
      if (hit.getCount() == null) {
        Assert.assertNull(read.getCount());
      } else {
        Assert.assertEquals(hit.getCount(), read.getCount());
      }
    }
  }

  @Test
  public void testSearch()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                    .query("a")
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "mezzanine", 279));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "travel", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "health", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "entertainment", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 186));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementishDimension, "a", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.partialNullDimension, "value", 186));

    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchWithCardinality()
  {
    final SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                          .dataSource(QueryRunnerTestHelper.dataSource)
                                          .granularity(QueryRunnerTestHelper.allGran)
                                          .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                          .query("a")
                                          .build();

    // double the value
    QueryRunner mergedRunner = toolChest.mergeResults(
        new QueryRunner<Result<SearchResultValue>>()
        {
          @Override
          public Sequence<Result<SearchResultValue>> run(
              QueryPlus<Result<SearchResultValue>> queryPlus,
              Map<String, Object> responseContext
          )
          {
            final QueryPlus<Result<SearchResultValue>> queryPlus1 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-01-12/2011-02-28")))
            );
            final QueryPlus<Result<SearchResultValue>> queryPlus2 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-03-01/2011-04-15")))
            );
            return Sequences.concat(runner.run(queryPlus1, responseContext), runner.run(queryPlus2, responseContext));
          }
        }
    );

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 91));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "mezzanine", 273));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "travel", 91));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "health", 91));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "entertainment", 91));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 182));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementishDimension, "a", 91));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.partialNullDimension, "value", 182));

    checkSearchQuery(searchQuery, mergedRunner, expectedHits);
  }

  @Test
  public void testSearchSameValueInMultiDims()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                    .dimensions(
                                        Arrays.asList(
                                            QueryRunnerTestHelper.placementDimension,
                                            QueryRunnerTestHelper.placementishDimension
                                        )
                                    )
                                    .query("e")
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementDimension, "preferred", 1209));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementishDimension, "e", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementishDimension, "preferred", 1209));

    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchSameValueInMultiDims2()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                    .dimensions(
                                        Arrays.asList(
                                            QueryRunnerTestHelper.placementDimension,
                                            QueryRunnerTestHelper.placementishDimension
                                        )
                                    )
                                    .sortSpec(new SearchSortSpec(StringComparators.STRLEN))
                                    .query("e")
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementishDimension, "e", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementDimension, "preferred", 1209));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementishDimension, "preferred", 1209));

    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testFragmentSearch()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                    .query(new FragmentSearchQuerySpec(Arrays.asList("auto", "ve")))
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 93));

    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchWithDimensionQuality()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "mezzanine", 279));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "travel", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "health", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "entertainment", 93));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .dimensions("quality")
              .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithDimensionProvider()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 186));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .dimensions("market")
              .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithDimensionsQualityAndProvider()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "mezzanine", 279));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "travel", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "health", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "entertainment", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 186));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .dimensions(
                  Arrays.asList(
                      QueryRunnerTestHelper.qualityDimension,
                      QueryRunnerTestHelper.marketDimension
                  )
              )
              .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithDimensionsPlacementAndProvider()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 186));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .dimensions(
                  Arrays.asList(
                      QueryRunnerTestHelper.placementishDimension,
                      QueryRunnerTestHelper.marketDimension
                  )
              )
              .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
              .query("mark")
              .build(),
        expectedHits
    );
  }


  @Test
  public void testSearchWithExtractionFilter1()
  {
    final String automotiveSnowman = "automotive☃";
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, automotiveSnowman, 93));

    final LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("automotive", automotiveSnowman), false),
        true,
        null,
        true,
        true
    );

    SearchQuery query = Druids.newSearchQueryBuilder()
                              .dataSource(QueryRunnerTestHelper.dataSource)
                              .granularity(QueryRunnerTestHelper.allGran)
                              .filters(
                                  new ExtractionDimFilter(
                                      QueryRunnerTestHelper.qualityDimension,
                                      automotiveSnowman,
                                      lookupExtractionFn,
                                      null
                                  )
                              )
                              .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                              .dimensions(
                                  new ExtractionDimensionSpec(
                                      QueryRunnerTestHelper.qualityDimension,
                                      null,
                                      lookupExtractionFn
                                  )
                              )
                              .query("☃")
                              .build();

    checkSearchQuery(query, expectedHits);
  }

  @Test
  public void testSearchWithSingleFilter1()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "mezzanine", 93));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .filters(
                  new AndDimFilter(
                      Arrays.asList(
                          new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "total_market", null),
                          new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "mezzanine", null)
                      )))
              .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
              .dimensions(QueryRunnerTestHelper.qualityDimension)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithSingleFilter2()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 186));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .filters(QueryRunnerTestHelper.marketDimension, "total_market")
              .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
              .dimensions(QueryRunnerTestHelper.marketDimension)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchMultiAndFilter()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 93));

    DimFilter filter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot", null),
        new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "automotive", null)
    );

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .filters(filter)
              .dimensions(QueryRunnerTestHelper.qualityDimension)
              .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithMultiOrFilter()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 93));

    DimFilter filter = new OrDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "total_market", null),
        new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "automotive", null)
    );

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .dimensions(QueryRunnerTestHelper.qualityDimension)
              .filters(filter)
              .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithEmptyResults()
  {
    List<SearchHit> expectedHits = new ArrayList<>();

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
              .query("abcd123")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithFilterEmptyResults()
  {
    List<SearchHit> expectedHits = new ArrayList<>();

    DimFilter filter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "total_market", null),
        new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "automotive", null)
    );

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .filters(filter)
              .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
              .query("a")
              .build(),
        expectedHits
    );
  }


  @Test
  public void testSearchNonExistingDimension()
  {
    List<SearchHit> expectedHits = new ArrayList<>();

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
              .dimensions("does_not_exist")
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchAll()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "spot", 837));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 186));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "upfront", 186));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
              .dimensions(QueryRunnerTestHelper.marketDimension)
              .query("")
              .build(),
        expectedHits
    );
    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
              .dimensions(QueryRunnerTestHelper.marketDimension)
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithNumericSort()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                    .query("a")
                                    .sortSpec(new SearchSortSpec(StringComparators.NUMERIC))
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.placementishDimension, "a", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "automotive", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "entertainment", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "health", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "mezzanine", 279));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.marketDimension, "total_market", 186));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.qualityDimension, "travel", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.partialNullDimension, "value", 186));

    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchOnTime()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                    .query("Friday")
                                    .dimensions(new ExtractionDimensionSpec(
                                        ColumnHolder.TIME_COLUMN_NAME,
                                        "__time2",
                                        new TimeFormatExtractionFn(
                                            "EEEE",
                                            null,
                                            null,
                                            null,
                                            false
                                        )
                                    ))
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit("__time2", "Friday", 169));

    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchOnLongColumn()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dimensions(
                                        new DefaultDimensionSpec(
                                            ColumnHolder.TIME_COLUMN_NAME,
                                            ColumnHolder.TIME_COLUMN_NAME,
                                            ValueType.LONG
                                        )
                                    )
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                    .query("1297123200000")
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(ColumnHolder.TIME_COLUMN_NAME, "1297123200000", 13));
    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchOnLongColumnWithExFn()
  {
    String jsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn jsExtractionFn = new JavaScriptExtractionFn(jsFn, false, JavaScriptConfig.getEnabledInstance());

    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dimensions(
                                        new ExtractionDimensionSpec(
                                            ColumnHolder.TIME_COLUMN_NAME,
                                            ColumnHolder.TIME_COLUMN_NAME,
                                            jsExtractionFn
                                        )
                                    )
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                    .query("1297123200000")
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(ColumnHolder.TIME_COLUMN_NAME, "super-1297123200000", 13));
    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchOnFloatColumn()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dimensions(
                                        new DefaultDimensionSpec(
                                            QueryRunnerTestHelper.indexMetric,
                                            QueryRunnerTestHelper.indexMetric,
                                            ValueType.DOUBLE
                                        )
                                    )
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                    .query("100.7")
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.indexMetric, "100.706057", 1));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.indexMetric, "100.775597", 1));
    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchOnFloatColumnWithExFn()
  {
    String jsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn jsExtractionFn = new JavaScriptExtractionFn(jsFn, false, JavaScriptConfig.getEnabledInstance());

    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dimensions(
                                        new ExtractionDimensionSpec(
                                            QueryRunnerTestHelper.indexMetric,
                                            QueryRunnerTestHelper.indexMetric,
                                            jsExtractionFn
                                        )
                                    )
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                    .query("100.7")
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.indexMetric, "super-100.706057", 1));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.indexMetric, "super-100.775597", 1));
    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchWithNullValueInDimension() throws Exception
  {
    IncrementalIndex<Aggregator> index = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMinTimestamp(DateTimes.of("2011-01-12T00:00:00.000Z").getMillis())
                .build()
        )
        .setMaxRowCount(10)
        .buildOnheap();

    index.add(
        new MapBasedInputRow(
            1481871600000L,
            Arrays.asList("name", "host"),
            ImmutableMap.of("name", "name1", "host", "host")
        )
    );
    index.add(
        new MapBasedInputRow(
            1481871670000L,
            Arrays.asList("name", "table"),
            ImmutableMap.of("name", "name2", "table", "table")
        )
    );

    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dimensions(
                                        new DefaultDimensionSpec("table", "table")
                                    )
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                    // simulate when cardinality is big enough to fallback to cursorOnly strategy
                                    .context(ImmutableMap.of("searchStrategy", "cursorOnly"))
                                    .build();

    QueryRunnerFactory factory = new SearchQueryRunnerFactory(
        selector,
        toolChest,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    QueryRunner runner = factory.createRunner(
        new QueryableIndexSegment(TestIndex.persistRealtimeAndLoadMMapped(index), SegmentId.dummy("asdf"))
    );
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit("table", "table", 1));
    expectedHits.add(new SearchHit("table", NullHandling.defaultStringValue(), 1));
    checkSearchQuery(searchQuery, runner, expectedHits);
  }

  @Test
  public void testSearchWithNotExistedDimension()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dimensions(
                                        new DefaultDimensionSpec("asdf", "asdf")
                                    )
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                    .build();

    List<SearchHit> noHit = new ArrayList<>();
    checkSearchQuery(searchQuery, noHit);
  }

  private void checkSearchQuery(Query searchQuery, List<SearchHit> expectedResults)
  {
    checkSearchQuery(searchQuery, runner, expectedResults);
    checkSearchQuery(searchQuery, decoratedRunner, expectedResults);
  }

  private void checkSearchQuery(Query searchQuery, QueryRunner runner, List<SearchHit> expectedResults)
  {
    Iterable<Result<SearchResultValue>> results = runner.run(QueryPlus.wrap(searchQuery), ImmutableMap.of()).toList();
    List<SearchHit> copy = new ArrayList<>(expectedResults);
    for (Result<SearchResultValue> result : results) {
      Assert.assertEquals(DateTimes.of("2011-01-12T00:00:00.000Z"), result.getTimestamp());
      Assert.assertTrue(result.getValue() instanceof Iterable);

      Iterable<SearchHit> resultValues = result.getValue();
      for (SearchHit resultValue : resultValues) {
        int index = copy.indexOf(resultValue);
        if (index < 0) {
          fail(
              expectedResults,
              results,
              "No result found containing " + resultValue.getDimension() + " and " + resultValue.getValue()
          );
        }
        SearchHit expected = copy.remove(index);
        if (!resultValue.toString().equals(expected.toString())) {
          fail(
              expectedResults,
              results,
              "Invalid count for " + resultValue + ".. which was expected to be " + expected.getCount()
          );
        }
      }
    }
    if (!copy.isEmpty()) {
      fail(expectedResults, results, "Some expected results are not shown: " + copy);
    }
  }

  private void fail(
      List<SearchHit> expectedResults,
      Iterable<Result<SearchResultValue>> results, String errorMsg
  )
  {
    LOG.info("Expected..");
    for (SearchHit expected : expectedResults) {
      LOG.info(expected.toString());
    }
    LOG.info("Result..");
    for (Result<SearchResultValue> r : results) {
      for (SearchHit v : r.getValue()) {
        LOG.info(v.toString());
      }
    }
    Assert.fail(errorMsg);
  }
}
