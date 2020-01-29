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
import org.apache.druid.query.context.ResponseContext;
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
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 */
@RunWith(Parameterized.class)
public class SearchQueryRunnerTest extends InitializedNullHandlingTest
{
  private static final Logger LOG = new Logger(SearchQueryRunnerTest.class);
  private static final SearchQueryConfig CONFIG = new SearchQueryConfig();
  private static final SearchQueryQueryToolChest TOOL_CHEST = new SearchQueryQueryToolChest(CONFIG);
  private static final SearchStrategySelector SELECTOR = new SearchStrategySelector(Suppliers.ofInstance(CONFIG));

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(
        QueryRunnerTestHelper.makeQueryRunners(
            new SearchQueryRunnerFactory(
                SELECTOR,
                TOOL_CHEST,
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
    this.decoratedRunner = TOOL_CHEST.postMergeQueryDecoration(
        TOOL_CHEST.mergeResults(TOOL_CHEST.preMergeQueryDecoration(runner)));
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
                                    .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                    .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                    .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                    .query("a")
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "mezzanine", 279));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "travel", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "health", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "entertainment", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market", 186));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "a", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.PARTIAL_NULL_DIMENSION, "value", 186));

    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchWithCardinality()
  {
    final SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                          .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                          .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                          .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                          .query("a")
                                          .build();

    // double the value
    QueryRunner mergedRunner = TOOL_CHEST.mergeResults(
        new QueryRunner<Result<SearchResultValue>>()
        {
          @Override
          public Sequence<Result<SearchResultValue>> run(
              QueryPlus<Result<SearchResultValue>> queryPlus,
              ResponseContext responseContext
          )
          {
            final QueryPlus<Result<SearchResultValue>> queryPlus1 = queryPlus.withQuery(
                queryPlus.getQuery().withQuerySegmentSpec(
                    new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-01-12/2011-02-28")))
                )
            );
            final QueryPlus<Result<SearchResultValue>> queryPlus2 = queryPlus.withQuery(
                queryPlus.getQuery().withQuerySegmentSpec(
                    new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-03-01/2011-04-15")))
                )
            );
            return Sequences.concat(runner.run(queryPlus1, responseContext), runner.run(queryPlus2, responseContext));
          }
        }
    );

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", 91));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "mezzanine", 273));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "travel", 91));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "health", 91));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "entertainment", 91));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market", 182));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "a", 91));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.PARTIAL_NULL_DIMENSION, "value", 182));

    checkSearchQuery(searchQuery, mergedRunner, expectedHits);
  }

  @Test
  public void testSearchSameValueInMultiDims()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                    .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                    .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                    .dimensions(
                                        Arrays.asList(
                                            QueryRunnerTestHelper.PLACEMENT_DIMENSION,
                                            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION
                                        )
                                    )
                                    .query("e")
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.PLACEMENT_DIMENSION, "preferred", 1209));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "e", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "preferred", 1209));

    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchSameValueInMultiDims2()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                    .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                    .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                    .dimensions(
                                        Arrays.asList(
                                            QueryRunnerTestHelper.PLACEMENT_DIMENSION,
                                            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION
                                        )
                                    )
                                    .sortSpec(new SearchSortSpec(StringComparators.STRLEN))
                                    .query("e")
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "e", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.PLACEMENT_DIMENSION, "preferred", 1209));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "preferred", 1209));

    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testFragmentSearch()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                    .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                    .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                    .query(new FragmentSearchQuerySpec(Arrays.asList("auto", "ve")))
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", 93));

    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchWithDimensionQuality()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "mezzanine", 279));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "travel", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "health", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "entertainment", 93));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
              .granularity(QueryRunnerTestHelper.ALL_GRAN)
              .dimensions("quality")
              .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithDimensionProvider()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market", 186));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
              .granularity(QueryRunnerTestHelper.ALL_GRAN)
              .dimensions("market")
              .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithDimensionsQualityAndProvider()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "mezzanine", 279));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "travel", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "health", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "entertainment", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market", 186));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
              .granularity(QueryRunnerTestHelper.ALL_GRAN)
              .dimensions(
                  Arrays.asList(
                      QueryRunnerTestHelper.QUALITY_DIMENSION,
                      QueryRunnerTestHelper.MARKET_DIMENSION
                  )
              )
              .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithDimensionsPlacementAndProvider()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market", 186));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
              .granularity(QueryRunnerTestHelper.ALL_GRAN)
              .dimensions(
                  Arrays.asList(
                      QueryRunnerTestHelper.PLACEMENTISH_DIMENSION,
                      QueryRunnerTestHelper.MARKET_DIMENSION
                  )
              )
              .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
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
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, automotiveSnowman, 93));

    final LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("automotive", automotiveSnowman), false),
        true,
        null,
        true,
        true
    );

    SearchQuery query = Druids.newSearchQueryBuilder()
                              .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                              .granularity(QueryRunnerTestHelper.ALL_GRAN)
                              .filters(
                                  new ExtractionDimFilter(
                                      QueryRunnerTestHelper.QUALITY_DIMENSION,
                                      automotiveSnowman,
                                      lookupExtractionFn,
                                      null
                                  )
                              )
                              .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                              .dimensions(
                                  new ExtractionDimensionSpec(
                                      QueryRunnerTestHelper.QUALITY_DIMENSION,
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
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "mezzanine", 93));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
              .granularity(QueryRunnerTestHelper.ALL_GRAN)
              .filters(
                  new AndDimFilter(
                      Arrays.asList(
                          new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market", null),
                          new SelectorDimFilter(QueryRunnerTestHelper.QUALITY_DIMENSION, "mezzanine", null)
                      )))
              .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
              .dimensions(QueryRunnerTestHelper.QUALITY_DIMENSION)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithSingleFilter2()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market", 186));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
              .granularity(QueryRunnerTestHelper.ALL_GRAN)
              .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market")
              .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
              .dimensions(QueryRunnerTestHelper.MARKET_DIMENSION)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchMultiAndFilter()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", 93));

    DimFilter filter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null),
        new SelectorDimFilter(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", null)
    );

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
              .granularity(QueryRunnerTestHelper.ALL_GRAN)
              .filters(filter)
              .dimensions(QueryRunnerTestHelper.QUALITY_DIMENSION)
              .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
              .query("a")
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithMultiOrFilter()
  {
    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", 93));

    DimFilter filter = new OrDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.QUALITY_DIMENSION, "total_market", null),
        new SelectorDimFilter(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", null)
    );

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
              .granularity(QueryRunnerTestHelper.ALL_GRAN)
              .dimensions(QueryRunnerTestHelper.QUALITY_DIMENSION)
              .filters(filter)
              .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
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
              .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
              .granularity(QueryRunnerTestHelper.ALL_GRAN)
              .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
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
        new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market", null),
        new SelectorDimFilter(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", null)
    );

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
              .granularity(QueryRunnerTestHelper.ALL_GRAN)
              .filters(filter)
              .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
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
              .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
              .granularity(QueryRunnerTestHelper.ALL_GRAN)
              .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
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
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", 837));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market", 186));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.MARKET_DIMENSION, "upfront", 186));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
              .granularity(QueryRunnerTestHelper.ALL_GRAN)
              .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
              .dimensions(QueryRunnerTestHelper.MARKET_DIMENSION)
              .query("")
              .build(),
        expectedHits
    );
    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
              .granularity(QueryRunnerTestHelper.ALL_GRAN)
              .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
              .dimensions(QueryRunnerTestHelper.MARKET_DIMENSION)
              .build(),
        expectedHits
    );
  }

  @Test
  public void testSearchWithNumericSort()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                    .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                    .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                    .query("a")
                                    .sortSpec(new SearchSortSpec(StringComparators.NUMERIC))
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "a", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "entertainment", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "health", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "mezzanine", 279));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market", 186));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.QUALITY_DIMENSION, "travel", 93));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.PARTIAL_NULL_DIMENSION, "value", 186));

    checkSearchQuery(searchQuery, expectedHits);
  }

  @Test
  public void testSearchOnTime()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                    .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                    .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
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
                                    .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                    .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                    .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
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
                                    .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                    .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                    .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
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
                                            QueryRunnerTestHelper.INDEX_METRIC,
                                            QueryRunnerTestHelper.INDEX_METRIC,
                                            ValueType.DOUBLE
                                        )
                                    )
                                    .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                    .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                    .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                    .query("100.7")
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.INDEX_METRIC, "100.706057", 1));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.INDEX_METRIC, "100.775597", 1));
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
                                            QueryRunnerTestHelper.INDEX_METRIC,
                                            QueryRunnerTestHelper.INDEX_METRIC,
                                            jsExtractionFn
                                        )
                                    )
                                    .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                    .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                    .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                    .query("100.7")
                                    .build();

    List<SearchHit> expectedHits = new ArrayList<>();
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.INDEX_METRIC, "super-100.706057", 1));
    expectedHits.add(new SearchHit(QueryRunnerTestHelper.INDEX_METRIC, "super-100.775597", 1));
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
                                    .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                    .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                    .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                    // simulate when cardinality is big enough to fallback to cursorOnly strategy
                                    .context(ImmutableMap.of("searchStrategy", "cursorOnly"))
                                    .build();

    QueryRunnerFactory factory = new SearchQueryRunnerFactory(
        SELECTOR,
        TOOL_CHEST,
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
                                    .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                    .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                    .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
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
    Iterable<Result<SearchResultValue>> results = runner.run(QueryPlus.wrap(searchQuery)).toList();
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
