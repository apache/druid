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

package io.druid.query.search;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.LookupExtractionFn;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ExtractionDimFilter;
import io.druid.query.search.search.FragmentSearchQuerySpec;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQueryConfig;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
@RunWith(Parameterized.class)
public class SearchQueryRunnerTest
{
  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(
        QueryRunnerTestHelper.makeQueryRunners(
            new SearchQueryRunnerFactory(
                new SearchQueryQueryToolChest(
                    new SearchQueryConfig(),
                    QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                ),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
    );
  }

  private final QueryRunner runner;

  public SearchQueryRunnerTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  @Test
  public void testSearch()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnInterval)
                                    .query("a")
                                    .build();

    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    expectedResults.put(
        QueryRunnerTestHelper.qualityDimension,
        Sets.newHashSet("automotive", "mezzanine", "travel", "health", "entertainment")
    );
    expectedResults.put(QueryRunnerTestHelper.marketDimension, Sets.newHashSet("total_market"));
    expectedResults.put(QueryRunnerTestHelper.placementishDimension, Sets.newHashSet("a"));
    expectedResults.put("partial_null_column", Sets.newHashSet("value"));
    checkSearchQuery(searchQuery, expectedResults);
  }

  @Test
  public void testSearchSameValueInMultiDims()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnInterval)
                                    .dimensions(
                                        Arrays.asList(
                                            QueryRunnerTestHelper.placementDimension,
                                            QueryRunnerTestHelper.placementishDimension
                                        )
                                    )
                                    .query("e")
                                    .build();

    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    expectedResults.put(QueryRunnerTestHelper.placementDimension, Sets.newHashSet("preferred"));
    expectedResults.put(QueryRunnerTestHelper.placementishDimension, Sets.newHashSet("e", "preferred"));

    checkSearchQuery(searchQuery, expectedResults);
  }

  @Test
  public void testFragmentSearch()
  {
    SearchQuery searchQuery = Druids.newSearchQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.allGran)
                                    .intervals(QueryRunnerTestHelper.fullOnInterval)
                                    .query(new FragmentSearchQuerySpec(Arrays.asList("auto", "ve")))
                                    .build();

    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    expectedResults.put(QueryRunnerTestHelper.qualityDimension, Sets.newHashSet("automotive"));

    checkSearchQuery(searchQuery, expectedResults);
  }

  @Test
  public void testSearchWithDimensionQuality()
  {
    Map<String, Set<String>> expectedResults = new HashMap<String, Set<String>>();
    expectedResults.put(
        QueryRunnerTestHelper.qualityDimension, new HashSet<String>(
            Arrays.asList(
                "automotive", "mezzanine", "travel", "health", "entertainment"
            )
        )
    );

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .dimensions("quality")
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("a")
              .build(),
        expectedResults
    );
  }

  @Test
  public void testSearchWithDimensionProvider()
  {
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    expectedResults.put(QueryRunnerTestHelper.marketDimension, new HashSet<String>(Arrays.asList("total_market")));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .dimensions("market")
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("a")
              .build(),
        expectedResults
    );
  }

  @Test
  public void testSearchWithDimensionsQualityAndProvider()
  {
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    expectedResults.putAll(
        ImmutableMap.<String, Set<String>>of(
            QueryRunnerTestHelper.qualityDimension,
            new HashSet<String>(
                Arrays.asList(
                    "automotive", "mezzanine", "travel", "health", "entertainment"
                )
            ),
            QueryRunnerTestHelper.marketDimension,
            new HashSet<String>(
                Arrays.asList("total_market")
            )
        )
    );

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
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("a")
              .build(),
        expectedResults
    );
  }

  @Test
  public void testSearchWithDimensionsPlacementAndProvider()
  {
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    expectedResults.put(QueryRunnerTestHelper.marketDimension, new HashSet<String>(Arrays.asList("total_market")));

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
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("mark")
              .build(),
        expectedResults
    );
  }


  @Test
  public void testSearchWithExtractionFilter1()
  {
    final String automotiveSnowman = "automotive☃";
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    expectedResults.put(
        QueryRunnerTestHelper.qualityDimension, new HashSet<String>(Arrays.asList(automotiveSnowman))
    );


    final LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(
      new MapLookupExtractor(ImmutableMap.of("automotive", automotiveSnowman), false),
      true,
      null,
      true,
      false
    );

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .filters(new ExtractionDimFilter(QueryRunnerTestHelper.qualityDimension, automotiveSnowman, lookupExtractionFn, null))
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .dimensions(
                  new ExtractionDimensionSpec(
                      QueryRunnerTestHelper.qualityDimension,
                      null,
                      lookupExtractionFn,
                      null
                  )
              )
              .query("☃")
              .build(),
        expectedResults
    );
  }

  @Test
  public void testSearchWithSingleFilter1()
  {
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    expectedResults.put(
        QueryRunnerTestHelper.qualityDimension, new HashSet<String>(Arrays.asList("automotive"))
    );

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .filters(QueryRunnerTestHelper.qualityDimension, "automotive")
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .dimensions(QueryRunnerTestHelper.qualityDimension)
              .query("a")
              .build(),
        expectedResults
    );
  }

  @Test
  public void testSearchWithSingleFilter2()
  {
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    expectedResults.put(QueryRunnerTestHelper.marketDimension, new HashSet<String>(Arrays.asList("total_market")));

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .filters(QueryRunnerTestHelper.marketDimension, "total_market")
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .dimensions(QueryRunnerTestHelper.marketDimension)
              .query("a")
              .build(),
        expectedResults
    );
  }

  @Test
  public void testSearchMultiAndFilter()
  {
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    expectedResults.put(QueryRunnerTestHelper.qualityDimension, new HashSet<String>(Arrays.asList("automotive")));

    DimFilter filter = Druids.newAndDimFilterBuilder()
                             .fields(
                                 Arrays.<DimFilter>asList(
                                     Druids.newSelectorDimFilterBuilder()
                                           .dimension(QueryRunnerTestHelper.marketDimension)
                                           .value("spot")
                                           .build(),
                                     Druids.newSelectorDimFilterBuilder()
                                           .dimension(QueryRunnerTestHelper.qualityDimension)
                                           .value("automotive")
                                           .build()
                                 )
                             )
                             .build();

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .filters(filter)
              .dimensions(QueryRunnerTestHelper.qualityDimension)
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("a")
              .build(),
        expectedResults
    );
  }

  @Test
  public void testSearchWithMultiOrFilter()
  {
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    expectedResults.put(QueryRunnerTestHelper.qualityDimension, new HashSet<String>(Arrays.asList("automotive")));

    DimFilter filter = Druids.newOrDimFilterBuilder()
                             .fields(
                                 Arrays.<DimFilter>asList(
                                     Druids.newSelectorDimFilterBuilder()
                                           .dimension(QueryRunnerTestHelper.qualityDimension)
                                           .value("total_market")
                                           .build(),
                                     Druids.newSelectorDimFilterBuilder()
                                           .dimension(QueryRunnerTestHelper.qualityDimension)
                                           .value("automotive")
                                           .build()
                                 )
                             )
                             .build();

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .dimensions(QueryRunnerTestHelper.qualityDimension)
              .filters(filter)
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("a")
              .build(),
        expectedResults
    );
  }

  @Test
  public void testSearchWithEmptyResults()
  {
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("abcd123")
              .build(),
        expectedResults
    );
  }

  @Test
  public void testSearchWithFilterEmptyResults()
  {
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    DimFilter filter = Druids.newAndDimFilterBuilder()
                             .fields(
                                 Arrays.<DimFilter>asList(
                                     Druids.newSelectorDimFilterBuilder()
                                           .dimension(QueryRunnerTestHelper.marketDimension)
                                           .value("total_market")
                                           .build(),
                                     Druids.newSelectorDimFilterBuilder()
                                           .dimension(QueryRunnerTestHelper.qualityDimension)
                                           .value("automotive")
                                           .build()
                                 )
                             )
                             .build();

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .filters(filter)
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .query("a")
              .build(),
        expectedResults
    );
  }


  @Test
  public void testSearchNonExistingDimension()
  {
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    checkSearchQuery(
        Druids.newSearchQueryBuilder()
              .dataSource(QueryRunnerTestHelper.dataSource)
              .granularity(QueryRunnerTestHelper.allGran)
              .intervals(QueryRunnerTestHelper.fullOnInterval)
              .dimensions("does_not_exist")
              .query("a")
              .build(),
        expectedResults
    );
  }

  private void checkSearchQuery(SearchQuery searchQuery, Map<String, Set<String>> expectedResults)
  {
    HashMap<String,List> context = new HashMap<String, List>();
    Iterable<Result<SearchResultValue>> results = Sequences.toList(
        runner.run(searchQuery, context),
        Lists.<Result<SearchResultValue>>newArrayList()
    );

    for (Result<SearchResultValue> result : results) {
      Assert.assertEquals(new DateTime("2011-01-12T00:00:00.000Z"), result.getTimestamp());
      Assert.assertTrue(result.getValue() instanceof Iterable);

      Iterable<SearchHit> resultValues = result.getValue();
      for (SearchHit resultValue : resultValues) {
        String dimension = resultValue.getDimension();
        String theValue = resultValue.getValue();
        Assert.assertTrue(
            String.format("Result had unknown dimension[%s]", dimension),
            expectedResults.containsKey(dimension)
        );

        Set<String> expectedSet = expectedResults.get(dimension);
        Assert.assertTrue(
            String.format("Couldn't remove dim[%s], value[%s]", dimension, theValue), expectedSet.remove(theValue)
        );
      }
    }

    for (Map.Entry<String, Set<String>> entry : expectedResults.entrySet()) {
      Assert.assertTrue(
          String.format(
              "Dimension[%s] should have had everything removed, still has[%s]", entry.getKey(), entry.getValue()
          ),
          entry.getValue().isEmpty()
      );
    }
  }
}
