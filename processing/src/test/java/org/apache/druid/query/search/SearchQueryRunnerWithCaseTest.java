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
import com.google.common.collect.Sets;
import com.google.common.io.CharSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Druids.SearchQueryBuilder;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@RunWith(Parameterized.class)
public class SearchQueryRunnerWithCaseTest extends InitializedNullHandlingTest
{
  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder()
  {
    final SearchQueryConfig[] configs = new SearchQueryConfig[3];
    configs[0] = new SearchQueryConfig();
    configs[0].setSearchStrategy(UseIndexesStrategy.NAME);
    configs[1] = new SearchQueryConfig();
    configs[1].setSearchStrategy(CursorOnlyStrategy.NAME);
    // test auto to ensure that it doesn't explode
    configs[2] = new SearchQueryConfig();
    configs[2].setSearchStrategy("auto");

    CharSource input = CharSource.wrap(
        "2011-01-12T00:00:00.000Z\tspot\tAutoMotive\t1000\t10000.0\t10000.0\t100000\t10\t10.0\t10.0\tPREFERRED\ta\u0001preferred\t100.000000\n" +
        "2011-01-12T00:00:00.000Z\tSPot\tbusiness\t1100\t11000.0\t11000.0\t110000\t20\t20.0\t20.0\tpreferred\tb\u0001Preferred\t100.000000\n" +
        "2011-01-12T00:00:00.000Z\tspot\tentertainment\t1200\t12000.0\t12000.0\t120000\t\t\t\tPREFERRed\te\u0001preferred\t100.000000\n" +
        "2011-01-13T00:00:00.000Z\tspot\tautomotive\t1000\t10000.0\t10000.0\t100000\t10\t10.0\t10.0\tpreferred\ta\u0001preferred\t94.874713"
    );

    IncrementalIndex index1 = TestIndex.makeRealtimeIndex(input);
    IncrementalIndex index2 = TestIndex.makeRealtimeIndex(input);

    QueryableIndex index3 = TestIndex.persistRealtimeAndLoadMMapped(index1);
    QueryableIndex index4 = TestIndex.persistRealtimeAndLoadMMapped(index2);

    final List<QueryRunner<Result<SearchResultValue>>> runners = new ArrayList<>();
    for (SearchQueryConfig config : configs) {
      runners.addAll(Arrays.asList(
          QueryRunnerTestHelper.makeQueryRunner(
              makeRunnerFactory(config),
              SegmentId.dummy("index1"),
              new IncrementalIndexSegment(index1, SegmentId.dummy("index1")),
              "index1"
          ),
          QueryRunnerTestHelper.makeQueryRunner(
              makeRunnerFactory(config),
              SegmentId.dummy("index2"),
              new IncrementalIndexSegment(index2, SegmentId.dummy("index2")),
              "index2"
          ),
          QueryRunnerTestHelper.makeQueryRunner(
              makeRunnerFactory(config),
              SegmentId.dummy("index3"),
              new QueryableIndexSegment(index3, SegmentId.dummy("index3")),
              "index3"
          ),
          QueryRunnerTestHelper.makeQueryRunner(
              makeRunnerFactory(config),
              SegmentId.dummy("index4"),
              new QueryableIndexSegment(index4, SegmentId.dummy("index4")),
              "index4"
          )
      ));
    }

    return QueryRunnerTestHelper.transformToConstructionFeeder(runners);
  }

  static SearchQueryRunnerFactory makeRunnerFactory(final SearchQueryConfig config)
  {
    return new SearchQueryRunnerFactory(
        new SearchStrategySelector(Suppliers.ofInstance(config)),
        new SearchQueryQueryToolChest(config),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
  }

  private final QueryRunner runner;

  public SearchQueryRunnerWithCaseTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  private Druids.SearchQueryBuilder testBuilder()
  {
    return Druids.newSearchQueryBuilder()
                 .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                 .granularity(QueryRunnerTestHelper.ALL_GRAN)
                 .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC);
  }

  @Test
  public void testSearch()
  {
    Druids.SearchQueryBuilder builder = testBuilder();
    Map<String, Set<String>> expectedResults = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    SearchQuery searchQuery;

    searchQuery = builder.query("SPOT").build();
    expectedResults.put(QueryRunnerTestHelper.MARKET_DIMENSION, Sets.newHashSet("spot", "SPot"));
    checkSearchQuery(searchQuery, expectedResults);

    searchQuery = builder.query("spot", true).build();
    expectedResults.put(QueryRunnerTestHelper.MARKET_DIMENSION, Sets.newHashSet("spot"));
    checkSearchQuery(searchQuery, expectedResults);

    searchQuery = builder.query("SPot", true).build();
    expectedResults.put(QueryRunnerTestHelper.MARKET_DIMENSION, Sets.newHashSet("SPot"));
    checkSearchQuery(searchQuery, expectedResults);
  }

  @Test
  public void testSearchSameValueInMultiDims()
  {
    SearchQuery searchQuery;
    Druids.SearchQueryBuilder builder = testBuilder()
        .dimensions(Arrays.asList(
            QueryRunnerTestHelper.PLACEMENT_DIMENSION,
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION
        ));
    Map<String, Set<String>> expectedResults = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    searchQuery = builder.query("PREFERRED").build();
    expectedResults.put(
        QueryRunnerTestHelper.PLACEMENT_DIMENSION,
        Sets.newHashSet("PREFERRED", "preferred", "PREFERRed")
    );
    expectedResults.put(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, Sets.newHashSet("preferred", "Preferred"));
    checkSearchQuery(searchQuery, expectedResults);

    searchQuery = builder.query("preferred", true).build();
    expectedResults.put(QueryRunnerTestHelper.PLACEMENT_DIMENSION, Sets.newHashSet("preferred"));
    expectedResults.put(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, Sets.newHashSet("preferred"));
    checkSearchQuery(searchQuery, expectedResults);
  }

  @Test
  public void testSearchIntervals()
  {
    SearchQuery searchQuery;
    Druids.SearchQueryBuilder builder = testBuilder()
        .dimensions(Collections.singletonList(QueryRunnerTestHelper.QUALITY_DIMENSION))
        .intervals("2011-01-12T00:00:00.000Z/2011-01-13T00:00:00.000Z");
    Map<String, Set<String>> expectedResults = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    searchQuery = builder.query("otive").build();
    expectedResults.put(QueryRunnerTestHelper.QUALITY_DIMENSION, Sets.newHashSet("AutoMotive"));
    checkSearchQuery(searchQuery, expectedResults);
  }

  @Test
  public void testSearchNoOverrappingIntervals()
  {
    SearchQuery searchQuery;
    Druids.SearchQueryBuilder builder = testBuilder()
        .dimensions(Collections.singletonList(QueryRunnerTestHelper.QUALITY_DIMENSION))
        .intervals("2011-01-10T00:00:00.000Z/2011-01-11T00:00:00.000Z");
    Map<String, Set<String>> expectedResults = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    searchQuery = builder.query("business").build();
    expectedResults.put(QueryRunnerTestHelper.QUALITY_DIMENSION, new HashSet<>());
    checkSearchQuery(searchQuery, expectedResults);
  }

  @Test
  public void testFragmentSearch()
  {
    Druids.SearchQueryBuilder builder = testBuilder();
    Map<String, Set<String>> expectedResults = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    SearchQuery searchQuery;

    searchQuery = builder.fragments(Arrays.asList("auto", "ve")).build();
    expectedResults.put(QueryRunnerTestHelper.QUALITY_DIMENSION, Sets.newHashSet("automotive", "AutoMotive"));
    checkSearchQuery(searchQuery, expectedResults);

    searchQuery = builder.fragments(Arrays.asList("auto", "ve"), true).build();
    expectedResults.put(QueryRunnerTestHelper.QUALITY_DIMENSION, Sets.newHashSet("automotive"));
    checkSearchQuery(searchQuery, expectedResults);
  }

  @Test
  public void testFallbackToCursorBasedPlan()
  {
    final SearchQueryBuilder builder = testBuilder();
    final SearchQuery query = builder.filters("qualityLong", "1000").build();
    final Map<String, Set<String>> expectedResults = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    expectedResults.put("qualityLong", Sets.newHashSet("1000"));
    expectedResults.put("qualityDouble", Sets.newHashSet("10000.0"));
    expectedResults.put("qualityFloat", Sets.newHashSet("10000.0"));
    expectedResults.put("qualityNumericString", Sets.newHashSet("100000"));
    expectedResults.put("longNumericNull", Sets.newHashSet("10"));
    expectedResults.put("floatNumericNull", Sets.newHashSet("10.0"));
    expectedResults.put("doubleNumericNull", Sets.newHashSet("10.0"));
    expectedResults.put("quality", Sets.newHashSet("AutoMotive", "automotive"));
    expectedResults.put("placement", Sets.newHashSet("PREFERRED", "preferred"));
    expectedResults.put("placementish", Sets.newHashSet("a", "preferred"));
    expectedResults.put("market", Sets.newHashSet("spot"));
    checkSearchQuery(query, expectedResults);
  }

  private void checkSearchQuery(SearchQuery searchQuery, Map<String, Set<String>> expectedResults)
  {
    Iterable<Result<SearchResultValue>> results = runner.run(QueryPlus.wrap(searchQuery)).toList();

    for (Result<SearchResultValue> result : results) {
      Assert.assertEquals(DateTimes.of("2011-01-12T00:00:00.000Z"), result.getTimestamp());
      Assert.assertNotNull(result.getValue());

      Iterable<SearchHit> resultValues = result.getValue();
      for (SearchHit resultValue : resultValues) {
        String dimension = resultValue.getDimension();
        String theValue = resultValue.getValue();
        Assert.assertTrue(
            StringUtils.format("Result had unknown dimension[%s]", dimension),
            expectedResults.containsKey(dimension)
        );

        Set<String> expectedSet = expectedResults.get(dimension);
        Assert.assertTrue(
            StringUtils.format("Couldn't remove dim[%s], value[%s]", dimension, theValue), expectedSet.remove(theValue)
        );
      }
    }

    for (Map.Entry<String, Set<String>> entry : expectedResults.entrySet()) {
      Assert.assertTrue(
          StringUtils.format(
              "Dimension[%s] should have had everything removed, still has[%s]", entry.getKey(), entry.getValue()
          ),
          entry.getValue().isEmpty()
      );
    }
    expectedResults.clear();
  }
}
