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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.CharSource;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndex;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.druid.query.QueryRunnerTestHelper.NOOP_QUERYWATCHER;
import static io.druid.query.QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator;
import static io.druid.query.QueryRunnerTestHelper.allGran;
import static io.druid.query.QueryRunnerTestHelper.dataSource;
import static io.druid.query.QueryRunnerTestHelper.fullOnInterval;
import static io.druid.query.QueryRunnerTestHelper.makeQueryRunner;
import static io.druid.query.QueryRunnerTestHelper.marketDimension;
import static io.druid.query.QueryRunnerTestHelper.placementDimension;
import static io.druid.query.QueryRunnerTestHelper.placementishDimension;
import static io.druid.query.QueryRunnerTestHelper.qualityDimension;
import static io.druid.query.QueryRunnerTestHelper.transformToConstructionFeeder;

/**
 */
@RunWith(Parameterized.class)
public class SearchQueryRunnerWithCaseTest
{
  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    SearchQueryRunnerFactory factory = new SearchQueryRunnerFactory(
        new SearchQueryQueryToolChest(
            new SearchQueryConfig(),
            NoopIntervalChunkingQueryRunnerDecorator()
        ),
        NOOP_QUERYWATCHER
    );

    CharSource input = CharSource.wrap(
        "2011-01-12T00:00:00.000Z\tspot\tAutoMotive\tPREFERRED\ta\u0001preferred\t100.000000\n" +
        "2011-01-12T00:00:00.000Z\tSPot\tbusiness\tpreferred\tb\u0001Preferred\t100.000000\n" +
        "2011-01-12T00:00:00.000Z\tspot\tentertainment\tPREFERRed\te\u0001preferred\t100.000000\n" +
        "2011-01-13T00:00:00.000Z\tspot\tautomotive\tpreferred\ta\u0001preferred\t94.874713"
    );

    IncrementalIndex index1 = TestIndex.makeRealtimeIndex(input);
    IncrementalIndex index2 = TestIndex.makeRealtimeIndex(input);

    QueryableIndex index3 = TestIndex.persistRealtimeAndLoadMMapped(index1);
    QueryableIndex index4 = TestIndex.persistRealtimeAndLoadMMapped(index2);

    return transformToConstructionFeeder(
        Arrays.asList(
            makeQueryRunner(factory, "index1", new IncrementalIndexSegment(index1, "index1"), "index1"),
            makeQueryRunner(factory, "index2", new IncrementalIndexSegment(index2, "index2"), "index2"),
            makeQueryRunner(factory, "index3", new QueryableIndexSegment("index3", index3), "index3"),
            makeQueryRunner(factory, "index4", new QueryableIndexSegment("index4", index4), "index4")
        )
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
                 .dataSource(dataSource)
                 .granularity(allGran)
                 .intervals(fullOnInterval);
  }

  @Test
  public void testSearch()
  {
    Druids.SearchQueryBuilder builder = testBuilder();
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    SearchQuery searchQuery;

    searchQuery = builder.query("SPOT").build();
    expectedResults.put(marketDimension, Sets.newHashSet("spot", "SPot"));
    checkSearchQuery(searchQuery, expectedResults);

    searchQuery = builder.query("spot", true).build();
    expectedResults.put(marketDimension, Sets.newHashSet("spot"));
    checkSearchQuery(searchQuery, expectedResults);

    searchQuery = builder.query("SPot", true).build();
    expectedResults.put(marketDimension, Sets.newHashSet("SPot"));
    checkSearchQuery(searchQuery, expectedResults);
  }

  @Test
  public void testSearchSameValueInMultiDims()
  {
    SearchQuery searchQuery;
    Druids.SearchQueryBuilder builder = testBuilder()
        .dimensions(Arrays.asList(placementDimension, placementishDimension));
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    searchQuery = builder.query("PREFERRED").build();
    expectedResults.put(placementDimension, Sets.newHashSet("PREFERRED", "preferred", "PREFERRed"));
    expectedResults.put(placementishDimension, Sets.newHashSet("preferred", "Preferred"));
    checkSearchQuery(searchQuery, expectedResults);

    searchQuery = builder.query("preferred", true).build();
    expectedResults.put(placementDimension, Sets.newHashSet("preferred"));
    expectedResults.put(placementishDimension, Sets.newHashSet("preferred"));
    checkSearchQuery(searchQuery, expectedResults);
  }

  @Test
  public void testSearchIntervals()
  {
    SearchQuery searchQuery;
    Druids.SearchQueryBuilder builder = testBuilder()
        .dimensions(Arrays.asList(qualityDimension))
        .intervals("2011-01-12T00:00:00.000Z/2011-01-13T00:00:00.000Z");
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    searchQuery = builder.query("otive").build();
    expectedResults.put(qualityDimension, Sets.newHashSet("AutoMotive"));
    checkSearchQuery(searchQuery, expectedResults);
  }

  @Test
  public void testSearchNoOverrappingIntervals()
  {
    SearchQuery searchQuery;
    Druids.SearchQueryBuilder builder = testBuilder()
        .dimensions(Arrays.asList(qualityDimension))
        .intervals("2011-01-10T00:00:00.000Z/2011-01-11T00:00:00.000Z");
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    searchQuery = builder.query("business").build();
    expectedResults.put(qualityDimension, Sets.<String>newHashSet());
    checkSearchQuery(searchQuery, expectedResults);
  }

  @Test
  public void testFragmentSearch()
  {
    Druids.SearchQueryBuilder builder = testBuilder();
    Map<String, Set<String>> expectedResults = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    SearchQuery searchQuery;

    searchQuery = builder.fragments(Arrays.asList("auto", "ve")).build();
    expectedResults.put(qualityDimension, Sets.newHashSet("automotive", "AutoMotive"));
    checkSearchQuery(searchQuery, expectedResults);

    searchQuery = builder.fragments(Arrays.asList("auto", "ve"), true).build();
    expectedResults.put(qualityDimension, Sets.newHashSet("automotive"));
    checkSearchQuery(searchQuery, expectedResults);
  }

  private void checkSearchQuery(SearchQuery searchQuery, Map<String, Set<String>> expectedResults)
  {
    HashMap<String, List> context = new HashMap<>();
    Iterable<Result<SearchResultValue>> results = Sequences.toList(
        runner.run(searchQuery, context),
        Lists.<Result<SearchResultValue>>newArrayList()
    );

    for (Result<SearchResultValue> result : results) {
      Assert.assertEquals(new DateTime("2011-01-12T00:00:00.000Z"), result.getTimestamp());
      Assert.assertNotNull(result.getValue());

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
    expectedResults.clear();
  }
}
