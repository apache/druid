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

package io.druid.query.aggregation;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.Pair;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AggregatorUtil
{
  /**
   * returns the list of dependent postAggregators that should be calculated in order to calculate given postAgg
   *
   * @param postAggregatorList List of postAggregator, there is a restriction that the list should be in an order
   *                           such that all the dependencies of any given aggregator should occur before that aggregator.
   *                           See AggregatorUtilTest.testOutOfOrderPruneDependentPostAgg for example.
   * @param postAggName        name of the postAgg on which dependency is to be calculated
   *
   * @return the list of dependent postAggregators
   */
  public static List<PostAggregator> pruneDependentPostAgg(List<PostAggregator> postAggregatorList, String postAggName)
  {
    LinkedList<PostAggregator> rv = Lists.newLinkedList();
    Set<String> deps = new HashSet<>();
    deps.add(postAggName);
    // Iterate backwards to find the last calculated aggregate and add dependent aggregator as we find dependencies in reverse order
    for (PostAggregator agg : Lists.reverse(postAggregatorList)) {
      if (deps.contains(agg.getName())) {
        rv.addFirst(agg); // add to the beginning of List
        deps.remove(agg.getName());
        deps.addAll(agg.getDependentFields());
      }
    }

    return rv;
  }

  public static Pair<List<AggregatorFactory>, List<PostAggregator>> condensedAggregators(
      List<AggregatorFactory> aggList,
      List<PostAggregator> postAggList,
      String metric
  )
  {

    List<PostAggregator> condensedPostAggs = AggregatorUtil.pruneDependentPostAgg(
        postAggList,
        metric
    );
    // calculate dependent aggregators for these postAgg
    Set<String> dependencySet = new HashSet<>();
    dependencySet.add(metric);
    for (PostAggregator postAggregator : condensedPostAggs) {
      dependencySet.addAll(postAggregator.getDependentFields());
    }

    List<AggregatorFactory> condensedAggs = Lists.newArrayList();
    for (AggregatorFactory aggregatorSpec : aggList) {
      if (dependencySet.contains(aggregatorSpec.getName())) {
        condensedAggs.add(aggregatorSpec);
      }
    }
    return new Pair(condensedAggs, condensedPostAggs);
  }

  /**
   * Merges the list of AggregatorFactory[] (presumable from metadata of some segments being merged) and
   * returns merged AggregatorFactory[] (for the metadata for merged segment).
   * Null is returned if it is not possible to do the merging for any of the following reason.
   * - one of the element in input list is null i.e. aggregatorsList for one the segments being merged is unknown
   * - AggregatorFactory of same name can not be merged if they are not compatible
   *
   * @param aggregatorsList list of aggregator factories to be merged
   *
   * @return merged AggregatorFactory[] or Null if merging is not possible.
   */
  public static Map<String, AggregatorFactory> mergeAggregators(
      @Nullable List<AggregatorFactory[]> aggregatorsList,
      boolean strict
  )
  {
    if (aggregatorsList == null || aggregatorsList.isEmpty()) {
      return null;
    }
    final Map<String, AggregatorFactory> result = Maps.newLinkedHashMap();
    // Merge each aggregator individually, ignoring nulls
    for (AggregatorFactory[] aggregators : aggregatorsList) {
      if (strict && aggregators == null) {
        return null;
      }
      if (aggregators != null) {
        for (AggregatorFactory aggregator : aggregators) {
          AggregatorFactory merged = tryMerge(result.get(aggregator.getName()), aggregator);
          if (strict && merged == null) {
            return null;
          }
          result.put(aggregator.getName(), merged);
        }
      }
    }
    return result;
  }

  public static Map<String, AggregatorFactory> mergeAggregatorMaps(
      @Nullable List<Map<String, AggregatorFactory>> aggregatorMaps,
      boolean strict
  )
  {
    return aggregatorMaps == null ? null : mergeAggregators(Lists.transform(aggregatorMaps, TO_ARRAY), strict);
  }

  private static AggregatorFactory tryMerge(@Nullable AggregatorFactory factory1, @Nullable AggregatorFactory factory2)
  {
    if (factory1 == null) {
      return factory2;
    }
    if (factory2 == null) {
      return factory1;
    }
    if (factory1 instanceof MergeableAggregatorFactory) {
      AggregatorFactory merged = ((MergeableAggregatorFactory) factory1).getMergingFactory(factory2);
      if (merged != null) {
        return merged;
      }
    }
    if (factory2 instanceof MergeableAggregatorFactory) {
      return ((MergeableAggregatorFactory) factory2).getMergingFactory(factory1);
    }
    return null;
  }

  public static AggregatorFactory[] toArray(@Nullable Map<String, AggregatorFactory> aggregatorMap)
  {
    return TO_ARRAY.apply(aggregatorMap);
  }

  private static final Function<Map<String, AggregatorFactory>, AggregatorFactory[]> TO_ARRAY =
      new Function<Map<String, AggregatorFactory>, AggregatorFactory[]>()
      {
        @Nullable
        @Override
        public AggregatorFactory[] apply(Map<String, AggregatorFactory> input)
        {
          return input == null ? null : input.values().toArray(new AggregatorFactory[0]);
        }
      };
}
