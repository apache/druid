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

package io.druid.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class Queries
{
  public static List<PostAggregator> decoratePostAggregators(
      List<PostAggregator> postAggs,
      Map<String, AggregatorFactory> aggFactories
  )
  {
    List<PostAggregator> decorated = Lists.newArrayListWithExpectedSize(postAggs.size());
    for (PostAggregator aggregator : postAggs) {
      decorated.add(aggregator.decorate(aggFactories));
    }
    return decorated;
  }

  /**
   * Like {@link #prepareAggregations(List, List, List)} but with otherOutputNames as an empty list. Deprecated
   * because it makes it easy to forget to include dimensions, etc. in "otherOutputNames".
   *
   * @param aggFactories aggregator factories for this query
   * @param postAggs     post-aggregators for this query
   *
   * @return decorated post-aggregators
   *
   * @throws NullPointerException     if aggFactories is null
   * @throws IllegalArgumentException if there are any output name collisions or missing post-aggregator inputs
   */
  @Deprecated
  public static List<PostAggregator> prepareAggregations(
      List<AggregatorFactory> aggFactories,
      List<PostAggregator> postAggs
  )
  {
    return prepareAggregations(Collections.emptyList(), aggFactories, postAggs);
  }

  /**
   * Returns decorated post-aggregators, based on original un-decorated post-aggregators. In addition, this method
   * also verifies that there are no output name collisions, and that all of the post-aggregators' required input
   * fields are present.
   *
   * @param otherOutputNames names of fields that will appear in the same output namespace as aggregators and
   *                         post-aggregators, and are also assumed to be valid inputs to post-aggregators. For most
   *                         built-in query types, this is either empty, or the list of dimension output names.
   * @param aggFactories     aggregator factories for this query
   * @param postAggs         post-aggregators for this query
   *
   * @return decorated post-aggregators
   *
   * @throws NullPointerException     if otherOutputNames or aggFactories is null
   * @throws IllegalArgumentException if there are any output name collisions or missing post-aggregator inputs
   */
  public static List<PostAggregator> prepareAggregations(
      List<String> otherOutputNames,
      List<AggregatorFactory> aggFactories,
      List<PostAggregator> postAggs
  )
  {
    Preconditions.checkNotNull(otherOutputNames, "otherOutputNames cannot be null");
    Preconditions.checkNotNull(aggFactories, "aggregations cannot be null");

    final Set<String> combinedOutputNames = new HashSet<>();
    combinedOutputNames.addAll(otherOutputNames);

    final Map<String, AggregatorFactory> aggsFactoryMap = new HashMap<>();
    for (AggregatorFactory aggFactory : aggFactories) {
      Preconditions.checkArgument(
          combinedOutputNames.add(aggFactory.getName()),
          "[%s] already defined", aggFactory.getName()
      );
      aggsFactoryMap.put(aggFactory.getName(), aggFactory);
    }

    if (postAggs != null && !postAggs.isEmpty()) {
      List<PostAggregator> decorated = Lists.newArrayListWithExpectedSize(postAggs.size());
      for (final PostAggregator postAgg : postAggs) {
        final Set<String> dependencies = postAgg.getDependentFields();
        final Set<String> missing = Sets.difference(dependencies, combinedOutputNames);

        Preconditions.checkArgument(
            missing.isEmpty(),
            "Missing fields [%s] for postAggregator [%s]", missing, postAgg.getName()
        );
        Preconditions.checkArgument(
            combinedOutputNames.add(postAgg.getName()),
            "[%s] already defined", postAgg.getName()
        );

        decorated.add(postAgg.decorate(aggsFactoryMap));
      }
      return decorated;
    }

    return postAggs;
  }
}
