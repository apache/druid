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

import com.google.common.collect.Lists;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.NumericColumnSelector;
import io.druid.java.util.common.Pair;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class AggregatorUtil
{
  public static final byte STRING_SEPARATOR = (byte) 0xFF;

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

  public static FloatColumnSelector getFloatColumnSelector(
      ColumnSelectorFactory metricFactory,
      String fieldName,
      String fieldExpression
  )
  {
    if (fieldName != null && fieldExpression == null) {
      return metricFactory.makeFloatColumnSelector(fieldName);
    }
    if (fieldName == null && fieldExpression != null) {
      final NumericColumnSelector numeric = metricFactory.makeMathExpressionSelector(fieldExpression);
      return new FloatColumnSelector()
      {
        @Override
        public float get()
        {
          return numeric.get().floatValue();
        }
      };
    }
    throw new IllegalArgumentException("Must have a valid, non-null fieldName or expression");
  }

  public static LongColumnSelector getLongColumnSelector(
      ColumnSelectorFactory metricFactory,
      String fieldName,
      String fieldExpression
  )
  {
    if (fieldName != null && fieldExpression == null) {
      return metricFactory.makeLongColumnSelector(fieldName);
    }
    if (fieldName == null && fieldExpression != null) {
      final NumericColumnSelector numeric = metricFactory.makeMathExpressionSelector(fieldExpression);
      return new LongColumnSelector()
      {
        @Override
        public long get()
        {
          return numeric.get().longValue();
        }
      };
    }
    throw new IllegalArgumentException("Must have a valid, non-null fieldName or expression");
  }
}
