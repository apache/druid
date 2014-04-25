/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation;

import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.Pair;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
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

}
