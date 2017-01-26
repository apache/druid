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

package io.druid.sql.calcite.aggregation;

import com.google.common.base.Predicate;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.filter.DimFilter;

public class Aggregations
{
  private Aggregations()
  {
    // No instantiation.
  }

  /**
   * Returns true if "factory" is an aggregator factory that either matches "predicate" (if filter is null) or is
   * a filtered aggregator factory whose filter is equal to "filter" and underlying aggregator matches "predicate".
   *
   * @param factory   factory to match
   * @param filter    filter, may be null
   * @param clazz     class of factory to match
   * @param predicate predicate
   *
   * @return true if the aggregator matches filter + predicate
   */
  public static <T extends AggregatorFactory> boolean aggregatorMatches(
      final AggregatorFactory factory,
      final DimFilter filter,
      final Class<T> clazz,
      final Predicate<T> predicate
  )
  {
    if (filter != null) {
      return factory instanceof FilteredAggregatorFactory &&
             ((FilteredAggregatorFactory) factory).getFilter().equals(filter)
             && aggregatorMatches(((FilteredAggregatorFactory) factory).getAggregator(), null, clazz, predicate);
    } else {
      return clazz.isAssignableFrom(factory.getClass()) && predicate.apply(clazz.cast(factory));
    }
  }
}
