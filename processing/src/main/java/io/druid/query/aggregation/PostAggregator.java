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

import io.druid.java.util.common.Cacheable;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 * Functionally similar to an Aggregator. See the Aggregator interface for more comments.
 */
public interface PostAggregator extends Cacheable
{
  Set<String> getDependentFields();

  Comparator getComparator();

  Object compute(Map<String, Object> combinedAggregators);

  String getName();

  /**
   * Returns a richer post aggregator which are built from the given aggregators with their names and some accessible
   * environmental variables such as ones in the object scope.
   *
   * @param aggregators A map of aggregator factories with their names.
   *
   */
  PostAggregator decorate(Map<String, AggregatorFactory> aggregators);
}
