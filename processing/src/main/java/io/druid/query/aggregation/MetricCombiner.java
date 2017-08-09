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

import io.druid.segment.ColumnValueSelector;

/**
 * MetricCombiner is used to combine rollup aggregation results from serveral "rows" of different indexes during index
 * merging (see {@link io.druid.segment.IndexMerger}).
 *
 * The state of the implementations of this interface is a metric value (either a primitive or an object), that could be
 * queried via {@link ColumnValueSelector}'s methods. Before {@link #reset} is ever called on a MetricCombiner, it's
 * state is undefined and {@link ColumnValueSelector}'s methods could return something random, or throw an exception.
 *
 * @see AggregatorFactory#makeMetricCombiner()
 * @see LongMetricCombiner
 * @see DoubleMetricCombiner
 * @see ObjectMetricCombiner
 */
public interface MetricCombiner extends ColumnValueSelector
{
  /**
   * Resets this MetricCombiner's state value to the value of the given selector, e. g. after calling this method
   * metricCombiner.get*() should return the same value as selector.get*().
   *
   * If the selector is an {@link io.druid.segment.ObjectColumnSelector}, the object returned from {@link
   * io.druid.segment.ObjectColumnSelector#get()} must not be modified, and must not become a subject for modification
   * during subsequent {@link #combine} calls.
   */
  void reset(ColumnValueSelector selector);

  /**
   * Combines this MetricCombiner's state value with the value of the given selector and saves it in this
   * MetricCombiner's state, e. g. after calling metricCombiner.combine(selector), metricCombiner.get*() should return
   * the value that would be the result of {@link AggregatorFactory#combine
   * aggregatorFactory.combine(metricCombiner.get*(), selector.get*())} call.
   *
   * Unlike {@link AggregatorFactory#combine}, if the selector is an {@link io.druid.segment.ObjectColumnSelector}, the
   * object returned from {@link io.druid.segment.ObjectColumnSelector#get()} must not be modified, and must not become
   * a subject for modification during subsequent combine() calls.
   *
   * Since the state of MetricCombiner is underfined before {@link #reset} is ever called on it, the effects of calling
   * combine() on a MetricCombiner instance are also underfined in this case.
   *
   * @see AggregatorFactory#combine
   */
  void combine(ColumnValueSelector selector);
}
