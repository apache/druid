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

package io.druid.query.filter;

import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.query.BitmapResultFactory;
import io.druid.query.DefaultBitmapResultFactory;
import io.druid.segment.ColumnSelector;
import io.druid.segment.ColumnSelectorFactory;

/**
 * {@link #getBitmapIndex} and {@link #getBitmapResult} methods both have default implementations, delegating to each
 * other. Every implementation of {@link Filter} should override {@link #getBitmapResult}, currently it has a default
 * implementation for compatibility with Filters in extensions. In Druid 0.11 {@link #getBitmapResult} is going to
 * become an abstract method without a default implementation.
 */
public interface Filter
{
  /**
   * Get a bitmap index, indicating rows that match this filter.
   *
   * @param selector Object used to retrieve bitmap indexes
   *
   * @return A bitmap indicating rows that match this filter.
   *
   * @see Filter#estimateSelectivity(BitmapIndexSelector)
   */
  default ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector)
  {
    return getBitmapResult(selector, new DefaultBitmapResultFactory(selector.getBitmapFactory()));
  }

  default <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory)
  {
    return bitmapResultFactory.wrapUnknown(getBitmapIndex(selector));
  }

  /**
   * Estimate selectivity of this filter.
   * This method can be used for cost-based query planning like in {@link io.druid.query.search.search.AutoStrategy}.
   * To avoid significant performance degradation for calculating the exact cost,
   * implementation of this method targets to achieve rapid selectivity estimation
   * with reasonable sacrifice of the accuracy.
   * As a result, the estimated selectivity might be different from the exact value.
   *
   * @param indexSelector Object used to retrieve bitmap indexes
   *
   * @return an estimated selectivity ranging from 0 (filter selects no rows) to 1 (filter selects all rows).
   *
   * @see Filter#getBitmapIndex(BitmapIndexSelector)
   */
  double estimateSelectivity(BitmapIndexSelector indexSelector);


  /**
   * Get a ValueMatcher that applies this filter to row values.
   *
   * @param factory Object used to create ValueMatchers
   *
   * @return ValueMatcher that applies this filter to row values.
   */
  ValueMatcher makeMatcher(ColumnSelectorFactory factory);


  /**
   * Indicates whether this filter can return a bitmap index for filtering, based on
   * the information provided by the input BitmapIndexSelector.
   *
   * @param selector Object used to retrieve bitmap indexes
   *
   * @return true if this Filter can provide a bitmap index using the selector, false otherwise.
   */
  boolean supportsBitmapIndex(BitmapIndexSelector selector);


  /**
   * Indicates whether this filter supports selectivity estimation.
   * A filter supports selectivity estimation if it supports bitmap index and
   * the dimension which the filter evaluates does not have multi values.
   *
   * @param columnSelector Object to check the dimension has multi values.
   * @param indexSelector  Object used to retrieve bitmap indexes
   *
   * @return true if this Filter supports selectivity estimation, false otherwise.
   */
  boolean supportsSelectivityEstimation(ColumnSelector columnSelector, BitmapIndexSelector indexSelector);
}
