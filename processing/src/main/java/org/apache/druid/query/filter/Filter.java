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

package org.apache.druid.query.filter;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

public interface Filter
{
  /**
   * Get a bitmap index, indicating rows that match this filter. Do not call this method unless
   * {@link #supportsBitmapIndex(BitmapIndexSelector)} returns true. Behavior in the case that
   * {@link #supportsBitmapIndex(BitmapIndexSelector)} returns false is undefined.
   *
   * This method is OK to be called, but generally should not be overridden, override {@link #getBitmapResult} instead.
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

  /**
   * Get a (possibly wrapped) bitmap index, indicating rows that match this filter. Do not call this method unless
   * {@link #supportsBitmapIndex(BitmapIndexSelector)} returns true. Behavior in the case that
   * {@link #supportsBitmapIndex(BitmapIndexSelector)} returns false is undefined.
   *
   * @param selector Object used to retrieve bitmap indexes
   *
   * @return A bitmap indicating rows that match this filter.
   *
   * @see Filter#estimateSelectivity(BitmapIndexSelector)
   */
  <T> T getBitmapResult(BitmapIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory);

  /**
   * Estimate selectivity of this filter.
   * This method can be used for cost-based query planning like in {@link org.apache.druid.query.search.AutoStrategy}.
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
   * Get a VectorValueMatcher that applies this filter to row vectors.
   *
   * @param factory Object used to create ValueMatchers
   *
   * @return VectorValueMatcher that applies this filter to row vectors.
   */
  default VectorValueMatcher makeVectorMatcher(VectorColumnSelectorFactory factory)
  {
    throw new UOE("Filter[%s] cannot vectorize", getClass().getName());
  }

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

  /**
   * Returns true if this filter can produce a vectorized matcher from its "makeVectorMatcher" method.
   */
  default boolean canVectorizeMatcher()
  {
    return false;
  }
}
