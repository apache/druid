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

import org.apache.druid.annotations.SubclassesMustOverrideEqualsAndHashCode;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnIndexCapabilities;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

@SubclassesMustOverrideEqualsAndHashCode
public interface Filter
{
  /**
   * Get a bitmap index, indicating rows that match this filter. Do not call this method unless
   * {@link #getIndexCapabilities(ColumnIndexSelector)} returns a non-null value. Behavior in the case that
   * {@link #getIndexCapabilities(ColumnIndexSelector)} returns null is undefined.
   *
   * This method is OK to be called, but generally should not be overridden, override {@link #getBitmapResult} instead.
   *
   * @param selector Object used to retrieve bitmap indexes
   *
   * @return A bitmap indicating rows that match this filter.
   *
   * @see Filter#estimateSelectivity(ColumnIndexSelector)
   */
  default ImmutableBitmap getBitmapIndex(ColumnIndexSelector selector)
  {
    return getBitmapResult(selector, new DefaultBitmapResultFactory(selector.getBitmapFactory()));
  }

  /**
   * Get a (possibly wrapped) bitmap index, indicating rows that match this filter. Do not call this method unless
   * {@link #getIndexCapabilities(ColumnIndexSelector)} returns a non-null value. Behavior in the case that
   * {@link #getIndexCapabilities(ColumnIndexSelector)} returns null is undefined.
   *
   * @param selector Object used to retrieve bitmap indexes
   *
   * @return A bitmap indicating rows that match this filter.
   *
   * @see Filter#estimateSelectivity(ColumnIndexSelector)
   */
  <T> T getBitmapResult(ColumnIndexSelector selector, BitmapResultFactory<T> bitmapResultFactory);

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
   * @see Filter#getBitmapIndex(ColumnIndexSelector)
   */
  double estimateSelectivity(ColumnIndexSelector indexSelector);

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
   * Provides {@link ColumnIndexCapabilities} for a filter, if any indexes can be used. A null return value from
   * this method means that a filter cannot use indexes during query processing for the given
   * {@link ColumnIndexSelector}
   *
   * @param selector Object used to retrieve bitmap indexes and provide information about the column
   *
   * @return information about the function of the indexes which will be used when processing this filter, or null if
   *         indexes may not be used to process this query.
   */
  @Nullable
  ColumnIndexCapabilities getIndexCapabilities(ColumnIndexSelector selector);

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
  boolean supportsSelectivityEstimation(ColumnSelector columnSelector, ColumnIndexSelector indexSelector);

  /**
   * Returns true if this filter can produce a vectorized matcher from its "makeVectorMatcher" method.
   * @param inspector Supplies type information for the selectors this filter will match against
   */
  default boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return false;
  }

  /**
   * Set of columns used by a filter. If {@link #getIndexCapabilities} returns a non-null value, all columns returned
   * by this method can be expected to have an index available for filtering.
   */
  Set<String> getRequiredColumns();

  /**
   * Returns true is this filter is able to return a copy of this filter that is identical to this filter except that it
   * operates on different columns, based on a renaming map.
   */
  default boolean supportsRequiredColumnRewrite()
  {
    return false;
  }

  /**
   * Return a copy of this filter that is identical to the this filter except that it operates on different columns,
   * based on a renaming map where the key is the column to be renamed in the filter, and the value is the new
   * column name.
   *
   * For example, if I have a filter (A = hello), and I have a renaming map (A -> B),
   * this should return the filter (B = hello)
   *
   * @param columnRewrites Column rewrite map
   * @return Copy of this filter that operates on new columns based on the rewrite map
   */
  default Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    throw new UnsupportedOperationException("Required column rewrite is not supported by this filter.");
  }
}
