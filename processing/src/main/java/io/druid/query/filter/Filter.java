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

import com.metamx.collections.bitmap.ImmutableBitmap;

/**
 */
public interface Filter
{
  /**
   * Get a bitmap index, indicating rows that match this filter.
   *
   * @param selector Object used to retrieve bitmap indexes
   * @return A bitmap indicating rows that match this filter.
   */
  public ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector);


  /**
   * Get a ValueMatcher that applies this filter to row values.
   *
   * @param factory Object used to create ValueMatchers
   * @return ValueMatcher that applies this filter to row values.
   */
  public ValueMatcher makeMatcher(ValueMatcherFactory factory);


  /**
   * Indicates whether this filter can return a bitmap index for filtering, based on
   * the information provided by the input BitmapIndexSelector.
   *
   * @param selector Object used to retrieve bitmap indexes
   * @return true if this Filter can provide a bitmap index using the selector, false otherwise
   */
  public boolean supportsBitmapIndex(BitmapIndexSelector selector);
}
