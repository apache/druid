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

package org.apache.druid.segment.index.semantic;

import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.segment.index.BitmapColumnIndex;

import javax.annotation.Nullable;

/**
 * An optimized column value {@link BitmapColumnIndex} provider for columns which are stored in 'lexicographical' order,
 * allowing short-circuit processing of string value ranges. This index does not match null values, union the results
 * of this index with {@link NullValueIndex} if null values should be considered part of the value range.
 */
public interface LexicographicalRangeIndexes
{
  /**
   * Get a {@link BitmapColumnIndex} corresponding to the values supplied in the specified range. If supplied starting
   * value is null, the range will begin at the first non-null value in the underlying value dictionary. If the end
   * value is null, the range will extend to the last value in the underlying value dictionary.
   * <p>
   * If this method returns null it indicates that there is no index available that matches the requested range and a
   * {@link org.apache.druid.query.filter.ValueMatcher} must be used instead.
   */
  @Nullable
  BitmapColumnIndex forRange(
      @Nullable String startValue,
      boolean startStrict,
      @Nullable String endValue,
      boolean endStrict
  );

  /**
   * Get a {@link BitmapColumnIndex} corresponding to the values supplied in the specified range whose dictionary ids
   * also match some predicate, such as to match a prefix. If supplied starting value is null, the range will begin at
   * the first non-null value in the underlying value dictionary that matches the predicate. If the end value is null,
   * the range will extend to the last value in the underlying value dictionary that matches the predicate.
   * <p>
   * If the provided {@code} matcher is always true, it's better to use the other
   * {@link #forRange(String, boolean, String, boolean)} method.
   * <p>
   * If this method returns null it indicates that there is no index available that matches the requested range and a
   * {@link org.apache.druid.query.filter.ValueMatcher} must be used instead.
   */
  @Nullable
  BitmapColumnIndex forRange(
      @Nullable String startValue,
      boolean startStrict,
      @Nullable String endValue,
      boolean endStrict,
      DruidObjectPredicate<String> matcher
  );
}
