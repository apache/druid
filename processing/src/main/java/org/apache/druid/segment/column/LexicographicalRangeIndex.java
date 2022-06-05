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

package org.apache.druid.segment.column;

import com.google.common.base.Predicate;

import javax.annotation.Nullable;

/**
 * An optimized column value {@link BitmapColumnIndex} provider for columns which are stored in 'lexicographical' order,
 * allowing short-circuit processing of string value ranges.
 */
public interface LexicographicalRangeIndex
{
  /**
   * Get a {@link BitmapColumnIndex} corresponding to the values supplied in the specified range.
   */
  BitmapColumnIndex forRange(
      @Nullable String startValue,
      boolean startStrict,
      @Nullable String endValue,
      boolean endStrict
  );

  /**
   * Get a {@link BitmapColumnIndex} corresponding to the values supplied in the specified range whose dictionary ids
   * also match some predicate, such as to match a prefix.
   *
   * If the provided {@code} matcher is always true, it's better to use the other
   * {@link #forRange(String, boolean, String, boolean)} method.
   */
  BitmapColumnIndex forRange(
      @Nullable String startValue,
      boolean startStrict,
      @Nullable String endValue,
      boolean endStrict,
      Predicate<String> matcher
  );
}
