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

import javax.annotation.Nullable;

/**
 * An optimized column value {@link BitmapColumnIndex} provider for specialized processing of numeric value ranges.
 * This index does not match null values, union the results of this index with {@link NullValueIndex} if null values
 * should be considered part of the value range.
 */
public interface NumericRangeIndex
{
  /**
   * Get a {@link BitmapColumnIndex} corresponding to the values supplied in the specified range. If supplied starting
   * value is null, the range will begin at the first non-null value in the underlying value dictionary. If the end
   * value is null, the range will extend to the last value in the underlying value dictionary.
   */
  BitmapColumnIndex forRange(
      @Nullable Number startValue,
      boolean startStrict,
      @Nullable Number endValue,
      boolean endStrict
  );
}
