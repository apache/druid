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

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Provides a mechanism to get {@link ImmutableBitmap} for a value, or {@link Iterable} of {@link ImmutableBitmap}
 * for a range or exact set of values, the set bits of which correspond to which rows contain the matching values.
 *
 * Used to power {@link org.apache.druid.segment.BitmapOffset} and
 * {@link org.apache.druid.segment.vector.BitmapVectorOffset} which are used with column cursors for fast filtering
 * of indexed values.
 *
 * The column must be ingested with a bitmap index for filters to use them and to participate in this "pre-filtering"
 * step when scanning segments
 *
 * @see org.apache.druid.segment.QueryableIndexStorageAdapter#analyzeFilter
 */
public interface BitmapIndex
{
  /**
   * Get the cardinality of the underlying value dictionary
   */
  int getCardinality();

  /**
   * Get the value in the underlying value dictionary of the specified dictionary id
   */
  @Nullable
  String getValue(int index);

  /**
   * Returns true if the underlying value dictionary has nulls
   */
  boolean hasNulls();

  BitmapFactory getBitmapFactory();

  /**
   * Returns the index of "value" in this BitmapIndex, or a negative value, if not present in the underlying dictionary
   */
  int getIndex(@Nullable String value);

  /**
   * Get the {@link ImmutableBitmap} for dictionary id of the underlying dictionary
   */
  ImmutableBitmap getBitmap(int idx);

  /**
   * Get the {@link ImmutableBitmap} corresponding to the supplied value
   */
  ImmutableBitmap getBitmapForValue(@Nullable String value);

  /**
   * Get an {@link Iterable} of {@link ImmutableBitmap} corresponding to the values supplied in the specified range
   */
  default Iterable<ImmutableBitmap> getBitmapsInRange(
      @Nullable String startValue,
      boolean startStrict,
      @Nullable String endValue,
      boolean endStrict
  )
  {
    return getBitmapsInRange(startValue, startStrict, endValue, endStrict, (index) -> true);
  }

  /**
   * Get an {@link Iterable} of {@link ImmutableBitmap} corresponding to the values supplied in the specified range
   * whose dictionary ids also match some predicate
   */
  Iterable<ImmutableBitmap> getBitmapsInRange(
      @Nullable String startValue,
      boolean startStrict,
      @Nullable String endValue,
      boolean endStrict,
      Predicate<String> matcher
  );

  /**
   * Get an {@link Iterable} of {@link ImmutableBitmap} corresponding to the specified set of values (if they are
   * contained in the underlying column)
   */
  Iterable<ImmutableBitmap> getBitmapsForValues(Set<String> values);
}
