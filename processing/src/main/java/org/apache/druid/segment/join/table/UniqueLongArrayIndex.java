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

package org.apache.druid.segment.join.table;

import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.ints.IntSortedSets;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

/**
 * An {@link IndexedTable.Index} backed by an int array.
 *
 * This is for nonnull long-typed keys whose values all fall in a "reasonable" range. Built by
 * {@link RowBasedIndexBuilder#build()} when these conditions are met.
 */
public class UniqueLongArrayIndex implements IndexedTable.Index
{
  /**
   * Array index is the key, value is the row number.
   */
  private final int[] index;
  private final long minKey;

  /**
   * Create a new instance backed by a given array.
   *
   * @param index  an int array where position {@code i} corresponds to the key {@code i + minKey}
   * @param minKey lowest-valued key
   *
   * @see RowBasedIndexBuilder#build() the main caller
   */
  UniqueLongArrayIndex(int[] index, long minKey)
  {
    this.index = index;
    this.minKey = minKey;
  }

  @Override
  public ColumnType keyType()
  {
    return ColumnType.LONG;
  }

  @Override
  public boolean areKeysUnique(final boolean includeNull)
  {
    return true;
  }

  @Override
  public IntSortedSet find(@Nullable Object key)
  {
    if (key == null) {
      // This index class never contains null keys.
      return IntSortedSets.EMPTY_SET;
    }

    final Long longKey = DimensionHandlerUtils.convertObjectToLong(key);

    if (longKey != null) {
      final int row = findUniqueLong(longKey);
      if (row >= 0) {
        return IntSortedSets.singleton(row);
      }
    }

    return IntSortedSets.EMPTY_SET;
  }

  @Override
  public int findUniqueLong(long key)
  {
    if (key >= minKey && key < (minKey + index.length)) {
      return index[(int) (key - minKey)];
    } else {
      return NOT_FOUND;
    }
  }
}
