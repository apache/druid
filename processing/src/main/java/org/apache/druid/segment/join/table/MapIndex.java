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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.ints.IntSortedSets;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * An {@link IndexedTable.Index} backed by a Map.
 */
public class MapIndex implements IndexedTable.Index
{
  /**
   * Type of keys in {@link #index}.
   */
  private final ColumnType keyType;

  /**
   * Index of all nonnull keys -> rows with those keys.
   */
  private final Map<Object, IntSortedSet> index;

  /**
   * Rows containing a null key.
   */
  @Nullable
  private final IntSortedSet nullIndex;

  /**
   * Whether nonnull keys are unique, i.e. everything in {@link #index} has exactly 1 element.
   */
  private final boolean nonNullKeysUnique;

  /**
   * Whether {@link #index} is a {@link Long2ObjectMap}.
   */
  private final boolean isLong2ObjectMap;

  /**
   * Creates a new instance based on a particular map.
   *
   * @param keyType           type of keys in "index"
   * @param index             a map of keys to matching row numbers
   * @param nonNullKeysUnique whether nonnull keys are unique (if true: all IntLists in the index must be exactly 1
   *                          element, except possibly the one corresponding to null)
   *
   * @see RowBasedIndexBuilder#build() the main caller
   */
  MapIndex(
      final ColumnType keyType,
      final Map<Object, IntSortedSet> index,
      final IntSortedSet nullIndex,
      final boolean nonNullKeysUnique
  )
  {
    this.keyType = Preconditions.checkNotNull(keyType, "keyType");
    this.index = Preconditions.checkNotNull(index, "index");
    this.nullIndex = nullIndex;
    this.nonNullKeysUnique = nonNullKeysUnique;
    this.isLong2ObjectMap = index instanceof Long2ObjectMap;
  }

  @Override
  public ColumnType keyType()
  {
    return keyType;
  }

  @Override
  public boolean areKeysUnique(final boolean includeNull)
  {
    if (includeNull) {
      return nonNullKeysUnique && find(null).size() < 2;
    } else {
      return nonNullKeysUnique;
    }
  }

  @Override
  public IntSortedSet find(@Nullable Object key)
  {
    final IntSortedSet found;

    if (key == null) {
      found = nullIndex;
    } else {
      final Object convertedKey = DimensionHandlerUtils.convertObjectToType(key, keyType, false);

      if (convertedKey != null) {
        found = index.get(convertedKey);
      } else {
        // Don't look up null in the index, since this convertedKey is null because it's a failed cast, not a true null.
        found = null;
      }
    }

    if (found != null) {
      return found;
    } else {
      return IntSortedSets.EMPTY_SET;
    }
  }

  @Override
  public int findUniqueLong(long key)
  {
    if (isLong2ObjectMap && nonNullKeysUnique) {
      final IntSortedSet rows = ((Long2ObjectMap<IntSortedSet>) (Map) index).get(key);
      assert rows == null || rows.size() == 1;
      return rows != null ? rows.firstInt() : NOT_FOUND;
    } else {
      throw new UnsupportedOperationException();
    }
  }
}
