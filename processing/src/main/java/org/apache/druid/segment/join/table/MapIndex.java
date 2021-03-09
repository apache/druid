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

import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ValueType;

import java.util.Map;

/**
 * An {@link IndexedTable.Index} backed by a Map.
 */
public class MapIndex implements IndexedTable.Index
{
  private final ValueType keyType;
  private final Map<Object, IntList> index;
  private final boolean keysUnique;
  private final boolean isLong2ObjectMap;

  /**
   * Creates a new instance based on a particular map.
   *
   * @param keyType    type of keys in "index"
   * @param index      a map of keys to matching row numbers
   * @param keysUnique whether the keys are unique (if true: all IntLists in the index must be exactly 1 element)
   *
   * @see RowBasedIndexBuilder#build() the main caller
   */
  MapIndex(final ValueType keyType, final Map<Object, IntList> index, final boolean keysUnique)
  {
    this.keyType = Preconditions.checkNotNull(keyType, "keyType");
    this.index = Preconditions.checkNotNull(index, "index");
    this.keysUnique = keysUnique;
    this.isLong2ObjectMap = index instanceof Long2ObjectMap;
  }

  @Override
  public ValueType keyType()
  {
    return keyType;
  }

  @Override
  public boolean areKeysUnique()
  {
    return keysUnique;
  }

  @Override
  public IntList find(Object key)
  {
    final Object convertedKey = DimensionHandlerUtils.convertObjectToType(key, keyType, false);

    if (convertedKey != null) {
      final IntList found = index.get(convertedKey);
      if (found != null) {
        return found;
      } else {
        return IntLists.EMPTY_LIST;
      }
    } else {
      return IntLists.EMPTY_LIST;
    }
  }

  @Override
  public int findUniqueLong(long key)
  {
    if (isLong2ObjectMap && keysUnique) {
      final IntList rows = ((Long2ObjectMap<IntList>) (Map) index).get(key);
      assert rows == null || rows.size() == 1;
      return rows != null ? rows.getInt(0) : NOT_FOUND;
    } else {
      throw new UnsupportedOperationException();
    }
  }
}
