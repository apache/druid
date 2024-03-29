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

package org.apache.druid.query.groupby.epinephelinae;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.DimensionDictionary;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utilities for parts of the groupBy engine that need to build dictionaries.
 */
public class DictionaryBuilding
{
  // Entry in dictionary, node pointer in reverseDictionary, hash + k/v/next pointer in reverseDictionary nodes
  private static final int ROUGH_OVERHEAD_PER_DICTIONARY_ENTRY = Long.BYTES * 5 + Integer.BYTES;

  /**
   * Creates a forward dictionary (dictionary ID -> value).
   */
  public static <T> List<T> createDictionary()
  {
    return new ArrayList<>();
  }

  /**
   * Creates a reverse dictionary (value -> dictionary ID). If a value is not present in the reverse dictionary,
   * {@link Object2IntMap#getInt} will return {@link DimensionDictionary#ABSENT_VALUE_ID}.
   *
   * WARNING: This assumes that the .hashCode and the .equals of the method are implemented correctly. This does not
   * apply for primitive array types, which donot consider new Object[]{1L, 2L} = new Object[]{1, 2}. For such objects,
   * (especially arrays), a custom hash strategy must be passed.
   */
  public static <T> Object2IntMap<T> createReverseDictionary()
  {
    final Object2IntOpenHashMap<T> m = new Object2IntOpenHashMap<>();
    m.defaultReturnValue(DimensionDictionary.ABSENT_VALUE_ID);
    return m;
  }

  private static <T> Object2IntMap<T> createReverseDictionary(final Hash.Strategy<T> hashStrategy)
  {
    final Object2IntOpenCustomHashMap<T> m = new Object2IntOpenCustomHashMap<>(hashStrategy);
    m.defaultReturnValue(DimensionDictionary.ABSENT_VALUE_ID);
    return m;
  }

  /**
   * Creates a reverse dictionary for arrays of primitive types.
   */
  public static Object2IntMap<Object[]> createReverseDictionaryForPrimitiveArray(TypeSignature<ValueType> arrayType)
  {
    if (!arrayType.isPrimitiveArray()) {
      throw DruidException.defensive("Dictionary building function expected an array of a primitive type");
    }
    return createReverseDictionary(new Hash.Strategy<Object[]>()
    {
      @Override
      public int hashCode(Object[] o)
      {
        // We don't do a deep comparison, because the array type is primitive, therefore we don't need to incur the extra
        // overhead of checking the nestings
        return Arrays.hashCode(o);
      }

      @Override
      public boolean equals(Object[] a, Object[] b)
      {
        return arrayType.getNullableStrategy().compare(a, b) == 0;
      }
    });
  }

  /**
   * Estimated footprint of a new entry.
   */
  public static int estimateEntryFootprint(final int valueFootprint)
  {
    return valueFootprint + ROUGH_OVERHEAD_PER_DICTIONARY_ENTRY;
  }
}
