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

package org.apache.druid.query.topn.vector;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryPointer;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link TopNVectorColumnSelector} for single-valued STRING columns that are not natively dictionary-encoded,
 * such as expression virtual columns. Builds a local int dictionary on-the-fly and encodes keys as 4-byte
 * dictionary IDs, matching the key format of {@link SingleValueStringTopNVectorColumnSelector}.
 */
public class DictionaryBuildingSingleValueStringTopNVectorColumnSelector implements TopNVectorColumnSelector
{
  private final VectorObjectSelector selector;
  private final List<String> dictionary = new ArrayList<>();
  private final Object2IntMap<String> reverseDictionary = new Object2IntOpenHashMap<>();

  DictionaryBuildingSingleValueStringTopNVectorColumnSelector(final VectorObjectSelector selector)
  {
    this.selector = selector;
    reverseDictionary.defaultReturnValue(-1);
  }

  @Override
  public int getGroupingKeySize()
  {
    return Integer.BYTES;
  }

  @Override
  public void writeKeys(
      final WritableMemory keySpace,
      final int keySize,
      final int keyOffset,
      final int startRow,
      final int endRow
  )
  {
    final Object[] vector = selector.getObjectVector();

    for (int i = startRow, j = keyOffset; i < endRow; i++, j += keySize) {
      final String value = DimensionHandlerUtils.convertObjectToString(vector[i]);
      int dictId = reverseDictionary.getInt(value);
      if (dictId < 0) {
        dictId = dictionary.size();
        dictionary.add(value);
        reverseDictionary.put(value, dictId);
      }
      keySpace.putInt(j, dictId);
    }
  }

  @Override
  @Nullable
  public Object getDimensionValue(final MemoryPointer keyMemory, final int keyOffset)
  {
    return dictionary.get(keyMemory.memory().getInt(keyMemory.position() + keyOffset));
  }

  @Override
  public void reset()
  {
    dictionary.clear();
    reverseDictionary.clear();
  }
}
