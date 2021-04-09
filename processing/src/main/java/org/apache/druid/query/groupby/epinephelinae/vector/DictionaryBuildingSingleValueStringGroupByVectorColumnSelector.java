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

package org.apache.druid.query.groupby.epinephelinae.vector;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.segment.vector.VectorObjectSelector;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link GroupByVectorColumnSelector} that builds an internal String<->Integer dictionary, used for grouping
 * single-valued STRING columns which are not natively dictionary encoded, e.g. expression virtual columns.
 *
 * This is effectively the {@link VectorGroupByEngine} analog of
 * {@link org.apache.druid.query.groupby.epinephelinae.column.DictionaryBuildingStringGroupByColumnSelectorStrategy}
 */
public class DictionaryBuildingSingleValueStringGroupByVectorColumnSelector implements GroupByVectorColumnSelector
{
  private static final int GROUP_BY_MISSING_VALUE = -1;

  private final VectorObjectSelector selector;

  private int nextId = 0;
  private final List<String> dictionary = new ArrayList<>();
  private final Object2IntOpenHashMap<String> reverseDictionary = new Object2IntOpenHashMap<>();

  {
    reverseDictionary.defaultReturnValue(-1);
  }

  public DictionaryBuildingSingleValueStringGroupByVectorColumnSelector(VectorObjectSelector selector)
  {
    this.selector = selector;
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
      final String value = (String) vector[i];
      final int dictId = reverseDictionary.getInt(value);
      if (dictId < 0) {
        dictionary.add(value);
        reverseDictionary.put(value, nextId);
        keySpace.putInt(j, nextId);
        nextId++;
      } else {
        keySpace.putInt(j, dictId);
      }
    }
  }

  @Override
  public void writeKeyToResultRow(
      final Memory keyMemory,
      final int keyOffset,
      final ResultRow resultRow,
      final int resultRowPosition
  )
  {
    final int id = keyMemory.getInt(keyOffset);
    // GROUP_BY_MISSING_VALUE is used to indicate empty rows, which are omitted from the result map.
    if (id != GROUP_BY_MISSING_VALUE) {
      final String value = dictionary.get(id);
      resultRow.set(resultRowPosition, value);
    } else {
      resultRow.set(resultRowPosition, NullHandling.defaultStringValue());
    }
  }
}
