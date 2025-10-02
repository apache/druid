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

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryPointer;
import org.apache.druid.query.groupby.epinephelinae.column.DictionaryBuildingGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.DimensionIdCodec;
import org.apache.druid.query.groupby.epinephelinae.column.MemoryFootprint;
import org.apache.druid.segment.vector.VectorObjectSelector;

/**
 * Base class for {@link GroupByVectorColumnSelector} that build dictionaries for values that are not
 * natively dictionary-encoded.
 *
 * @see DictionaryBuildingGroupByColumnSelectorStrategy the nonvectorized version
 */
public abstract class DictionaryBuildingGroupByVectorColumnSelector<T> implements GroupByVectorColumnSelector
{
  protected final VectorObjectSelector selector;
  protected final DimensionIdCodec<T> dimensionIdCodec;

  protected DictionaryBuildingGroupByVectorColumnSelector(
      final VectorObjectSelector selector,
      final DimensionIdCodec<T> dimensionIdCodec
  )
  {
    this.selector = selector;
    this.dimensionIdCodec = dimensionIdCodec;
  }

  @Override
  public final int getGroupingKeySize()
  {
    return Integer.BYTES;
  }

  @Override
  public final int writeKeys(
      final WritableMemory keySpace,
      final int keySize,
      final int keyOffset,
      final int startRow,
      final int endRow
  )
  {
    final Object[] vector = selector.getObjectVector();
    int stateFootprintIncrease = 0;

    for (int i = startRow, j = keyOffset; i < endRow; i++, j += keySize) {
      final T value = convertValue(vector[i]);
      final MemoryFootprint<Integer> idAndMemoryIncrease = dimensionIdCodec.lookupId(value);
      keySpace.putInt(j, idAndMemoryIncrease.value());
      stateFootprintIncrease += idAndMemoryIncrease.memoryIncrease();
    }

    return stateFootprintIncrease;
  }

  @Override
  public final void writeKeyToResultRow(
      final MemoryPointer keyMemory,
      final int keyOffset,
      final ResultRow resultRow,
      final int resultRowPosition
  )
  {
    final int id = keyMemory.memory().getInt(keyMemory.position() + keyOffset);
    final T value = dimensionIdCodec.idToKey(id);
    resultRow.set(resultRowPosition, value);
  }

  @Override
  public final void reset()
  {
    dimensionIdCodec.reset();
  }

  /**
   * Convert raw value from the vector to the appropriate type for this selector.
   */
  protected abstract T convertValue(Object rawValue);
}
