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

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryPointer;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;

/**
 * {@link TopNVectorColumnSelector} for nullable FLOAT columns.
 * Keys are encoded as a 1-byte null flag followed by a 4-byte float value.
 */
public class NullableFloatTopNVectorColumnSelector implements TopNVectorColumnSelector
{
  private final VectorValueSelector selector;

  NullableFloatTopNVectorColumnSelector(final VectorValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public int getGroupingKeySize()
  {
    return Byte.BYTES + Float.BYTES;
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
    final float[] vector = selector.getFloatVector();
    final boolean[] nulls = selector.getNullVector();

    if (nulls != null) {
      for (int i = startRow, j = keyOffset; i < endRow; i++, j += keySize) {
        keySpace.putByte(j, nulls[i] ? TypeStrategies.IS_NULL_BYTE : TypeStrategies.IS_NOT_NULL_BYTE);
        keySpace.putFloat(j + 1, vector[i]);
      }
    } else {
      for (int i = startRow, j = keyOffset; i < endRow; i++, j += keySize) {
        keySpace.putByte(j, TypeStrategies.IS_NOT_NULL_BYTE);
        keySpace.putFloat(j + 1, vector[i]);
      }
    }
  }

  @Override
  @Nullable
  public Object getDimensionValue(final MemoryPointer keyMemory, final int keyOffset)
  {
    if (keyMemory.memory().getByte(keyMemory.position() + keyOffset) == TypeStrategies.IS_NULL_BYTE) {
      return null;
    }
    return keyMemory.memory().getFloat(keyMemory.position() + keyOffset + 1);
  }

  @Override
  public void reset()
  {
    // Nothing to do.
  }
}
