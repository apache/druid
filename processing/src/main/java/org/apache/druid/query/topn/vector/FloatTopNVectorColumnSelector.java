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
import org.apache.druid.segment.vector.VectorValueSelector;

/**
 * {@link TopNVectorColumnSelector} for non-nullable FLOAT columns.
 * Keys are encoded as 4-byte floats in the shared key space.
 */
public class FloatTopNVectorColumnSelector implements TopNVectorColumnSelector
{
  private final VectorValueSelector selector;

  FloatTopNVectorColumnSelector(final VectorValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public int getGroupingKeySize()
  {
    return Float.BYTES;
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

    if (keySize == Float.BYTES) {
      keySpace.putFloatArray(keyOffset, vector, startRow, endRow - startRow);
    } else {
      for (int i = startRow, j = keyOffset; i < endRow; i++, j += keySize) {
        keySpace.putFloat(j, vector[i]);
      }
    }
  }

  @Override
  public Object getDimensionValue(final MemoryPointer keyMemory, final int keyOffset)
  {
    return keyMemory.memory().getFloat(keyMemory.position() + keyOffset);
  }

  @Override
  public void reset()
  {
    // Nothing to do.
  }
}
