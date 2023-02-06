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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryPointer;
import org.apache.druid.segment.vector.VectorValueSelector;

public class NullableDoubleGroupByVectorColumnSelector implements GroupByVectorColumnSelector
{
  private final VectorValueSelector selector;

  NullableDoubleGroupByVectorColumnSelector(final VectorValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public int getGroupingKeySize()
  {
    return Byte.BYTES + Double.BYTES;
  }

  @Override
  public int writeKeys(
      final WritableMemory keySpace,
      final int keySize,
      final int keyOffset,
      final int startRow,
      final int endRow
  )
  {
    final double[] vector = selector.getDoubleVector();
    final boolean[] nulls = selector.getNullVector();

    if (nulls != null) {
      for (int i = startRow, j = keyOffset; i < endRow; i++, j += keySize) {
        keySpace.putByte(j, nulls[i] ? NullHandling.IS_NULL_BYTE : NullHandling.IS_NOT_NULL_BYTE);
        keySpace.putDouble(j + 1, vector[i]);
      }
    } else {
      for (int i = startRow, j = keyOffset; i < endRow; i++, j += keySize) {
        keySpace.putByte(j, NullHandling.IS_NOT_NULL_BYTE);
        keySpace.putDouble(j + 1, vector[i]);
      }
    }

    return 0;
  }

  @Override
  public void writeKeyToResultRow(
      final MemoryPointer keyMemory,
      final int keyOffset,
      final ResultRow resultRow,
      final int resultRowPosition
  )
  {
    if (keyMemory.memory().getByte(keyMemory.position() + keyOffset) == NullHandling.IS_NULL_BYTE) {
      resultRow.set(resultRowPosition, null);
    } else {
      resultRow.set(resultRowPosition, keyMemory.memory().getDouble(keyMemory.position() + keyOffset + 1));
    }
  }

  @Override
  public void reset()
  {
    // Nothing to do.
  }
}
