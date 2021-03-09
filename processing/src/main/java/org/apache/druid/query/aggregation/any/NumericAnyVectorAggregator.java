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

package org.apache.druid.query.aggregation.any;

import org.apache.druid.com.google.common.annotations.VisibleForTesting;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public abstract class NumericAnyVectorAggregator implements VectorAggregator
{
  // Rightmost bit for is null check (0 for is null and 1 for not null)
  // Second rightmost bit for is found check (0 for not found and 1 for found)
  @VisibleForTesting
  static final byte BYTE_FLAG_FOUND_MASK = 0x02;
  private static final byte BYTE_FLAG_NULL_MASK = 0x01;
  private static final int FOUND_VALUE_OFFSET = Byte.BYTES;

  private final boolean replaceWithDefault = NullHandling.replaceWithDefault();
  protected final VectorValueSelector vectorValueSelector;

  public NumericAnyVectorAggregator(VectorValueSelector vectorValueSelector)
  {
    this.vectorValueSelector = vectorValueSelector;
  }

  /**
   * Initialize the buffer value given the initial offset position within the byte buffer for initialization
   */
  abstract void initValue(ByteBuffer buf, int position);

  /**
   * Place any primitive value from the rows, starting at {@param startRow}(inclusive) up to {@param endRow}(exclusive),
   * in the buffer given the initial offset position within the byte buffer at which the current aggregate value
   * is stored.
   * @return true if a value was added, false otherwise
   */
  abstract boolean putAnyValueFromRow(ByteBuffer buf, int position, int startRow, int endRow);

  /**
   * @return The primitive object stored at the position in the buffer.
   */
  abstract Object getNonNullObject(ByteBuffer buf, int position);

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.put(position, replaceWithDefault ? NullHandling.IS_NOT_NULL_BYTE : NullHandling.IS_NULL_BYTE);
    initValue(buf, position + FOUND_VALUE_OFFSET);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    if ((buf.get(position) & BYTE_FLAG_FOUND_MASK) != BYTE_FLAG_FOUND_MASK) {
      boolean[] nulls = vectorValueSelector.getNullVector();
      // check if there are any nulls
      if (nulls != null) {
        for (int i = startRow; i < endRow && i < nulls.length; i++) {
          // And there is actually a null
          if (nulls[i]) {
            putNull(buf, position);
            return;
          }
        }
      }
      // There are no nulls, so try to put a value from the value selector
      if (putAnyValueFromRow(buf, position + FOUND_VALUE_OFFSET, startRow, endRow)) {
        buf.put(position, (byte) (BYTE_FLAG_FOUND_MASK | NullHandling.IS_NOT_NULL_BYTE));
      }
    }
  }

  @Override
  public void aggregate(
      ByteBuffer buf,
      int numRows,
      int[] positions,
      @Nullable int[] rows,
      int positionOffset
  )
  {
    for (int i = 0; i < numRows; i++) {
      int position = positions[i] + positionOffset;
      int row = rows == null ? i : rows[i];
      aggregate(buf, position, row, row + 1);
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    final boolean isNull = isValueNull(buf, position);
    return isNull ? null : getNonNullObject(buf, position + FOUND_VALUE_OFFSET);
  }

  @Override
  public void close()
  {
    // No resources to cleanup.
  }

  @VisibleForTesting
  boolean isValueNull(ByteBuffer buf, int position)
  {
    return (buf.get(position) & BYTE_FLAG_NULL_MASK) == NullHandling.IS_NULL_BYTE;
  }

  private void putNull(ByteBuffer buf, int position)
  {
    if (!replaceWithDefault) {
      buf.put(position, (byte) (BYTE_FLAG_FOUND_MASK | NullHandling.IS_NULL_BYTE));
    } else {
      initValue(buf, position + FOUND_VALUE_OFFSET);
      buf.put(position, (byte) (BYTE_FLAG_FOUND_MASK | NullHandling.IS_NOT_NULL_BYTE));
    }
  }
}
