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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public abstract class NumericAnyVectorAggregator implements VectorAggregator
{
  // Rightmost bit for is null check (0 for is null and 1 for not null)
  // Second rightmost bit for is found check (0 for not found and 1 for found)
  private static final byte BYTE_FLAG_FOUND_MASK = 0x02;
  private static final byte BYTE_FLAG_NULL_MASK = 0x01;
  protected static final int FOUND_VALUE_OFFSET = Byte.BYTES;

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
   * Place the primitive value in the buffer given the initial offset position within the byte buffer
   * at which the current aggregate value is stored.
   * @return true if a value was put on the buffer, false otherwise.
   */
  abstract boolean putValue(ByteBuffer buf, int position, int startRow, int endRow);

  /**
   * Place the primitive null value in the buffer, fiven the initial offset position within the byte buffer
   * at which the current aggregate value is stored.
   */
  abstract void putNonNullValue(ByteBuffer buf, int position, Object value);

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.put(position, replaceWithDefault ? NullHandling.IS_NOT_NULL_BYTE : NullHandling.IS_NULL_BYTE);
    initValue(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    if ((buf.get(position) & BYTE_FLAG_FOUND_MASK) != BYTE_FLAG_FOUND_MASK) {
      boolean[] nulls = vectorValueSelector.getNullVector();
      // check if there are any nulls
      if (nulls != null && startRow <= nulls.length) {
        for (int i = startRow; i < endRow; i++) {
          // And there is actually a null
          if (nulls[i]) {
            putValue(buf, position, null);
            return;
          }
        }
      }
      // There are no nulls, so put a value from the value selector
      if (putValue(buf, position, startRow, endRow)) {
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
    int prevPosition = -1;
    @Nullable Object theValue = null;
    boolean found = false;
    for (int i = 0; i < numRows; i++) {
      int position = positions[i] + positionOffset;
      int row = rows == null ? i : rows[i];
      // If the aggregate is not found at the position
      if ((buf.get(position) & BYTE_FLAG_FOUND_MASK) != BYTE_FLAG_FOUND_MASK) {
        // If there's a value at the previous position, use it in this position.
        if (prevPosition >= 0 && (found || (buf.get(prevPosition) & BYTE_FLAG_FOUND_MASK) == BYTE_FLAG_FOUND_MASK)) {
          if (!found) {
            theValue = get(buf, prevPosition);
            found = true;
          }
          putValue(buf, position, theValue);
        } else {
          aggregate(buf, position, row, row);
        }
      }
      prevPosition = position;
    }
  }

  @Override
  public void close()
  {
    // No resources to cleanup.
  }

  protected boolean isValueNull(ByteBuffer buf, int position)
  {
    return (buf.get(position) & BYTE_FLAG_NULL_MASK) == NullHandling.IS_NULL_BYTE;
  }

  private void putValue(ByteBuffer buf, int position, @Nullable Object value)
  {
    if (value == null) {
      if (!replaceWithDefault) {
        buf.put(position, (byte) (BYTE_FLAG_FOUND_MASK | NullHandling.IS_NULL_BYTE));
      } else {
        putNonNullValue(buf, position, 0);
        buf.put(position, (byte) (BYTE_FLAG_FOUND_MASK | NullHandling.IS_NOT_NULL_BYTE));
      }
    } else {
      putNonNullValue(buf, position, value);
      buf.put(position, (byte) (BYTE_FLAG_FOUND_MASK | NullHandling.IS_NOT_NULL_BYTE));
    }
  }
}
