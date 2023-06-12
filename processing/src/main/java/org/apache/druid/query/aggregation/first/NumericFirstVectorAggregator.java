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

package org.apache.druid.query.aggregation.first;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Class for vectorized version of first/earliest aggregator over numeric types
 */
public abstract class NumericFirstVectorAggregator implements VectorAggregator
{
  static final int NULL_OFFSET = Long.BYTES;
  static final int VALUE_OFFSET = NULL_OFFSET + Byte.BYTES;
  final VectorValueSelector valueSelector;
  private final boolean useDefault = NullHandling.replaceWithDefault();
  private final VectorValueSelector timeSelector;
  private long firstTime;

  public NumericFirstVectorAggregator(VectorValueSelector timeSelector, VectorValueSelector valueSelector)
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
    firstTime = Long.MAX_VALUE;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, Long.MAX_VALUE);
    buf.put(position + NULL_OFFSET, useDefault ? NullHandling.IS_NOT_NULL_BYTE : NullHandling.IS_NULL_BYTE);
    initValue(buf, position + VALUE_OFFSET);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    final long[] timeVector = timeSelector.getLongVector();
    final boolean[] nullValueVector = valueSelector.getNullVector();
    boolean nullAbsent = false;
    firstTime = buf.getLong(position);
    // check if nullVector is found or not
    // the nullVector is null if no null values are found
    // set the nullAbsent flag accordingly
    if (nullValueVector == null) {
      nullAbsent = true;
    }

    // the time vector is already sorted so the first element would be the earliest
    // traverse accordingly
    int index = startRow;
    if (!useDefault && !nullAbsent) {
      for (int i = startRow; i < endRow; i++) {
        if (!nullValueVector[i]) {
          index = i;
          break;
        }
      }
    }

    // find the first non-null value
    final long earliestTime = timeVector[index];
    if (earliestTime < firstTime) {
      firstTime = earliestTime;
      if (useDefault || nullValueVector == null || !nullValueVector[index]) {
        updateTimeWithValue(buf, position, firstTime, index);
      } else {
        updateTimeWithNull(buf, position, firstTime);
      }
    }
  }

  /**
   *
   * Checks if the aggregated value at a position in the buffer is null or not
   *
   * @param buf         byte buffer storing the byte array representation of the aggregate
   * @param position    offset within the byte buffer at which the current aggregate value is stored
   * @return
   */
  boolean isValueNull(ByteBuffer buf, int position)
  {
    return buf.get(position + NULL_OFFSET) == NullHandling.IS_NULL_BYTE;
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
    boolean[] nulls = useDefault ? null : valueSelector.getNullVector();
    long[] timeVector = timeSelector.getLongVector();

    for (int i = 0; i < numRows; i++) {
      int position = positions[i] + positionOffset;
      int row = rows == null ? i : rows[i];
      long firstTime = buf.getLong(position);
      if (timeVector[row] < firstTime) {
        if (useDefault || nulls == null || !nulls[row]) {
          updateTimeWithValue(buf, position, timeVector[row], row);
        } else {
          updateTimeWithNull(buf, position, timeVector[row]);
        }
      }
    }
  }

  /**
   * Updates the time and the non null values to the appropriate position in buffer
   *
   * @param buf         byte buffer storing the byte array representation of the aggregate
   * @param position    offset within the byte buffer at which the current aggregate value is stored
   * @param time        the time to be updated in the buffer as the last time
   * @param index       the index of the vectorized vector which is the last value
   */
  void updateTimeWithValue(ByteBuffer buf, int position, long time, int index)
  {
    buf.putLong(position, time);
    buf.put(position + NULL_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
    putValue(buf, position + VALUE_OFFSET, index);
  }

  /**
   *Updates the time only to the appropriate position in buffer as the value is null
   *
   * @param buf         byte buffer storing the byte array representation of the aggregate
   * @param position    offset within the byte buffer at which the current aggregate value is stored
   * @param time        the time to be updated in the buffer as the last time
   */
  void updateTimeWithNull(ByteBuffer buf, int position, long time)
  {
    buf.putLong(position, time);
    buf.put(position + NULL_OFFSET, NullHandling.IS_NULL_BYTE);
  }

  /**
   *Abstract function which needs to be overridden by subclasses to set the initial value
   */
  abstract void initValue(ByteBuffer buf, int position);

  /**
   *Abstract function which needs to be overridden by subclasses to set the
   * latest value in the buffer depending on the datatype
   */
  abstract void putValue(ByteBuffer buf, int position, int index);

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
