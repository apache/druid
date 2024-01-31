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

import org.apache.druid.collections.SerializablePair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorObjectSelector;
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
  final VectorObjectSelector objectSelector;
  final VectorValueSelector valueSelector;
  private final boolean useDefault = NullHandling.replaceWithDefault();
  private final VectorValueSelector timeSelector;
  private long firstTime;

  NumericFirstVectorAggregator(VectorValueSelector timeSelector, VectorValueSelector valueSelector, VectorObjectSelector objectSelector)
  {
    this.timeSelector = timeSelector;
    this.objectSelector = objectSelector;
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
    final boolean[] nullTimeVector = timeSelector.getNullVector();

    Object[] objectsWhichMightBeNumeric = null;
    boolean[] nullValueVector = null;

    if (objectSelector != null) {
      objectsWhichMightBeNumeric = objectSelector.getObjectVector();
    } else if (valueSelector != null) {
      nullValueVector = valueSelector.getNullVector();
    }

    firstTime = buf.getLong(position);

    // the time vector is already sorted
    // if earliest is on the default time dimension
    // but if earliest uses earliest_by it might use a secondary timestamp
    // which is not sorted. For correctness, we need to go over all elements.
    // A possible optimization here is to have 2 paths one for earliest where
    // we can take advantage of the sorted nature of time
    // and the earliest_by where we have to go over all elements.

    int index;
    for (int i = startRow; i < endRow; i++) {
      if (nullTimeVector != null && nullTimeVector[i]) {
        continue;
      }

      if (timeVector[i] >= firstTime) {
        continue;
      }
      index = i;

      if (objectsWhichMightBeNumeric != null) {
        final SerializablePair<Long, Number> inPair = (SerializablePair<Long, Number>) objectsWhichMightBeNumeric[index];
        if (inPair.lhs != null && inPair.lhs < firstTime) {
          firstTime = inPair.lhs;
          if (useDefault || inPair.rhs != null) {
            updateTimeWithValue(buf, position, firstTime, index);
          } else {
            updateTimeWithNull(buf, position, firstTime);
          }
        }
      } else {
        final long earliestTime = timeVector[index];

        if (earliestTime < firstTime) {
          firstTime = earliestTime;
          if (useDefault || nullValueVector == null || !nullValueVector[index]) {
            updateTimeWithValue(buf, position, earliestTime, index);
          } else {
            updateTimeWithNull(buf, position, earliestTime);
          }
        }
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
    final long[] timeVector = timeSelector.getLongVector();

    Object[] objectsWhichMightBeNumeric = null;
    boolean[] nulls = null;
    if (objectSelector != null) {
      objectsWhichMightBeNumeric = objectSelector.getObjectVector();
    } else if (valueSelector != null) {
      nulls = useDefault ? null : valueSelector.getNullVector();
    }

    for (int i = 0; i < numRows; i++) {
      int position = positions[i] + positionOffset;
      int row = rows == null ? i : rows[i];
      firstTime = buf.getLong(position);

      if (objectsWhichMightBeNumeric != null) {
        final SerializablePair<Long, Number> inPair = (SerializablePair<Long, Number>) objectsWhichMightBeNumeric[row];
        if (useDefault || inPair != null) {
          if (inPair.lhs != null && inPair.lhs < firstTime) {
            if (inPair.rhs != null) {
              updateTimeWithValue(buf, position, inPair.lhs, row);
            } else {
              updateTimeWithNull(buf, position, inPair.lhs);
            }
          }
        }
      } else {
        if (timeVector[row] < firstTime) {
          if (useDefault || nulls == null || !nulls[row]) {
            updateTimeWithValue(buf, position, timeVector[row], row);
          } else {
            updateTimeWithNull(buf, position, timeVector[row]);
          }
        }
      }
    }
  }

  /**
   * Updates the time and the non null values to the appropriate position in buffer
   *
   * @param buf         byte buffer storing the byte array representation of the aggregate
   * @param position    offset within the byte buffer at which the current aggregate value is stored
   * @param time        the time to be updated in the buffer as the first time
   * @param index       he index of the vectorized vector which is the first value
   */
  void updateTimeWithValue(ByteBuffer buf, int position, long time, int index)
  {
    buf.putLong(position, time);
    buf.put(position + NULL_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
    putValue(buf, position + VALUE_OFFSET, index);
  }

  /**
   * Updates the time only to the appropriate position in buffer as the value is null
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
   * Abstract function which needs to be overridden by subclasses to set the
   * latest value in the buffer depending on the datatype
   */
  abstract void putValue(ByteBuffer buf, int position, int index);

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
