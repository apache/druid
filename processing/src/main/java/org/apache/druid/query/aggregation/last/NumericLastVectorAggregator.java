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

package org.apache.druid.query.aggregation.last;

import org.apache.druid.collections.SerializablePair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Base type for vectorized version of on heap 'last' aggregator for primitive numeric column selectors..
 */
public abstract class NumericLastVectorAggregator implements VectorAggregator
{
  static final int NULL_OFFSET = Long.BYTES;
  static final int VALUE_OFFSET = NULL_OFFSET + Byte.BYTES;
  final VectorValueSelector valueSelector;
  final VectorObjectSelector objectSelector;
  private final boolean useDefault = NullHandling.replaceWithDefault();
  private final VectorValueSelector timeSelector;
  private long lastTime;


  NumericLastVectorAggregator(VectorValueSelector timeSelector, VectorValueSelector valueSelector, VectorObjectSelector objectSelector)
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
    this.objectSelector = objectSelector;
    lastTime = Long.MIN_VALUE;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, Long.MIN_VALUE);
    buf.put(position + NULL_OFFSET, useDefault ? NullHandling.IS_NOT_NULL_BYTE : NullHandling.IS_NULL_BYTE);
    initValue(buf, position + VALUE_OFFSET);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    if (timeSelector == null) {
      return;
    }

    final long[] timeVector = timeSelector.getLongVector();
    Object[] objectsWhichMightBeNumeric = null;
    boolean[] nullValueVector = null;
    boolean nullAbsent = false;

    if (objectSelector != null) {
      objectsWhichMightBeNumeric = objectSelector.getObjectVector();
    } else if (valueSelector != null) {
      nullValueVector = valueSelector.getNullVector();
    }

    lastTime = buf.getLong(position);

    if (nullValueVector == null) {
      nullAbsent = true;
    }

    //the time vector is already sorted so the last element would be the latest
    //traverse the value vector from the back (for latest)
    int index = endRow - 1;
    if (!useDefault && !nullAbsent) {
      for (int i = endRow - 1; i >= startRow; i--) {
        if (!nullValueVector[i]) {
          index = i;
          break;
        }
      }
    }

    if (objectsWhichMightBeNumeric != null) {
      final SerializablePair<Long, Number> inPair = (SerializablePair<Long, Number>) objectsWhichMightBeNumeric[index];
      if (inPair.lhs != null && inPair.lhs >= lastTime) {
        lastTime = inPair.lhs;
        if (useDefault || inPair.rhs != null) {
          updateTimeWithValue(buf, position, lastTime, index);
        } else {
          updateTimeWithNull(buf, position, lastTime);
        }
      }
    } else {
      final long latestTime = timeVector[index];
      if (latestTime >= lastTime) {
        lastTime = latestTime;
        if (useDefault || nullValueVector == null || !nullValueVector[index]) {
          updateTimeWithValue(buf, position, lastTime, index);
        } else {
          updateTimeWithNull(buf, position, lastTime);
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
    if (timeSelector == null) {
      return;
    }

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
      long lastTime = buf.getLong(position);

      if (objectsWhichMightBeNumeric != null) {
        final SerializablePair<Long, Number> inPair = (SerializablePair<Long, Number>) objectsWhichMightBeNumeric[row];
        if (useDefault || inPair != null) {
          if (inPair.lhs != null && inPair.lhs >= lastTime) {
            if (inPair.rhs != null) {
              updateTimeWithValue(buf, position, inPair.lhs, row);
            } else {
              updateTimeWithNull(buf, position, inPair.lhs);
            }
          }
        }
      } else {
        if (timeVector[row] >= lastTime) {
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
   *
   * @param buf         byte buffer storing the byte array representation of the aggregate
   * @param position    offset within the byte buffer at which the current aggregate value is stored
   * @param time        the time to be updated in the buffer as the last time
   * @param index       he index of the vectorized vector which is the last value
   */
  void updateTimeWithValue(ByteBuffer buf, int position, long time, int index)
  {
    buf.putLong(position, time);
    buf.put(position + NULL_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
    putValue(buf, position + VALUE_OFFSET, index);
  }

  /**
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
