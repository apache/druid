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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.VectorAggregator;
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
  private final boolean useDefault = NullHandling.replaceWithDefault();
  private final VectorValueSelector timeSelector;
  long lastTime;
  boolean rhsNull;

  public NumericLastVectorAggregator(VectorValueSelector timeSelector, VectorValueSelector valueSelector)
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
    lastTime = Long.MIN_VALUE;
    rhsNull = !useDefault;
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
    final long[] timeVector = timeSelector.getLongVector();
    final boolean[] nullValueVector = valueSelector.getNullVector();
    boolean nullAbsent = false;
    lastTime = buf.getLong(position);
    //check if nullVector is found or not
    // the nullVector is null if no null values are found
    // set the nullAbsent flag accordingly
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

    //find the first non-null value
    final long latestTime = timeVector[index];
    if (latestTime >= lastTime) {
      lastTime = latestTime;
      if (useDefault || nullValueVector == null || !nullValueVector[index]) {
        updateTimeWithValue(buf, position, lastTime, index);
        rhsNull = false;
      } else {
        updateTimeWithNull(buf, position, lastTime);
        rhsNull = true;
      }
    }
  }

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
      long lastTime = buf.getLong(position);
      if (timeVector[row] >= lastTime) {
        if (useDefault || nulls == null || !nulls[row]) {
          updateTimeWithValue(buf, position, timeVector[row], row);
        } else {
          updateTimeWithNull(buf, position, timeVector[row]);
        }
      }
    }
  }

  void updateTimeWithValue(ByteBuffer buf, int position, long time, int index)
  {
    buf.putLong(position, time);
    buf.put(position + NULL_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
    putValue(buf, position + VALUE_OFFSET, index);
  }

  void updateTimeWithNull(ByteBuffer buf, int position, long time)
  {
    buf.putLong(position, time);
    buf.put(position + NULL_OFFSET, NullHandling.IS_NULL_BYTE);
  }

  abstract void initValue(ByteBuffer buf, int position);

  abstract void putValue(ByteBuffer buf, int position, int index);

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
