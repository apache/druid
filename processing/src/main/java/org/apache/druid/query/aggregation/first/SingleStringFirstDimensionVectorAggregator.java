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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.BaseLongVectorValueSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SingleStringFirstDimensionVectorAggregator implements VectorAggregator
{
  private final BaseLongVectorValueSelector timeSelector;
  private final SingleValueDimensionVectorSelector valueDimensionVectorSelector;
  private long firstTime;
  private final int maxStringBytes;
  private final boolean useDefault = NullHandling.replaceWithDefault();

  public SingleStringFirstDimensionVectorAggregator(
      BaseLongVectorValueSelector timeSelector,
      SingleValueDimensionVectorSelector valueDimensionVectorSelector,
      int maxStringBytes
  )
  {
    this.timeSelector = timeSelector;
    this.valueDimensionVectorSelector = valueDimensionVectorSelector;
    this.maxStringBytes = maxStringBytes;
    this.firstTime = Long.MAX_VALUE;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, Long.MAX_VALUE);
    buf.put(
        position + NumericFirstVectorAggregator.NULL_OFFSET,
        useDefault ? NullHandling.IS_NOT_NULL_BYTE : NullHandling.IS_NULL_BYTE
    );
    buf.putLong(position + NumericFirstVectorAggregator.VALUE_OFFSET, 0);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    final long[] timeVector = timeSelector.getLongVector();
    final int[] valueVector = valueDimensionVectorSelector.getRowVector();
    firstTime = buf.getLong(position);
    int index;
    long earliestTime;

    // Now we are iterating over the values to find the minima as the
    // timestamp expression in EARLIEST_BY has no established sorting order
    // If we know that the time is already sorted this can be optimized
    // for the general EARLIEST call which is always on __time which is sorted
    for (index = startRow; index < endRow; index++) {
      earliestTime = timeVector[index];
      if (earliestTime < firstTime) {
        firstTime = earliestTime;
        buf.putLong(position, firstTime);
        buf.put(position + NumericFirstVectorAggregator.NULL_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
        buf.putInt(position + NumericFirstVectorAggregator.VALUE_OFFSET, valueVector[index]);
      }
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    long[] timeVector = timeSelector.getLongVector();
    boolean[] nullTimeVector = timeSelector.getNullVector();
    int[] values = valueDimensionVectorSelector.getRowVector();
    // Now we are iterating over the values to find the minima as the
    // timestamp expression in EARLIEST_BY has no established sorting order
    // If we know that the time is already sorted this can be optimized
    // for the general EARLIEST call which is always on __time which is sorted

    // The hotpath is separated out into 2 cases when nullTimeVector
    // is null and not-null so that the check is not on every value
    if (nullTimeVector != null) {
      for (int i = 0; i < numRows; i++) {
        if (nullTimeVector[i]) {
          continue;
        }
        int position = positions[i] + positionOffset;
        int row = rows == null ? i : rows[i];
        long firstTime = buf.getLong(position);
        if (timeVector[row] < firstTime) {
          firstTime = timeVector[row];
          buf.putLong(position, firstTime);
          buf.put(position + NumericFirstVectorAggregator.NULL_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
          buf.putInt(position + NumericFirstVectorAggregator.VALUE_OFFSET, values[row]);
        }
      }
    } else {
      for (int i = 0; i < numRows; i++) {
        int position = positions[i] + positionOffset;
        int row = rows == null ? i : rows[i];
        long firstTime = buf.getLong(position);
        if (timeVector[row] < firstTime) {
          firstTime = timeVector[row];
          buf.putLong(position, firstTime);
          buf.put(position + NumericFirstVectorAggregator.NULL_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
          buf.putInt(position + NumericFirstVectorAggregator.VALUE_OFFSET, values[row]);
        }
      }
    }


  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    int index = buf.getInt(position + NumericFirstVectorAggregator.VALUE_OFFSET);
    long earliest = buf.getLong(position);
    String strValue = valueDimensionVectorSelector.lookupName(index);
    return new SerializablePairLongString(earliest, StringUtils.chop(strValue, maxStringBytes));
  }

  @Override
  public void close()
  {
    // nothing to close
  }
}
