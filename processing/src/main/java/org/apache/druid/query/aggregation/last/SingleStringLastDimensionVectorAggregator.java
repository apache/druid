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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SingleStringLastDimensionVectorAggregator implements VectorAggregator
{
  private final VectorValueSelector timeSelector;
  private final SingleValueDimensionVectorSelector valueDimensionVectorSelector;
  private long lastTime;
  private final int maxStringBytes;
  private final boolean useDefault = NullHandling.replaceWithDefault();

  public SingleStringLastDimensionVectorAggregator(
      VectorValueSelector timeSelector,
      SingleValueDimensionVectorSelector valueDimensionVectorSelector,
      int maxStringBytes
  )
  {
    this.timeSelector = timeSelector;
    this.valueDimensionVectorSelector = valueDimensionVectorSelector;
    this.maxStringBytes = maxStringBytes;
    this.lastTime = Long.MIN_VALUE;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, Long.MIN_VALUE);
    buf.put(
        position + NumericLastVectorAggregator.NULL_OFFSET,
        useDefault ? NullHandling.IS_NOT_NULL_BYTE : NullHandling.IS_NULL_BYTE
    );
    buf.putInt(position + NumericLastVectorAggregator.VALUE_OFFSET, 0);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    final long[] timeVector = timeSelector.getLongVector();
    final boolean[] nullTimeVector = timeSelector.getNullVector();
    final int[] valueVector = valueDimensionVectorSelector.getRowVector();
    lastTime = buf.getLong(position);
    int index;

    long latestTime;
    for (index = endRow - 1; index >= startRow; index--) {
      if (nullTimeVector != null && nullTimeVector[index]) {
        continue;
      }
      latestTime = timeVector[index];
      if (latestTime > lastTime) {
        lastTime = latestTime;
        buf.putLong(position, lastTime);
        buf.put(position + NumericLastVectorAggregator.NULL_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
        buf.putInt(position + NumericLastVectorAggregator.VALUE_OFFSET, valueVector[index]);
      }
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    final long[] timeVector = timeSelector.getLongVector();
    final boolean[] nullTimeVector = timeSelector.getNullVector();
    final int[] values = valueDimensionVectorSelector.getRowVector();
    for (int i = numRows - 1; i >= 0; i--) {
      if (nullTimeVector != null && nullTimeVector[i]) {
        continue;
      }
      int position = positions[i] + positionOffset;
      int row = rows == null ? i : rows[i];
      lastTime = buf.getLong(position);
      if (timeVector[row] > lastTime) {
        lastTime = timeVector[row];
        buf.putLong(position, lastTime);
        buf.put(position + NumericLastVectorAggregator.NULL_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
        buf.putInt(position + NumericLastVectorAggregator.VALUE_OFFSET, values[row]);
      }
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    int index = buf.getInt(position + NumericLastVectorAggregator.VALUE_OFFSET);
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
