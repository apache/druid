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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.aggregation.first.StringFirstLastUtils;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class StringLastVectorAggregator implements VectorAggregator
{
  private static final SerializablePairLongString INIT = new SerializablePairLongString(
      DateTimes.MIN.getMillis(),
      null
  );
  private final VectorValueSelector timeSelector;
  private final VectorObjectSelector valueSelector;
  private final int maxStringBytes;
  private final boolean needsFoldCheck;
  protected long lastTime;
  protected String lastValue;

  public StringLastVectorAggregator(
      final VectorValueSelector timeSelector,
      final VectorObjectSelector valueSelector,
      final int maxStringBytes,
      final boolean needsFoldCheck
  )
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
    this.maxStringBytes = maxStringBytes;
    this.needsFoldCheck = needsFoldCheck;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    StringFirstLastUtils.writePair(buf, position, INIT, maxStringBytes);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    long[] times = timeSelector.getLongVector();
    Object[] strings = valueSelector.getObjectVector();

    lastTime = buf.getLong(position);
    int index = endRow - 1;
    for (int i = endRow - 1; i >= startRow; i--) {
      if (strings[i] != null) {
        index = i;
        break;
      }
    }
    if (needsFoldCheck) {
      // Less efficient code path when folding is a possibility (we must read the value selector first just in case
      // it's a foldable object).
      SerializablePairLongString inPair;
      if (strings[index] != null) {
        inPair = new SerializablePairLongString(
            times[index],
            DimensionHandlerUtils.convertObjectToString(strings[index])
        );
      } else {
        inPair = null;
      }
      if (inPair != null && inPair.lhs >= lastTime) {
        lastTime = inPair.lhs;
        StringFirstLastUtils.writePair(
            buf,
            position,
            new SerializablePairLongString(inPair.lhs, inPair.rhs),
            maxStringBytes
        );
      }
    } else {
      final long time = times[index];

      if (time >= lastTime) {
        final String value = DimensionHandlerUtils.convertObjectToString(strings[index]);
        lastTime = time;
        StringFirstLastUtils.writePair(
            buf,
            position,
            new SerializablePairLongString(time, value),
            maxStringBytes
        );
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
    long[] timeVector = timeSelector.getLongVector();
    Object[] strings = valueSelector.getObjectVector();
    for (int i = 0; i < numRows; i++) {
      int position = positions[i] + positionOffset;
      int row = rows == null ? i : rows[i];
      long lastTime = buf.getLong(position);
      if (timeVector[row] >= lastTime) {
        final String value = DimensionHandlerUtils.convertObjectToString(strings[row]);
        lastTime = timeVector[row];
        StringFirstLastUtils.writePair(
            buf,
            position,
            new SerializablePairLongString(lastTime, value),
            maxStringBytes
        );
      }

    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return StringFirstLastUtils.readPair(buf, position);
  }

  @Override
  public void close()
  {

  }
}

