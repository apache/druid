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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class StringLastVectorAggregator implements VectorAggregator
{
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

  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    long[] times = timeSelector.getLongVector();
    Object[] strings = valueSelector.getObjectVector();
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
        inPair = new SerializablePairLongString(times[index],
                                                DimensionHandlerUtils.convertObjectToString(strings[index]));
      } else {
        inPair = null;
      }
      if (inPair != null && inPair.lhs >= lastTime) {
        lastTime = inPair.lhs;
        lastValue = StringUtils.fastLooseChop(inPair.rhs, maxStringBytes);
      }
    } else {
      final long time = times[index];

      if (time >= lastTime) {
        final String value = DimensionHandlerUtils.convertObjectToString(strings[index]);
        lastTime = time;
        lastValue = StringUtils.fastLooseChop(value, maxStringBytes);
      }
    }
    //StringFirstLastUtils.writePair(buf, position, new SerializablePairLongString(lastTime, lastValue), maxStringBytes);
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

  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return new SerializablePairLongString(lastTime, StringUtils.chop(lastValue, maxStringBytes));
  }

  @Override
  public void close()
  {

  }
}
