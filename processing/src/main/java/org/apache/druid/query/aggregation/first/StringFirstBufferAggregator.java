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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;

import java.nio.ByteBuffer;

public class StringFirstBufferAggregator implements BufferAggregator
{
  private static final SerializablePairLongString INIT = new SerializablePairLongString(
      DateTimes.MAX.getMillis(),
      null
  );

  private final BaseLongColumnValueSelector timeSelector;
  private final BaseObjectColumnValueSelector<?> valueSelector;
  private final int maxStringBytes;
  private final boolean needsFoldCheck;

  public StringFirstBufferAggregator(
      BaseLongColumnValueSelector timeSelector,
      BaseObjectColumnValueSelector<?> valueSelector,
      int maxStringBytes,
      boolean needsFoldCheck
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
  public void aggregate(ByteBuffer buf, int position)
  {
    if (needsFoldCheck) {
      // Less efficient code path when folding is a possibility (we must read the value selector first just in case
      // it's a foldable object).
      final SerializablePairLongString inPair = StringFirstLastUtils.readPairFromSelectors(
          timeSelector,
          valueSelector
      );

      if (inPair != null) {
        final long firstTime = buf.getLong(position);
        if (inPair.lhs < firstTime) {
          StringFirstLastUtils.writePair(
              buf,
              position,
              new SerializablePairLongString(inPair.lhs, inPair.rhs),
              maxStringBytes
          );
        }
      }
    } else {
      final long time = timeSelector.getLong();
      final long firstTime = buf.getLong(position);

      if (time < firstTime) {
        final String value = DimensionHandlerUtils.convertObjectToString(valueSelector.getObject());

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
  public Object get(ByteBuffer buf, int position)
  {
    return StringFirstLastUtils.readPair(buf, position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringFirstAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringFirstAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringFirstAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("timeSelector", timeSelector);
    inspector.visit("valueSelector", valueSelector);
  }
}
