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
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;

/**
 * Base type for buffer based 'last' aggregator for primitive numeric column selectors
 *
 * This could probably share a base type with
 * {@link org.apache.druid.query.aggregation.first.NumericFirstBufferAggregator} ...
 */
public abstract class NumericLastBufferAggregator implements BufferAggregator
{
  static final int NULL_OFFSET = Long.BYTES;
  static final int VALUE_OFFSET = NULL_OFFSET + Byte.BYTES;

  private final boolean useDefault = NullHandling.replaceWithDefault();
  private final BaseLongColumnValueSelector timeSelector;

  final ColumnValueSelector valueSelector;
  final boolean needsFoldCheck;

  public NumericLastBufferAggregator(BaseLongColumnValueSelector timeSelector, ColumnValueSelector valueSelector, boolean needsFoldCheck)
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
    this.needsFoldCheck = needsFoldCheck;
  }

  /**
   * Initialize the buffer value at the position of {@link #VALUE_OFFSET}
   */
  abstract void initValue(ByteBuffer buf, int position);

  /**
   * Place the primitive value in the buffer at the position of {@link #VALUE_OFFSET}
   */
  abstract void putValue(ByteBuffer buf, int position, ColumnValueSelector valueSelector);

  abstract void putValue(ByteBuffer buf, int position, Number value);

  boolean isValueNull(ByteBuffer buf, int position)
  {
    return buf.get(position + NULL_OFFSET) == NullHandling.IS_NULL_BYTE;
  }

  void updateTimeWithValue(ByteBuffer buf, int position, long time, ColumnValueSelector valueSelector)
  {
    buf.putLong(position, time);
    buf.put(position + NULL_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
    putValue(buf, position + VALUE_OFFSET, valueSelector);
  }

  void updateTimeWithValue(ByteBuffer buf, int position, long time, Number value)
  {
    buf.putLong(position, time);
    buf.put(position + NULL_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
    putValue(buf, position + VALUE_OFFSET, value);
  }

  void updateTimeWithNull(ByteBuffer buf, int position, long time)
  {
    buf.putLong(position, time);
    buf.put(position + NULL_OFFSET, NullHandling.IS_NULL_BYTE);
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, Long.MIN_VALUE);
    buf.put(position + NULL_OFFSET, useDefault ? NullHandling.IS_NOT_NULL_BYTE : NullHandling.IS_NULL_BYTE);
    initValue(buf, position + VALUE_OFFSET);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (timeSelector.isNull()) {
      return;
    }

    long lastTime = buf.getLong(position);
    if (needsFoldCheck) {
      final Object object = valueSelector.getObject();
      if (object instanceof SerializablePair) {
        final SerializablePair<Long, Number> inPair = (SerializablePair<Long, Number>) object;

        if (inPair.lhs >= lastTime) {
          if (inPair.rhs == null) {
            updateTimeWithNull(buf, position, inPair.lhs);
          } else {
            updateTimeWithValue(buf, position, inPair.lhs, inPair.rhs);
          }
        }
        return;
      }
    }

    long time = timeSelector.getLong();

    if (time >= lastTime) {
      if (useDefault || !valueSelector.isNull()) {
        updateTimeWithValue(buf, position, time, valueSelector);
      } else {
        updateTimeWithNull(buf, position, time);
      }
    }
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
