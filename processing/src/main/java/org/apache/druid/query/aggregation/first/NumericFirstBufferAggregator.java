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
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseNullableColumnValueSelector;

import java.nio.ByteBuffer;

/**
 * Base type for buffer based 'first' aggregator for primitive numeric column selectors
 */
public abstract class NumericFirstBufferAggregator<TSelector extends BaseNullableColumnValueSelector>
    implements BufferAggregator
{
  static final int NULL_OFFSET = Long.BYTES;
  static final int VALUE_OFFSET = NULL_OFFSET + Byte.BYTES;

  private final boolean useDefault = NullHandling.replaceWithDefault();
  private final BaseLongColumnValueSelector timeSelector;

  final TSelector valueSelector;

  public NumericFirstBufferAggregator(BaseLongColumnValueSelector timeSelector, TSelector valueSelector)
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
  }

  /**
   * Initialize the buffer value at the position of {@link #VALUE_OFFSET}
   */
  abstract void initValue(ByteBuffer buf, int position);

  /**
   * Place the primitive value in the buffer at the position of {@link #VALUE_OFFSET}
   */
  abstract void putValue(ByteBuffer buf, int position);

  void updateTimeWithValue(ByteBuffer buf, int position, long time)
  {
    buf.putLong(position, time);
    buf.put(position + NULL_OFFSET, NullHandling.IS_NOT_NULL_BYTE);
    putValue(buf, position + VALUE_OFFSET);
  }

  void updateTimeWithNull(ByteBuffer buf, int position, long time)
  {
    buf.putLong(position, time);
    buf.put(position + NULL_OFFSET, NullHandling.IS_NULL_BYTE);
  }

  boolean isValueNull(ByteBuffer buf, int position)
  {
    return buf.get(position + NULL_OFFSET) == NullHandling.IS_NULL_BYTE;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, Long.MAX_VALUE);
    buf.put(position + NULL_OFFSET, useDefault ? NullHandling.IS_NOT_NULL_BYTE : NullHandling.IS_NULL_BYTE);
    initValue(buf, position + VALUE_OFFSET);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    long time = timeSelector.getLong();
    long firstTime = buf.getLong(position);
    if (time < firstTime) {
      if (useDefault || !valueSelector.isNull()) {
        updateTimeWithValue(buf, position, time);
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
