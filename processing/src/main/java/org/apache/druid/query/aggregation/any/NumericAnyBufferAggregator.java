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

package org.apache.druid.query.aggregation.any;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseNullableColumnValueSelector;

import java.nio.ByteBuffer;

/**
 * Base type for buffer based 'any' aggregator for primitive numeric column selectors
 */
public abstract class NumericAnyBufferAggregator<TSelector extends BaseNullableColumnValueSelector>
    implements BufferAggregator
{
  // Rightmost bit for is null check (0 for is null and 1 for not null)
  // Second rightmost bit for is found check (0 for not found and 1 for found)
  private static final byte BYTE_FLAG_FOUND_MASK = 0x02;
  private static final byte BYTE_FLAG_NULL_MASK = 0x01;
  static final int FOUND_VALUE_OFFSET = Byte.BYTES;

  private final boolean useDefault = NullHandling.replaceWithDefault();

  final TSelector valueSelector;

  public NumericAnyBufferAggregator(TSelector valueSelector)
  {
    this.valueSelector = valueSelector;
  }

  /**
   * Initialize the buffer value given the initial offset position within the byte buffer for initialization
   */
  abstract void initValue(ByteBuffer buf, int position);

  /**
   * Place the primitive value in the buffer given the initial offset position within the byte buffer
   * at which the current aggregate value is stored
   */
  abstract void putValue(ByteBuffer buf, int position);

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.put(position, useDefault ? NullHandling.IS_NOT_NULL_BYTE : NullHandling.IS_NULL_BYTE);
    initValue(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if ((buf.get(position) & BYTE_FLAG_FOUND_MASK) != BYTE_FLAG_FOUND_MASK) {
      if (useDefault || !valueSelector.isNull()) {
        putValue(buf, position);
        buf.put(position, (byte) (BYTE_FLAG_FOUND_MASK | NullHandling.IS_NOT_NULL_BYTE));
      } else {
        buf.put(position, (byte) (BYTE_FLAG_FOUND_MASK | NullHandling.IS_NULL_BYTE));
      }
    }
  }

  boolean isValueNull(ByteBuffer buf, int position)
  {
    return (buf.get(position) & BYTE_FLAG_NULL_MASK) == NullHandling.IS_NULL_BYTE;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("valueSelector", valueSelector);
  }
}
