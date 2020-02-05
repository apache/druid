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
  private static final byte BYTE_FLAG_IS_NOT_SET = 0;
  private static final byte BYTE_FLAG_IS_SET = 1;
  private static final int IS_FOUND_FLAG_OFFSET_POSITION = 0;
  private static final int IS_NULL_FLAG_OFFSET_POSITION = IS_FOUND_FLAG_OFFSET_POSITION + Byte.BYTES;
  private static final int FOUND_VALUE_OFFSET_POSITION = IS_NULL_FLAG_OFFSET_POSITION + Byte.BYTES;

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
    buf.put(position + IS_FOUND_FLAG_OFFSET_POSITION, BYTE_FLAG_IS_NOT_SET);
    buf.put(position + IS_NULL_FLAG_OFFSET_POSITION, useDefault ? BYTE_FLAG_IS_NOT_SET : BYTE_FLAG_IS_SET);
    initValue(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (buf.get(position + IS_FOUND_FLAG_OFFSET_POSITION) == BYTE_FLAG_IS_NOT_SET) {
      if (useDefault || !valueSelector.isNull()) {
        putValue(buf, position);
        buf.put(position + IS_NULL_FLAG_OFFSET_POSITION, BYTE_FLAG_IS_NOT_SET);
      } else {
        buf.put(position + IS_NULL_FLAG_OFFSET_POSITION, BYTE_FLAG_IS_SET);
      }
      buf.put(position + IS_FOUND_FLAG_OFFSET_POSITION, BYTE_FLAG_IS_SET);
    }
  }

  boolean isValueNull(ByteBuffer buf, int position)
  {
    return buf.get(position + IS_NULL_FLAG_OFFSET_POSITION) == BYTE_FLAG_IS_SET;
  }

  int getFoundValueStoredPosition(int position) {
    return position + FOUND_VALUE_OFFSET_POSITION;
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
