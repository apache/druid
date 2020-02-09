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

import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.NullableNumericAggregator;
import org.apache.druid.query.aggregation.NullableNumericAggregatorFactory;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;

import java.nio.ByteBuffer;

/**
 * This Aggregator is created by the {@link FloatAnyAggregatorFactory} which extends from
 * {@link NullableNumericAggregatorFactory}. If null needs to be handle, then {@link NullableNumericAggregatorFactory}
 * will wrap this aggregator in {@link NullableNumericAggregator} and can handle all null in that class.
 * Hence, no null will ever be pass into this aggregator from the valueSelector.
 */
public class FloatAnyBufferAggregator implements BufferAggregator
{
  private static final byte BYTE_FLAG_IS_NOT_SET = 0;
  private static final byte BYTE_FLAG_IS_SET = 1;
  private static final float NULL_VALUE = 0;
  private final BaseFloatColumnValueSelector valueSelector;

  public FloatAnyBufferAggregator(BaseFloatColumnValueSelector valueSelector)
  {
    this.valueSelector = valueSelector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.put(position, BYTE_FLAG_IS_NOT_SET);
    buf.putFloat(position + Byte.BYTES, NULL_VALUE);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (buf.get(position) == BYTE_FLAG_IS_NOT_SET) {
      buf.put(position, BYTE_FLAG_IS_SET);
      buf.putFloat(position + Byte.BYTES, valueSelector.getFloat());
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getFloat(position + Byte.BYTES);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return buf.getFloat(position + Byte.BYTES);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return (long) buf.getFloat(position + Byte.BYTES);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return buf.getFloat(position + Byte.BYTES);
  }

  @Override
  public void close()
  {
    // no-op
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("valueSelector", valueSelector);
  }
}
