/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation;

import io.druid.segment.BaseNullableColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * The result of a NullableBufferAggregator will be null if all the values to be aggregated are null values or no values are aggregated at all.
 * If any of the value is non-null, the result would be the aggregated value of the delegate aggregator.
 * Note that the delegate aggregator is not required to perform check for {@link BaseNullableColumnValueSelector#isNull()} on the selector as only non-null values
 * will be passed to the delegate aggregator.
 */
public class NullableBufferAggregator implements BufferAggregator
{
  private static final byte IS_NULL_BYTE = (byte) 0;
  private static final byte IS_NOT_NULL_BYTE = (byte) 1;

  private final BufferAggregator delegate;
  private final BaseNullableColumnValueSelector selector;

  public NullableBufferAggregator(BufferAggregator delegate, BaseNullableColumnValueSelector selector)
  {
    this.delegate = delegate;
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.put(position, IS_NULL_BYTE);
    delegate.init(buf, position + Byte.BYTES);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    boolean isNotNull = !selector.isNull();
    if (isNotNull) {
      if (buf.get(position) == IS_NULL_BYTE) {
        buf.put(position, IS_NOT_NULL_BYTE);
      }
      delegate.aggregate(buf, position + Byte.BYTES);
    }
  }

  @Override
  @Nullable
  public Object get(ByteBuffer buf, int position)
  {
    if (buf.get(position) == IS_NULL_BYTE) {
      return null;
    }
    return delegate.get(buf, position + Byte.BYTES);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    if (isNull(buf, position)) {
      throw new IllegalStateException("Cannot return float for Null Value");
    }
    return delegate.getFloat(buf, position + Byte.BYTES);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    if (isNull(buf, position)) {
      throw new IllegalStateException("Cannot return long for Null Value");
    }
    return delegate.getLong(buf, position + Byte.BYTES);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    if (isNull(buf, position)) {
      throw new IllegalStateException("Cannot return double for Null Value");
    }
    return delegate.getDouble(buf, position + Byte.BYTES);
  }

  @Override
  public boolean isNull(ByteBuffer buf, int position)
  {
    return buf.get(position) == IS_NULL_BYTE || delegate.isNull(buf, position + Byte.BYTES);
  }

  @Override
  public void close()
  {
    delegate.close();
  }
}
