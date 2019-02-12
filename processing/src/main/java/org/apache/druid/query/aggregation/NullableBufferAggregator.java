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

package org.apache.druid.query.aggregation;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseNullableColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * The result of a NullableBufferAggregator will be null if all the values to be aggregated are null values or no values
 * are aggregated at all. If any of the value is non-null, the result would be the aggregated value of the delegate
 * aggregator. Note that the delegate aggregator is not required to perform check for
 * {@link BaseNullableColumnValueSelector#isNull()} on the selector as only non-null values will be passed to the
 * delegate aggregator. This class is only used when SQL compatible null handling is enabled.
 * When writing aggregated result to buffer, it will write an additional byte to store the nullability of the
 * aggregated result.
 * Buffer Layout - 1 byte for storing nullability + delegate storage bytes.
 */
@PublicApi
public final class NullableBufferAggregator implements BufferAggregator
{
  private final BufferAggregator delegate;
  private final BaseNullableColumnValueSelector nullSelector;

  public NullableBufferAggregator(BufferAggregator delegate, BaseNullableColumnValueSelector nullSelector)
  {
    this.delegate = delegate;
    this.nullSelector = nullSelector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.put(position, NullHandling.IS_NULL_BYTE);
    delegate.init(buf, position + Byte.BYTES);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    boolean isNotNull = !nullSelector.isNull();
    if (isNotNull) {
      if (buf.get(position) == NullHandling.IS_NULL_BYTE) {
        buf.put(position, NullHandling.IS_NOT_NULL_BYTE);
      }
      delegate.aggregate(buf, position + Byte.BYTES);
    }
  }

  @Override
  @Nullable
  public Object get(ByteBuffer buf, int position)
  {
    if (buf.get(position) == NullHandling.IS_NULL_BYTE) {
      return null;
    }
    return delegate.get(buf, position + Byte.BYTES);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    if (buf.get(position) == NullHandling.IS_NULL_BYTE) {
      throw new IllegalStateException("Cannot return float for Null Value");
    }
    return delegate.getFloat(buf, position + Byte.BYTES);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    if (buf.get(position) == NullHandling.IS_NULL_BYTE) {
      throw new IllegalStateException("Cannot return long for Null Value");
    }
    return delegate.getLong(buf, position + Byte.BYTES);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    if (buf.get(position) == NullHandling.IS_NULL_BYTE) {
      throw new IllegalStateException("Cannot return double for Null Value");
    }
    return delegate.getDouble(buf, position + Byte.BYTES);
  }

  @Override
  public boolean isNull(ByteBuffer buf, int position)
  {
    return buf.get(position) == NullHandling.IS_NULL_BYTE || delegate.isNull(buf, position + Byte.BYTES);
  }

  @Override
  public void close()
  {
    delegate.close();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("delegate", delegate);
    inspector.visit("nullSelector", nullSelector);
  }
}
