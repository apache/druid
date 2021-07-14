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
import org.apache.druid.segment.ColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Base class for null-aware numeric {@link BufferAggregator}s, which will be initialized to a null value.
 *
 * When wrapped by this class, the underlying aggregator's required storage space is increased by one byte. The extra
 * byte is a boolean that stores whether or not any non-null values have been seen. The extra byte is placed before
 * the underlying aggregator's normal state. (Buffer layout = [nullability byte] [delegate storage bytes])
 *
 * Used by {@link NullableNumericAggregatorFactory#factorizeBuffered(ColumnSelectorFactory)} to wrap non-null aware
 * aggregators. This class is only used when SQL compatible null handling is enabled.
 *
 * @see NullableNumericAggregatorFactory#factorizeBuffered(ColumnSelectorFactory)
 * @see NullAwareNumericAggregator for the non-vectorized heap version.
 * @see NullAwareNumericVectorAggregator the vectorized version.
 */
@PublicApi
public abstract class NullAwareNumericBufferAggregator implements BufferAggregator
{
  protected final BufferAggregator delegate;
  protected final BaseNullableColumnValueSelector nullSelector;

  public NullAwareNumericBufferAggregator(BufferAggregator delegate, BaseNullableColumnValueSelector nullSelector)
  {
    this.delegate = delegate;
    this.nullSelector = nullSelector;
  }

  @Override
  public final void init(ByteBuffer buf, int position)
  {
    buf.put(position, NullHandling.IS_NULL_BYTE);
    delegate.init(buf, position + Byte.BYTES);
  }

  @Override
  @Nullable
  public final Object get(ByteBuffer buf, int position)
  {
    if (buf.get(position) == NullHandling.IS_NULL_BYTE) {
      return null;
    }
    return delegate.get(buf, position + Byte.BYTES);
  }

  @Override
  public final float getFloat(ByteBuffer buf, int position)
  {
    if (buf.get(position) == NullHandling.IS_NULL_BYTE) {
      throw new IllegalStateException("Cannot return float for Null Value");
    }
    return delegate.getFloat(buf, position + Byte.BYTES);
  }

  @Override
  public final long getLong(ByteBuffer buf, int position)
  {
    if (buf.get(position) == NullHandling.IS_NULL_BYTE) {
      throw new IllegalStateException("Cannot return long for Null Value");
    }
    return delegate.getLong(buf, position + Byte.BYTES);
  }

  @Override
  public final double getDouble(ByteBuffer buf, int position)
  {
    if (buf.get(position) == NullHandling.IS_NULL_BYTE) {
      throw new IllegalStateException("Cannot return double for Null Value");
    }
    return delegate.getDouble(buf, position + Byte.BYTES);
  }

  @Override
  public final boolean isNull(ByteBuffer buf, int position)
  {
    return buf.get(position) == NullHandling.IS_NULL_BYTE || delegate.isNull(buf, position + Byte.BYTES);
  }

  @Override
  public final void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    delegate.relocate(oldPosition + Byte.BYTES, newPosition + Byte.BYTES, oldBuffer, newBuffer);
  }

  @Override
  public final void close()
  {
    delegate.close();
  }

  @Override
  public final void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("delegate", delegate);
    inspector.visit("nullSelector", nullSelector);
  }

  protected final void doAggregate(ByteBuffer buf, int position)
  {
    if (buf.get(position) == NullHandling.IS_NULL_BYTE) {
      buf.put(position, NullHandling.IS_NOT_NULL_BYTE);
    }
    delegate.aggregate(buf, position + Byte.BYTES);
  }
}
