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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * A wrapper around a non-null-aware VectorAggregator that makes it null-aware. This removes the need for each
 * aggregator class to handle nulls on its own. This class only makes sense as a wrapper for "primitive" aggregators,
 * i.e., ones that take {@link VectorValueSelector} as input.
 *
 * When wrapped by this class, the underlying aggregator's required storage space is increased by one byte. The extra
 * byte is a boolean that stores whether or not any non-null values have been seen. The extra byte is placed before
 * the underlying aggregator's normal state. (Buffer layout = [nullability byte] [delegate storage bytes])
 *
 * @see NullableNumericAggregatorFactory#factorizeVector(VectorColumnSelectorFactory)
 * @see NullAwareNumericAggregator for the non-vectorized heap version.
 * @see NullAwareNumericBufferAggregator for the non-vectorized version.
 */
public abstract class NullAwareNumericVectorAggregator implements VectorAggregator
{
  protected final VectorAggregator delegate;
  protected final VectorValueSelector selector;

  @Nullable
  protected int[] vAggregationPositions = null;

  @Nullable
  protected int[] vAggregationRows = null;

  NullAwareNumericVectorAggregator(VectorAggregator delegate, VectorValueSelector selector)
  {
    this.delegate = delegate;
    this.selector = selector;
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
    switch (buf.get(position)) {
      case NullHandling.IS_NULL_BYTE:
        return null;
      case NullHandling.IS_NOT_NULL_BYTE:
        return delegate.get(buf, position + Byte.BYTES);
      default:
        // Corrupted byte?
        throw new ISE("Bad null-marker byte, delegate class[%s]", delegate.getClass().getName());
    }
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

  protected final void doAggregate(ByteBuffer buf, int position, int start, int end)
  {
    buf.put(position, NullHandling.IS_NOT_NULL_BYTE);
    delegate.aggregate(buf, position + Byte.BYTES, start, end);
  }

  protected final void doAggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    for (int i = 0; i < numRows; i++) {
      buf.put(positions[i] + positionOffset, NullHandling.IS_NOT_NULL_BYTE);
    }

    delegate.aggregate(buf, numRows, positions, rows, positionOffset + Byte.BYTES);
  }
}
