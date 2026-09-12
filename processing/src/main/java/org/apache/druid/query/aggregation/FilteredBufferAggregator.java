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

import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Aggregates an underlying {@link #delegate} into a buffer when {@link #matcher} matches.
 *
 * <p>If {@link #elseValue} is set, an extra byte is added to the beginning of the buffer to track whether we've
 * seen an unmatched row.
 */
public class FilteredBufferAggregator implements BufferAggregator
{
  private final ValueMatcher matcher;
  private final BufferAggregator delegate;
  @Nullable
  private final Number elseValue;
  private final int valueOffset;

  public FilteredBufferAggregator(ValueMatcher matcher, BufferAggregator delegate, @Nullable Number elseValue)
  {
    this.matcher = matcher;
    this.delegate = delegate;
    this.elseValue = elseValue;
    this.valueOffset = elseValue != null ? Byte.BYTES : 0;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    if (elseValue != null) {
      buf.put(position, (byte) 0);
    }
    delegate.init(buf, position + valueOffset);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (matcher.matches(false)) {
      delegate.aggregate(buf, position + valueOffset);
    } else if (elseValue != null) {
      markUnmatched(buf, position);
    }
  }

  @Override
  @Nullable
  public Object get(ByteBuffer buf, int position)
  {
    if (shouldSubstituteElse(buf, position)) {
      return elseValue;
    } else {
      return delegate.get(buf, position + valueOffset);
    }
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    if (shouldSubstituteElse(buf, position)) {
      return elseValue.longValue();
    } else {
      return delegate.getLong(buf, position + valueOffset);
    }
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    if (shouldSubstituteElse(buf, position)) {
      return elseValue.floatValue();
    } else {
      return delegate.getFloat(buf, position + valueOffset);
    }
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    if (shouldSubstituteElse(buf, position)) {
      return elseValue.doubleValue();
    } else {
      return delegate.getDouble(buf, position + valueOffset);
    }
  }

  @Override
  public void relocate(
      final int oldPosition,
      final int newPosition,
      final ByteBuffer oldBuffer,
      final ByteBuffer newBuffer
  )
  {
    delegate.relocate(oldPosition + valueOffset, newPosition + valueOffset, oldBuffer, newBuffer);
  }

  @Override
  public boolean isNull(final ByteBuffer buf, final int position)
  {
    if (elseValue != null) {
      return !hasUnmatched(buf, position) && delegate.isNull(buf, position + valueOffset);
    }
    return delegate.isNull(buf, position + valueOffset);
  }

  private boolean shouldSubstituteElse(final ByteBuffer buf, final int position)
  {
    return elseValue != null && hasUnmatched(buf, position) && delegate.isNull(buf, position + valueOffset);
  }

  private static void markUnmatched(final ByteBuffer buf, final int position)
  {
    buf.put(position, (byte) 1);
  }

  private static boolean hasUnmatched(final ByteBuffer buf, final int position)
  {
    return buf.get(position) == 1;
  }

  @Override
  public void close()
  {
    delegate.close();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("matcher", matcher);
    inspector.visit("delegate", delegate);
  }
}
