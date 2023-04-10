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

package org.apache.druid.query.aggregation.tdigestsketch;

import com.google.common.base.Preconditions;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Aggregator that builds T-Digest backed sketch using numeric values read from {@link ByteBuffer}
 */
public class TDigestSketchBufferAggregator implements BufferAggregator
{

  @Nonnull
  private final ColumnValueSelector selector;
  private final TDigestSketchAggregatorHelper innerAggregator;

  public TDigestSketchBufferAggregator(
      final ColumnValueSelector valueSelector,
      @Nullable final Integer compression
  )
  {
    Preconditions.checkNotNull(valueSelector);
    this.selector = valueSelector;
    this.innerAggregator = new TDigestSketchAggregatorHelper(compression == null
                                                             ? TDigestSketchAggregatorFactory.DEFAULT_COMPRESSION
                                                             : compression);
  }

  @Override
  public void init(ByteBuffer buffer, int position)
  {
    innerAggregator.init(buffer, position);
  }

  @Override
  public void aggregate(ByteBuffer buffer, int position)
  {
    Object x = selector.getObject();
    if (x == null) {
      return;
    }
    innerAggregator.merge(x, buffer, position);
  }

  @Override
  public Object get(final ByteBuffer buffer, final int position)
  {
    return innerAggregator.get(buffer, position);
  }

  @Override
  public float getFloat(final ByteBuffer buffer, final int position)
  {
    return innerAggregator.getFloat(buffer, position);
  }

  @Override
  public long getLong(final ByteBuffer buffer, final int position)
  {
    return innerAggregator.getLong(buffer, position);
  }

  @Override
  public void close()
  {
    innerAggregator.close();
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    innerAggregator.relocate(oldPosition, newPosition, oldBuffer, newBuffer);
  }
}
