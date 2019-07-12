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

package org.apache.druid.query.aggregation.hyperloglog;

import com.google.common.base.Preconditions;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class HyperUniquesVectorAggregator implements VectorAggregator
{
  private final VectorObjectSelector selector;

  public HyperUniquesVectorAggregator(final VectorObjectSelector selector)
  {
    this.selector = Preconditions.checkNotNull(selector, "selector");
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    HyperUniquesBufferAggregator.doInit(buf, position);
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position, final int startRow, final int endRow)
  {
    // Save position, limit and restore later instead of allocating a new ByteBuffer object
    final int oldPosition = buf.position();
    final int oldLimit = buf.limit();
    buf.limit(position + HyperLogLogCollector.getLatestNumBytesForDenseStorage());
    buf.position(position);

    try {
      final HyperLogLogCollector collector = HyperLogLogCollector.makeCollector(buf);
      final Object[] vector = selector.getObjectVector();
      for (int i = startRow; i < endRow; i++) {
        final HyperLogLogCollector otherCollector = (HyperLogLogCollector) vector[i];
        if (otherCollector != null) {
          collector.fold(otherCollector);
        }
      }
    }
    finally {
      buf.limit(oldLimit);
      buf.position(oldPosition);
    }
  }

  @Override
  public void aggregate(
      final ByteBuffer buf,
      final int numRows,
      final int[] positions,
      @Nullable final int[] rows,
      final int positionOffset
  )
  {
    final Object[] vector = selector.getObjectVector();

    for (int i = 0; i < numRows; i++) {
      final HyperLogLogCollector otherCollector = (HyperLogLogCollector) vector[rows != null ? rows[i] : i];
      if (otherCollector == null) {
        continue;
      }

      final int position = positions[i] + positionOffset;

      // Save position, limit and restore later instead of allocating a new ByteBuffer object
      final int oldPosition = buf.position();
      final int oldLimit = buf.limit();
      buf.limit(position + HyperLogLogCollector.getLatestNumBytesForDenseStorage());
      buf.position(position);

      try {
        HyperLogLogCollector.makeCollector(buf).fold(otherCollector);
      }
      finally {
        buf.limit(oldLimit);
        buf.position(oldPosition);
      }
    }
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    return HyperUniquesBufferAggregator.doGet(buf, position);
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }
}
