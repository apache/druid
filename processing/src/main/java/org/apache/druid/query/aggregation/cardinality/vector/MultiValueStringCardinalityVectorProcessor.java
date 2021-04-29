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

package org.apache.druid.query.aggregation.cardinality.vector;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.query.aggregation.cardinality.types.StringCardinalityAggregatorColumnSelectorStrategy;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class MultiValueStringCardinalityVectorProcessor implements CardinalityVectorProcessor
{
  private final MultiValueDimensionVectorSelector selector;

  public MultiValueStringCardinalityVectorProcessor(final MultiValueDimensionVectorSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    // Save position, limit and restore later instead of allocating a new ByteBuffer object
    final int oldPosition = buf.position();
    final int oldLimit = buf.limit();

    try {
      final IndexedInts[] vector = selector.getRowVector();

      buf.limit(position + HyperLogLogCollector.getLatestNumBytesForDenseStorage());
      buf.position(position);

      final HyperLogLogCollector collector = HyperLogLogCollector.makeCollector(buf);

      for (int i = startRow; i < endRow; i++) {
        final IndexedInts ids = vector[i];
        final int sz = ids.size();

        for (int j = 0; j < sz; j++) {
          final String value = selector.lookupName(ids.get(j));
          StringCardinalityAggregatorColumnSelectorStrategy.addStringToCollector(collector, value);
        }
      }
    }
    finally {
      buf.limit(oldLimit);
      buf.position(oldPosition);
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    // Save position, limit and restore later instead of allocating a new ByteBuffer object
    final int oldPosition = buf.position();
    final int oldLimit = buf.limit();

    try {
      final IndexedInts[] vector = selector.getRowVector();

      for (int i = 0; i < numRows; i++) {
        final IndexedInts ids = vector[rows != null ? rows[i] : i];
        final int sz = ids.size();

        for (int j = 0; j < sz; j++) {
          final String s = selector.lookupName(ids.get(j));
          if (NullHandling.replaceWithDefault() || s != null) {
            final int position = positions[i] + positionOffset;
            buf.limit(position + HyperLogLogCollector.getLatestNumBytesForDenseStorage());
            buf.position(position);
            final HyperLogLogCollector collector = HyperLogLogCollector.makeCollector(buf);
            StringCardinalityAggregatorColumnSelectorStrategy.addStringToCollector(collector, s);
          }
        }
      }
    }
    finally {
      buf.limit(oldLimit);
      buf.position(oldPosition);
    }
  }
}
