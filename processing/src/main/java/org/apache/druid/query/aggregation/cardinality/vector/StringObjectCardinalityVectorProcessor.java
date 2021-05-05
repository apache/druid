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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.cardinality.types.StringCardinalityAggregatorColumnSelectorStrategy;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public class StringObjectCardinalityVectorProcessor implements CardinalityVectorProcessor
{
  private final VectorObjectSelector selector;

  public StringObjectCardinalityVectorProcessor(final VectorObjectSelector selector)
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
      final Object[] vector = selector.getObjectVector();

      buf.limit(position + HyperLogLogCollector.getLatestNumBytesForDenseStorage());
      buf.position(position);

      final HyperLogLogCollector collector = HyperLogLogCollector.makeCollector(buf);

      for (int i = startRow; i < endRow; i++) {
        addStringObjectToCollector(collector, vector[i]);
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
      final Object[] vector = selector.getObjectVector();

      for (int i = 0; i < numRows; i++) {
        final Object obj = vector[rows != null ? rows[i] : i];

        if (NullHandling.replaceWithDefault() || obj != null) {
          final int position = positions[i] + positionOffset;
          buf.limit(position + HyperLogLogCollector.getLatestNumBytesForDenseStorage());
          buf.position(position);
          final HyperLogLogCollector collector = HyperLogLogCollector.makeCollector(buf);
          addStringObjectToCollector(collector, obj);
        }
      }
    }
    finally {
      buf.limit(oldLimit);
      buf.position(oldPosition);
    }
  }

  /**
   * Adds a stringy Object to a HyperLogLogCollector. The object may be a {@code List<String>}, {@code String},
   * or {@code null}.
   */
  private static void addStringObjectToCollector(final HyperLogLogCollector collector, @Nullable final Object obj)
  {
    if (obj instanceof String) {
      StringCardinalityAggregatorColumnSelectorStrategy.addStringToCollector(collector, (String) obj);
    } else if (obj instanceof List) {
      for (String o : (List<String>) obj) {
        addStringObjectToCollector(collector, o);
      }
    } else if (obj != null) {
      // Shouldn't happen in production, because this class is only used for STRING typed object selectors.
      throw new ISE("Encountered unexpected class");
    }
  }
}
