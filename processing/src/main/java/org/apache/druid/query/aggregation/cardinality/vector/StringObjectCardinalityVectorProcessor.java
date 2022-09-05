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
        addObjectIfString(collector, vector[i]);
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
          addObjectIfString(collector, obj);
        }
      }
    }
    finally {
      buf.limit(oldLimit);
      buf.position(oldPosition);
    }
  }

  /**
   * Adds an Object to a HyperLogLogCollector. If the object is a {@code List<String>} or {@code String} then
   * the individual Strings are added to the collector.
   *
   * If the object is any other type (including null) then behavior depends on null-handling mode:
   *
   * - In SQL-compatible mode, ignore non-strings and nulls.
   * - In replace-with-default mode, treat all non-strings and nulls as empty strings.
   */
  private static void addObjectIfString(final HyperLogLogCollector collector, @Nullable final Object obj)
  {
    if (obj instanceof String) {
      StringCardinalityAggregatorColumnSelectorStrategy.addStringToCollector(collector, (String) obj);
    } else if (obj instanceof List) {
      //noinspection unchecked
      for (String s : (List<String>) obj) {
        StringCardinalityAggregatorColumnSelectorStrategy.addStringToCollector(collector, s);
      }
    } else {
      StringCardinalityAggregatorColumnSelectorStrategy.addStringToCollector(collector, null);
    }
  }
}
