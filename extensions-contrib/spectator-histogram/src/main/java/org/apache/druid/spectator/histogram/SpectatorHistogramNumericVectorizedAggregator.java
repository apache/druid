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

package org.apache.druid.spectator.histogram;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/*
  Aggregator used during ingestion time when aggregating against numeric values.
*/
public class SpectatorHistogramNumericVectorizedAggregator implements VectorAggregator
{
  private final SpectatorHistogramAggregateHelper innerAggregator = new SpectatorHistogramAggregateHelper();
  private final VectorValueSelector selector;

  public SpectatorHistogramNumericVectorizedAggregator(VectorValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buffer, int position)
  {
    // Map buf to a new histogram
    innerAggregator.init(buffer, position);
  }

  @Override
  public void aggregate(ByteBuffer buffer, int position, int startRow, int endRow)
  {
    final SpectatorHistogram histogram = innerAggregator.get(buffer, position);
    if (histogram == null) {
      return;
    }

    boolean[] isNull = selector.getNullVector();
    long[] vector = selector.getLongVector();
    boolean hasNulls = isNull != null;

    for (int i = startRow; i < endRow; ++i) {
      boolean rowIsNull = hasNulls && isNull[i];
      if (!rowIsNull) {
        innerAggregator.merge(histogram, vector[i]);
      }
    }
  }

  @Override
  public void aggregate(ByteBuffer buffer, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    boolean[] isNull = selector.getNullVector();
    long[] vector = selector.getLongVector();
    boolean hasNulls = isNull != null;

    final Int2ObjectMap<SpectatorHistogram> histMap = innerAggregator.get(buffer);
    for (int i = 0; i < numRows; ++i) {
      int rowIndex = rows != null ? rows[i] : i;
      boolean rowIsNull = hasNulls && isNull[rowIndex];
      if (!rowIsNull) {
        int position = positions[i] + positionOffset;
        innerAggregator.merge(histMap.get(position), vector[rowIndex]);
      }
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buffer, int position)
  {
    SpectatorHistogram histogram = innerAggregator.get(buffer, position);
    if (histogram == null || histogram.isEmpty()) {
      return null;
    }
    return histogram;
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    innerAggregator.relocate(oldPosition, newPosition, oldBuffer, newBuffer);
  }

  @Override
  public void close()
  {
    innerAggregator.close();
  }
}
