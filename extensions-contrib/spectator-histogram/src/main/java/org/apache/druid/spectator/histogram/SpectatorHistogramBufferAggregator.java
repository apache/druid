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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

/**
 * Aggregator that builds Spectator Histograms over numeric values read from {@link ByteBuffer}
 */
public class SpectatorHistogramBufferAggregator implements BufferAggregator
{

  @Nonnull
  private final ColumnValueSelector selector;
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<SpectatorHistogram>> histogramCache = new IdentityHashMap<>();

  public SpectatorHistogramBufferAggregator(
      final ColumnValueSelector valueSelector
  )
  {
    Preconditions.checkNotNull(valueSelector);
    this.selector = valueSelector;
  }

  @Override
  public void init(ByteBuffer buffer, int position)
  {
    SpectatorHistogram emptyCounts = new SpectatorHistogram();
    addToCache(buffer, position, emptyCounts);
  }

  @Override
  public void aggregate(ByteBuffer buffer, int position)
  {
    Object obj = selector.getObject();
    if (obj == null) {
      return;
    }
    SpectatorHistogram counts = histogramCache.get(buffer).get(position);
    if (obj instanceof SpectatorHistogram) {
      SpectatorHistogram other = (SpectatorHistogram) obj;
      counts.merge(other);
    } else if (obj instanceof Number) {
      counts.insert((Number) obj);
    } else {
      throw new IAE(
          "Expected a number or a long[], but received [%s] of type [%s]",
          obj,
          obj.getClass()
      );
    }
  }

  @Override
  public Object get(final ByteBuffer buffer, final int position)
  {
    // histogramCache is an IdentityHashMap where the reference of buffer is used for equality checks.
    // So the returned object isn't impacted by the changes in the buffer object made by concurrent threads.

    SpectatorHistogram spectatorHistogram = histogramCache.get(buffer).get(position);
    if (spectatorHistogram.isEmpty()) {
      return null;
    }
    return spectatorHistogram;
  }

  @Override
  public float getFloat(final ByteBuffer buffer, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(final ByteBuffer buffer, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
    histogramCache.clear();
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    SpectatorHistogram histogram = histogramCache.get(oldBuffer).get(oldPosition);
    addToCache(newBuffer, newPosition, histogram);
    final Int2ObjectMap<SpectatorHistogram> map = histogramCache.get(oldBuffer);
    map.remove(oldPosition);
    if (map.isEmpty()) {
      histogramCache.remove(oldBuffer);
    }
  }

  private void addToCache(final ByteBuffer buffer, final int position, final SpectatorHistogram histogram)
  {
    Int2ObjectMap<SpectatorHistogram> map = histogramCache.computeIfAbsent(
        buffer,
        b -> new Int2ObjectOpenHashMap<>()
    );
    map.put(position, histogram);
  }
}
