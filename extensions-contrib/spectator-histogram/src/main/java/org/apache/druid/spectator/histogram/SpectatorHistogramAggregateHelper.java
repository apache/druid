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
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.java.util.common.IAE;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

/**
 * Helper class for Spectator histogram implementations of {@link org.apache.druid.query.aggregation.BufferAggregator} and {@link org.apache.druid.query.aggregation.VectorAggregator}.
 */
public class SpectatorHistogramAggregateHelper
{
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<SpectatorHistogram>> histogramCache = new IdentityHashMap<>();

  public void init(ByteBuffer buffer, int position)
  {
    SpectatorHistogram emptyCounts = new SpectatorHistogram();
    addToCache(buffer, position, emptyCounts);
  }

  /**
   * Merge obj ({@link SpectatorHistogram} or {@link Object}) into {@param current}.
   */
  public void merge(SpectatorHistogram current, Object obj)
  {
    if (obj instanceof SpectatorHistogram) {
      SpectatorHistogram other = (SpectatorHistogram) obj;
      current.merge(other);
    } else if (obj instanceof Number) {
      current.insert((Number) obj);
    } else {
      throw new IAE(
          "Expected a Number, but received [%s] of type [%s]",
          obj,
          obj.getClass()
      );
    }
  }

  /**
   * Merge {@param value} into {@param current}.
   */
  public void merge(SpectatorHistogram current, long value)
  {
    current.insert(value);
  }

  /**
   * Fetches the SpectatorHistogram at the given buffer/position pair in the cache
   */
  public SpectatorHistogram get(final ByteBuffer buffer, final int position)
  {
    // histogramCache is an IdentityHashMap where the reference of buffer is used for equality checks.
    // So the returned object isn't impacted by the changes in the buffer object made by concurrent threads.
    final Int2ObjectMap<SpectatorHistogram> map = histogramCache.get(buffer);
    if (map == null) {
      return null;
    }
    return map.get(position);
  }

  /**
   * Fetches the SpectatorHistogram cache for the given buffer
   */
  public Int2ObjectMap<SpectatorHistogram> get(final ByteBuffer buffer)
  {
    return histogramCache.get(buffer);
  }

  public float getFloat(final ByteBuffer buffer, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  public long getLong(final ByteBuffer buffer, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Resets the helper by clearing the buffer/histogram cache.
   */
  public void close()
  {
    histogramCache.clear();
  }

  /**
   * Move histogram located at {@param oldBuffer} in position {@param oldPosition} to {@param newBuffer} in position {@param newPosition}.
   */
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    final SpectatorHistogram histogram = histogramCache.get(oldBuffer).get(oldPosition);
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
