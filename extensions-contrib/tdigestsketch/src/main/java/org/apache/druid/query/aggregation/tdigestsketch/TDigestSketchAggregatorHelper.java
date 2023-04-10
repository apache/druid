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

import com.tdunning.math.stats.MergingDigest;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.java.util.common.IAE;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

public class TDigestSketchAggregatorHelper
{
  private final int compression;
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<MergingDigest>> sketchCache = new IdentityHashMap();

  public TDigestSketchAggregatorHelper(int compression)
  {
    this.compression = compression;
  }

  public void init(ByteBuffer buffer, int position)
  {
    MergingDigest emptyDigest = new MergingDigest(compression);
    addToCache(buffer, position, emptyDigest);
  }

  public void aggregate(Object x, ByteBuffer buffer, int position)
  {
    if (x == null) {
      return;
    }
    merge(x, buffer, position);
  }

  void merge(Object x, ByteBuffer buffer, int position)
  {
    MergingDigest sketch = sketchCache.get(buffer).get(position);
    if (x instanceof Number) {
      sketch.add(((Number) x).doubleValue());
    } else if (x instanceof MergingDigest) {
      sketch.add((MergingDigest) x);
    } else {
      throw new IAE(
          "Expected a number or an instance of MergingDigest, but received [%s] of type [%s]",
          x,
          x.getClass()
      );
    }
  }

  public Object get(final ByteBuffer buffer, final int position)
  {
    // sketchCache is an IdentityHashMap where the reference of buffer is used for equality checks.
    // So the returned object isn't impacted by the changes in the buffer object made by concurrent threads.
    return sketchCache.get(buffer).get(position);
  }

  public float getFloat(final ByteBuffer buffer, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  public long getLong(final ByteBuffer buffer, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  public void close()
  {
    sketchCache.clear();
  }

  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    MergingDigest sketch = sketchCache.get(oldBuffer).get(oldPosition);
    addToCache(newBuffer, newPosition, sketch);
    final Int2ObjectMap<MergingDigest> map = sketchCache.get(oldBuffer);
    map.remove(oldPosition);
    if (map.isEmpty()) {
      sketchCache.remove(oldBuffer);
    }
  }

  private void addToCache(final ByteBuffer buffer, final int position, final MergingDigest sketch)
  {
    Int2ObjectMap<MergingDigest> map = sketchCache.computeIfAbsent(buffer, b -> new Int2ObjectOpenHashMap<>());
    map.put(position, sketch);
  }

}
