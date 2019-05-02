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
import com.tdunning.math.stats.MergingDigest;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Aggregator that builds t-digest backed sketches using numeric values read from {@link ByteBuffer}
 */
public class TDigestBuildSketchBufferAggregator implements BufferAggregator
{

  @Nonnull
  private final ColumnValueSelector selector;
  private final int compression;

  @GuardedBy("this")
  private final Map<ByteBuffer, Int2ObjectMap<MergingDigest>> sketches = new IdentityHashMap<>();

  public TDigestBuildSketchBufferAggregator(
      final ColumnValueSelector valueSelector,
      @Nullable final Integer compression
  )
  {
    Preconditions.checkNotNull(valueSelector);
    this.selector = valueSelector;
    if (compression != null) {
      this.compression = compression;
    } else {
      this.compression = TDigestBuildSketchAggregatorFactory.DEFAULT_COMPRESSION;
    }
  }

  @Override
  public synchronized void init(ByteBuffer buffer, int position)
  {
    MergingDigest emptyDigest = new MergingDigest(compression);
    putSketch(buffer, position, emptyDigest);
  }

  @Override
  public synchronized void aggregate(ByteBuffer buffer, int position)
  {
    MergingDigest sketch = sketches.get(buffer).get(position);
    Object x = selector.getObject();
    if (x instanceof Number) {
      sketch.add(((Number) x).doubleValue());
    } else {
      throw new IAE("Unexpected value of type " + x.getClass().getName() + " encountered");
    }
  }

  @Override
  public synchronized Object get(final ByteBuffer buffer, final int position)
  {
    return sketches.get(buffer).get(position);
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
  public synchronized void close()
  {
    sketches.clear();
  }

  @Override
  public synchronized void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    MergingDigest sketch = sketches.get(oldBuffer).get(oldPosition);
    putSketch(newBuffer, newPosition, sketch);
    final Int2ObjectMap<MergingDigest> map = sketches.get(oldBuffer);
    map.remove(oldPosition);
    if (map.isEmpty()) {
      sketches.remove(oldBuffer);
    }
  }

  private synchronized void putSketch(final ByteBuffer buffer, final int position, final MergingDigest sketch)
  {
    Int2ObjectMap<MergingDigest> map = sketches.computeIfAbsent(buffer, buf -> new Int2ObjectOpenHashMap<>());
    map.put(position, sketch);
  }

}
