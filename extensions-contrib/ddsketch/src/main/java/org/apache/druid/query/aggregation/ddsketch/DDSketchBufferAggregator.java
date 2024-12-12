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

package org.apache.druid.query.aggregation.ddsketch;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketches;
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
 * Aggregator that builds DDSketch backed sketch using numeric values read from {@link ByteBuffer}
 */
public class DDSketchBufferAggregator implements BufferAggregator
{

  @Nonnull
  private final ColumnValueSelector selector;
  private final double relativeError;
  private final int numBins;
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<DDSketch>> sketchCache = new IdentityHashMap();

  public DDSketchBufferAggregator(
      final ColumnValueSelector valueSelector,
      final double relativeError,
      final int numBins
  )
  {
    Preconditions.checkNotNull(valueSelector);
    this.selector = valueSelector;
    this.relativeError = relativeError;
    this.numBins = numBins;
  }

  @Override
  public void init(ByteBuffer buffer, int position)
  {
    DDSketch sketch = DDSketches.collapsingLowestDense(relativeError, numBins);
    ByteBuffer mutationBuffer = buffer.duplicate();
    mutationBuffer.position(position);
    addToCache(buffer, position, sketch);
  }

  @Override
  public void aggregate(ByteBuffer buffer, int position)
  {
    Object x = selector.getObject();
    if (x == null) {
      return;
    }
    DDSketch sketch = sketchCache.get(buffer).get(position);

    if (x instanceof Number) {
      sketch.accept(((Number) x).doubleValue());
    } else if (x instanceof DDSketch) {
      sketch.mergeWith((DDSketch) x);
    } else {
      throw new IAE(
          "Expected a number or an instance of DDSketch, but received [%s] of type [%s]",
          x,
          x.getClass()
      );
    }
  }

  @Override
  public Object get(final ByteBuffer buffer, final int position)
  {
    // sketchCache is an IdentityHashMap where the reference of buffer is used for equality checks.
    // So the returned object isn't impacted by the changes in the buffer object made by concurrent threads.
    DDSketch obj = sketchCache.get(buffer).get(position);
    return obj;
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
    sketchCache.clear();
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    DDSketch sketch = sketchCache.get(oldBuffer).get(oldPosition);
    addToCache(newBuffer, newPosition, sketch);
    final Int2ObjectMap<DDSketch> map = sketchCache.get(oldBuffer);
    map.remove(oldPosition);
    if (map.isEmpty()) {
      sketchCache.remove(oldBuffer);
    }
  }

  private void addToCache(final ByteBuffer buffer, final int position, final DDSketch sketch)
  {
    Int2ObjectMap<DDSketch> map = sketchCache.computeIfAbsent(buffer, b -> new Int2ObjectOpenHashMap<>());
    map.put(position, sketch);
  }
}
