/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.datasketches.quantiles;

import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.quantiles.DoublesUnion;
import com.yahoo.sketches.quantiles.DoublesUnionBuilder;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class DoublesSketchOffheapBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector selector;
  private final int size;
  private final int maxIntermediateSize;

  private NativeMemory nm;

  private final Map<Integer, DoublesUnion> unions = new HashMap<>(); //position in BB -> DoublesUnion Object

  public DoublesSketchOffheapBufferAggregator(ObjectColumnSelector selector, int size, int maxIntermediateSize)
  {
    this.selector = selector;
    this.size = size;
    this.maxIntermediateSize = maxIntermediateSize;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    if (nm == null) {
      nm = new NativeMemory(buf);
    }

    Memory mem = new MemoryRegion(nm, position, maxIntermediateSize);
    unions.put(position, new DoublesUnionBuilder().setMaxK(size).initMemory(mem).build());
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Object update = selector.get();
    if (update == null) {
      return;
    }

    DoublesUnion union = getUnion(buf, position);
    DoublesSketchAggregator.updateUnion(union, update);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    //in the code below, I am returning SetOp.getResult(true, null)
    //"true" returns an ordered sketch but slower to compute than unordered sketch.
    //however, advantage of ordered sketch is that they are faster to "union" later
    //given that results from the aggregator will be combined further, it is better
    //to return the ordered sketch here
    return DoublesSketchHolder.of(getUnion(buf, position).getResult());
  }

  private DoublesUnion getUnion(ByteBuffer buf, int position)
  {
    DoublesUnion union = unions.get(position);
    if (union == null) {
      Memory mem = new MemoryRegion(nm, position, maxIntermediateSize);
      union = DoublesUnionBuilder.wrap(mem);
      unions.put(position, union);
    }
    return union;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
    unions.clear();
  }
}
