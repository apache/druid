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

package io.druid.query.aggregation.datasketches.theta;

import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Union;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ObjectColumnSelector;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

public class SketchBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector selector;
  private final int size;
  private final int maxIntermediateSize;
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<Union>> unions = new IdentityHashMap<>();
  private final IdentityHashMap<ByteBuffer, NativeMemory> nmCache = new IdentityHashMap<>();

  public SketchBufferAggregator(ObjectColumnSelector selector, int size, int maxIntermediateSize)
  {
    this.selector = selector;
    this.size = size;
    this.maxIntermediateSize = maxIntermediateSize;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    createNewUnion(buf, position, false);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Object update = selector.get();
    if (update == null) {
      return;
    }

    Union union = getUnion(buf, position);
    SketchAggregator.updateUnion(union, update);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    //in the code below, I am returning SetOp.getResult(true, null)
    //"true" returns an ordered sketch but slower to compute than unordered sketch.
    //however, advantage of ordered sketch is that they are faster to "union" later
    //given that results from the aggregator will be combined further, it is better
    //to return the ordered sketch here
    return SketchHolder.of(getUnion(buf, position).getResult(true, null));
  }

  //Note that this is not threadsafe and I don't think it needs to be
  private Union getUnion(ByteBuffer buf, int position)
  {
    Int2ObjectMap<Union> unionMap = unions.get(buf);
    Union union = unionMap != null ? unionMap.get(position) : null;
    if (union != null) {
      return union;
    }
    return createNewUnion(buf, position, true);
  }

  private Union createNewUnion(ByteBuffer buf, int position, boolean isWrapped)
  {
    NativeMemory nm = getNativeMemory(buf);
    Memory mem = new MemoryRegion(nm, position, maxIntermediateSize);
    Union union = isWrapped
                  ? (Union) SetOperation.wrap(mem)
                  : (Union) SetOperation.builder().initMemory(mem).build(size, Family.UNION);
    Int2ObjectMap<Union> unionMap = unions.get(buf);
    if (unionMap == null) {
      unionMap = new Int2ObjectOpenHashMap<>();
      unions.put(buf, unionMap);
    }
    unionMap.put(position, union);
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
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
    unions.clear();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    createNewUnion(newBuffer, newPosition, true);
    Int2ObjectMap<Union> unionMap = unions.get(oldBuffer);
    if (unionMap != null) {
      unionMap.remove(oldPosition);
      if (unionMap.isEmpty()) {
        unions.remove(oldBuffer);
        nmCache.remove(oldBuffer);
      }
    }
  }

  private NativeMemory getNativeMemory(ByteBuffer buffer)
  {
    NativeMemory nm = nmCache.get(buffer);
    if (nm == null) {
      nm = new NativeMemory(buffer);
      nmCache.put(buffer, nm);
    }
    return nm;
  }

}
