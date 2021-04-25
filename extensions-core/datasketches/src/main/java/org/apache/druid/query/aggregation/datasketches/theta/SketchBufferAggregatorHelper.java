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

package org.apache.druid.query.aggregation.datasketches.theta;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.datasketches.Family;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.IdentityHashMap;

/**
 * A helper class used by {@link SketchBufferAggregator} and {@link SketchVectorAggregator}
 * for aggregation operations on byte buffers. Getting the object from value selectors is outside this class.
 */
final class SketchBufferAggregatorHelper
{
  private final int size;
  private final int maxIntermediateSize;
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<Union>> unions = new IdentityHashMap<>();
  private final IdentityHashMap<ByteBuffer, WritableMemory> memCache = new IdentityHashMap<>();

  public SketchBufferAggregatorHelper(final int size, final int maxIntermediateSize)
  {
    this.size = size;
    this.maxIntermediateSize = maxIntermediateSize;
  }

  /**
   * Helper for implementing {@link org.apache.druid.query.aggregation.BufferAggregator#init} and
   * {@link org.apache.druid.query.aggregation.VectorAggregator#init}.
   */
  public void init(ByteBuffer buf, int position)
  {
    createNewUnion(buf, position, false);
  }

  /**
   * Helper for implementing {@link org.apache.druid.query.aggregation.BufferAggregator#get} and
   * {@link org.apache.druid.query.aggregation.VectorAggregator#get}.
   */
  public Object get(ByteBuffer buf, int position)
  {
    Int2ObjectMap<Union> unionMap = unions.get(buf);
    Union union = unionMap != null ? unionMap.get(position) : null;
    if (union == null) {
      return SketchHolder.EMPTY;
    }
    //in the code below, I am returning SetOp.getResult(true, null)
    //"true" returns an ordered sketch but slower to compute than unordered sketch.
    //however, advantage of ordered sketch is that they are faster to "union" later
    //given that results from the aggregator will be combined further, it is better
    //to return the ordered sketch here
    return SketchHolder.of(union.getResult(true, null));
  }

  /**
   * Helper for implementing {@link org.apache.druid.query.aggregation.BufferAggregator#relocate} and
   * {@link org.apache.druid.query.aggregation.VectorAggregator#relocate}.
   */
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    createNewUnion(newBuffer, newPosition, true);
    Int2ObjectMap<Union> unionMap = unions.get(oldBuffer);
    if (unionMap != null) {
      unionMap.remove(oldPosition);
      if (unionMap.isEmpty()) {
        unions.remove(oldBuffer);
        memCache.remove(oldBuffer);
      }
    }
  }

  /**
   * Returns a {@link Union} associated with a particular buffer location.
   *
   * The Union object will be cached in this helper until {@link #close()} is called.
   */
  public Union getOrCreateUnion(ByteBuffer buf, int position)
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
    WritableMemory mem = getMemory(buf).writableRegion(position, maxIntermediateSize);
    Union union = isWrapped
                  ? (Union) SetOperation.wrap(mem)
                  : (Union) SetOperation.builder().setNominalEntries(size).build(Family.UNION, mem);
    Int2ObjectMap<Union> unionMap = unions.get(buf);
    if (unionMap == null) {
      unionMap = new Int2ObjectOpenHashMap<>();
      unions.put(buf, unionMap);
    }
    unionMap.put(position, union);
    return union;
  }

  public void close()
  {
    unions.clear();
    memCache.clear();
  }

  private WritableMemory getMemory(ByteBuffer buffer)
  {
    WritableMemory mem = memCache.get(buffer);
    if (mem == null) {
      mem = WritableMemory.wrap(buffer, ByteOrder.LITTLE_ENDIAN);
      memCache.put(buffer, mem);
    }
    return mem;
  }
}
