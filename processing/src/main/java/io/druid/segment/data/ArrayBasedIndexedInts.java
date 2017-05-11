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

package io.druid.segment.data;

import io.druid.java.util.common.IAE;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;

import java.io.IOException;

/**
 */
public final class ArrayBasedIndexedInts implements IndexedInts
{
  private static final ArrayBasedIndexedInts EMPTY = new ArrayBasedIndexedInts(IntArrays.EMPTY_ARRAY, 0);

  public static ArrayBasedIndexedInts of(int[] expansion)
  {
    if (expansion.length == 0) {
      return EMPTY;
    }
    return new ArrayBasedIndexedInts(expansion, expansion.length);
  }

  public static ArrayBasedIndexedInts of(int[] expansion, int size)
  {
    if (size == 0) {
      return EMPTY;
    }
    if (size < 0 || size > expansion.length) {
      throw new IAE("Size[%s] should be between 0 and %s", size, expansion.length);
    }
    return new ArrayBasedIndexedInts(expansion, size);
  }

  private final int[] expansion;
  private final int size;

  private ArrayBasedIndexedInts(int[] expansion, int size)
  {
    this.expansion = expansion;
    this.size = size;
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public int get(int index)
  {
    if (index >= size) {
      throw new IndexOutOfBoundsException("index: " + index + ", size: " + size);
    }
    return expansion[index];
  }

  @Override
  public IntIterator iterator()
  {
    return IntIterators.wrap(expansion, 0, size);
  }

  @Override
  public void fill(int index, int[] toFill)
  {
    throw new UnsupportedOperationException("fill not supported");
  }

  @Override
  public void close() throws IOException
  {
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // nothing to inspect
  }
}
