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

/**
 */
public final class ArrayBasedIndexedInts implements IndexedInts
{
  static final IndexedInts EMPTY = new ArrayBasedIndexedInts();

  private int[] expansion;
  private int size;

  public ArrayBasedIndexedInts()
  {
    expansion = IntArrays.EMPTY_ARRAY;
    size = 0;
  }

  public ArrayBasedIndexedInts(int[] expansion)
  {
    this.expansion = expansion;
    this.size = expansion.length;
  }

  public void ensureSize(int size)
  {
    if (expansion.length < size) {
      expansion = new int[size];
    }
  }

  public void setSize(int size)
  {
    if (size < 0 || size > expansion.length) {
      throw new IAE("Size[%d] > expansion.length[%d] or < 0", size, expansion.length);
    }
    this.size = size;
  }

  /**
   * Sets the values from the given array. The given values array is not reused and not prone to be mutated later.
   * Instead, the values from this array are copied into an array which is internal to ArrayBasedIndexedInts.
   */
  public void setValues(int[] values, int size)
  {
    if (size < 0 || size > values.length) {
      throw new IAE("Size[%d] should be between 0 and %d", size, values.length);
    }
    ensureSize(size);
    System.arraycopy(values, 0, expansion, 0, size);
    this.size = size;
  }

  public void setValue(int index, int value)
  {
    expansion[index] = value;
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public int get(int index)
  {
    if (index < 0 || index >= size) {
      throw new IAE("index[%d] >= size[%d] or < 0", index, size);
    }
    return expansion[index];
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // nothing to inspect
  }
}
