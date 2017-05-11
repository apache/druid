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

import com.google.common.base.Preconditions;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;

import java.io.IOException;

/**
 * An IndexedInts that always returns [0, 1, ..., N].
 */
public class RangeIndexedInts implements IndexedInts
{
  private static final int CACHE_LIMIT = 8;
  private static final RangeIndexedInts[] CACHE = new RangeIndexedInts[CACHE_LIMIT];

  static {
    for (int i = 0; i < CACHE_LIMIT; i++) {
      CACHE[i] = new RangeIndexedInts(i);
    }
  }

  private final int size;

  private RangeIndexedInts(int size)
  {
    this.size = size;
  }

  public static RangeIndexedInts create(final int size)
  {
    Preconditions.checkArgument(size >= 0, "size >= 0");
    if (size < CACHE_LIMIT) {
      return CACHE[size];
    } else {
      return new RangeIndexedInts(size);
    }
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
      throw new IndexOutOfBoundsException("index: " + index);
    }
    return index;
  }

  @Override
  public void fill(int index, int[] toFill)
  {
    throw new UnsupportedOperationException("fill");
  }

  @Override
  public IntIterator iterator()
  {
    return IntIterators.fromTo(0, size);
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
