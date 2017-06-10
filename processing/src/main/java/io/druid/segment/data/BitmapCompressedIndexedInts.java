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

import com.google.common.collect.Ordering;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.IntIteratorUtils;
import it.unimi.dsi.fastutil.ints.IntIterator;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 */
public class BitmapCompressedIndexedInts implements IndexedInts, Comparable<ImmutableBitmap>
{
  private static final Ordering<ImmutableBitmap> COMPARATOR = new Ordering<ImmutableBitmap>()
  {
    @Override
    public int compare(
        ImmutableBitmap set, ImmutableBitmap set1
    )
    {
      if (set.size() == 0 && set1.size() == 0) {
        return 0;
      }
      if (set.size() == 0) {
        return -1;
      }
      if (set1.size() == 0) {
        return 1;
      }
      return set.compareTo(set1);
    }
  }.nullsFirst();

  private final ImmutableBitmap immutableBitmap;

  public BitmapCompressedIndexedInts(ImmutableBitmap immutableBitmap)
  {
    this.immutableBitmap = immutableBitmap;
  }

  @Override
  public int compareTo(@Nullable ImmutableBitmap otherBitmap)
  {
    return COMPARATOR.compare(immutableBitmap, otherBitmap);
  }

  @Override
  public int size()
  {
    return immutableBitmap.size();
  }

  @Override
  public int get(int index)
  {
    throw new UnsupportedOperationException("This is really slow, so it's just not supported.");
  }

  public ImmutableBitmap getImmutableBitmap()
  {
    return immutableBitmap;
  }

  @Override
  public IntIterator iterator()
  {
    return IntIteratorUtils.fromRoaringBitmapIntIterator(immutableBitmap.iterator());
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
    inspector.visit("immutableBitmap", immutableBitmap);
  }
}
