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

package io.druid.collections.bitmap;

import com.google.common.primitives.Ints;
import io.druid.extendedset.intset.ConciseSet;
import io.druid.extendedset.intset.ImmutableConciseSet;
import org.roaringbitmap.IntIterator;

import java.nio.ByteBuffer;

public class WrappedConciseBitmap implements MutableBitmap
{
  /**
   * Underlying bitmap.
   */
  private ConciseSet bitmap;

  /**
   * Create a new WrappedConciseBitmap wrapping an empty  ConciseSet
   */
  public WrappedConciseBitmap()
  {
    this.bitmap = new ConciseSet();
  }

  /**
   * Create a bitmap wrapping the given bitmap
   *
   * @param conciseSet bitmap to be wrapped
   */
  public WrappedConciseBitmap(ConciseSet conciseSet)
  {
    this.bitmap = conciseSet;
  }

  ConciseSet getBitmap()
  {
    return bitmap;
  }

  @Override
  public byte[] toBytes()
  {
    return ImmutableConciseSet.newImmutableFromMutable(bitmap).toBytes();
  }

  @Override
  public int compareTo(ImmutableBitmap other)
  {
    return bitmap.compareTo(((WrappedConciseBitmap) other).getBitmap());
  }

  @Override
  public void clear()
  {
    bitmap.clear();
  }

  @Override
  public void or(MutableBitmap mutableBitmap)
  {
    WrappedConciseBitmap other = (WrappedConciseBitmap) mutableBitmap;
    ConciseSet unwrappedOtherBitmap = other.bitmap;
    bitmap.addAll(unwrappedOtherBitmap);
  }

  @Override
  public void and(MutableBitmap mutableBitmap)
  {
    WrappedConciseBitmap other = (WrappedConciseBitmap) mutableBitmap;
    ConciseSet unwrappedOtherBitmap = other.bitmap;
    bitmap = bitmap.intersection(unwrappedOtherBitmap);
  }

  @Override
  public void xor(MutableBitmap mutableBitmap)
  {
    WrappedConciseBitmap other = (WrappedConciseBitmap) mutableBitmap;
    ConciseSet unwrappedOtherBitmap = other.bitmap;
    bitmap = bitmap.symmetricDifference(unwrappedOtherBitmap);
  }

  @Override
  public void andNot(MutableBitmap mutableBitmap)
  {
    WrappedConciseBitmap other = (WrappedConciseBitmap) mutableBitmap;
    ConciseSet unwrappedOtherBitmap = other.bitmap;
    bitmap = bitmap.difference(unwrappedOtherBitmap);
  }

  @Override
  public int getSizeInBytes()
  {
    return bitmap.getWords().length * Ints.BYTES;
  }

  @Override
  public void add(int entry)
  {
    bitmap.add(entry);
  }

  @Override
  public int size()
  {
    return bitmap.size();
  }

  @Override
  public void serialize(ByteBuffer buffer)
  {
    buffer.put(toBytes());
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + bitmap.toString();
  }

  @Override
  public void remove(int entry)
  {
    bitmap.remove(entry);
  }

  @Override
  public IntIterator iterator()
  {
    return bitmap.iterator();
  }

  @Override
  public boolean isEmpty()
  {
    return bitmap.size() == 0;
  }

  @Override
  public ImmutableBitmap union(ImmutableBitmap otherBitmap)
  {
    WrappedConciseBitmap other = (WrappedConciseBitmap) otherBitmap;
    ConciseSet unwrappedOtherBitmap = other.bitmap;
    return new WrappedConciseBitmap(bitmap.clone().union(unwrappedOtherBitmap));
  }

  @Override
  public ImmutableBitmap intersection(ImmutableBitmap otherBitmap)
  {
    WrappedConciseBitmap other = (WrappedConciseBitmap) otherBitmap;
    ConciseSet unwrappedOtherBitmap = other.bitmap;
    return new WrappedConciseBitmap(bitmap.clone().intersection(unwrappedOtherBitmap));
  }

  @Override
  public ImmutableBitmap difference(ImmutableBitmap otherBitmap)
  {
    WrappedConciseBitmap other = (WrappedConciseBitmap) otherBitmap;
    ConciseSet unwrappedOtherBitmap = other.bitmap;
    return new WrappedConciseBitmap(bitmap.clone().difference(unwrappedOtherBitmap));
  }

  @Override
  public boolean get(int value)
  {
    return bitmap.contains(value);
  }
}
