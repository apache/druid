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


import io.druid.extendedset.intset.ImmutableConciseSet;
import org.roaringbitmap.IntIterator;

import java.nio.ByteBuffer;

public class WrappedImmutableConciseBitmap implements ImmutableBitmap
{
  /**
   * Underlying bitmap.
   */
  private final ImmutableConciseSet bitmap;

  public WrappedImmutableConciseBitmap(ByteBuffer byteBuffer)
  {
    this.bitmap = new ImmutableConciseSet(byteBuffer);
  }

  /**
   * Wrap an ImmutableConciseSet
   *
   * @param immutableConciseSet bitmap to be wrapped
   */
  public WrappedImmutableConciseBitmap(ImmutableConciseSet immutableConciseSet)
  {
    this.bitmap = immutableConciseSet;
  }

  public ImmutableConciseSet getBitmap()
  {
    return bitmap;
  }

  @Override
  public boolean get(int value)
  {
    return bitmap.contains(value);
  }

  @Override
  public byte[] toBytes()
  {
    return bitmap.toBytes();
  }

  @Override
  public int compareTo(ImmutableBitmap other)
  {
    return bitmap.compareTo(((WrappedImmutableConciseBitmap) other).getBitmap());
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + bitmap.toString();
  }

  @Override
  public IntIterator iterator()
  {
    return bitmap.iterator();
  }

  @Override
  public int size()
  {
    return bitmap.size();
  }

  @Override
  public boolean isEmpty()
  {
    return bitmap.size() == 0;
  }

  @Override
  public ImmutableBitmap union(ImmutableBitmap otherBitmap)
  {
    WrappedImmutableConciseBitmap other = (WrappedImmutableConciseBitmap) otherBitmap;
    ImmutableConciseSet unwrappedOtherBitmap = other.bitmap;
    return new WrappedImmutableConciseBitmap(ImmutableConciseSet.union(bitmap, unwrappedOtherBitmap));
  }

  @Override
  public ImmutableBitmap intersection(ImmutableBitmap otherBitmap)
  {
    WrappedImmutableConciseBitmap other = (WrappedImmutableConciseBitmap) otherBitmap;
    ImmutableConciseSet unwrappedOtherBitmap = other.bitmap;
    return new WrappedImmutableConciseBitmap(ImmutableConciseSet.intersection(bitmap, unwrappedOtherBitmap));
  }

  @Override
  public ImmutableBitmap difference(ImmutableBitmap otherBitmap)
  {
    WrappedImmutableConciseBitmap other = (WrappedImmutableConciseBitmap) otherBitmap;
    ImmutableConciseSet unwrappedOtherBitmap = other.bitmap;
    return new WrappedImmutableConciseBitmap(
        ImmutableConciseSet.intersection(
            bitmap,
            ImmutableConciseSet.complement(unwrappedOtherBitmap)
        )
    );
  }
}
