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

package org.apache.druid.collections.bitmap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.extendedset.intset.ConciseSet;
import org.apache.druid.extendedset.intset.ImmutableConciseSet;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;

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

  @VisibleForTesting
  public ConciseSet getBitmap()
  {
    return bitmap;
  }

  @Override
  public byte[] toBytes()
  {
    return ImmutableConciseSet.newImmutableFromMutable(bitmap).toBytes();
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
  public int getSizeInBytes()
  {
    return bitmap.getWords().length * Integer.BYTES;
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
  public String toString()
  {
    return getClass().getSimpleName() + bitmap;
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
  public PeekableIntIterator peekableIterator()
  {
    return new ConcisePeekableIteratorAdapter(bitmap.iterator());
  }

  @Override
  public boolean isEmpty()
  {
    return bitmap.size() == 0;
  }

  @Override
  public ImmutableBitmap intersection(ImmutableBitmap otherBitmap)
  {
    WrappedConciseBitmap other = (WrappedConciseBitmap) otherBitmap;
    ConciseSet unwrappedOtherBitmap = other.bitmap;
    return new WrappedConciseBitmap(bitmap.clone().intersection(unwrappedOtherBitmap));
  }

  @Override
  public boolean get(int value)
  {
    return bitmap.contains(value);
  }
}
