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

import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.nio.ByteBuffer;

public class WrappedImmutableRoaringBitmap implements ImmutableBitmap
{
  /**
   * Underlying bitmap.
   */
  private final ImmutableRoaringBitmap bitmap;

  protected WrappedImmutableRoaringBitmap(ByteBuffer byteBuffer)
  {
    this.bitmap = new ImmutableRoaringBitmap(byteBuffer);
  }

  /**
   * Wrap an ImmutableRoaringBitmap
   *
   * @param immutableRoaringBitmap bitmap to be wrapped
   */
  public WrappedImmutableRoaringBitmap(ImmutableRoaringBitmap immutableRoaringBitmap)
  {
    this.bitmap = immutableRoaringBitmap;
  }

  public ImmutableRoaringBitmap getBitmap()
  {
    return bitmap;
  }

  @Override
  public byte[] toBytes()
  {
    try {
      ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
      bitmap.serialize(buffer);
      return buffer.array();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + bitmap;
  }

  @Override
  public IntIterator iterator()
  {
    return bitmap.getIntIterator();
  }

  @Override
  public PeekableIntIterator peekableIterator()
  {
    return bitmap.getIntIterator();
  }

  @Override
  public BatchIterator batchIterator()
  {
    return bitmap.getBatchIterator();
  }

  @Override
  public int size()
  {
    return bitmap.getCardinality();
  }

  @Override
  public boolean isEmpty()
  {
    return bitmap.isEmpty();
  }

  @Override
  public boolean get(int value)
  {
    return bitmap.contains(value);
  }

  @Override
  public ImmutableBitmap intersection(ImmutableBitmap otherBitmap)
  {
    WrappedImmutableRoaringBitmap other = (WrappedImmutableRoaringBitmap) otherBitmap;
    ImmutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    return new WrappedImmutableRoaringBitmap(ImmutableRoaringBitmap.and(bitmap, unwrappedOtherBitmap));
  }

}
