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

import com.google.common.base.Throwables;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      bitmap.serialize(new DataOutputStream(out));
      return out.toByteArray();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int compareTo(ImmutableBitmap other)
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + bitmap.toString();
  }

  @Override
  public IntIterator iterator()
  {
    return bitmap.getIntIterator();
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
  public ImmutableBitmap union(ImmutableBitmap otherBitmap)
  {
    WrappedImmutableRoaringBitmap other = (WrappedImmutableRoaringBitmap) otherBitmap;
    ImmutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    return new WrappedImmutableRoaringBitmap(ImmutableRoaringBitmap.or(bitmap, unwrappedOtherBitmap));
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

  @Override
  public ImmutableBitmap difference(ImmutableBitmap otherBitmap)
  {
    WrappedImmutableRoaringBitmap other = (WrappedImmutableRoaringBitmap) otherBitmap;
    ImmutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    return new WrappedImmutableRoaringBitmap(ImmutableRoaringBitmap.andNot(bitmap, unwrappedOtherBitmap));
  }
}
