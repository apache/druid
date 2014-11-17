/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.data;

import com.google.common.collect.Ordering;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.RoaringBitmapFactory;
import com.metamx.collections.bitmap.WrappedImmutableRoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.nio.ByteBuffer;

/**
 */
public class RoaringBitmapSerdeFactory implements BitmapSerdeFactory
{
  private static final ObjectStrategy<ImmutableBitmap> objectStrategy = new ImmutableRoaringBitmapObjectStrategy();
  private static final BitmapFactory bitmapFactory = new RoaringBitmapFactory();

  @Override
  public ObjectStrategy<ImmutableBitmap> getObjectStrategy()
  {
    return objectStrategy;
  }

  @Override
  public BitmapFactory getBitmapFactory()
  {
    return bitmapFactory;
  }

  private static Ordering<WrappedImmutableRoaringBitmap> roaringComparator = new Ordering<WrappedImmutableRoaringBitmap>()
  {
    @Override
    public int compare(
        WrappedImmutableRoaringBitmap set1, WrappedImmutableRoaringBitmap set2
    )
    {
      if (set1.size() == 0 && set2.size() == 0) {
        return 0;
      }
      if (set1.size() == 0) {
        return -1;
      }
      if (set2.size() == 0) {
        return 1;
      }

      return set1.compareTo(set2);
    }
  }.nullsFirst();

  private static class ImmutableRoaringBitmapObjectStrategy
      implements ObjectStrategy<ImmutableBitmap>
  {
    @Override
    public Class<ImmutableBitmap> getClazz()
    {
      return ImmutableBitmap.class;
    }

    @Override
    public ImmutableBitmap fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
      readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
      return new WrappedImmutableRoaringBitmap(new ImmutableRoaringBitmap(readOnlyBuffer));
    }

    @Override
    public byte[] toBytes(ImmutableBitmap val)
    {
      if (val == null || val.size() == 0) {
        return new byte[]{};
      }
      return val.toBytes();
    }

    @Override
    public int compare(ImmutableBitmap o1, ImmutableBitmap o2)
    {
      return roaringComparator.compare((WrappedImmutableRoaringBitmap) o1, (WrappedImmutableRoaringBitmap) o2);
    }
  }
}
