/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.data;

import com.google.common.collect.Ordering;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ConciseBitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.WrappedImmutableConciseBitmap;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import java.nio.ByteBuffer;

/**
 */
public class ConciseBitmapSerdeFactory implements BitmapSerdeFactory
{
  private static final ObjectStrategy<ImmutableBitmap> objectStrategy = new ImmutableConciseSetObjectStrategy();
  private static final BitmapFactory bitmapFactory = new ConciseBitmapFactory();

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

  private static Ordering<WrappedImmutableConciseBitmap> conciseComparator = new Ordering<WrappedImmutableConciseBitmap>()
  {
    @Override
    public int compare(
        WrappedImmutableConciseBitmap conciseSet, WrappedImmutableConciseBitmap conciseSet1
    )
    {
      if (conciseSet.size() == 0 && conciseSet1.size() == 0) {
        return 0;
      }
      if (conciseSet.size() == 0) {
        return -1;
      }
      if (conciseSet1.size() == 0) {
        return 1;
      }
      return conciseSet.compareTo(conciseSet1);
    }
  }.nullsFirst();

  private static class ImmutableConciseSetObjectStrategy
      implements ObjectStrategy<ImmutableBitmap>
  {
    @Override
    public Class<ImmutableBitmap> getClazz()
    {
      return ImmutableBitmap.class;
    }

    @Override
    public WrappedImmutableConciseBitmap fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
      readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
      return new WrappedImmutableConciseBitmap(new ImmutableConciseSet(readOnlyBuffer));
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
      return conciseComparator.compare((WrappedImmutableConciseBitmap) o1, (WrappedImmutableConciseBitmap) o2);
    }
  }

  @Override
  public String toString()
  {
    return "ConciseBitmapSerdeFactory{}";
  }

  @Override
  public boolean equals(Object o)
  {
    return this == o || o instanceof ConciseBitmapSerdeFactory;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }
}
