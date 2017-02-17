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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Ordering;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.RoaringBitmapFactory;
import io.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.nio.ByteBuffer;

/**
 */
public class RoaringBitmapSerdeFactory implements BitmapSerdeFactory
{
  private static final boolean DEFAULT_COMPRESS_RUN_ON_SERIALIZATION = true;
  private static final ObjectStrategy<ImmutableBitmap> objectStrategy = new ImmutableRoaringBitmapObjectStrategy();

  private final boolean compressRunOnSerialization;
  private final BitmapFactory bitmapFactory;

  @JsonCreator
  public RoaringBitmapSerdeFactory(
      @JsonProperty("compressRunOnSerialization") Boolean compressRunOnSerialization
  )
  {
    this.compressRunOnSerialization = compressRunOnSerialization == null
                                      ? DEFAULT_COMPRESS_RUN_ON_SERIALIZATION
                                      : compressRunOnSerialization;
    this.bitmapFactory = new RoaringBitmapFactory(this.compressRunOnSerialization);
  }

  @JsonProperty
  public boolean getCompressRunOnSerialization()
  {
    return compressRunOnSerialization;
  }

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
      buffer.limit(buffer.position() + numBytes);
      return new WrappedImmutableRoaringBitmap(new ImmutableRoaringBitmap(buffer));
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

  @Override
  public String toString()
  {
    return "RoaringBitmapSerdeFactory{}";
  }

  @Override
  public boolean equals(Object o)
  {
    return this == o || o instanceof RoaringBitmapSerdeFactory;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }
}
