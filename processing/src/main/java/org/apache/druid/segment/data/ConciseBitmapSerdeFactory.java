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

package org.apache.druid.segment.data;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ConciseBitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.WrappedImmutableConciseBitmap;
import org.apache.druid.extendedset.intset.ImmutableConciseSet;

import java.nio.ByteBuffer;

/**
 */
public class ConciseBitmapSerdeFactory implements BitmapSerdeFactory
{
  private static final ObjectStrategy<ImmutableBitmap> OBJECT_STRATEGY = new ImmutableConciseSetObjectStrategy();
  private static final BitmapFactory BITMAP_FACTORY = new ConciseBitmapFactory();

  @Override
  public ObjectStrategy<ImmutableBitmap> getObjectStrategy()
  {
    return OBJECT_STRATEGY;
  }

  @Override
  public BitmapFactory getBitmapFactory()
  {
    return BITMAP_FACTORY;
  }

  private static class ImmutableConciseSetObjectStrategy implements ObjectStrategy<ImmutableBitmap>
  {
    @Override
    public Class<ImmutableBitmap> getClazz()
    {
      return ImmutableBitmap.class;
    }

    @Override
    public WrappedImmutableConciseBitmap fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      buffer.limit(buffer.position() + numBytes);
      return new WrappedImmutableConciseBitmap(new ImmutableConciseSet(buffer.asIntBuffer()));
    }

    @Override
    public byte[] toBytes(ImmutableBitmap val)
    {
      if (val == null || val.isEmpty()) {
        return new byte[]{};
      }
      return val.toBytes();
    }

    @Override
    public int compare(ImmutableBitmap o1, ImmutableBitmap o2)
    {
      throw new UnsupportedOperationException();
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
