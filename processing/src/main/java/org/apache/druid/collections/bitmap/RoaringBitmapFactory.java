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

import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.ISE;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;

/**
 * As the name suggests, this class instantiates bitmaps of the types
 * WrappedRoaringBitmap and WrappedImmutableRoaringBitmap.
 */
public class RoaringBitmapFactory implements BitmapFactory
{
  static final boolean DEFAULT_COMPRESS_RUN_ON_SERIALIZATION = false;
  private static final ImmutableRoaringBitmap EMPTY_IMMUTABLE_BITMAP;

  static {
    try {
      final RoaringBitmap roaringBitmap = new RoaringBitmap();
      final ByteBuffer buffer = ByteBuffer.allocate(roaringBitmap.serializedSizeInBytes());
      roaringBitmap.serialize(buffer);
      buffer.flip();
      EMPTY_IMMUTABLE_BITMAP = new ImmutableRoaringBitmap(buffer);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final WrappedImmutableRoaringBitmap WRAPPED_IMMUTABLE_ROARING_BITMAP =
      new WrappedImmutableRoaringBitmap(EMPTY_IMMUTABLE_BITMAP);

  private final boolean compressRunOnSerialization;

  public RoaringBitmapFactory()
  {
    this(DEFAULT_COMPRESS_RUN_ON_SERIALIZATION);
  }

  public RoaringBitmapFactory(boolean compressRunOnSerialization)
  {
    this.compressRunOnSerialization = compressRunOnSerialization;
  }

  private static Iterable<ImmutableRoaringBitmap> unwrap(
      final Iterable<ImmutableBitmap> b
  )
  {
    return new Iterable<ImmutableRoaringBitmap>()
    {
      @Override
      public Iterator<ImmutableRoaringBitmap> iterator()
      {
        final Iterator<ImmutableBitmap> i = b.iterator();
        return new Iterator<ImmutableRoaringBitmap>()
        {
          @Override
          public void remove()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean hasNext()
          {
            return i.hasNext();
          }

          @Override
          public ImmutableRoaringBitmap next()
          {
            WrappedImmutableRoaringBitmap wrappedBitmap = (WrappedImmutableRoaringBitmap) i.next();

            if (wrappedBitmap == null) {
              return EMPTY_IMMUTABLE_BITMAP;
            }

            return wrappedBitmap.getBitmap();
          }
        };
      }
    };
  }

  @Override
  public MutableBitmap makeEmptyMutableBitmap()
  {
    return new WrappedRoaringBitmap(compressRunOnSerialization);
  }

  @Override
  public ImmutableBitmap makeEmptyImmutableBitmap()
  {
    return WRAPPED_IMMUTABLE_ROARING_BITMAP;
  }

  @Override
  public ImmutableBitmap makeImmutableBitmap(MutableBitmap mutableBitmap)
  {
    if (!(mutableBitmap instanceof WrappedRoaringBitmap)) {
      throw new ISE("Cannot convert [%s]", mutableBitmap.getClass());
    }
    try {
      return ((WrappedRoaringBitmap) mutableBitmap).toImmutableBitmap();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ImmutableBitmap mapImmutableBitmap(ByteBuffer b)
  {
    return new WrappedImmutableRoaringBitmap(b);
  }

  @Override
  public ImmutableBitmap union(Iterable<ImmutableBitmap> b)
  {
    if (b instanceof Collection) {
      final Collection<ImmutableBitmap> bitmapList = (Collection<ImmutableBitmap>) b;
      final int size = bitmapList.size();
      if (size == 0) {
        return makeEmptyImmutableBitmap();
      } else if (size == 1) {
        return Iterables.getOnlyElement(b);
      }
    }

    return new WrappedImmutableRoaringBitmap(ImmutableRoaringBitmap.or(unwrap(b).iterator()));
  }

  @Override
  public ImmutableBitmap intersection(Iterable<ImmutableBitmap> b)
  {
    return new WrappedImmutableRoaringBitmap(BufferFastAggregation.and(unwrap(b).iterator()));
  }

  @Override
  public ImmutableBitmap complement(ImmutableBitmap b, int length)
  {
    return new WrappedImmutableRoaringBitmap(
        ImmutableRoaringBitmap.flip(((WrappedImmutableRoaringBitmap) b).getBitmap(), 0, length)
    );
  }
}
