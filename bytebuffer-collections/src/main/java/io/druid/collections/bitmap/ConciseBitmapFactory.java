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

import com.google.common.collect.Iterables;
import io.druid.extendedset.intset.ImmutableConciseSet;
import io.druid.java.util.common.ISE;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;

/**
 * As the name suggests, this class instantiates bitmaps of the types
 * WrappedConciseBitmap and WrappedImmutableConciseBitmap.
 */
public class ConciseBitmapFactory implements BitmapFactory
{
  private static final ImmutableConciseSet EMPTY_IMMUTABLE_BITMAP = new ImmutableConciseSet();
  private static final WrappedImmutableConciseBitmap WRAPPED_IMMUTABLE_CONCISE_BITMAP =
      new WrappedImmutableConciseBitmap(EMPTY_IMMUTABLE_BITMAP);

  private static Iterable<ImmutableConciseSet> unwrap(
      final Iterable<ImmutableBitmap> b
  )
  {
    return new Iterable<ImmutableConciseSet>()
    {
      @Override
      public Iterator<ImmutableConciseSet> iterator()
      {
        final Iterator<ImmutableBitmap> i = b.iterator();
        return new Iterator<ImmutableConciseSet>()
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
          public ImmutableConciseSet next()
          {
            final WrappedImmutableConciseBitmap wrappedBitmap = (WrappedImmutableConciseBitmap) i.next();

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
    return new WrappedConciseBitmap();
  }

  @Override
  public ImmutableBitmap makeEmptyImmutableBitmap()
  {
    return WRAPPED_IMMUTABLE_CONCISE_BITMAP;
  }

  @Override
  public ImmutableBitmap makeImmutableBitmap(MutableBitmap mutableBitmap)
  {
    if (!(mutableBitmap instanceof WrappedConciseBitmap)) {
      throw new ISE("Cannot convert [%s]", mutableBitmap.getClass());
    }
    return new WrappedImmutableConciseBitmap(
        ImmutableConciseSet.newImmutableFromMutable(
            ((WrappedConciseBitmap) mutableBitmap).getBitmap()
        )
    );
  }

  @Override
  public ImmutableBitmap mapImmutableBitmap(ByteBuffer b)
  {
    return new WrappedImmutableConciseBitmap(b);
  }

  @Override
  public ImmutableBitmap union(Iterable<ImmutableBitmap> b)
      throws ClassCastException
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

    return new WrappedImmutableConciseBitmap(ImmutableConciseSet.union(unwrap(b)));
  }

  @Override
  public ImmutableBitmap intersection(Iterable<ImmutableBitmap> b)
      throws ClassCastException
  {
    return new WrappedImmutableConciseBitmap(ImmutableConciseSet.intersection(unwrap(b)));
  }

  @Override
  public ImmutableBitmap complement(ImmutableBitmap b)
  {
    return new WrappedImmutableConciseBitmap(ImmutableConciseSet.complement(((WrappedImmutableConciseBitmap) b).getBitmap()));
  }

  @Override
  public ImmutableBitmap complement(ImmutableBitmap b, int length)
  {
    return new WrappedImmutableConciseBitmap(
        ImmutableConciseSet.complement(
            ((WrappedImmutableConciseBitmap) b).getBitmap(),
            length
        )
    );
  }
}
