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

package org.apache.druid.segment.column;

import com.google.common.collect.Iterables;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;

public final class IndexedUtf8ValueSetIndex<T extends Indexed<ByteBuffer>>
    implements StringValueSetIndex, Utf8ValueSetIndex
{
  private static final int SIZE_WORTH_CHECKING_MIN = 8;

  private final BitmapFactory bitmapFactory;
  private final T dictionary;
  private final Indexed<ImmutableBitmap> bitmaps;

  public IndexedUtf8ValueSetIndex(
      BitmapFactory bitmapFactory,
      T dictionary,
      Indexed<ImmutableBitmap> bitmaps
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.dictionary = dictionary;
    this.bitmaps = bitmaps;
  }

  @Override
  public BitmapColumnIndex forValue(@Nullable String value)
  {
    return new SimpleBitmapColumnIndex()
    {
      @Override
      public double estimateSelectivity(int totalRows)
      {
        return Math.min(1, (double) getBitmapForValue().size() / totalRows);
      }

      @Override
      public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
      {

        return bitmapResultFactory.wrapDimensionValue(getBitmapForValue());
      }

      private ImmutableBitmap getBitmapForValue()
      {
        final ByteBuffer valueUtf8 = value == null ? null : ByteBuffer.wrap(StringUtils.toUtf8(value));
        final int idx = dictionary.indexOf(valueUtf8);
        return getBitmap(idx);
      }
    };
  }

  @Override
  public BitmapColumnIndex forSortedValues(SortedSet<String> values)
  {
    return getBitmapColumnIndexForSortedIterableUtf8(
        Iterables.transform(
            values,
            input -> input != null ? ByteBuffer.wrap(StringUtils.toUtf8(input)) : null
        )
    );
  }

  @Override
  public BitmapColumnIndex forSortedValuesUtf8(SortedSet<ByteBuffer> valuesUtf8)
  {
    final SortedSet<ByteBuffer> tailSet;

    if (valuesUtf8.size() >= SIZE_WORTH_CHECKING_MIN) {
      final ByteBuffer minValueInColumn = dictionary.get(0);
      tailSet = valuesUtf8.tailSet(minValueInColumn);
    } else {
      tailSet = valuesUtf8;
    }

    return getBitmapColumnIndexForSortedIterableUtf8(tailSet);
  }

  private ImmutableBitmap getBitmap(int idx)
  {
    if (idx < 0) {
      return bitmapFactory.makeEmptyImmutableBitmap();
    }

    final ImmutableBitmap bitmap = bitmaps.get(idx);
    return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
  }

  /**
   * Helper used by {@link #forSortedValues} and {@link #forSortedValuesUtf8}.
   */
  private BitmapColumnIndex getBitmapColumnIndexForSortedIterableUtf8(Iterable<ByteBuffer> valuesUtf8)
  {
    return new SimpleImmutableBitmapIterableIndex()
    {
      @Override
      public Iterable<ImmutableBitmap> getBitmapIterable()
      {
        final int dictionarySize = dictionary.size();

        return () -> new Iterator<ImmutableBitmap>()
        {
          final Iterator<ByteBuffer> iterator = valuesUtf8.iterator();
          int next = -1;

          @Override
          public boolean hasNext()
          {
            if (next < 0) {
              findNext();
            }
            return next >= 0;
          }

          @Override
          public ImmutableBitmap next()
          {
            if (next < 0) {
              findNext();
              if (next < 0) {
                throw new NoSuchElementException();
              }
            }
            final int swap = next;
            next = -1;
            return getBitmap(swap);
          }

          private void findNext()
          {
            while (next < 0 && iterator.hasNext()) {
              ByteBuffer nextValue = iterator.next();
              next = dictionary.indexOf(nextValue);

              if (next == -dictionarySize - 1) {
                // nextValue is past the end of the dictionary.
                // Note: we can rely on indexOf returning (-(insertion point) - 1), even though Indexed doesn't
                // guarantee it, because "dictionary" comes from GenericIndexed singleThreaded().
                break;
              }
            }
          }
        };
      }
    };
  }
}
