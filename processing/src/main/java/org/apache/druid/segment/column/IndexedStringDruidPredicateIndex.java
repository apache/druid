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

import com.google.common.base.Predicate;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class IndexedStringDruidPredicateIndex<TDictionary extends Indexed<String>> implements DruidPredicateIndex
{
  private final BitmapFactory bitmapFactory;
  private final TDictionary dictionary;
  private final Indexed<ImmutableBitmap> bitmaps;

  public IndexedStringDruidPredicateIndex(
      BitmapFactory bitmapFactory,
      TDictionary dictionary,
      Indexed<ImmutableBitmap> bitmaps
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.dictionary = dictionary;
    this.bitmaps = bitmaps;
  }

  @Override
  public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
  {
    return new SimpleImmutableBitmapIterableIndex()
    {
      @Override
      public Iterable<ImmutableBitmap> getBitmapIterable()
      {
        return () -> new Iterator<ImmutableBitmap>()
        {
          final Predicate<String> stringPredicate = matcherFactory.makeStringPredicate();
          final Iterator<String> iterator = dictionary.iterator();
          @Nullable
          String next = null;
          boolean nextSet = false;

          @Override
          public boolean hasNext()
          {
            if (!nextSet) {
              findNext();
            }
            return nextSet;
          }

          @Override
          public ImmutableBitmap next()
          {
            if (!nextSet) {
              findNext();
              if (!nextSet) {
                throw new NoSuchElementException();
              }
            }
            nextSet = false;
            final int idx = dictionary.indexOf(next);
            if (idx < 0) {
              return bitmapFactory.makeEmptyImmutableBitmap();
            }

            final ImmutableBitmap bitmap = bitmaps.get(idx);
            return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
          }

          private void findNext()
          {
            while (!nextSet && iterator.hasNext()) {
              String nextValue = iterator.next();
              nextSet = stringPredicate.apply(nextValue);
              if (nextSet) {
                next = nextValue;
              }
            }
          }
        };
      }
    };
  }
}
