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

package io.druid.segment.serde;

import com.google.common.base.Supplier;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.data.GenericIndexed;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

/**
*/
public class BitmapIndexColumnPartSupplier implements Supplier<BitmapIndex>
{
  private static final ImmutableConciseSet EMPTY_SET = new ImmutableConciseSet();

  private final GenericIndexed<ImmutableConciseSet> bitmaps;
  private final GenericIndexed<String> dictionary;

  public BitmapIndexColumnPartSupplier(
      GenericIndexed<ImmutableConciseSet> bitmaps,
      GenericIndexed<String> dictionary
  ) {
    this.bitmaps = bitmaps;
    this.dictionary = dictionary;
  }

  @Override
  public BitmapIndex get()
  {
    return new BitmapIndex()
    {
      @Override
      public int getCardinality()
      {
        return dictionary.size();
      }

      @Override
      public String getValue(int index)
      {
        return dictionary.get(index);
      }

      @Override
      public boolean hasNulls()
      {
        return dictionary.indexOf(null) >= 0;
      }

      @Override
      public ImmutableConciseSet getConciseSet(String value)
      {
        final int index = dictionary.indexOf(value);

        return getConciseSet(index);
      }

      @Override
      public ImmutableConciseSet getConciseSet(int idx)
      {
        if (idx < 0) {
          return EMPTY_SET;
        }

        final ImmutableConciseSet bitmap = bitmaps.get(idx);
        return bitmap == null ? EMPTY_SET : bitmap;
      }
    };
  }
}
