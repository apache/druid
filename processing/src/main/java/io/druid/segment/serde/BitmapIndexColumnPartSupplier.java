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
