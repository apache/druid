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

package org.apache.druid.segment.serde;

import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;

/**
 * Provides {@link BitmapIndex} for some dictionary encoded column, where the dictionary and bitmaps are stored in some
 * {@link Indexed}. A {@link BitmapIndexConverter} must be provided to convert the {@link String} from the
 * {@link BitmapIndex} interface to whatever actual value type is stored in the dictionary {@link Indexed}.
 *
 * This might change in the future if {@link BitmapIndex} is modified to also be generic
 */
public class BitmapIndexColumnPartSupplier<T extends Comparable<T>> implements Supplier<BitmapIndex>
{
  private final BitmapFactory bitmapFactory;
  private final Indexed<ImmutableBitmap> bitmaps;
  private final Indexed<T> dictionary;
  private final BitmapIndexConverter<T> indexConverter;

  public static final StringBitmapIndexConverter STRING_CONVERTER = new StringBitmapIndexConverter();

  public BitmapIndexColumnPartSupplier(
      BitmapFactory bitmapFactory,
      Indexed<ImmutableBitmap> bitmaps,
      Indexed<T> dictionary,
      BitmapIndexConverter<T> indexConverter
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.bitmaps = bitmaps;
    this.dictionary = dictionary;
    this.indexConverter = indexConverter;
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
        return indexConverter.toString(dictionary.get(index));
      }

      @Override
      public boolean hasNulls()
      {
        return dictionary.indexOf(null) >= 0;
      }

      @Override
      public BitmapFactory getBitmapFactory()
      {
        return bitmapFactory;
      }

      @Override
      public int getIndex(@Nullable String value)
      {
        // GenericIndexed.indexOf satisfies contract needed by BitmapIndex.indexOf
        return dictionary.indexOf(indexConverter.fromString(value));
      }

      @Override
      public ImmutableBitmap getBitmap(int idx)
      {
        if (idx < 0) {
          return bitmapFactory.makeEmptyImmutableBitmap();
        }

        final ImmutableBitmap bitmap = bitmaps.get(idx);
        return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
      }
    };
  }

  /**
   * Convert String values from {@link BitmapIndex} to and from whatever type of value is actually stored in the
   * dictionary {@link GenericIndexed} of the {@link BitmapIndexColumnPartSupplier}
   */
  interface BitmapIndexConverter<T>
  {
    @Nullable
    T fromString(@Nullable String value);

    @Nullable
    String toString(@Nullable T value);
  }

  /**
   * The original, strings all the way down
   */
  public static class StringBitmapIndexConverter implements BitmapIndexColumnPartSupplier.BitmapIndexConverter<String>
  {
    @Nullable
    @Override
    public String fromString(@Nullable String value)
    {
      return value;
    }

    @Override
    public String toString(@Nullable String value)
    {
      return value;
    }
  }
}
