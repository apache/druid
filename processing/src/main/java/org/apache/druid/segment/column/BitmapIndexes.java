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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.serde.StringBitmapIndexColumnPartSupplier;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;
import java.util.function.IntSupplier;
import java.util.function.Predicate;

public final class BitmapIndexes
{
  /**
   * Returns a bitmapIndex for null-only columns.
   *
   * @param rowCountSupplier a supplier that returns the row count of the segment that the column belongs to.
   *                         Getting from the supplier the row count can be expensive and thus should be
   *                         evaluated lazily.
   * @param bitmapFactory    a bitmapFactory to create a bitmapIndex.
   */
  public static BitmapIndex forNilColumn(IntSupplier rowCountSupplier, BitmapFactory bitmapFactory)
  {
    return new BitmapIndex()
    {
      private final Supplier<ImmutableBitmap> nullBitmapSupplier = Suppliers.memoize(
          () -> getBitmapFactory().complement(
              getBitmapFactory().makeEmptyImmutableBitmap(),
              rowCountSupplier.getAsInt()
          )
      );

      @Override
      public int getCardinality()
      {
        return 1;
      }

      @Nullable
      @Override
      public String getValue(int index)
      {
        return null;
      }

      @Override
      public boolean hasNulls()
      {
        return true;
      }

      @Override
      public BitmapFactory getBitmapFactory()
      {
        return bitmapFactory;
      }

      /**
       * Return -2 for non-null values to match what the {@link BitmapIndex} implementation in
       * {@link StringBitmapIndexColumnPartSupplier}
       * would return for {@link BitmapIndex#getIndex(String)} when there is only a single index, for the null value.
       * i.e., return an 'insertion point' of 1 for non-null values (see {@link BitmapIndex} interface)
       */
      @Override
      public int getIndex(@Nullable String value)
      {
        return NullHandling.isNullOrEquivalent(value) ? 0 : -2;
      }

      @Override
      public ImmutableBitmap getBitmap(int idx)
      {
        if (idx == 0) {
          return nullBitmapSupplier.get();
        } else {
          return bitmapFactory.makeEmptyImmutableBitmap();
        }
      }

      @Override
      public ImmutableBitmap getBitmapForValue(@Nullable String value)
      {
        if (NullHandling.isNullOrEquivalent(value)) {
          return nullBitmapSupplier.get();
        } else {
          return bitmapFactory.makeEmptyImmutableBitmap();
        }
      }

      @Override
      public Iterable<ImmutableBitmap> getBitmapsInRange(
          @Nullable String startValue,
          boolean startStrict,
          @Nullable String endValue,
          boolean endStrict,
          Predicate<String> matcher
      )
      {
        final int startIndex; // inclusive
        int endIndex; // exclusive

        if (startValue == null) {
          startIndex = 0;
        } else {
          if (NullHandling.isNullOrEquivalent(startValue)) {
            startIndex = startStrict ? 1 : 0;
          } else {
            startIndex = 1;
          }
        }

        if (endValue == null) {
          endIndex = 1;
        } else {
          if (NullHandling.isNullOrEquivalent(endValue)) {
            endIndex = endStrict ? 0 : 1;
          } else {
            endIndex = 1;
          }
        }

        endIndex = Math.max(startIndex, endIndex);
        if (startIndex == endIndex) {
          return Collections.emptyList();
        }
        if (matcher.test(null)) {
          return ImmutableList.of(getBitmap(0));
        }
        return ImmutableList.of(bitmapFactory.makeEmptyImmutableBitmap());
      }

      @Override
      public Iterable<ImmutableBitmap> getBitmapsForValues(Set<String> values)
      {
        if (values.contains(null) || (NullHandling.replaceWithDefault() && values.contains(""))) {
          return ImmutableList.of(getBitmap(0));
        }
        return ImmutableList.of(bitmapFactory.makeEmptyImmutableBitmap());
      }
    };
  }

  private BitmapIndexes()
  {
  }
}
