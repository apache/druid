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
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.serde.StringBitmapIndexColumnPartSupplier;

import javax.annotation.Nullable;
import java.util.function.IntSupplier;

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
  public static BitmapIndex forNullOnlyColumn(IntSupplier rowCountSupplier, BitmapFactory bitmapFactory)
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
    };
  }

  private BitmapIndexes()
  {
  }
}
