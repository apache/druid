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
import org.apache.druid.collections.spatial.ImmutableRTree;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.column.BitmapColumnIndex;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.column.DictionaryEncodedValueIndex;
import org.apache.druid.segment.column.DruidPredicateIndex;
import org.apache.druid.segment.column.IndexedStringDictionaryEncodedStringValueIndex;
import org.apache.druid.segment.column.IndexedStringDruidPredicateIndex;
import org.apache.druid.segment.column.IndexedUtf8LexicographicalRangeIndex;
import org.apache.druid.segment.column.IndexedUtf8ValueSetIndex;
import org.apache.druid.segment.column.LexicographicalRangeIndex;
import org.apache.druid.segment.column.NullValueIndex;
import org.apache.druid.segment.column.SimpleImmutableBitmapIndex;
import org.apache.druid.segment.column.SpatialIndex;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class StringUtf8ColumnIndexSupplier<TIndexed extends Indexed<ByteBuffer>> implements ColumnIndexSupplier
{
  private final BitmapFactory bitmapFactory;
  private final Supplier<TIndexed> utf8Dictionary;

  @Nullable
  private final GenericIndexed<ImmutableBitmap> bitmaps;

  @Nullable
  private final ImmutableRTree indexedTree;

  private final ColumnConfig columnConfig;
  private final int numRows;

  public StringUtf8ColumnIndexSupplier(
      BitmapFactory bitmapFactory,
      Supplier<TIndexed> utf8Dictionary,
      @Nullable GenericIndexed<ImmutableBitmap> bitmaps,
      @Nullable ImmutableRTree indexedTree
  )
  {
    this(bitmapFactory, utf8Dictionary, bitmaps, indexedTree, ColumnConfig.ALWAYS_USE_INDEXES, Integer.MAX_VALUE);
  }

  public StringUtf8ColumnIndexSupplier(
          BitmapFactory bitmapFactory,
          Supplier<TIndexed> utf8Dictionary,
          @Nullable GenericIndexed<ImmutableBitmap> bitmaps,
          @Nullable ImmutableRTree indexedTree,
          @Nullable ColumnConfig columnConfig,
          int numRows
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.bitmaps = bitmaps;
    this.utf8Dictionary = utf8Dictionary;
    this.indexedTree = indexedTree;
    this.columnConfig = columnConfig;
    this.numRows = numRows;
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T> T as(Class<T> clazz)
  {
    if (bitmaps != null) {
      Indexed<ByteBuffer> dict = utf8Dictionary.get();
      Indexed<ImmutableBitmap> singleThreadedBitmaps = bitmaps.singleThreaded();

      if (NullHandling.mustCombineNullAndEmptyInDictionary(dict)) {
        dict = CombineFirstTwoEntriesIndexed.returnNull(dict);
        singleThreadedBitmaps = CombineFirstTwoEntriesIndexed.unionBitmaps(bitmapFactory, singleThreadedBitmaps);
      } else if (NullHandling.mustReplaceFirstValueWithNullInDictionary(dict)) {
        dict = new ReplaceFirstValueWithNullIndexed<>(dict);
      }

      if (clazz.equals(NullValueIndex.class)) {
        final BitmapColumnIndex nullIndex;
        final ByteBuffer firstValue = dict.get(0);
        if (NullHandling.isNullOrEquivalent(firstValue)) {
          ImmutableBitmap bitmap = singleThreadedBitmaps.get(0);
          nullIndex = new SimpleImmutableBitmapIndex(bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap);
        } else {
          nullIndex = new SimpleImmutableBitmapIndex(bitmapFactory.makeEmptyImmutableBitmap());
        }
        return (T) (NullValueIndex) () -> nullIndex;
      } else if (clazz.equals(StringValueSetIndex.class)) {
        return (T) new IndexedUtf8ValueSetIndex<>(
            bitmapFactory,
            dict,
            singleThreadedBitmaps
        );
      } else if (clazz.equals(DruidPredicateIndex.class)) {
        return (T) new IndexedStringDruidPredicateIndex<>(
            bitmapFactory,
            new StringEncodingStrategies.Utf8ToStringIndexed(dict),
            singleThreadedBitmaps,
            columnConfig,
            numRows
        );
      } else if (clazz.equals(LexicographicalRangeIndex.class)) {
        return (T) new IndexedUtf8LexicographicalRangeIndex<>(
            bitmapFactory,
            dict,
            singleThreadedBitmaps,
            dict.get(0) == null,
            columnConfig,
            numRows
        );
      } else if (clazz.equals(DictionaryEncodedStringValueIndex.class)
                 || clazz.equals(DictionaryEncodedValueIndex.class)) {
        // Need string dictionary instead of UTF8 dictionary
        return (T) new IndexedStringDictionaryEncodedStringValueIndex<>(
            bitmapFactory,
            new StringEncodingStrategies.Utf8ToStringIndexed(dict),
            singleThreadedBitmaps
        );
      }
    }
    if (indexedTree != null && clazz.equals(SpatialIndex.class)) {
      return (T) (SpatialIndex) () -> indexedTree;
    }
    return null;
  }
}
