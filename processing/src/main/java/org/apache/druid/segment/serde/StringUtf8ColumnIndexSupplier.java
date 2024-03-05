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
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.IndexedStringDictionaryEncodedStringValueIndex;
import org.apache.druid.segment.index.IndexedStringDruidPredicateIndexes;
import org.apache.druid.segment.index.IndexedUtf8LexicographicalRangeIndexes;
import org.apache.druid.segment.index.IndexedUtf8ValueIndexes;
import org.apache.druid.segment.index.SimpleImmutableBitmapIndex;
import org.apache.druid.segment.index.semantic.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.index.semantic.DictionaryEncodedValueIndex;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.index.semantic.LexicographicalRangeIndexes;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.index.semantic.SpatialIndex;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
import org.apache.druid.segment.index.semantic.Utf8ValueSetIndexes;
import org.apache.druid.segment.index.semantic.ValueIndexes;
import org.apache.druid.segment.index.semantic.ValueSetIndexes;

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

  public StringUtf8ColumnIndexSupplier(
          BitmapFactory bitmapFactory,
          Supplier<TIndexed> utf8Dictionary,
          @Nullable GenericIndexed<ImmutableBitmap> bitmaps,
          @Nullable ImmutableRTree indexedTree
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.bitmaps = bitmaps;
    this.utf8Dictionary = utf8Dictionary;
    this.indexedTree = indexedTree;
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
      } else if (
          clazz.equals(StringValueSetIndexes.class) ||
          clazz.equals(Utf8ValueSetIndexes.class) ||
          clazz.equals(ValueIndexes.class) ||
          clazz.equals(ValueSetIndexes.class)
      ) {
        return (T) new IndexedUtf8ValueIndexes<>(
            bitmapFactory,
            dict,
            singleThreadedBitmaps
        );
      } else if (clazz.equals(DruidPredicateIndexes.class)) {
        return (T) new IndexedStringDruidPredicateIndexes<>(
            bitmapFactory,
            new StringEncodingStrategies.Utf8ToStringIndexed(dict),
            singleThreadedBitmaps
        );
      } else if (clazz.equals(LexicographicalRangeIndexes.class)) {
        return (T) new IndexedUtf8LexicographicalRangeIndexes<>(
            bitmapFactory,
            dict,
            singleThreadedBitmaps,
            dict.get(0) == null
        );
      } else if (
          clazz.equals(DictionaryEncodedStringValueIndex.class) ||
          clazz.equals(DictionaryEncodedValueIndex.class)
      ) {
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
