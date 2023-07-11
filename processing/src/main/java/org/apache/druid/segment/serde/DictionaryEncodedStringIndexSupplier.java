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

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.spatial.ImmutableRTree;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.column.BitmapColumnIndex;
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
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.segment.column.Utf8ValueSetIndex;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class DictionaryEncodedStringIndexSupplier implements ColumnIndexSupplier
{
  private final BitmapFactory bitmapFactory;
  private final GenericIndexed<String> dictionary;
  private final GenericIndexed<ByteBuffer> dictionaryUtf8;
  @Nullable
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  @Nullable
  private final ImmutableRTree indexedTree;

  public DictionaryEncodedStringIndexSupplier(
      BitmapFactory bitmapFactory,
      GenericIndexed<String> dictionary,
      GenericIndexed<ByteBuffer> dictionaryUtf8,
      @Nullable GenericIndexed<ImmutableBitmap> bitmaps,
      @Nullable ImmutableRTree indexedTree
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.dictionary = dictionary;
    this.dictionaryUtf8 = dictionaryUtf8;
    this.bitmaps = bitmaps;
    this.indexedTree = indexedTree;
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T> T as(Class<T> clazz)
  {
    if (bitmaps != null) {
      final Indexed<String> singleThreadedStrings = dictionary.singleThreaded();
      final Indexed<ByteBuffer> singleThreadedUtf8 = dictionaryUtf8.singleThreaded();
      final Indexed<ImmutableBitmap> singleThreadedBitmaps = bitmaps.singleThreaded();
      if (clazz.equals(NullValueIndex.class)) {
        final BitmapColumnIndex nullIndex;
        if (NullHandling.isNullOrEquivalent(dictionary.get(0))) {
          nullIndex = new SimpleImmutableBitmapIndex(bitmaps.get(0));
        } else {
          nullIndex = new SimpleImmutableBitmapIndex(bitmapFactory.makeEmptyImmutableBitmap());
        }
        return (T) (NullValueIndex) () -> nullIndex;
      } else if (clazz.equals(StringValueSetIndex.class)) {
        return (T) new IndexedUtf8ValueSetIndex<>(bitmapFactory, singleThreadedUtf8, singleThreadedBitmaps);
      } else if (clazz.equals(Utf8ValueSetIndex.class)) {
        return (T) new IndexedUtf8ValueSetIndex<>(bitmapFactory, singleThreadedUtf8, singleThreadedBitmaps);
      } else if (clazz.equals(DruidPredicateIndex.class)) {
        return (T) new IndexedStringDruidPredicateIndex<>(bitmapFactory, singleThreadedStrings, singleThreadedBitmaps);
      } else if (clazz.equals(LexicographicalRangeIndex.class)) {
        return (T) new IndexedUtf8LexicographicalRangeIndex<>(
            bitmapFactory,
            singleThreadedUtf8,
            singleThreadedBitmaps,
            NullHandling.isNullOrEquivalent(dictionary.get(0))
        );
      } else if (clazz.equals(DictionaryEncodedStringValueIndex.class) || clazz.equals(DictionaryEncodedValueIndex.class)) {
        return (T) new IndexedStringDictionaryEncodedStringValueIndex<>(
            bitmapFactory,
            singleThreadedStrings,
            bitmaps
        );
      }
    }
    if (indexedTree != null && clazz.equals(SpatialIndex.class)) {
      return (T) (SpatialIndex) () -> indexedTree;
    }
    return null;
  }
}
