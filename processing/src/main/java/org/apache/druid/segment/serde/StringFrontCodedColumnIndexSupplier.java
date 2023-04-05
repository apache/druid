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
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;

public class StringFrontCodedColumnIndexSupplier implements ColumnIndexSupplier
{
  private final BitmapFactory bitmapFactory;
  private final Supplier<StringEncodingStrategies.Utf8ToStringIndexed> dictionary;
  private final Supplier<FrontCodedIndexed> utf8Dictionary;

  @Nullable
  private final GenericIndexed<ImmutableBitmap> bitmaps;

  @Nullable
  private final ImmutableRTree indexedTree;

  public StringFrontCodedColumnIndexSupplier(
      BitmapFactory bitmapFactory,
      Supplier<FrontCodedIndexed> utf8Dictionary,
      @Nullable GenericIndexed<ImmutableBitmap> bitmaps,
      @Nullable ImmutableRTree indexedTree
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.bitmaps = bitmaps;
    this.utf8Dictionary = utf8Dictionary;
    this.dictionary = () -> new StringEncodingStrategies.Utf8ToStringIndexed(this.utf8Dictionary.get());
    this.indexedTree = indexedTree;
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (bitmaps != null) {
      final Indexed<ImmutableBitmap> singleThreadedBitmaps = bitmaps.singleThreaded();
      if (clazz.equals(NullValueIndex.class)) {
        final BitmapColumnIndex nullIndex;
        final StringEncodingStrategies.Utf8ToStringIndexed stringDictionary = dictionary.get();
        if (NullHandling.isNullOrEquivalent(stringDictionary.get(0))) {
          nullIndex = new SimpleImmutableBitmapIndex(bitmaps.get(0));
        } else {
          nullIndex = new SimpleImmutableBitmapIndex(bitmapFactory.makeEmptyImmutableBitmap());
        }
        return (T) (NullValueIndex) () -> nullIndex;
      } else if (clazz.equals(StringValueSetIndex.class)) {
        return (T) new IndexedUtf8ValueSetIndex<>(
            bitmapFactory,
            utf8Dictionary.get(),
            singleThreadedBitmaps
        );
      } else if (clazz.equals(DruidPredicateIndex.class)) {
        return (T) new IndexedStringDruidPredicateIndex<>(
            bitmapFactory,
            dictionary.get(),
            singleThreadedBitmaps
        );
      } else if (clazz.equals(LexicographicalRangeIndex.class)) {
        final FrontCodedIndexed dict = utf8Dictionary.get();
        return (T) new IndexedUtf8LexicographicalRangeIndex<>(
            bitmapFactory,
            dict,
            singleThreadedBitmaps,
            dict.get(0) == null
        );
      } else if (clazz.equals(DictionaryEncodedStringValueIndex.class)
                 || clazz.equals(DictionaryEncodedValueIndex.class)) {
        return (T) new IndexedStringDictionaryEncodedStringValueIndex<>(
            bitmapFactory,
            dictionary.get(),
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
