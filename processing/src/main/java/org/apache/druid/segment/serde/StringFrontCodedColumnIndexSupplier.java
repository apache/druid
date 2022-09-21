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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
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
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class StringFrontCodedColumnIndexSupplier implements ColumnIndexSupplier
{
  private final BitmapFactory bitmapFactory;
  private final FrontCodedStringIndexed dictionary;
  private final FrontCodedIndexed utf8Dictionary;

  @Nullable
  private final GenericIndexed<ImmutableBitmap> bitmaps;

  @Nullable
  private final ImmutableRTree indexedTree;

  public StringFrontCodedColumnIndexSupplier(
      BitmapFactory bitmapFactory,
      FrontCodedIndexed utf8Dictionary,
      @Nullable GenericIndexed<ImmutableBitmap> bitmaps,
      @Nullable ImmutableRTree indexedTree
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.bitmaps = bitmaps;
    this.dictionary = new FrontCodedStringIndexed(utf8Dictionary);
    this.utf8Dictionary = utf8Dictionary;
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
        if (NullHandling.isNullOrEquivalent(dictionary.get(0))) {
          nullIndex = new SimpleImmutableBitmapIndex(bitmaps.get(0));
        } else {
          nullIndex = new SimpleImmutableBitmapIndex(bitmapFactory.makeEmptyImmutableBitmap());
        }
        return (T) (NullValueIndex) () -> nullIndex;
      } else if (clazz.equals(StringValueSetIndex.class)) {
        return (T) new IndexedUtf8ValueSetIndex<>(
            bitmapFactory,
            utf8Dictionary,
            singleThreadedBitmaps
        );
      } else if (clazz.equals(DruidPredicateIndex.class)) {
        return (T) new IndexedStringDruidPredicateIndex<>(
            bitmapFactory,
            dictionary,
            singleThreadedBitmaps
        );
      } else if (clazz.equals(LexicographicalRangeIndex.class)) {
        return (T) new IndexedUtf8LexicographicalRangeIndex<>(
            bitmapFactory,
            utf8Dictionary,
            singleThreadedBitmaps,
            NullHandling.isNullOrEquivalent(dictionary.get(0))
        );
      } else if (clazz.equals(DictionaryEncodedStringValueIndex.class)
                 || clazz.equals(DictionaryEncodedValueIndex.class)) {
        return (T) new IndexedStringDictionaryEncodedStringValueIndex<>(
            bitmapFactory,
            dictionary,
            bitmaps
        );
      }
    }
    if (indexedTree != null && clazz.equals(SpatialIndex.class)) {
      return (T) (SpatialIndex) () -> indexedTree;
    }
    return null;
  }

  private static final class FrontCodedStringIndexed implements Indexed<String>
  {
    private final FrontCodedIndexed delegate;

    FrontCodedStringIndexed(FrontCodedIndexed delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public int size()
    {
      return delegate.size();
    }

    @Nullable
    @Override
    public String get(int index)
    {
      final ByteBuffer buffer = delegate.get(index);
      if (buffer == null) {
        return null;
      }
      return StringUtils.fromUtf8(buffer);
    }

    @Override
    public int indexOf(@Nullable String value)
    {
      return delegate.indexOf(StringUtils.toUtf8ByteBuffer(value));
    }

    @Override
    public Iterator<String> iterator()
    {
      final Iterator<ByteBuffer> delegateIterator = delegate.iterator();
      return new Iterator<String>()
      {
        @Override
        public boolean hasNext()
        {
          return delegateIterator.hasNext();
        }

        @Override
        public String next()
        {
          final ByteBuffer buffer = delegateIterator.next();
          if (buffer == null) {
            return null;
          }
          return StringUtils.fromUtf8(buffer);
        }
      };
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("delegateIndex", delegate);
    }
  }
}
