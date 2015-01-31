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

package io.druid.segment;

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.common.guava.CloseQuietly;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.segment.column.Column;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedIterable;

import java.util.Iterator;

/**
 */
public class ColumnSelectorBitmapIndexSelector implements BitmapIndexSelector
{
  private final BitmapFactory bitmapFactory;
  private final ColumnSelector index;

  public ColumnSelectorBitmapIndexSelector(
      final BitmapFactory bitmapFactory,
      final ColumnSelector index
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.index = index;
  }

  @Override
  public Indexed<String> getDimensionValues(String dimension)
  {
    final Column columnDesc = index.getColumn(dimension);
    if (columnDesc == null || !columnDesc.getCapabilities().isDictionaryEncoded()) {
      return null;
    }
    final DictionaryEncodedColumn column = columnDesc.getDictionaryEncoding();
    return new Indexed<String>()
    {
      @Override
      public Class<? extends String> getClazz()
      {
        return String.class;
      }

      @Override
      public int size()
      {
        return column.getCardinality();
      }

      @Override
      public String get(int index)
      {
        return column.lookupName(index);
      }

      @Override
      public int indexOf(String value)
      {
        return column.lookupId(value);
      }

      @Override
      public Iterator<String> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }
    };
  }

  @Override
  public int getNumRows()
  {
    GenericColumn column = null;
    try {
      column = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn();
      return column.length();
    }
    finally {
      CloseQuietly.close(column);
    }
  }

  @Override
  public BitmapFactory getBitmapFactory()
  {
    return bitmapFactory;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(String dimension, String value)
  {
    final Column column = index.getColumn(dimension);
    if (column == null) {
      return bitmapFactory.makeEmptyImmutableBitmap();
    }
    if (!column.getCapabilities().hasBitmapIndexes()) {
      bitmapFactory.makeEmptyImmutableBitmap();
    }

    return column.getBitmapIndex().getBitmap(value);
  }

  @Override
  public ImmutableBitmap getBitmapIndex(String dimension, int idx)
  {
    final Column column = index.getColumn(dimension);
    if (column == null || column.getCapabilities() == null) {
      bitmapFactory.makeEmptyImmutableBitmap();
    }
    if (!column.getCapabilities().hasBitmapIndexes()) {
      bitmapFactory.makeEmptyImmutableBitmap();
    }

    // This is a workaround given the current state of indexing, I feel shame
    final int index1 = column.getBitmapIndex().hasNulls() ? idx + 1 : idx;

    return column.getBitmapIndex().getBitmap(index1);
  }

  @Override
  public ImmutableRTree getSpatialIndex(String dimension)
  {
    final Column column = index.getColumn(dimension);
    if (column == null || !column.getCapabilities().hasSpatialIndexes()) {
      return new ImmutableRTree();
    }

    return column.getSpatialIndex().getRTree();
  }
}
