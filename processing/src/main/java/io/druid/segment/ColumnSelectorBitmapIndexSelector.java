/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.google.common.base.Strings;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.common.guava.CloseQuietly;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.segment.column.BitmapIndex;
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
  public BitmapIndex getBitmapIndex(String dimension)
  {
    final Column column = index.getColumn(dimension);
    if (column != null && column.getCapabilities().hasBitmapIndexes()) {
      return column.getBitmapIndex();
    } else {
      return null;
    }
  }

  @Override
  public ImmutableBitmap getBitmapIndex(String dimension, String value)
  {
    final Column column = index.getColumn(dimension);
    if (column == null) {
      if (Strings.isNullOrEmpty(value)) {
        return bitmapFactory.complement(bitmapFactory.makeEmptyImmutableBitmap(), getNumRows());
      } else {
        return bitmapFactory.makeEmptyImmutableBitmap();
      }
    }

    if (!column.getCapabilities().hasBitmapIndexes()) {
      return bitmapFactory.makeEmptyImmutableBitmap();
    }

    final BitmapIndex bitmapIndex = column.getBitmapIndex();
    return bitmapIndex.getBitmap(bitmapIndex.getIndex(value));
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
