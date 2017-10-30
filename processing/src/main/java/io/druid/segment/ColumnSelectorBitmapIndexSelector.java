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
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.spatial.ImmutableRTree;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.filter.Filters;

import java.util.Iterator;

/**
 */
public class ColumnSelectorBitmapIndexSelector implements BitmapIndexSelector
{
  private final BitmapFactory bitmapFactory;
  private final VirtualColumns virtualColumns;
  private final ColumnSelector index;

  public ColumnSelectorBitmapIndexSelector(
      final BitmapFactory bitmapFactory,
      final VirtualColumns virtualColumns,
      final ColumnSelector index
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.virtualColumns = virtualColumns;
    this.index = index;
  }

  @Override
  public Indexed<String> getDimensionValues(String dimension)
  {
    if (isVirtualColumn(dimension)) {
      // Virtual columns don't have dictionaries or indexes.
      return null;
    }

    final Column columnDesc = index.getColumn(dimension);
    if (columnDesc == null || !columnDesc.getCapabilities().isDictionaryEncoded()) {
      return null;
    }
    final DictionaryEncodedColumn<String> column = columnDesc.getDictionaryEncoding();
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

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("column", column);
      }
    };
  }

  @Override
  public boolean hasMultipleValues(final String dimension)
  {
    if (isVirtualColumn(dimension)) {
      return virtualColumns.getVirtualColumn(dimension).capabilities(dimension).hasMultipleValues();
    }

    final Column column = index.getColumn(dimension);
    return column != null && column.getCapabilities().hasMultipleValues();
  }

  @Override
  public int getNumRows()
  {
    try (final GenericColumn column = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn()) {
      return column.length();
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
    if (isVirtualColumn(dimension)) {
      // Virtual columns don't have dictionaries or indexes.
      return null;
    }

    final Column column = index.getColumn(dimension);
    if (column == null || !columnSupportsFiltering(column)) {
      // for missing columns and columns with types that do not support filtering,
      // treat the column as if it were a String column full of nulls.
      // Create a BitmapIndex so that filters applied to null columns can use
      // bitmap indexes. Filters check for the presence of a bitmap index, this is used to determine
      // whether the filter is applied in the pre or post filtering stage.
      return new BitmapIndex()
      {
        @Override
        public int getCardinality()
        {
          return 1;
        }

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

        @Override
        public int getIndex(String value)
        {
          // Return -2 for non-null values to match what the BitmapIndex implementation in BitmapIndexColumnPartSupplier
          // would return for getIndex() when there is only a single index, for the null value.
          // i.e., return an 'insertion point' of 1 for non-null values (see BitmapIndex interface)
          return Strings.isNullOrEmpty(value) ? 0 : -2;
        }

        @Override
        public ImmutableBitmap getBitmap(int idx)
        {
          if (idx == 0) {
            return bitmapFactory.complement(bitmapFactory.makeEmptyImmutableBitmap(), getNumRows());
          } else {
            return bitmapFactory.makeEmptyImmutableBitmap();
          }
        }
      };
    } else if (column.getCapabilities().hasBitmapIndexes()) {
      return column.getBitmapIndex();
    } else {
      return null;
    }
  }

  @Override
  public ImmutableBitmap getBitmapIndex(String dimension, String value)
  {
    if (isVirtualColumn(dimension)) {
      // Virtual columns don't have dictionaries or indexes.
      return null;
    }

    final Column column = index.getColumn(dimension);
    if (column == null || !columnSupportsFiltering(column)) {
      if (Strings.isNullOrEmpty(value)) {
        return bitmapFactory.complement(bitmapFactory.makeEmptyImmutableBitmap(), getNumRows());
      } else {
        return bitmapFactory.makeEmptyImmutableBitmap();
      }
    }

    if (!column.getCapabilities().hasBitmapIndexes()) {
      return null;
    }

    final BitmapIndex bitmapIndex = column.getBitmapIndex();
    return bitmapIndex.getBitmap(bitmapIndex.getIndex(value));
  }

  @Override
  public ImmutableRTree getSpatialIndex(String dimension)
  {
    if (isVirtualColumn(dimension)) {
      return new ImmutableRTree();
    }

    final Column column = index.getColumn(dimension);
    if (column == null || !column.getCapabilities().hasSpatialIndexes()) {
      return new ImmutableRTree();
    }

    return column.getSpatialIndex().getRTree();
  }

  private boolean isVirtualColumn(final String columnName)
  {
    return virtualColumns.getVirtualColumn(columnName) != null;
  }

  private static boolean columnSupportsFiltering(Column column)
  {
    ValueType columnType = column.getCapabilities().getType();
    return Filters.FILTERABLE_TYPES.contains(columnType);
  }
}
