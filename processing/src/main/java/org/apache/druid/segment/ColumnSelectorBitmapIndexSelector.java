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

package org.apache.druid.segment;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.spatial.ImmutableRTree;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.Column;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.GenericColumn;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedIterable;

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
    if (column == null || !column.getCapabilities().isFilterable()) {
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

        /**
         * Return -2 for non-null values to match what the {@link BitmapIndex} implementation in
         * {@link org.apache.druid.segment.serde.BitmapIndexColumnPartSupplier}
         * would return for {@link BitmapIndex#getIndex(String)} when there is only a single index, for the null value.
         * i.e., return an 'insertion point' of 1 for non-null values (see {@link BitmapIndex} interface)
         */
        @Override
        public int getIndex(String value)
        {
          return NullHandling.isNullOrEquivalent(value) ? 0 : -2;
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
    if (column == null || !column.getCapabilities().isFilterable()) {
      if (NullHandling.isNullOrEquivalent(value)) {
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
      return ImmutableRTree.empty();
    }

    final Column column = index.getColumn(dimension);
    if (column == null || !column.getCapabilities().hasSpatialIndexes()) {
      return ImmutableRTree.empty();
    }

    return column.getSpatialIndex().getRTree();
  }

  private boolean isVirtualColumn(final String columnName)
  {
    return virtualColumns.getVirtualColumn(columnName) != null;
  }
}
