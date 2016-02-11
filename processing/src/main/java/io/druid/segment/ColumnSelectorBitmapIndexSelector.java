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

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.common.ISE;
import com.metamx.common.guava.CloseQuietly;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.segment.column.Column;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedIterable;

import javax.annotation.Nullable;
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
      if (Strings.isNullOrEmpty(value)) {
        return bitmapFactory.complement(bitmapFactory.makeEmptyImmutableBitmap(), getNumRows());
      } else {
        return bitmapFactory.makeEmptyImmutableBitmap();
      }
    }

    if (!column.getCapabilities().hasBitmapIndexes()) {
      return bitmapFactory.makeEmptyImmutableBitmap();
    }

    return column.getBitmapIndex().getBitmap(value);
  }

  private Predicate getSingleValueMatchPredicate(String value, ValueType type)
  {
    switch (type) {
      case LONG:
        final long parsedLong = Long.parseLong(value);
        return new Predicate<Long>() {
          @Override
          public boolean apply(@Nullable Long input) {
            return parsedLong == input;
          }
        };
      case FLOAT:
        final float parsedFloat = Float.parseFloat(value);
        return new Predicate<Long>() {
          @Override
          public boolean apply(@Nullable Long input) {
            return parsedFloat == input;
          }
        };
      default:
        throw new UnsupportedOperationException("Full scan predicate on string column is not supported.");
    }
  }

  @Override
  public ImmutableBitmap getBitmapIndexFromColumnScan(String dimension, String value)
  {
    final Column column = index.getColumn(dimension);
    if (column == null) {
      if (value == null || ((value instanceof String) && ((String) value).length() == 0)) {
        return bitmapFactory.complement(bitmapFactory.makeEmptyImmutableBitmap(), getNumRows());
      } else {
        return bitmapFactory.makeEmptyImmutableBitmap();
      }
    }
    final ValueType type = column.getCapabilities().getType();
    final GenericColumn genericColumn = column.getGenericColumn();
    final MutableBitmap bitmap = bitmapFactory.makeEmptyMutableBitmap();

    switch (type) {
      case LONG:
        final long parsedLong = Long.parseLong(value);
        for (int rowIdx = 0; rowIdx < column.getLength(); rowIdx++) {
          long rowVal = genericColumn.getLongSingleValueRow(rowIdx);
          if (rowVal == parsedLong) {
            bitmap.add(rowIdx);
          }
        }
        break;
      case FLOAT:
        final float parsedFloat = Float.parseFloat(value);
        for (int rowIdx = 0; rowIdx < column.getLength(); rowIdx++) {
          float rowVal = genericColumn.getFloatSingleValueRow(rowIdx);
          if (rowVal == parsedFloat) {
            bitmap.add(rowIdx);
          }
        }
        break;
      default:
        throw new UnsupportedOperationException("Full scan on string column is not supported.");
    }
    return bitmapFactory.makeImmutableBitmap(bitmap);
  }

  @Override
  public ImmutableBitmap getBitmapIndexFromColumnScan(String dimension, Predicate predicate)
  {
    final Column column = index.getColumn(dimension);
    if (column == null) {
      if (predicate.apply(null) || predicate.apply("")) {
        return bitmapFactory.complement(bitmapFactory.makeEmptyImmutableBitmap(), getNumRows());
      } else {
        return bitmapFactory.makeEmptyImmutableBitmap();
      }
    }
    final ValueType type = column.getCapabilities().getType();
    final GenericColumn genericColumn = column.getGenericColumn();
    final MutableBitmap bitmap = bitmapFactory.makeEmptyMutableBitmap();

    switch (type) {
      case LONG:
        Long longVal;
        for (int rowIdx = 0; rowIdx < column.getLength(); rowIdx++) {
          longVal = genericColumn.getLongSingleValueRow(rowIdx);
          if (predicate.apply(longVal)) {
            bitmap.add(rowIdx);
          }
        }
        break;
      case FLOAT:
        Float floatVal;
        for (int rowIdx = 0; rowIdx < column.getLength(); rowIdx++) {
          floatVal = genericColumn.getFloatSingleValueRow(rowIdx);
          if (predicate.apply(floatVal)) {
            bitmap.add(rowIdx);
          }
        }
        break;
      default:
        throw new UnsupportedOperationException("Full scan on string column is not supported.");
    }

    return bitmapFactory.makeImmutableBitmap(bitmap);

  }

  @Override
  public ValueType getDimensionType(String dimension)
  {
    final Column column = index.getColumn(dimension);
    if (column == null) {
      return null;
    } else {
      return column.getCapabilities().getType();
    }
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

  @Override
  public boolean hasBitmapIndexes(String dimension) {
    final Column column = index.getColumn(dimension);
    if (column == null) {
      // default to "has bitmap index" path for null columns
      return true;
    }
    return column.getCapabilities().hasBitmapIndexes();
  }

}
