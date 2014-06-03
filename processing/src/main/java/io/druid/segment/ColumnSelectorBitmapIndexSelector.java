/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment;

import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.common.guava.CloseQuietly;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.segment.column.Column;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedIterable;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import java.util.Iterator;

/**
*/
public class ColumnSelectorBitmapIndexSelector implements BitmapIndexSelector
{
  private final ColumnSelector index;

  public ColumnSelectorBitmapIndexSelector(
      final ColumnSelector index
  )
  {
    this.index = index;
  }

  @Override
  public Indexed<String> getDimensionValues(String dimension)
  {
    final Column columnDesc = index.getColumn(dimension.toLowerCase());
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
      column = index.getTimeColumn().getGenericColumn();
      return column.length();
    }
    finally {
      CloseQuietly.close(column);
    }
  }

  @Override
  public ImmutableConciseSet getConciseInvertedIndex(String dimension, String value)
  {
    final Column column = index.getColumn(dimension.toLowerCase());
    if (column == null) {
      return new ImmutableConciseSet();
    }
    if (!column.getCapabilities().hasBitmapIndexes()) {
      return new ImmutableConciseSet();
    }

    return column.getBitmapIndex().getConciseSet(value);
  }

  @Override
  public ImmutableConciseSet getConciseInvertedIndex(String dimension, int idx)
  {
    final Column column = index.getColumn(dimension.toLowerCase());
    if (column == null) {
      return new ImmutableConciseSet();
    }
    if (!column.getCapabilities().hasBitmapIndexes()) {
      return new ImmutableConciseSet();
    }
    // This is a workaround given the current state of indexing, I feel shame
    final int index1 = column.getBitmapIndex().hasNulls() ? idx + 1 : idx;

    return column.getBitmapIndex().getConciseSet(index1);
  }

  @Override
  public ImmutableRTree getSpatialIndex(String dimension)
  {
    final Column column = index.getColumn(dimension.toLowerCase());
    if (column == null || !column.getCapabilities().hasSpatialIndexes()) {
      return new ImmutableRTree();
    }

    return column.getSpatialIndex().getRTree();
  }
}
