/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.index.column;

import com.metamx.druid.kv.Indexed;
import com.metamx.druid.kv.IndexedInts;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

/**
 */
public class StringMultiValueColumn extends AbstractColumn
{
  private static final ImmutableConciseSet emptySet = new ImmutableConciseSet();
  private static final ColumnCapabilitiesImpl CAPABILITIES = new ColumnCapabilitiesImpl()
      .setType(ValueType.STRING)
      .setDictionaryEncoded(true)
      .setHasBitmapIndexes(true)
      .setHasMultipleValues(true);

  private final Indexed<String> lookups;
  private final Indexed<? extends IndexedInts> column;
  private final Indexed<ImmutableConciseSet> bitmapIndexes;

  public StringMultiValueColumn(
      Indexed<String> lookups,
      Indexed<? extends IndexedInts> column,
      Indexed<ImmutableConciseSet> bitmapIndexes
  )
  {
    this.lookups = lookups;
    this.column = column;
    this.bitmapIndexes = bitmapIndexes;
  }

  @Override
  public ColumnCapabilities getCapabilities()
  {
    return CAPABILITIES;
  }

  @Override
  public int getLength()
  {
    return column.size();
  }

  @Override
  public DictionaryEncodedColumn getDictionaryEncoding()
  {
    return new DictionaryEncodedColumn()
    {
      @Override
      public int length()
      {
        return column.size();
      }

      @Override
      public boolean hasMultipleValues()
      {
        return true;
      }

      @Override
      public int getSingleValueRow(int rowNum)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public IndexedInts getMultiValueRow(int rowNum)
      {
        return column.get(rowNum);
      }

      @Override
      public String lookupName(int id)
      {
        return lookups.get(id);
      }

      @Override
      public int lookupId(String name)
      {
        return lookups.indexOf(name);
      }

      @Override
      public int getCardinality()
      {
        return lookups.size();
      }
    };
  }

  @Override
  public BitmapIndex getBitmapIndex()
  {
    throw new UnsupportedOperationException();
  }
}
