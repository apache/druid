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

package io.druid.segment.column;

import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.VSizeIndexed;
import io.druid.segment.data.VSizeIndexedInts;

import java.io.IOException;

/**
*/
public class SimpleDictionaryEncodedColumn implements DictionaryEncodedColumn
{
  private final VSizeIndexedInts column;
  private final VSizeIndexed multiValueColumn;
  private final GenericIndexed<String> lookups;

  public SimpleDictionaryEncodedColumn(
      VSizeIndexedInts singleValueColumn,
      VSizeIndexed multiValueColumn,
      GenericIndexed<String> lookups
  )
  {
    this.column = singleValueColumn;
    this.multiValueColumn = multiValueColumn;
    this.lookups = lookups;
  }

  @Override
  public int length()
  {
    return hasMultipleValues() ? multiValueColumn.size() : column.size();
  }

  @Override
  public boolean hasMultipleValues()
  {
    return column == null;
  }

  @Override
  public int getSingleValueRow(int rowNum)
  {
    return column.get(rowNum);
  }

  @Override
  public IndexedInts getMultiValueRow(int rowNum)
  {
    return multiValueColumn.get(rowNum);
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

  @Override
  public void close() throws IOException
  {
    lookups.close();
  }
}
