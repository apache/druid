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

package com.metamx.druid.index.serde;

import com.google.common.base.Supplier;
import com.metamx.druid.index.column.DictionaryEncodedColumn;
import com.metamx.druid.index.column.SimpleDictionaryEncodedColumn;
import com.metamx.druid.kv.GenericIndexed;
import com.metamx.druid.kv.VSizeIndexed;
import com.metamx.druid.kv.VSizeIndexedInts;

/**
*/
public class DictionaryEncodedColumnSupplier implements Supplier<DictionaryEncodedColumn>
{
  private final GenericIndexed<String> dictionary;
  private final VSizeIndexedInts singleValuedColumn;
  private final VSizeIndexed multiValuedColumn;

  public DictionaryEncodedColumnSupplier(
      GenericIndexed<String> dictionary,
      VSizeIndexedInts singleValuedColumn,
      VSizeIndexed multiValuedColumn
  )
  {
    this.dictionary = dictionary;
    this.singleValuedColumn = singleValuedColumn;
    this.multiValuedColumn = multiValuedColumn;
  }

  @Override
  public DictionaryEncodedColumn get()
  {
    return new SimpleDictionaryEncodedColumn(singleValuedColumn, multiValuedColumn, dictionary);
  }
}
