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

package org.apache.druid.query.rowsandcols;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.semantic.AppendableRowsAndColumns;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;

public class AppendableMapOfColumns implements AppendableRowsAndColumns
{
  private final RowsAndColumns base;
  private final LinkedHashMap<String, Column> appendedColumns;
  private Set<String> colNames = null;

  public AppendableMapOfColumns(
      RowsAndColumns base
  )
  {
    this.base = base;
    this.appendedColumns = new LinkedHashMap<>();
  }

  @Override
  public void addColumn(String name, Column column)
  {
    final Column prevValue = appendedColumns.put(name, column);
    if (prevValue != null) {
      throw new ISE("Tried to override column[%s]!?  Was[%s], now[%s]", name, prevValue, column);
    }
    if (colNames != null) {
      colNames.add(name);
    }
  }

  @Override
  public Collection<String> getColumnNames()
  {
    if (colNames == null) {
      this.colNames = new LinkedHashSet<>(base.getColumnNames());
      this.colNames.addAll(appendedColumns.keySet());
    }
    return colNames;
  }

  @Override
  public int numRows()
  {
    return base.numRows();
  }

  @Override
  public Column findColumn(String name)
  {
    Column retVal = base.findColumn(name);
    if (retVal == null) {
      retVal = appendedColumns.get(name);
    }
    return retVal;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T as(Class<T> clazz)
  {
    if (AppendableRowsAndColumns.class.equals(clazz)) {
      return (T) this;
    }
    return null;
  }
}
