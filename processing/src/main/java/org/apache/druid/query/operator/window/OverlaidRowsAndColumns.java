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

package org.apache.druid.query.operator.window;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.AppendableRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;

import javax.annotation.Nullable;
import java.util.Map;

public class OverlaidRowsAndColumns implements AppendableRowsAndColumns
{
  private final RowsAndColumns base;
  private final Map<String, Column> overlay;

  public OverlaidRowsAndColumns(
      RowsAndColumns base,
      Map<String, Column> overlay
  )
  {
    this.base = base;
    this.overlay = overlay;
  }

  @Override
  public int numRows()
  {
    return base.numRows();
  }

  @Override
  public Column findColumn(String name)
  {
    final Column overlayCol = overlay.get(name);
    if (overlayCol == null) {
      return base.findColumn(name);
    }
    return overlayCol;
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    return null;
  }

  @Override
  public void addColumn(String name, Column column)
  {
    final Column prevValue = overlay.put(name, column);
    if (prevValue != null) {
      throw new ISE("Tried to override column[%s]!?  Was[%s], now[%s]", name, prevValue, column);
    }
  }
}
