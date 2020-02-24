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

package org.apache.druid.segment.join.table;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.util.function.IntSupplier;

public class IndexedTableColumnValueSelector implements ColumnValueSelector<Object>
{
  private final IntSupplier currentRow;
  private final IndexedTable.Reader columnReader;
  private final RowHolder currentRowHolder;

  IndexedTableColumnValueSelector(IndexedTable table, IntSupplier currentRow, int columnNumber)
  {
    this.currentRow = currentRow;
    this.columnReader = table.columnReader(columnNumber);
    currentRowHolder = new RowHolder(columnReader);
  }

  @Override
  public double getDouble()
  {
    final int rowNum = currentRow.getAsInt();

    Object value = currentRowHolder.computeForRow(rowNum);
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    // Otherwise this shouldn't have been called (due to isNull returning true).
    assert NullHandling.replaceWithDefault();
    return NullHandling.defaultDoubleValue();
  }

  @Override
  public float getFloat()
  {
    final int rowNum = currentRow.getAsInt();

    Object value = currentRowHolder.computeForRow(rowNum);
    if (value instanceof Number) {
      return ((Number) value).floatValue();
    }

    // Otherwise this shouldn't have been called (due to isNull returning true).
    assert NullHandling.replaceWithDefault();
    return NullHandling.defaultFloatValue();
  }

  @Override
  public long getLong()
  {
    final int rowNum = currentRow.getAsInt();

    Object value = currentRowHolder.computeForRow(rowNum);
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }

    // Otherwise this shouldn't have been called (due to isNull returning true).
    assert NullHandling.replaceWithDefault();
    return NullHandling.defaultLongValue();
  }

  @Override
  public boolean isNull()
  {
    final int rowNum = currentRow.getAsInt();

    if (rowNum == -1) {
      return true;
    }

    Object value = currentRowHolder.computeForRow(rowNum);
    return !(value instanceof Number);
  }

  @Nullable
  @Override
  public Object getObject()
  {
    final int rowNum = currentRow.getAsInt();

    return currentRowHolder.computeForRow(rowNum);
  }

  @Override
  public Class<?> classOfObject()
  {
    return Object.class;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("columnReader", columnReader);
    inspector.visit("currentRow", currentRow);
  }

  private static final class RowHolder
  {
    private static final int NOT_INITIALIZED = -2;

    private final IndexedTable.Reader columnReader;
    private int thisRow;
    // TODO: store primitive values instead of the raw object for perf!
    private Object value;

    private RowHolder(IndexedTable.Reader columnReader)
    {
      this.columnReader = columnReader;
      thisRow = NOT_INITIALIZED;
      value = null;
    }

    private Object computeForRow(int row)
    {
      if (thisRow == row) {
        return value;
      }
      thisRow = row;
      if (row != -1) {
        value = columnReader.read(row);
      } else {
        value = null;
      }
      return value;
    }
  }
}
