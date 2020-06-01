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

  IndexedTableColumnValueSelector(IndexedTable table, IntSupplier currentRow, int columnNumber)
  {
    this.currentRow = currentRow;
    this.columnReader = table.columnReader(columnNumber);
  }

  @Override
  public double getDouble()
  {
    final int rowNum = currentRow.getAsInt();

    if (rowNum != -1) {
      final Object value = columnReader.read(currentRow.getAsInt());

      if (value instanceof Number) {
        return ((Number) value).doubleValue();
      }
    }

    // Otherwise this shouldn't have been called (due to isNull returning true).
    assert NullHandling.replaceWithDefault();
    //noinspection ConstantConditions assert statement above guarantees this is non null.
    return NullHandling.defaultDoubleValue();
  }

  @Override
  public float getFloat()
  {
    final int rowNum = currentRow.getAsInt();

    if (rowNum != -1) {
      final Object value = columnReader.read(currentRow.getAsInt());

      if (value instanceof Number) {
        return ((Number) value).floatValue();
      }
    }

    // Otherwise this shouldn't have been called (due to isNull returning true).
    assert NullHandling.replaceWithDefault();
    //noinspection ConstantConditions assert statement above guarantees this is non null.
    return NullHandling.defaultFloatValue();
  }

  @Override
  public long getLong()
  {
    final int rowNum = currentRow.getAsInt();

    if (rowNum != -1) {
      final Object value = columnReader.read(currentRow.getAsInt());

      if (value instanceof Number) {
        return ((Number) value).longValue();
      }
    }

    // Otherwise this shouldn't have been called (due to isNull returning true).
    assert NullHandling.replaceWithDefault();
    //noinspection ConstantConditions assert statement above guarantees this is non null.
    return NullHandling.defaultLongValue();
  }

  @Override
  public boolean isNull()
  {
    final int rowNum = currentRow.getAsInt();

    if (rowNum == -1) {
      return true;
    }

    final Object value = columnReader.read(rowNum);
    return !(value instanceof Number);
  }

  @Nullable
  @Override
  public Object getObject()
  {
    final int rowNum = currentRow.getAsInt();

    if (rowNum == -1) {
      return null;
    } else {
      return columnReader.read(currentRow.getAsInt());
    }
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
}
