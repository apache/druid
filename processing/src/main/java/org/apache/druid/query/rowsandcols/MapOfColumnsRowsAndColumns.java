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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.DoubleArrayColumn;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.query.rowsandcols.semantic.AppendableRowsAndColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class MapOfColumnsRowsAndColumns implements RowsAndColumns
{
  public static MapOfColumnsRowsAndColumns.Builder builder()
  {
    return new Builder();
  }

  public static MapOfColumnsRowsAndColumns of(String name, Column col)
  {
    return fromMap(ImmutableMap.of(name, col));
  }

  public static MapOfColumnsRowsAndColumns of(String name, Column col, String name2, Column col2)
  {
    return fromMap(ImmutableMap.of(name, col, name2, col2));
  }

  @SuppressWarnings("unchecked")
  public static MapOfColumnsRowsAndColumns fromMap(Map<String, ? extends Column> map)
  {
    if (map == null || map.isEmpty()) {
      throw new ISE("map[%s] cannot be null or empty.", map);
    }

    final Iterator<? extends Map.Entry<String, ? extends Column>> iter = map.entrySet().iterator();
    Map.Entry<String, ? extends Column> entry = iter.next();
    int numCells = entry.getValue().toAccessor().numRows();
    if (iter.hasNext()) {
      entry = iter.next();
      final int newCells = entry.getValue().toAccessor().numRows();
      if (numCells != newCells) {
        throw new ISE(
            "Mismatched numCells, expectedNumCells[%s], actual[%s] from col[%s].",
            numCells,
            newCells,
            entry.getKey()
        );
      }
    }

    return new MapOfColumnsRowsAndColumns(
        (Map<String, Column>) map,
        map.values().iterator().next().toAccessor().numRows()
    );
  }

  public static MapOfColumnsRowsAndColumns fromRowObjects(Object[][] objs, RowSignature signature)
  {
    final Builder bob = builder();

    if (objs.length > 0) {
      Object[][] columnOriented = new Object[objs[0].length][objs.length];
      for (int i = 0; i < objs.length; ++i) {
        for (int j = 0; j < objs[i].length; ++j) {
          columnOriented[j][i] = objs[i][j];
        }
      }

      for (int i = 0; i < signature.size(); ++i) {
        final ColumnType type = signature.getColumnType(i).orElse(null);
        bob.add(signature.getColumnName(i), columnOriented[i], type);
      }
    }

    return bob.build();
  }

  private final Map<String, Column> mapOfColumns;
  private final int numRows;

  public MapOfColumnsRowsAndColumns(
      Map<String, Column> mapOfColumns,
      int numRows
  )
  {
    this.mapOfColumns = mapOfColumns;
    this.numRows = numRows;
  }

  @Override
  public Set<String> getColumnNames()
  {
    return mapOfColumns.keySet();
  }

  @Override
  public int numRows()
  {
    return numRows;
  }

  @Override
  public Column findColumn(String name)
  {
    return mapOfColumns.get(name);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T as(Class<T> clazz)
  {
    if (AppendableRowsAndColumns.class.equals(clazz)) {
      return (T) new AppendableMapOfColumns(this);
    }
    return null;
  }

  public static class Builder
  {
    public LinkedHashMap<String, Column> cols = new LinkedHashMap<>();

    public Builder add(String name, int[] vals)
    {
      return add(name, new IntArrayColumn(vals));
    }

    public Builder add(String name, double[] vals)
    {
      return add(name, new DoubleArrayColumn(vals));
    }

    public Builder add(String name, ColumnType type, Object... vals)
    {
      return add(name, vals, type);
    }

    public Builder add(String name, Object[] vals, ColumnType type)
    {
      return add(name, new ObjectArrayColumn(vals, type));
    }

    public Builder add(String name, Column col)
    {
      cols.put(name, col);
      return this;
    }

    public MapOfColumnsRowsAndColumns build()
    {
      return MapOfColumnsRowsAndColumns.fromMap(cols);
    }
  }

}
