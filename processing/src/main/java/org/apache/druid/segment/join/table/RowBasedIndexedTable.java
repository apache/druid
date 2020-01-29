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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.column.ValueType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An IndexedTable composed of a List-based table and Map-based indexes. The implementation is agnostic to the
 * specific row type; it uses a {@link RowAdapter} to work with any sort of object.
 */
public class RowBasedIndexedTable<RowType> implements IndexedTable
{
  private final List<RowType> table;
  private final List<Map<Object, IntList>> index;
  private final Map<String, ValueType> rowSignature;
  private final List<String> columns;
  private final List<ValueType> columnTypes;
  private final List<Function<RowType, Object>> columnFunctions;
  private final List<String> keyColumns;

  public RowBasedIndexedTable(
      final List<RowType> table,
      final RowAdapter<RowType> rowAdapter,
      final Map<String, ValueType> rowSignature,
      final List<String> keyColumns
  )
  {
    this.table = table;
    this.rowSignature = rowSignature;
    this.columns = rowSignature.keySet().stream().sorted().collect(Collectors.toList());
    this.columnTypes = new ArrayList<>(columns.size());
    this.columnFunctions = columns.stream().map(rowAdapter::columnFunction).collect(Collectors.toList());
    this.keyColumns = keyColumns;

    if (new HashSet<>(keyColumns).size() != keyColumns.size()) {
      throw new ISE("keyColumns[%s] must not contain duplicates", keyColumns);
    }

    if (!rowSignature.keySet().containsAll(keyColumns)) {
      throw new ISE(
          "keyColumns[%s] must all be contained in rowSignature[%s]",
          String.join(", ", keyColumns),
          String.join(", ", rowSignature.keySet())
      );
    }

    index = new ArrayList<>(columns.size());

    for (int i = 0; i < columns.size(); i++) {
      final String column = columns.get(i);
      final Map<Object, IntList> m;
      final ValueType columnType = rowSignature.get(column);

      columnTypes.add(columnType);

      if (keyColumns.contains(column)) {
        final Function<RowType, Object> columnFunction = columnFunctions.get(i);

        m = new HashMap<>();

        for (int j = 0; j < table.size(); j++) {
          final RowType row = table.get(j);
          final Object key = DimensionHandlerUtils.convertObjectToType(columnFunction.apply(row), columnType);
          if (key != null) {
            final IntList array = m.computeIfAbsent(key, k -> new IntArrayList());
            array.add(j);
          }
        }
      } else {
        m = null;
      }

      index.add(m);
    }
  }

  @Override
  public List<String> keyColumns()
  {
    return keyColumns;
  }

  @Override
  public List<String> allColumns()
  {
    return columns;
  }

  @Override
  public Map<String, ValueType> rowSignature()
  {
    return rowSignature;
  }

  @Override
  public Index columnIndex(int column)
  {
    final Map<Object, IntList> indexMap = index.get(column);

    if (indexMap == null) {
      throw new IAE("Column[%d] is not a key column", column);
    }

    final ValueType columnType = columnTypes.get(column);

    return key -> {
      final Object convertedKey = DimensionHandlerUtils.convertObjectToType(key, columnType, false);

      if (convertedKey != null) {
        final IntList found = indexMap.get(convertedKey);
        if (found != null) {
          return found;
        } else {
          return IntLists.EMPTY_LIST;
        }
      } else {
        return IntLists.EMPTY_LIST;
      }
    };
  }

  @Override
  public Reader columnReader(int column)
  {
    final Function<RowType, Object> columnFn = columnFunctions.get(column);

    if (columnFn == null) {
      throw new IAE("Column[%d] is not a valid column", column);
    }

    return row -> columnFn.apply(table.get(row));
  }

  @Override
  public int numRows()
  {
    return table.size();
  }
}
