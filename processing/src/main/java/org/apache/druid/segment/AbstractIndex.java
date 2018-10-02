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

package org.apache.druid.segment;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.column.ColumnHolder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractIndex
{
  public abstract List<String> getColumnNames();

  public abstract StorageAdapter toStorageAdapter();

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    StorageAdapter storageAdapter = toStorageAdapter();
    List<Cursor> cursors = storageAdapter.makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    ).toList();
    List<String> columnNames = new ArrayList<>();
    columnNames.add(ColumnHolder.TIME_COLUMN_NAME);
    columnNames.addAll(getColumnNames());
    for (Cursor cursor : cursors) {
      ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
      List<ColumnValueSelector> selectors =
          columnNames.stream().map(columnSelectorFactory::makeColumnValueSelector).collect(Collectors.toList());
      while (!cursor.isDone()) {
        sb.append('[');
        for (int i = 0; i < selectors.size(); i++) {
          sb.append(columnNames.get(i)).append('=');
          ColumnValueSelector selector = selectors.get(i);
          Object columnValue = selector.getObject();
          sb.append(columnValue);
          sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append("]\n");
        cursor.advance();
      }
      sb.append("\n");
    }
    return sb.toString();
  }
}
