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

import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinMatcher;
import org.apache.druid.segment.join.Joinable;

import javax.annotation.Nullable;
import java.util.List;

public class IndexedTableJoinable implements Joinable
{
  private final IndexedTable table;

  public IndexedTableJoinable(final IndexedTable table)
  {
    this.table = table;
  }

  @Override
  public List<String> getAvailableColumns()
  {
    return table.allColumns();
  }

  @Override
  public int getCardinality(String columnName)
  {
    if (table.allColumns().contains(columnName)) {
      return table.numRows();
    } else {
      // NullDimensionSelector has cardinality = 1 (one null, nothing else).
      return 1;
    }
  }

  @Override
  @Nullable
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    return IndexedTableColumnSelectorFactory.columnCapabilities(table, columnName);
  }

  @Override
  public JoinMatcher makeJoinMatcher(
      final ColumnSelectorFactory leftColumnSelectorFactory,
      final JoinConditionAnalysis condition,
      final boolean remainderNeeded
  )
  {
    return new IndexedTableJoinMatcher(
        table,
        leftColumnSelectorFactory,
        condition,
        remainderNeeded
    );
  }
}
