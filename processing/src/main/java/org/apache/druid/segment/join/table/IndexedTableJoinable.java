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

import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinMatcher;
import org.apache.druid.segment.join.Joinable;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    return table.rowSignature().getColumnNames();
  }

  @Override
  public int getCardinality(String columnName)
  {
    if (table.rowSignature().contains(columnName)) {
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

  @Override
  public Set<String> getCorrelatedColumnValues(
      String searchColumnName,
      String searchColumnValue,
      String retrievalColumnName,
      long maxCorrelationSetSize,
      boolean allowNonKeyColumnSearch
  )
  {
    int filterColumnPosition = table.rowSignature().indexOf(searchColumnName);
    int correlatedColumnPosition = table.rowSignature().indexOf(retrievalColumnName);

    if (filterColumnPosition < 0 || correlatedColumnPosition < 0) {
      return ImmutableSet.of();
    }

    Set<String> correlatedValues = new HashSet<>();
    if (table.keyColumns().contains(searchColumnName)) {
      IndexedTable.Index index = table.columnIndex(filterColumnPosition);
      IndexedTable.Reader reader = table.columnReader(correlatedColumnPosition);
      IntList rowIndex = index.find(searchColumnValue);
      for (int i = 0; i < rowIndex.size(); i++) {
        int rowNum = rowIndex.getInt(i);
        correlatedValues.add(reader.read(rowNum).toString());

        if (correlatedValues.size() > maxCorrelationSetSize) {
          return ImmutableSet.of();
        }
      }
      return correlatedValues;
    } else {
      if (!allowNonKeyColumnSearch) {
        return ImmutableSet.of();
      }

      IndexedTable.Reader dimNameReader = table.columnReader(filterColumnPosition);
      IndexedTable.Reader correlatedColumnReader = table.columnReader(correlatedColumnPosition);
      for (int i = 0; i < table.numRows(); i++) {
        if (searchColumnValue.equals(dimNameReader.read(i).toString())) {
          correlatedValues.add(correlatedColumnReader.read(i).toString());
        }
        if (correlatedValues.size() > maxCorrelationSetSize) {
          return ImmutableSet.of();
        }
      }

      return correlatedValues;
    }
  }
}
