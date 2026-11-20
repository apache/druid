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

package org.apache.druid.server.system.table;

import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SystemTableCursorFactoryTest
{
  static final RowSignature SIGNATURE = RowSignature.builder()
                                                            .add("datasource", ColumnType.STRING)
                                                            .add("task_id", ColumnType.STRING)
                                                            .build();
  static final List<Object[]> ROWS = List.of(
      new Object[]{"allowed", "task1"},
      new Object[]{"secret", "task2"}
  );

  /**
   * Only conjuncts that read pushdown columns, and that no virtual column replaces, reach the provider.
   */
  @Test
  public void testPushesDownOnlyConjunctsOnPushdownColumns()
  {
    final RecordingProvider provider = new RecordingProvider();
    final Filter onDatasource = new EqualityFilter("datasource", ColumnType.STRING, "allowed", null);
    final Filter onTaskId = new EqualityFilter("task_id", ColumnType.STRING, "task1", null);

    final List<Object> taskIds = readTaskIds(
        new SystemTableCursorFactory(provider, SIGNATURE, SystemTableAccess.ALL_ROWS),
        CursorBuildSpec.builder().setFilter(new AndFilter(List.of(onDatasource, onTaskId))).build()
    );

    Assertions.assertEquals(List.of(onDatasource), provider.filters);
    // The whole filter applies to the provider's rows, which may include rows that don't match the pushed filters.
    Assertions.assertEquals(List.of("task1"), taskIds);
  }

  @Test
  public void testDoesNotPushDownFilterOnReplacedColumn()
  {
    final RecordingProvider provider = new RecordingProvider();

    readTaskIds(
        new SystemTableCursorFactory(provider, SIGNATURE, SystemTableAccess.ALL_ROWS),
        CursorBuildSpec.builder()
                       .setVirtualColumns(VirtualColumns.create(spoofedDatasource()))
                       .setFilter(new EqualityFilter("datasource", ColumnType.STRING, "allowed", null))
                       .build()
    );

    Assertions.assertEquals(List.of(), provider.filters);
  }

  /**
   * Rows are authorized before virtual columns are computed, so a virtual column that replaces the column the row
   * predicate reads can't reveal rows the caller may not read.
   */
  @Test
  public void testRowsAreAuthorizedBeforeVirtualColumns()
  {
    final SystemTableAccess access = new SystemTableAccess(row -> "allowed".equals(row[0]));

    final List<Object> taskIds = readTaskIds(
        new SystemTableCursorFactory(new RecordingProvider(), SIGNATURE, access),
        CursorBuildSpec.builder().setVirtualColumns(VirtualColumns.create(spoofedDatasource())).build()
    );

    Assertions.assertEquals(List.of("task1"), taskIds);
  }

  private static ExpressionVirtualColumn spoofedDatasource()
  {
    return new ExpressionVirtualColumn("datasource", "'allowed'", ColumnType.STRING, ExprMacroTable.nil());
  }

  static List<Object> readTaskIds(final CursorFactory cursorFactory, final CursorBuildSpec spec)
  {
    final List<Object> taskIds = new ArrayList<>();
    try (CursorHolder holder = cursorFactory.makeCursorHolder(spec)) {
      final Cursor cursor = holder.asCursor();
      final ColumnValueSelector<?> taskId = cursor.getColumnSelectorFactory().makeColumnValueSelector("task_id");
      while (!cursor.isDone()) {
        taskIds.add(taskId.getObject());
        cursor.advance();
      }
    }
    return taskIds;
  }

  static class RecordingProvider implements SystemTableDataProvider
  {
    private List<Filter> filters;

    @Override
    public Set<String> getPushdownColumns()
    {
      return Set.of("datasource");
    }

    @Override
    public Iterable<Object[]> getRows(final List<Filter> filters)
    {
      this.filters = filters;
      return ROWS;
    }
  }
}
