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

import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.SystemTableAccess;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.ResidentCursorFactory;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.RowBasedCursorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.AndFilter;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Reads the rows of a system table from its {@link SystemTableDataProvider}.
 *
 * <p>Each cursor passes the provider the conjuncts of the query filter that read only the provider's
 * {@link SystemTableDataProvider#getPushdownColumns() pushdown columns}, keeps the rows the caller may read according
 * to its {@link SystemTableAccess}, and then applies the whole query filter and the query's virtual columns. Rows are
 * authorized before any virtual column is computed, so a virtual column can't change which rows the caller sees.</p>
 */
public class SystemTableCursorFactory implements ResidentCursorFactory
{
  private final SystemTableDataProvider provider;
  private final RowSignature rowSignature;
  private final SystemTableAccess access;
  private final RowAdapter<Object[]> rowAdapter;
  private final RowBasedCursorFactory<Object[]> capabilitiesCursorFactory;

  public SystemTableCursorFactory(
      final SystemTableDataProvider provider,
      final RowSignature rowSignature,
      final SystemTableAccess access
  )
  {
    this.provider = provider;
    this.rowSignature = rowSignature;
    this.access = access;
    this.rowAdapter = columnName -> {
      final int columnNumber = rowSignature.indexOf(columnName);
      return columnNumber >= 0 ? row -> row[columnNumber] : row -> null;
    };
    // Row-based cursors report the same capabilities whatever their rows.
    this.capabilitiesCursorFactory = new RowBasedCursorFactory<>(Sequences.empty(), rowAdapter, rowSignature);
  }

  @Override
  public CursorHolder makeCursorHolder(final CursorBuildSpec spec)
  {
    final Iterable<Object[]> rows = access.filter(provider.getRows(getPushdownFilters(spec)));
    return new RowBasedCursorFactory<>(Sequences.simple(rows), rowAdapter, rowSignature).makeCursorHolder(spec);
  }

  /**
   * Reads the columns of a row of the table by name.
   */
  public RowAdapter<Object[]> getRowAdapter()
  {
    return rowAdapter;
  }

  @Override
  public RowSignature getRowSignature()
  {
    return rowSignature;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(final String column)
  {
    return capabilitiesCursorFactory.getColumnCapabilities(column);
  }

  /**
   * Returns the conjuncts of the query filter that read only pushdown columns. A column that a virtual column of the
   * query replaces isn't the provider's column, so filters on it aren't pushed down.
   */
  private List<Filter> getPushdownFilters(final CursorBuildSpec spec)
  {
    final Set<String> pushdownColumns = provider.getPushdownColumns();
    final Filter filter = spec.getFilter();
    if (pushdownColumns.isEmpty() || filter == null) {
      return List.of();
    }

    final Collection<Filter> conjuncts = filter instanceof AndFilter andFilter
                                         ? andFilter.getFilters()
                                         : List.of(filter);
    final VirtualColumns virtualColumns = spec.getVirtualColumns();
    final List<Filter> pushdownFilters = new ArrayList<>();
    for (final Filter conjunct : conjuncts) {
      final Set<String> requiredColumns = conjunct.getRequiredColumns();
      if (!requiredColumns.isEmpty()
          && pushdownColumns.containsAll(requiredColumns)
          && requiredColumns.stream().noneMatch(virtualColumns::exists)) {
        pushdownFilters.add(conjunct);
      }
    }
    return pushdownFilters;
  }
}
