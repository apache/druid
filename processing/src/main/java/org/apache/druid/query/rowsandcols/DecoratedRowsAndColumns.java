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

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.concrete.FrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.DecoratableRowsAndColumns;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class DecoratedRowsAndColumns implements DecoratableRowsAndColumns
{
  private final RowsAndColumns base;

  private RowsAndColumns materialized = null;

  private Interval interval = null;
  private Filter filter = null;
  private VirtualColumns virtualColumns = null;
  private int limit = -1;
  private LinkedHashSet<String> viewableColumns = null;
  private List<ColumnWithDirection> ordering = null;

  public DecoratedRowsAndColumns(
      RowsAndColumns base
  )
  {
    this.base = base;
  }

  @Override
  public Collection<String> getColumnNames()
  {
    return viewableColumns == null ? base.getColumnNames() : viewableColumns;
  }

  @Override
  public int numRows()
  {
    return materializedOrBase().numRows();
  }

  @Nullable
  @Override
  public Column findColumn(String name)
  {
    if (viewableColumns != null && !viewableColumns.contains(name)) {
      return null;
    }

    return materializedOrBase().findColumn(name);
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    return null;
  }

  @Override
  public void limitTimeRange(Interval interval)
  {
    this.interval = interval;
  }

  @Override
  public void addFilter(Filter filter)
  {
    this.filter = filter;
  }

  @Override
  public void addVirtualColumns(VirtualColumns virtualColumns)
  {
    this.virtualColumns = virtualColumns;
  }

  @Override
  public void setLimit(int numRows)
  {
    this.limit = numRows;
  }

  @Override
  public void restrictColumns(List<String> columns)
  {
    this.viewableColumns = new LinkedHashSet<>(columns);
  }

  @Override
  public void setOrdering(List<ColumnWithDirection> ordering)
  {
    this.ordering = ordering;
  }

  private boolean maybeChangesWithMaterialization()
  {
    return !(interval == null && filter == null && limit == -1 && ordering == null);
  }

  private RowsAndColumns materializedOrBase()
  {
    if (materialized != null) {
      return materialized;
    }

    if (maybeChangesWithMaterialization()) {
      materialized = materialize();
      return materialized;
    }
    return base;
  }

  private RowsAndColumns materialize()
  {
    final StorageAdapter as = base.as(StorageAdapter.class);
    if (as == null) {
      throw new ISE("base[%s] could not become a StorageAdapter", base.getClass());
    }

    if (ordering != null) {
      throw new ISE("Cannot reorder[%s] scan data right now", ordering);
    }

    final Sequence<Cursor> cursors = as.makeCursors(
        filter,
        interval == null ? Intervals.ETERNITY : interval,
        virtualColumns,
        Granularities.ALL,
        false,
        null
    );

    Collection<String> cols = viewableColumns == null ? base.getColumnNames() : viewableColumns;
    AtomicReference<RowSignature> siggy = new AtomicReference<>(null);

    FrameWriter writer = cursors.accumulate(null, (accumulated, in) -> {
      if (accumulated != null) {
        // We should not get multiple cursors because we set the granularity to ALL.  So, this should never
        // actually happen, but it doesn't hurt us to defensive here, so we test against it.
        throw new ISE("accumulated[%s] non-null, why did we get multiple cursors?", accumulated);
      }

      int theLimit = limit == -1 ? Integer.MAX_VALUE : limit;

      final ColumnSelectorFactory columnSelectorFactory = in.getColumnSelectorFactory();
      final RowSignature.Builder sigBob = RowSignature.builder();

      for (String col : cols) {
        final ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(col);
        if (capabilities != null) {
          sigBob.add(col, capabilities.toColumnType());
        }
      }
      final RowSignature signature = sigBob.build();
      siggy.set(signature);

      List<KeyColumn> sortColumns = new ArrayList<>();
      if (ordering != null) {
        for (ColumnWithDirection columnWithDirection : ordering) {
          final KeyOrder order;

          if (columnWithDirection.getDirection() == ColumnWithDirection.Direction.DESC) {
            order = KeyOrder.DESCENDING;
          } else {
            order = KeyOrder.ASCENDING;
          }

          sortColumns.add(new KeyColumn(columnWithDirection.getColumn(), order));
        }
      }

      final FrameWriterFactory frameWriterFactory = FrameWriters.makeFrameWriterFactory(
          FrameType.COLUMNAR,
          new ArenaMemoryAllocatorFactory(200 << 20), // 200 MB, because, why not?
          signature,
          sortColumns
      );

      final FrameWriter frameWriter = frameWriterFactory.newFrameWriter(columnSelectorFactory);
      while (!in.isDoneOrInterrupted()) {
        frameWriter.addSelection();
        in.advance();
        if (--theLimit <= 0) {
          break;
        }
      }

      return frameWriter;
    });

    if (writer == null) {
      // This means that the accumulate was never called, which can only happen if we didn't have any cursors.
      // We would only have zero cursors if we essentially didn't match anything, meaning that our RowsAndColumns
      // should be completely empty.
      return new EmptyRowsAndColumns();
    } else {
      final byte[] bytes = writer.toByteArray();
      return new FrameRowsAndColumns(Frame.wrap(bytes), siggy.get());
    }
  }
}
