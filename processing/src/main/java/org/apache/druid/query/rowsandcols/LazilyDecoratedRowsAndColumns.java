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

import com.google.common.collect.ImmutableList;
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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.operator.OffsetLimit;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.concrete.FrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.ColumnSelectorFactoryMaker;
import org.apache.druid.query.rowsandcols.semantic.DefaultRowsAndColumnsDecorator;
import org.apache.druid.query.rowsandcols.semantic.RowsAndColumnsDecorator;
import org.apache.druid.query.rowsandcols.semantic.WireTransferable;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class LazilyDecoratedRowsAndColumns implements RowsAndColumns
{
  private static final Map<Class<?>, Function<LazilyDecoratedRowsAndColumns, ?>> AS_MAP = RowsAndColumns
      .makeAsMap(LazilyDecoratedRowsAndColumns.class);

  private RowsAndColumns base;
  private Interval interval;
  private Filter filter;
  private VirtualColumns virtualColumns;
  private OffsetLimit limit;
  private LinkedHashSet<String> viewableColumns;
  private List<ColumnWithDirection> ordering;

  public LazilyDecoratedRowsAndColumns(
      RowsAndColumns base,
      Interval interval,
      Filter filter,
      VirtualColumns virtualColumns,
      OffsetLimit limit,
      List<ColumnWithDirection> ordering,
      LinkedHashSet<String> viewableColumns
  )
  {
    this.base = base;
    this.interval = interval;
    this.filter = filter;
    this.virtualColumns = virtualColumns;
    this.limit = limit;
    this.ordering = ordering;
    this.viewableColumns = viewableColumns;
  }

  @Override
  public Collection<String> getColumnNames()
  {
    return viewableColumns == null ? base.getColumnNames() : viewableColumns;
  }

  public RowsAndColumns getBase()
  {
    return base;
  }

  @Override
  public int numRows()
  {
    maybeMaterialize();
    return base.numRows();
  }

  @Nullable
  @Override
  public Column findColumn(String name)
  {
    if (viewableColumns != null && !viewableColumns.contains(name)) {
      return null;
    }
    maybeMaterialize();
    return base.findColumn(name);
  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    //noinspection ReturnOfNull
    return (T) AS_MAP.getOrDefault(clazz, arg -> null).apply(this);
  }

  @SuppressWarnings("unused")
  @SemanticCreator
  public RowsAndColumnsDecorator toRowsAndColumnsDecorator()
  {
    // If we don't have a projection defined, then it's safe to continue collecting more decorations as we
    // can meaningfully merge them together.
    if (viewableColumns == null || viewableColumns.isEmpty()) {
      return new DefaultRowsAndColumnsDecorator(base, interval, filter, virtualColumns, limit, ordering);
    } else {
      return new DefaultRowsAndColumnsDecorator(this);
    }
  }

  @SuppressWarnings("unused")
  @SemanticCreator
  public WireTransferable toWireTransferable()
  {
    return () -> {
      final Pair<byte[], RowSignature> materialized = materialize();
      if (materialized == null) {
        return new byte[]{};
      } else {
        return materialized.lhs;
      }
    };
  }

  private void maybeMaterialize()
  {
    if (needsMaterialization()) {
      final Pair<byte[], RowSignature> thePair = materialize();
      if (thePair == null) {
        reset(new EmptyRowsAndColumns());
      } else {
        reset(new FrameRowsAndColumns(Frame.wrap(thePair.lhs), thePair.rhs));
      }
    }
  }

  private boolean needsMaterialization()
  {
    return interval != null || filter != null || limit.isPresent() || ordering != null || virtualColumns != null;
  }

  private Pair<byte[], RowSignature> materialize()
  {
    if (ordering != null) {
      throw new ISE("Cannot reorder[%s] scan data right now", ordering);
    }

    final StorageAdapter as = base.as(StorageAdapter.class);
    if (as == null) {
      return naiveMaterialize(base);
    } else {
      return materializeStorageAdapter(as);
    }
  }

  private void reset(RowsAndColumns rac)
  {
    base = rac;
    interval = null;
    filter = null;
    virtualColumns = null;
    limit = OffsetLimit.NONE;
    viewableColumns = null;
    ordering = null;
  }

  @Nullable
  private Pair<byte[], RowSignature> materializeStorageAdapter(StorageAdapter as)
  {
    final Sequence<Cursor> cursors = as.makeCursors(
        filter,
        interval == null ? Intervals.ETERNITY : interval,
        virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns,
        Granularities.ALL,
        false,
        null
    );


    final Collection<String> cols;
    if (viewableColumns != null) {
      cols = viewableColumns;
    } else {
      if (virtualColumns == null) {
        cols = base.getColumnNames();
      } else {
        cols = ImmutableList.<String>builder()
            .addAll(base.getColumnNames())
            .addAll(virtualColumns.getColumnNames())
            .build();
      }
    }
    AtomicReference<RowSignature> siggy = new AtomicReference<>(null);

    FrameWriter writer = cursors.accumulate(null, (accumulated, in) -> {
      if (accumulated != null) {
        // We should not get multiple cursors because we set the granularity to ALL.  So, this should never
        // actually happen, but it doesn't hurt us to defensive here, so we test against it.
        throw new ISE("accumulated[%s] non-null, why did we get multiple cursors?", accumulated);
      }

      long remainingRowsToSkip = limit.getOffset();
      long remainingRowsToFetch = limit.getLimitOrMax();

      final ColumnSelectorFactory columnSelectorFactory = in.getColumnSelectorFactory();
      final RowSignature.Builder sigBob = RowSignature.builder();

      for (String col : cols) {
        final ColumnCapabilities capabilities;
        if (virtualColumns != null) {
          capabilities = virtualColumns.getColumnCapabilitiesWithFallback(columnSelectorFactory, col);
        } else {
          capabilities = columnSelectorFactory.getColumnCapabilities(col);
        }

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
      for (; !in.isDoneOrInterrupted() && remainingRowsToSkip > 0; remainingRowsToSkip--) {
        in.advance();
      }
      for (; !in.isDoneOrInterrupted() && remainingRowsToFetch > 0; remainingRowsToFetch--) {
        frameWriter.addSelection();
        in.advance();
      }

      return frameWriter;
    });

    if (writer == null) {
      // This means that the accumulate was never called, which can only happen if we didn't have any cursors.
      // We would only have zero cursors if we essentially didn't match anything, meaning that our RowsAndColumns
      // should be completely empty.
      return null;
    } else {
      final byte[] bytes = writer.toByteArray();
      return Pair.of(bytes, siggy.get());
    }
  }

  @Nullable
  private Pair<byte[], RowSignature> naiveMaterialize(RowsAndColumns rac)
  {
    final int numRows = rac.numRows();

    BitSet rowsToSkip = null;

    if (interval != null) {
      rowsToSkip = new BitSet(numRows);

      final Column racColumn = rac.findColumn("__time");
      if (racColumn == null) {
        // The time column doesn't exist, this means that we have a null column.  A null column when coerced into a
        // long as is required by the time filter produces all 0s, so either 0 is included and matches all rows or
        // it's not and we skip all rows.
        if (!interval.contains(0)) {
          return null;
        }
      } else {
        final ColumnAccessor accessor = racColumn.toAccessor();
        for (int i = 0; i < accessor.numRows(); ++i) {
          rowsToSkip.set(i, !interval.contains(accessor.getLong(i)));
        }
      }
    }

    AtomicInteger rowId = new AtomicInteger(0);
    final ColumnSelectorFactoryMaker csfm = ColumnSelectorFactoryMaker.fromRAC(rac);
    final ColumnSelectorFactory selectorFactory = csfm.make(rowId);

    if (filter != null) {
      if (rowsToSkip == null) {
        rowsToSkip = new BitSet(numRows);
      }

      final ValueMatcher matcher = filter.makeMatcher(selectorFactory);

      for (; rowId.get() < numRows; rowId.incrementAndGet()) {
        final int theId = rowId.get();
        if (rowsToSkip.get(theId)) {
          continue;
        }

        if (!matcher.matches(false)) {
          rowsToSkip.set(theId);
        }
      }
    }

    if (virtualColumns != null) {
      throw new UOE("Cannot apply virtual columns [%s] with naive apply.", virtualColumns);
    }

    ArrayList<String> columnsToGenerate = new ArrayList<>();
    if (viewableColumns != null) {
      columnsToGenerate.addAll(viewableColumns);
    } else {
      columnsToGenerate.addAll(rac.getColumnNames());
      // When/if we support virtual columns from here, we should auto-add them to the list here as well as they expand
      // the implicit project when no projection is defined
    }

    // There is all sorts of sub-optimal things in this code, but we just ignore them for now as it is difficult to
    // optimally build frames for incremental data processing.  In order to build the frame object here, we must first
    // materialize everything in memory so that we know how long things are, such that we can allocate a big byte[]
    // so that the Frame.wrap() call can be given just a big byte[] to do its reading from.
    //
    // It would be a bit better if we could just build the per-column byte[] and somehow re-generate a Frame from
    // that.  But it's also possible that this impedence mis-match is a function of using column-oriented frames and
    // row-oriented frames are a much more friendly format.  Anyway, this long comment is here because this exploration
    // is being left as an exercise for the future.

    final RowSignature.Builder sigBob = RowSignature.builder();
    final ArenaMemoryAllocatorFactory memFactory = new ArenaMemoryAllocatorFactory(200 << 20);


    for (String column : columnsToGenerate) {
      final Column racColumn = rac.findColumn(column);
      if (racColumn == null) {
        continue;
      }
      sigBob.add(column, racColumn.toAccessor().getType());
    }

    long remainingRowsToSkip = limit.getOffset();
    long remainingRowsToFetch = limit.getLimitOrMax();

    final FrameWriter frameWriter = FrameWriters.makeFrameWriterFactory(
        FrameType.COLUMNAR,
        memFactory,
        sigBob.build(),
        Collections.emptyList()
    ).newFrameWriter(selectorFactory);

    rowId.set(0);
    for (; rowId.get() < numRows && remainingRowsToFetch > 0; rowId.incrementAndGet()) {
      final int theId = rowId.get();
      if (rowsToSkip != null && rowsToSkip.get(theId)) {
        continue;
      }
      if (remainingRowsToSkip > 0) {
        remainingRowsToSkip--;
        continue;
      }
      remainingRowsToFetch--;
      frameWriter.addSelection();
    }

    return Pair.of(frameWriter.toByteArray(), sigBob.build());
  }
}
