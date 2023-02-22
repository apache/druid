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

package org.apache.druid.frame.segment.columnar;

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.read.columnar.FrameColumnReader;
import org.apache.druid.frame.segment.FrameCursor;
import org.apache.druid.frame.segment.FrameCursorUtils;
import org.apache.druid.frame.segment.FrameFilteredOffset;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.ColumnCache;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.QueryableIndexColumnSelectorFactory;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.SimpleDescendingOffset;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.vector.FilteredVectorOffset;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.QueryableIndexVectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorOffset;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A {@link CursorFactory} implementation based on a single columnar {@link Frame}.
 *
 * This class is only used for columnar frames. It is not used for row-based frames.
 *
 * @see org.apache.druid.frame.segment.row.FrameCursorFactory the row-based version
 */
public class FrameCursorFactory implements CursorFactory
{
  private final Frame frame;
  private final RowSignature signature;
  private final List<FrameColumnReader> columnReaders;

  public FrameCursorFactory(
      final Frame frame,
      final RowSignature signature,
      final List<FrameColumnReader> columnReaders
  )
  {
    this.frame = FrameType.COLUMNAR.ensureType(frame);
    this.signature = signature;
    this.columnReaders = columnReaders;
  }

  @Override
  public boolean canVectorize(@Nullable Filter filter, VirtualColumns virtualColumns, boolean descending)
  {
    return (filter == null || filter.canVectorizeMatcher(signature))
           && virtualColumns.canVectorize(signature)
           && !descending;
  }

  @Override
  public Sequence<Cursor> makeCursors(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      Granularity gran,
      boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    final FrameQueryableIndex index = new FrameQueryableIndex(frame, signature, columnReaders);

    if (Granularities.ALL.equals(gran)) {
      final Closer closer = Closer.create();
      final Cursor cursor = makeGranularityAllCursor(
          new ColumnCache(index, closer),
          frame.numRows(),
          filter,
          interval,
          virtualColumns,
          descending
      );

      return Sequences.simple(Collections.singletonList(cursor)).withBaggage(closer);
    } else {
      // Not currently needed for the intended use cases of frame-based cursors.
      throw new UOE("Granularity [%s] not supported", gran);
    }
  }

  @Nullable
  @Override
  public VectorCursor makeVectorCursor(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      boolean descending,
      int vectorSize,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    if (!canVectorize(filter, virtualColumns, descending)) {
      throw new ISE("Cannot vectorize. Check 'canVectorize' before calling 'makeVectorCursor'.");
    }

    final Closer closer = Closer.create();
    final FrameQueryableIndex index = new FrameQueryableIndex(frame, signature, columnReaders);
    final Filter filterToUse = FrameCursorUtils.buildFilter(filter, interval);
    final VectorOffset baseOffset = new NoFilterVectorOffset(vectorSize, 0, frame.numRows());
    final ColumnCache columnCache = new ColumnCache(index, closer);

    // baseColumnSelectorFactory using baseOffset is the column selector for filtering.
    final VectorColumnSelectorFactory baseColumnSelectorFactory = new QueryableIndexVectorColumnSelectorFactory(
        index,
        baseOffset,
        columnCache,
        virtualColumns
    );

    if (filterToUse == null) {
      return new FrameVectorCursor(baseOffset, baseColumnSelectorFactory, closer);
    } else {
      final FilteredVectorOffset filteredOffset = FilteredVectorOffset.create(
          baseOffset,
          baseColumnSelectorFactory,
          filterToUse
      );

      final VectorColumnSelectorFactory filteredColumnSelectorFactory = new QueryableIndexVectorColumnSelectorFactory(
          index,
          filteredOffset,
          columnCache,
          virtualColumns
      );

      return new FrameVectorCursor(filteredOffset, filteredColumnSelectorFactory, closer);
    }
  }

  private static Cursor makeGranularityAllCursor(
      final ColumnCache columnSelector,
      final int numRows,
      @Nullable final Filter filter,
      final Interval interval,
      final VirtualColumns virtualColumns,
      final boolean descending
  )
  {
    final Filter filterToUse = FrameCursorUtils.buildFilter(filter, interval);
    final Offset baseOffset = descending ? new SimpleDescendingOffset(numRows) : new SimpleAscendingOffset(numRows);
    final Offset offset;

    final QueryableIndexColumnSelectorFactory columnSelectorFactory =
        new QueryableIndexColumnSelectorFactory(virtualColumns, descending, baseOffset, columnSelector);

    if (filterToUse == null) {
      offset = baseOffset;
    } else {
      offset = new FrameFilteredOffset(baseOffset, columnSelectorFactory, filterToUse);
    }

    return new FrameCursor(offset, columnSelectorFactory);
  }
}
