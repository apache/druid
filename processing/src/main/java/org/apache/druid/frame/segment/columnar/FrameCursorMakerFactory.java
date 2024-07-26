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
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.ColumnCache;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorMaker;
import org.apache.druid.segment.CursorMakerFactory;
import org.apache.druid.segment.QueryableIndexColumnSelectorFactory;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.SimpleDescendingOffset;
import org.apache.druid.segment.SimpleSettableOffset;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.vector.FilteredVectorOffset;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.QueryableIndexVectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorOffset;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A {@link CursorMakerFactory} implementation based on a single columnar {@link Frame}.
 *
 * This class is only used for columnar frames. It is not used for row-based frames.
 *
 * @see org.apache.druid.frame.segment.row.FrameCursorMakerFactory the row-based version
 */
public class FrameCursorMakerFactory implements CursorMakerFactory
{
  private final Frame frame;
  private final RowSignature signature;
  private final List<FrameColumnReader> columnReaders;

  public FrameCursorMakerFactory(
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
  public CursorMaker asCursorMaker(CursorBuildSpec spec)
  {
    final Closer closer = Closer.create();
    return new CursorMaker()
    {
      @Override
      public boolean canVectorize()
      {
        return (spec.getFilter() == null || spec.getFilter().canVectorizeMatcher(signature))
               && spec.getVirtualColumns().canVectorize(signature)
               && !spec.isDescending();
      }

      @Override
      public Cursor makeCursor()
      {
        final FrameQueryableIndex index = new FrameQueryableIndex(frame, signature, columnReaders);

        if (Granularities.ALL.equals(spec.getGranularity())) {
          final Cursor cursor = makeGranularityAllCursor(
              new ColumnCache(index, closer),
              frame.numRows(),
              spec.getFilter(),
              spec.getInterval(),
              spec.getVirtualColumns(),
              spec.isDescending()
          );

          return cursor;
        } else {
          // Not currently needed for the intended use cases of frame-based cursors.
          throw new UOE("Granularity [%s] not supported", spec.getGranularity());
        }
      }

      @Nullable
      @Override
      public VectorCursor makeVectorCursor()
      {
        if (!canVectorize()) {
          throw new ISE("Cannot vectorize. Check 'canVectorize' before calling 'makeVectorCursor'.");
        }

        final FrameQueryableIndex index = new FrameQueryableIndex(frame, signature, columnReaders);
        final Filter filterToUse = FrameCursorUtils.buildFilter(spec.getFilter(), spec.getInterval());
        final VectorOffset baseOffset = new NoFilterVectorOffset(
            spec.getQueryContext().getVectorSize(),
            0,
            frame.numRows()
        );
        final ColumnCache columnCache = new ColumnCache(index, closer);

        // baseColumnSelectorFactory using baseOffset is the column selector for filtering.
        final VectorColumnSelectorFactory baseColumnSelectorFactory = new QueryableIndexVectorColumnSelectorFactory(
            index,
            baseOffset,
            columnCache,
            spec.getVirtualColumns()
        );

        if (filterToUse == null) {
          return new FrameVectorCursor(baseOffset, baseColumnSelectorFactory, closer);
        } else {
          final VectorValueMatcher matcher = filterToUse.makeVectorMatcher(baseColumnSelectorFactory);
          final FilteredVectorOffset filteredOffset = FilteredVectorOffset.create(
              baseOffset,
              matcher
          );

          final VectorColumnSelectorFactory filteredColumnSelectorFactory = new QueryableIndexVectorColumnSelectorFactory(
              index,
              filteredOffset,
              columnCache,
              spec.getVirtualColumns()
          );

          return new FrameVectorCursor(filteredOffset, filteredColumnSelectorFactory, closer);
        }
      }

      @Override
      public void close()
      {
        CloseableUtils.closeAndWrapExceptions(closer);
      }
    };
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
    final SimpleSettableOffset baseOffset =
        descending ? new SimpleDescendingOffset(numRows) : new SimpleAscendingOffset(numRows);
    final SimpleSettableOffset offset;

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
