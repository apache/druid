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
import org.apache.druid.frame.segment.row.RowFrameCursorFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.ColumnCache;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.QueryableIndexColumnSelectorFactory;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.SimpleSettableOffset;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.vector.FilteredVectorOffset;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.QueryableIndexVectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorOffset;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A {@link CursorFactory} implementation based on a single columnar {@link Frame}.
 *
 * This class is only used for columnar frames. It is not used for row-based frames.
 *
 * @see RowFrameCursorFactory the row-based version
 */
public class ColumnarFrameCursorFactory implements CursorFactory
{
  private final Frame frame;
  private final RowSignature signature;
  private final List<FrameColumnReader> columnReaders;

  public ColumnarFrameCursorFactory(
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
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    final Closer closer = Closer.create();

    // Frames are not self-describing as to their sort order, so we can't determine the sort order by looking at
    // the Frame object. We could populate this with information from the relevant ClusterBy, but that's not available
    // at this point in the code. It could be plumbed in at some point. For now, use an empty list.
    final List<OrderBy> ordering = Collections.emptyList();

    return new CursorHolder()
    {
      @Override
      public boolean canVectorize()
      {
        return (spec.getFilter() == null || spec.getFilter().canVectorizeMatcher(signature))
               && spec.getVirtualColumns().canVectorize(signature);
      }

      @Override
      public Cursor asCursor()
      {
        final FrameQueryableIndex index = new FrameQueryableIndex(frame, signature, columnReaders);
        final ColumnCache columnCache = new ColumnCache(index, closer);
        final Filter filterToUse = FrameCursorUtils.buildFilter(spec.getFilter(), spec.getInterval());
        final SimpleSettableOffset baseOffset = new SimpleAscendingOffset(frame.numRows());

        final QueryableIndexColumnSelectorFactory columnSelectorFactory = new QueryableIndexColumnSelectorFactory(
            spec.getVirtualColumns(),
            Order.NONE,
            baseOffset,
            columnCache
        );

        final SimpleSettableOffset offset;
        if (filterToUse == null) {
          offset = baseOffset;
        } else {
          offset = new FrameFilteredOffset(baseOffset, columnSelectorFactory, filterToUse);
        }

        return new FrameCursor(offset, columnSelectorFactory);
      }

      @Override
      public List<OrderBy> getOrdering()
      {
        return ordering;
      }

      @Nullable
      @Override
      public VectorCursor asVectorCursor()
      {
        if (!canVectorize()) {
          throw new ISE("Cannot vectorize. Check 'canVectorize' before calling 'asVectorCursor'.");
        }

        final FrameQueryableIndex index = new FrameQueryableIndex(frame, signature, columnReaders);
        final Filter filterToUse = FrameCursorUtils.buildFilter(spec.getFilter(), spec.getInterval());
        final VectorOffset baseOffset = new NoFilterVectorOffset(
            spec.getQueryContext().getVectorSize(),
            0,
            frame.numRows()
        );
        final ColumnCache columnCache = new ColumnCache(index, closer);

        // baseSelectorFactory using baseOffset is the column selector for filtering.
        final VectorColumnSelectorFactory baseSelectorFactory = new QueryableIndexVectorColumnSelectorFactory(
            index,
            baseOffset,
            columnCache,
            spec.getVirtualColumns()
        );

        if (filterToUse == null) {
          return new FrameVectorCursor(baseOffset, baseSelectorFactory);
        } else {
          final VectorValueMatcher matcher = filterToUse.makeVectorMatcher(baseSelectorFactory);
          final FilteredVectorOffset filteredOffset = FilteredVectorOffset.create(
              baseOffset,
              matcher
          );

          final VectorColumnSelectorFactory filteredSelectorFactory = new QueryableIndexVectorColumnSelectorFactory(
              index,
              filteredOffset,
              columnCache,
              spec.getVirtualColumns()
          );

          return new FrameVectorCursor(filteredOffset, filteredSelectorFactory);
        }
      }

      @Override
      public void close()
      {
        CloseableUtils.closeAndWrapExceptions(closer);
      }
    };
  }

  @Override
  public RowSignature getRowSignature()
  {
    return signature;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    final int columnNumber = signature.indexOf(column);

    if (columnNumber < 0) {
      return null;
    } else {
      // Better than frameReader.frameSignature().getColumnCapabilities(columnName), because this method has more
      // insight into what's actually going on with this column (nulls, multivalue, etc).
      return columnReaders.get(columnNumber).readColumn(frame).getCapabilities();
    }
  }
}
