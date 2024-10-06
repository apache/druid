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

package org.apache.druid.frame.segment.row;

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.field.FieldReader;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameCursor;
import org.apache.druid.frame.segment.FrameCursorUtils;
import org.apache.druid.frame.segment.FrameFilteredOffset;
import org.apache.druid.frame.segment.columnar.ColumnarFrameCursorFactory;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.SimpleSettableOffset;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A {@link CursorFactory} implementation based on a single row-based {@link Frame}.
 *
 * This class is only used for row-based frames.
 *
 * @see ColumnarFrameCursorFactory the columnar version
 */
public class RowFrameCursorFactory implements CursorFactory
{
  private final Frame frame;
  private final FrameReader frameReader;
  private final List<FieldReader> fieldReaders;

  public RowFrameCursorFactory(
      final Frame frame,
      final FrameReader frameReader,
      final List<FieldReader> fieldReaders
  )
  {
    this.frame = FrameType.ROW_BASED.ensureType(frame);
    this.frameReader = frameReader;
    this.fieldReaders = fieldReaders;
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    // Frames are not self-describing as to their sort order, so we can't determine the sort order by looking at
    // the Frame object. We could populate this with information from the relevant ClusterBy, but that's not available
    // at this point in the code. It could be plumbed in at some point. For now, use an empty list.
    final List<OrderBy> ordering = Collections.emptyList();

    return new CursorHolder()
    {
      @Nullable
      @Override
      public Cursor asCursor()
      {
        final Filter filterToUse = FrameCursorUtils.buildFilter(spec.getFilter(), spec.getInterval());

        final SimpleSettableOffset baseOffset = new SimpleAscendingOffset(frame.numRows());

        final ColumnSelectorFactory columnSelectorFactory =
            spec.getVirtualColumns().wrap(
                new FrameColumnSelectorFactory(
                    frame,
                    frameReader.signature(),
                    fieldReaders,
                    new CursorFrameRowPointer(frame, baseOffset)
                )
            );

        final SimpleSettableOffset offset;
        if (filterToUse == null) {
          offset = baseOffset;
        } else {
          offset = new FrameFilteredOffset(baseOffset, columnSelectorFactory, filterToUse);
        }

        final FrameCursor cursor = new FrameCursor(offset, columnSelectorFactory);

        // Note: if anything closeable is ever added to this Sequence, make sure to update FrameProcessors.makeCursor.
        // Currently, it assumes that closing the Sequence does nothing.
        return cursor;
      }

      @Nullable
      @Override
      public List<OrderBy> getOrdering()
      {
        return ordering;
      }
    };
  }

  @Override
  public RowSignature getRowSignature()
  {
    return frameReader.signature();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return frameReader.signature().getColumnCapabilities(column);
  }
}
