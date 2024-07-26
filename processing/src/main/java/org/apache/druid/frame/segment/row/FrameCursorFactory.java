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
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorMaker;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.SimpleDescendingOffset;
import org.apache.druid.segment.SimpleSettableOffset;

import java.util.List;

/**
 * A {@link CursorFactory} implementation based on a single row-based {@link Frame}.
 *
 * This class is only used for row-based frames.
 *
 * @see org.apache.druid.frame.segment.columnar.FrameCursorFactory the columnar version
 */
public class FrameCursorFactory implements CursorFactory
{
  private final Frame frame;
  private final FrameReader frameReader;
  private final List<FieldReader> fieldReaders;

  public FrameCursorFactory(
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
  public CursorMaker asCursorMaker(CursorBuildSpec spec)
  {
    if (!Granularities.ALL.equals(spec.getGranularity())) {
      // Not currently needed for the intended use cases of frame-based cursors.
      throw new UOE("Granularity [%s] not supported", spec.getGranularity());
    }
    return () -> {
      final Filter filterToUse = FrameCursorUtils.buildFilter(spec.getFilter(), spec.getInterval());

      final SimpleSettableOffset baseOffset = spec.isDescending()
                                              ? new SimpleDescendingOffset(frame.numRows())
                                              : new SimpleAscendingOffset(frame.numRows());

      final SimpleSettableOffset offset;

      final ColumnSelectorFactory columnSelectorFactory =
          spec.getVirtualColumns().wrap(
              new FrameColumnSelectorFactory(
                  frame,
                  frameReader.signature(),
                  fieldReaders,
                  new CursorFrameRowPointer(frame, baseOffset)
              )
          );

      if (filterToUse == null) {
        offset = baseOffset;
      } else {
        offset = new FrameFilteredOffset(baseOffset, columnSelectorFactory, filterToUse);
      }

      final FrameCursor cursor = new FrameCursor(offset, columnSelectorFactory);

      // Note: if anything closeable is ever added to this Sequence, make sure to update FrameProcessors.makeCursor.
      // Currently, it assumes that closing the Sequence does nothing.
      return cursor;
    };
  }
}
