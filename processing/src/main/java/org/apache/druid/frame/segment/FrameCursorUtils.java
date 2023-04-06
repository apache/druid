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

package org.apache.druid.frame.segment;

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.processor.FrameRowTooLargeException;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.BoundFilter;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;

public class FrameCursorUtils
{
  private FrameCursorUtils()
  {
    // No instantiation.
  }

  /**
   * Builds a {@link Filter} from a {@link Filter} plus an {@link Interval}. Useful when we want to do a time filter
   * on a frame, but can't push the time filter into the frame itself (perhaps because it isn't time-sorted).
   */
  @Nullable
  public static Filter buildFilter(@Nullable Filter filter, Interval interval)
  {
    if (Intervals.ETERNITY.equals(interval)) {
      return filter;
    } else {
      return Filters.and(
          Arrays.asList(
              new BoundFilter(
                  new BoundDimFilter(
                      ColumnHolder.TIME_COLUMN_NAME,
                      String.valueOf(interval.getStartMillis()),
                      String.valueOf(interval.getEndMillis()),
                      false,
                      true,
                      null,
                      null,
                      StringComparators.NUMERIC
                  )
              ),
              filter
          )
      );
    }
  }

  /**
   * Writes a {@link Cursor} to a {@link Frame}. This method iterates over the rows of the cursor, and writes the columns
   * to the cursor
   * @param cursor Cursor to write to the frame
   * @param frameWriterFactory Frame writer factory to write to the frame.
   *                           Determines the signature of the rows that are written to the frames
   * @param memoryLimitBytes Limit in bytes, if needs to be enforced while converting the cursor to the frame. If adding
   *                         a row causes the frame size to exceed this limit, we throw an {@link ResourceLimitExceededException}
   */
  public static Frame cursorToFrame(
      Cursor cursor,
      FrameWriterFactory frameWriterFactory,
      @Nullable Long memoryLimitBytes
  )
  {
    Frame frame;

    try (final FrameWriter frameWriter = frameWriterFactory.newFrameWriter(cursor.getColumnSelectorFactory())) {
      while (!cursor.isDone()) {
        if (!frameWriter.addSelection()) {
          throw new FrameRowTooLargeException(frameWriterFactory.allocatorCapacity());
        }

        if (memoryLimitBytes != null && memoryLimitBytes < frameWriter.getTotalSize()) {
          throw new ResourceLimitExceededException(
              StringUtils.format(
                  "The row limit exceeded the bytes allocated for the conversion (allocated = [%d] bytes)",
                  memoryLimitBytes
              )
          );
        }

        cursor.advance();
      }

      frame = Frame.wrap(frameWriter.toByteArray());
    }

    return frame;
  }
}
