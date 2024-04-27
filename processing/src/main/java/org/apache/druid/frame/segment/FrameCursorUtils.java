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

import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryContexts;
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
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FrameCursorUtils
{

  /**
   * Exception to be thrown when the subquery's rows are too wide to fit in a single frame. In such case, byte based
   * limiting should be disabled or the user should modify the query.
   * <p>
   * NOTE: This error message is not appropriate when a similar exception is hit in MSQ, since this workaround
   * is not applicable in that scenario
   */
  public static final DruidException SUBQUERY_ROW_TOO_LARGE_EXCEPTION =
      DruidException
          .forPersona(DruidException.Persona.OPERATOR)
          .ofCategory(DruidException.Category.CAPACITY_EXCEEDED)
          .build(
              "Subquery's row size exceeds the frame size and therefore cannot write the subquery's "
              + "row to the frame. Either modify the subqueries to materialize smaller rows by removing wide columns, "
              + "or disable byte based limiting by setting '%s' to 'disabled'",
              QueryContexts.MAX_SUBQUERY_BYTES_KEY
          );

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
   * Writes a {@link Cursor} to a sequence of {@link Frame}. This method iterates over the rows of the cursor,
   * and writes the columns to the frames. The iterable is lazy, and it traverses the required portion of the cursor
   * as required
   */
  public static Iterable<Frame> cursorToFramesIterable(
      final Cursor cursor,
      final FrameWriterFactory frameWriterFactory
  )
  {
    return () -> new Iterator<Frame>()
    {
      @Override
      public boolean hasNext()
      {
        return !cursor.isDone();
      }

      @Override
      public Frame next()
      {
        // Makes sure that cursor contains some elements prior. This ensures if no row is written, then the row size
        // is larger than the MemoryAllocators returned by the provided factory
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        boolean firstRowWritten = false;
        Frame frame;
        try (final FrameWriter frameWriter = frameWriterFactory.newFrameWriter(cursor.getColumnSelectorFactory())) {
          while (!cursor.isDone()) {
            if (!frameWriter.addSelection()) {
              break;
            }
            firstRowWritten = true;
            cursor.advance();
          }

          if (!firstRowWritten) {
            throw SUBQUERY_ROW_TOO_LARGE_EXCEPTION;
          }

          frame = Frame.wrap(frameWriter.toByteArray());
        }
        return frame;
      }
    };
  }

  /**
   * Writes a {@link Cursor} to a sequence of {@link Frame}. This method iterates over the rows of the cursor,
   * and writes the columns to the frames
   *
   * @param cursor             Cursor to write to the frame
   * @param frameWriterFactory Frame writer factory to write to the frame.
   *                           It also determines the signature of the rows that are written to the frames
   */
  public static Sequence<Frame> cursorToFramesSequence(
      final Cursor cursor,
      final FrameWriterFactory frameWriterFactory
  )
  {

    return Sequences.simple(cursorToFramesIterable(cursor, frameWriterFactory));
  }
}
