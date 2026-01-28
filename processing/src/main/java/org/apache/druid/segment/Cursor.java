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

package org.apache.druid.segment;

import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.segment.incremental.IncrementalIndexCursorHolder;

/**
 * Cursor is an interface for iteration over a range of data points, used during query execution. Cursors are available
 * from {@link CursorFactory#makeCursorHolder(CursorBuildSpec)} via {@link CursorHolder#asCursor()}.
 * <p>
 * A typical usage pattern might look something like this:
 * <pre>
 *   try (CursorHolder cursorHolder = adapter.makeCursorHolder(...)) {
 *     Cursor cursor = cursorHolder.asCursor();
 *     ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();
 *     ColumnValueSelector timeSelector = factory.makeColumnValueSelector("__time");
 *     // ...
 *     while (!cursor.isDone()) {}
 *       long time = timeSelector.getLong();
 *       // do stuff with column values
 *       // ...
 *       cursor.advance();
 *     }
 *   }
 * </pre>
 *
 * {@link QueryableIndexCursorHolder.QueryableIndexCursor} is an implementation for historical segments, and
 * {@link IncrementalIndexCursorHolder.IncrementalIndexCursor} is an implementation for
 * {@link org.apache.druid.segment.incremental.IncrementalIndex}.
 * <p>
 * Cursor is conceptually similar to {@link TimeAndDimsPointer}, but the latter is used for historical segment creation
 * rather than query execution (as Cursor). If those abstractions could be collapsed (and if it is worthwhile) is yet to
 * be determined.
 *
 * @see org.apache.druid.segment.vector.VectorCursor, the vectorized version
 */
public interface Cursor
{
  /**
   * Get a {@link ColumnSelectorFactory} whose selectors will be backed by the row values at the current position of
   * the cursor
   */
  ColumnSelectorFactory getColumnSelectorFactory();

  /**
   * Advance the cursor to the next position, checking if thread has been interrupted after advancing and possibly
   * throwing {@link QueryInterruptedException} if so. Callers should check {@link #isDone()} or
   * {@link #isDoneOrInterrupted()} before getting the next value from a selector.
   */
  void advance();

  /**
   * Advance to the cursor to the next position. Callers should check {@link #isDone()} or
   * {@link #isDoneOrInterrupted()} before getting the next value from a selector. However, underlying
   * implementation may still check for thread interruption if advancing the cursor is a long-running operation.
   */
  void advanceUninterruptibly();

  /**
   * Check if the current cursor position is valid, returning false if there are values to read from selectors created
   * by {@link #getColumnSelectorFactory()}. If true, any such selectors will no longer produce values.
   */
  boolean isDone();

  /**
   * Check if the current cursor position is valid, or if the thread has been interrupted.
   *
   * @see #isDone()
   */
  boolean isDoneOrInterrupted();

  /**
   * Reset to start of cursor. Most cursor implementations are backed by immutable data, but there is generically no
   * guarantee that advancing through a cursor again will read exactly the same data or even number of rows, since the
   * underlying data might be mutable in some cases.
   */
  void reset();
}
