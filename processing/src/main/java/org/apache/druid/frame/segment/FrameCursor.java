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

import org.apache.druid.frame.segment.columnar.ColumnarFrameCursorFactory;
import org.apache.druid.frame.segment.row.RowFrameCursorFactory;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.SimpleSettableOffset;

/**
 * An implementation of {@link Cursor} used by {@link RowFrameCursorFactory}
 * and {@link ColumnarFrameCursorFactory}.
 *
 * Adds the methods {@link #getCurrentRow()} and {@link #setCurrentRow(int)} so the cursor can be moved to
 * particular rows.
 */
public class FrameCursor implements Cursor
{
  private final SimpleSettableOffset offset;
  private final ColumnSelectorFactory columnSelectorFactory;

  public FrameCursor(
      SimpleSettableOffset offset,
      ColumnSelectorFactory columnSelectorFactory
  )
  {
    this.offset = offset;
    this.columnSelectorFactory = columnSelectorFactory;
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return columnSelectorFactory;
  }

  @Override
  public void advance()
  {
    offset.increment();
    BaseQuery.checkInterrupted();
  }

  @Override
  public void advanceUninterruptibly()
  {
    offset.increment();
  }

  @Override
  public boolean isDone()
  {
    return !offset.withinBounds();
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return isDone() || Thread.currentThread().isInterrupted();
  }

  @Override
  public void reset()
  {
    offset.reset();
  }

  /**
   * Returns the current row number.
   */
  public int getCurrentRow()
  {
    return offset.getOffset();
  }

  /**
   * Moves this cursor to a particular row number.
   */
  public void setCurrentRow(final int rowNumber)
  {
    offset.setCurrentOffset(rowNumber);
  }
}
