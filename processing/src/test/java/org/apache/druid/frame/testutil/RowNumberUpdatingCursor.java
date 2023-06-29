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

package org.apache.druid.frame.testutil;

import org.apache.druid.frame.util.SettableLongVirtualColumn;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.joda.time.DateTime;

/**
 * Used by {@link FrameTestUtil#readRowsFromAdapter} and {@link FrameTestUtil#readRowsFromCursor}.
 */
public class RowNumberUpdatingCursor implements Cursor
{
  private final Cursor baseCursor;
  private final SettableLongVirtualColumn rowNumberVirtualColumn;

  RowNumberUpdatingCursor(Cursor baseCursor, SettableLongVirtualColumn rowNumberVirtualColumn)
  {
    this.baseCursor = baseCursor;
    this.rowNumberVirtualColumn = rowNumberVirtualColumn;
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return baseCursor.getColumnSelectorFactory();
  }

  @Override
  public DateTime getTime()
  {
    return baseCursor.getTime();
  }

  @Override
  public void advance()
  {
    rowNumberVirtualColumn.setValue(rowNumberVirtualColumn.getValue() + 1);
    baseCursor.advance();
  }

  @Override
  public void advanceUninterruptibly()
  {
    rowNumberVirtualColumn.setValue(rowNumberVirtualColumn.getValue() + 1);
    baseCursor.advanceUninterruptibly();
  }

  @Override
  public boolean isDone()
  {
    return baseCursor.isDone();
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return baseCursor.isDoneOrInterrupted();
  }

  @Override
  public void reset()
  {
    rowNumberVirtualColumn.setValue(0);
    baseCursor.reset();
  }
}
