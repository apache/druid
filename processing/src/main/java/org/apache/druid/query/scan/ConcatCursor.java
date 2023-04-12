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

package org.apache.druid.query.scan;

import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.joda.time.DateTime;

import java.util.List;

public class ConcatCursor implements Cursor
{

  private final List<Cursor> cursors;
  private int currentCursor;

  public ConcatCursor(
      List<Cursor> cursors
  )
  {
    this.cursors = cursors;
    currentCursor = 0;
    skipEmptyCursors();
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return cursors.get(currentCursor).getColumnSelectorFactory();
  }

  @Override
  public DateTime getTime()
  {
    return cursors.get(currentCursor).getTime();
  }

  @Override
  public void advance()
  {
    if (currentCursor < cursors.size()) {
      cursors.get(currentCursor).advance();
      if (cursors.get(currentCursor).isDone()) {
        ++currentCursor;
        skipEmptyCursors();
      }
    }
  }

  @Override
  public void advanceUninterruptibly()
  {
    if (currentCursor < cursors.size()) {
      cursors.get(currentCursor).advanceUninterruptibly();
      if (cursors.get(currentCursor).isDone()) {
        ++currentCursor;
        skipEmptyCursors();
      }
    }
  }

  @Override
  public boolean isDone()
  {
    return currentCursor == cursors.size();
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return isDone() || Thread.currentThread().isInterrupted();
  }

  @Override
  public void reset()
  {
    while (currentCursor >= 0) {
      if (currentCursor < cursors.size()) {
        cursors.get(currentCursor).reset();
      }
      --currentCursor;
    }
    currentCursor = 0;
    skipEmptyCursors();
  }

  // This method should be called whenever the currentCursor gets updated.
  private void skipEmptyCursors()
  {
    while (currentCursor < cursors.size() && cursors.get(currentCursor).isDone()) {
      ++currentCursor;
    }
  }
}
