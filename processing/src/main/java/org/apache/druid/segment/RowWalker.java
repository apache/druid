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

import com.google.common.base.Preconditions;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.function.ToLongFunction;

/**
 * Used by {@link RowBasedStorageAdapter} and {@link RowBasedCursor} to walk through rows. It allows multiple
 * {@link RowBasedCursor} to share the same underlying Iterable.
 */
public class RowWalker<T>
{
  private final Iterable<T> rowIterable;
  private final ToLongFunction<T> timestampFunction;

  private Iterator<T> rowIterator;

  @Nullable
  private T current = null;

  RowWalker(final Iterable<T> rowIterable, final RowAdapter<T> rowAdapter)
  {
    this.rowIterable = rowIterable;
    this.timestampFunction = rowAdapter.timestampFunction();

    reset();
  }

  public boolean isDone()
  {
    return current == null;
  }

  public T currentRow()
  {
    return Preconditions.checkNotNull(current, "cannot call currentRow when isDone == true");
  }

  public void advance()
  {
    if (rowIterator.hasNext()) {
      current = rowIterator.next();

      if (current == null) {
        throw new NullPointerException("null row encountered in walker");
      }
    } else {
      current = null;
    }
  }

  public void reset()
  {
    rowIterator = rowIterable.iterator();
    advance();
  }

  public void skipToDateTime(final DateTime timestamp, final boolean descending)
  {
    while (current != null && (descending
                               ? timestamp.isBefore(timestampFunction.applyAsLong(current))
                               : timestamp.isAfter(timestampFunction.applyAsLong(current)))) {
      advance();
    }
  }
}
