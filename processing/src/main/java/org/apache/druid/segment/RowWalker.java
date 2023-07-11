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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.function.ToLongFunction;

/**
 * Used by {@link RowBasedStorageAdapter} and {@link RowBasedCursor} to walk through rows. It allows multiple
 * {@link RowBasedCursor} to share the same underlying Iterable.
 */
public class RowWalker<T>
{
  private final Sequence<T> rowSequence;
  private final ToLongFunction<T> timestampFunction;

  @Nullable // null = closed
  private Yielder<T> rowYielder;

  RowWalker(final Sequence<T> rowSequence, final RowAdapter<T> rowAdapter)
  {
    this.rowSequence = rowSequence;
    this.timestampFunction = rowAdapter.timestampFunction();
    this.rowYielder = Yielders.each(rowSequence);
  }

  public boolean isDone()
  {
    return rowYielder == null || rowYielder.isDone();
  }

  public T currentRow()
  {
    if (isDone()) {
      throw new ISE("cannot call currentRow when isDone == true");
    }

    return rowYielder.get();
  }

  public void advance()
  {
    if (isDone()) {
      throw new ISE("cannot call advance when isDone == true");
    } else {
      rowYielder = rowYielder.next(null);
    }
  }

  public void reset()
  {
    close();
    rowYielder = Yielders.each(rowSequence);
  }

  public void skipToDateTime(final DateTime timestamp, final boolean descending)
  {
    while (!isDone() && (descending
                         ? timestamp.isBefore(timestampFunction.applyAsLong(rowYielder.get()))
                         : timestamp.isAfter(timestampFunction.applyAsLong(rowYielder.get())))) {
      advance();
    }
  }

  public void close()
  {
    if (rowYielder != null) {
      try {
        rowYielder.close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      finally {
        rowYielder = null;
      }
    }
  }
}
