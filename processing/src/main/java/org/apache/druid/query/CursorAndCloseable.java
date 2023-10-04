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

package org.apache.druid.query;

import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.io.IOException;

/**
 * {@link Cursor} that also has a {@link #close()}. Used for cursors that hold some resources and need to be closed by
 * the caller
 */
public interface CursorAndCloseable extends Cursor, Closeable
{
  static CursorAndCloseable create(Cursor cursor, Closeable closeable)
  {
    return new CursorAndCloseable()
    {
      @Override
      public void close() throws IOException
      {
        closeable.close();
      }

      @Override
      public ColumnSelectorFactory getColumnSelectorFactory()
      {
        return cursor.getColumnSelectorFactory();
      }

      @Override
      public DateTime getTime()
      {
        return cursor.getTime();
      }

      @Override
      public void advance()
      {
        cursor.advance();
      }

      @Override
      public void advanceUninterruptibly()
      {
        cursor.advanceUninterruptibly();
      }

      @Override
      public boolean isDone()
      {
        return cursor.isDone();
      }

      @Override
      public boolean isDoneOrInterrupted()
      {
        return cursor.isDoneOrInterrupted();
      }

      @Override
      public void reset()
      {
        cursor.reset();
      }
    };
  }
}
