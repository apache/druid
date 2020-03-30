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

import org.joda.time.DateTime;

/**
 * Cursor is an interface for iteration over a range of data points, used during query execution. {@link
 * QueryableIndexCursorSequenceBuilder.QueryableIndexCursor} is an implementation for historical segments, and {@link
 * org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter.IncrementalIndexCursor} is an implementation for {@link
 * org.apache.druid.segment.incremental.IncrementalIndex}.
 *
 * Cursor is conceptually similar to {@link TimeAndDimsPointer}, but the latter is used for historical segment creation
 * rather than query execution (as Cursor). If those abstractions could be collapsed (and if it is worthwhile) is yet to
 * be determined.
 *
 * @see org.apache.druid.segment.vector.VectorCursor, the vectorized version
 */
public interface Cursor
{
  ColumnSelectorFactory getColumnSelectorFactory();
  DateTime getTime();
  void advance();
  void advanceUninterruptibly();
  boolean isDone();
  boolean isDoneOrInterrupted();
  void reset();
}
