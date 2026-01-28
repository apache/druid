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

package org.apache.druid.segment.incremental;

import java.util.Comparator;
import java.util.Iterator;

/**
 * {@link IncrementalIndexRow} storage interface, a mutable data structure for building up a set or rows to eventually
 * persist into an immutable segment
 *
 * @see IncrementalIndex for the data processor which constructs {@link IncrementalIndexRow} to store here
 */
public interface FactsHolder
{
  /**
   * @return the previous rowIndex associated with the specified key, or
   * {@link IncrementalIndexRow#EMPTY_ROW_INDEX} if there was no mapping for the key.
   */
  int getPriorIndex(IncrementalIndexRow key);

  /**
   * Get minimum {@link IncrementalIndexRow#getTimestamp()} present in the facts holder
   */
  long getMinTimeMillis();

  /**
   * Get maximum {@link IncrementalIndexRow#getTimestamp()} present in the facts holder
   */
  long getMaxTimeMillis();

  /**
   * Get all {@link IncrementalIndex}, depending on the implementation, these rows may or may not be ordered in the same
   * order they will be persisted in. Use {@link #persistIterable()} if this is required.
   */
  Iterator<IncrementalIndexRow> iterator(boolean descending);

  /**
   * Get all {@link IncrementalIndexRow} with {@link IncrementalIndexRow#getTimestamp()} between the start and end
   * timestamps specified
   */
  Iterable<IncrementalIndexRow> timeRangeIterable(boolean descending, long timeStart, long timeEnd);

  /**
   * Get all row {@link IncrementalIndexRow} 'keys', which is distinct groups if this is an aggregating facts holder or
   * just every row present if not
   */
  Iterable<IncrementalIndexRow> keySet();

  /**
   * Get all {@link IncrementalIndexRow} to persist, ordered with {@link Comparator <IncrementalIndexRow>}
   */
  Iterable<IncrementalIndexRow> persistIterable();

  /**
   * @return the previous rowIndex associated with the specified key, or
   * {@link IncrementalIndexRow#EMPTY_ROW_INDEX} if there was no mapping for the key.
   */
  int putIfAbsent(IncrementalIndexRow key, int rowIndex);

  /**
   * Clear all rows present in the facts holder
   */
  void clear();
}
