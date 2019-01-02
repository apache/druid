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

package org.apache.druid.query.groupby.epinephelinae;

import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.Closeable;
import java.nio.ByteBuffer;

/**
 * Like a {@link Grouper}, but vectorized. Keys are always int arrays, so there is no generic type parameter KeyType.
 * <p>
 * This interface is designed such that an implementation can implement both Grouper and VectorGrouper. Of course,
 * it would generally only make sense for a particular instance to be called with one set of functionality or the
 * other.
 */
public interface VectorGrouper extends Closeable
{
  /**
   * Initialize the grouper. This method needs to be called before calling {@link #aggregateVector}.
   */
  void initVectorized(int maxVectorSize);

  /**
   * Aggregate the current vector of rows from "startVectorOffset" to "endVectorOffset" using the provided keys.
   *
   * @param keySpace array holding keys, chunked into ints. First (endVectorOffset - startVectorOffset) keys
   *                 must be valid.
   * @param startRow row to start at (inclusive).
   * @param endRow   row to end at (exclusive).
   *
   * @return result that indicates how many keys were aggregated (may be partial due to resource limits)
   */
  AggregateResult aggregateVector(int[] keySpace, int startRow, int endRow);

  /**
   * Reset the grouper to its initial state.
   */
  void reset();

  /**
   * Close the grouper and release associated resources.
   */
  @Override
  void close();

  /**
   * Iterate through entries.
   * <p>
   * Some implementations allow writes even after this method is called.  After you are done with the iterator
   * returned by this method, you should either call {@link #close()} (if you are done with the VectorGrouper) or
   * {@link #reset()} (if you want to reuse it).
   * <p>
   * Callers must process and discard the returned {@link Grouper.Entry}s immediately, because the keys may
   * be reused.
   *
   * @return entry iterator
   */
  CloseableIterator<Grouper.Entry<ByteBuffer>> iterator();
}
