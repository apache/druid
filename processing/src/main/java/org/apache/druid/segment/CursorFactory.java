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

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.vector.VectorCursor;
import org.joda.time.Interval;

import javax.annotation.Nullable;

/**
 * Interface extended by {@link StorageAdapter}, which gives them the power to create cursors.
 *
 * @see StorageAdapter
 *
 * @deprecated This interface is deprecated and no longer implemented by any built-in {@link StorageAdapter}. Callers
 * should use {@link CursorHolderFactory#makeCursorHolder(CursorBuildSpec)} instead. Implementors should implement
 * {@link CursorHolderFactory#makeCursorHolder(CursorBuildSpec)} instead.
 */
@Deprecated
public interface CursorFactory
{
  /**
   * Returns true if the provided combination of parameters can be handled by "makeVectorCursor".
   *
   * Query engines should use this before running in vectorized mode, and be prepared to fall back to non-vectorized
   * mode if this method returns false.
   *
   * @deprecated Callers should use {@link CursorHolderFactory#makeCursorHolder(CursorBuildSpec)} and call
   * {@link CursorHolder#canVectorize()}.
   * Implementors should implement {@link CursorHolderFactory#makeCursorHolder(CursorBuildSpec)} instead.  This method is
   * no longer implemented by any built-in factories.
   */
  @Deprecated
  default boolean canVectorize(
      @Nullable Filter filter,
      VirtualColumns virtualColumns,
      boolean descending
  )
  {
    throw DruidException.defensive(
        "CursorFactory.canVectorize is no longer supported, use CursorHolderFactory.makeCursorHolder instead"
    );
  }

  /**
   * Creates a sequence of Cursors, one for each time-granular bucket (based on the provided Granularity).
   *
   * @deprecated Callers should use {@link CursorHolderFactory#makeCursorHolder(CursorBuildSpec)} and call
   * {@link CursorHolder#asCursor()}.
   * Implementors should implement {@link CursorHolderFactory#makeCursorHolder(CursorBuildSpec)} instead.
   * This method is no longer implemented by any built-in factories.
   */
  @Deprecated
  default Sequence<Cursor> makeCursors(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      Granularity gran,
      boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    throw DruidException.defensive(
        "CursorFactory.makeCursors is no longer supported, use CursorHolderFactory.makeCursorHolder instead"
    );
  }

  /**
   * Creates a VectorCursor. Unlike the Cursor returned by "makeCursor", there is just one of these. Hence, this method
   * does not take a "granularity" parameter. Before calling this method, check "canVectorize" to see if the call you
   * are about to make will throw an error or not.
   *
   * Returns null if there is no data to walk over (for example, if the "interval" does not overlap the data interval
   * of this segment).
   *
   * @deprecated Callers should use {@link CursorHolderFactory#makeCursorHolder(CursorBuildSpec)} and call
   * {@link CursorHolder#asVectorCursor()}. Implementors should implement
   * {@link CursorHolderFactory#makeCursorHolder(CursorBuildSpec)} instead. This method is no longer implemented by any
   * built-in factories.
   */
  @Deprecated
  @Nullable
  default VectorCursor makeVectorCursor(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      boolean descending,
      int vectorSize,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    throw DruidException.defensive(
        "CursorFactory.makeVectorCursor is no longer supported, use CursorHolderFactory.makeCursorHolder instead"
    );
  }
}
