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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.vector.VectorCursor;
import org.joda.time.Interval;

import javax.annotation.Nullable;

/**
 * Interface extended by {@link StorageAdapter}, which gives them the power to create cursors.
 *
 * @see StorageAdapter
 */
public interface CursorFactory
{
  default CursorMaker asCursorMaker(CursorBuildSpec spec)
  {

    return new CursorMaker()
    {
      @Override
      public boolean canVectorize()
      {
        return CursorFactory.this.canVectorize(spec.getFilter(), spec.getVirtualColumns(), spec.isDescending());
      }

      @Override
      public Sequence<Cursor> makeCursors()
      {
        return CursorFactory.this.makeCursors(
            spec.getFilter(),
            spec.getInterval(),
            spec.getVirtualColumns(),
            spec.getGranularity(),
            spec.isDescending(),
            spec.getQueryMetrics()
        );
      }

      @Override
      public VectorCursor makeVectorCursor()
      {
        return CursorFactory.this.makeVectorCursor(
            spec.getFilter(),
            spec.getInterval(),
            spec.getVirtualColumns(),
            spec.isDescending(),
            spec.getQueryContext().getVectorSize(),
            spec.getQueryMetrics()
        );
      }
    };
  }

  /**
   * Returns true if the provided combination of parameters can be handled by "makeVectorCursor".
   *
   * Query engines should use this before running in vectorized mode, and be prepared to fall back to non-vectorized
   * mode if this method returns false.
   *
   * @deprecated Callers should use {@link #asCursorMaker(CursorBuildSpec)} and call {@link CursorMaker#canVectorize()}.
   * Implementors should implement {@link #asCursorMaker(CursorBuildSpec)} instead.
   */
  @Deprecated
  default boolean canVectorize(
      @Nullable Filter filter,
      VirtualColumns virtualColumns,
      boolean descending
  )
  {
    return false;
  }

  /**
   * Creates a sequence of Cursors, one for each time-granular bucket (based on the provided Granularity).
   *
   * @deprecated Callers should use {@link #asCursorMaker(CursorBuildSpec)} and call {@link CursorMaker#makeCursors()}.
   * Implementors should implement {@link #asCursorMaker(CursorBuildSpec)} instead. Recommend for implementors to fill
   * this method in with:
   * <pre>
   *     final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
   *                                                      .setFilter(filter)
   *                                                      .setInterval(interval)
   *                                                      .setGranularity(gran)
   *                                                      .setVirtualColumns(virtualColumns)
   *                                                      .isDescending(descending)
   *                                                      .setQueryMetrics(queryMetrics)
   *                                                      .build();
   *     return asCursorMaker(buildSpec).makeCursors();
   * </pre>
   */
  @Deprecated
  Sequence<Cursor> makeCursors(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      Granularity gran,
      boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  );

  /**
   * Creates a VectorCursor. Unlike the Cursor returned by "makeCursor", there is just one of these. Hence, this method
   * does not take a "granularity" parameter. Before calling this method, check "canVectorize" to see if the call you
   * are about to make will throw an error or not.
   *
   * Returns null if there is no data to walk over (for example, if the "interval" does not overlap the data interval
   * of this segment).
   *
   * @deprecated Callers should use {@link #asCursorMaker(CursorBuildSpec)} and call
   * {@link CursorMaker#makeVectorCursor()}. Implementors should implement {@link #asCursorMaker(CursorBuildSpec)}
   * instead.
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
    throw new UnsupportedOperationException("Cannot vectorize. Check 'canVectorize' before calling 'makeVectorCursor'.");
  }

  default Sequence<Cursor> delegateMakeCursorToMaker(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      Granularity gran,
      boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    return asCursorMaker(
        CursorBuildSpec.builder()
                       .setFilter(filter)
                       .setInterval(interval)
                       .setVirtualColumns(virtualColumns)
                       .setGranularity(gran)
                       .isDescending(descending)
                       .setQueryMetrics(queryMetrics)
                       .build()
    ).makeCursors();
  }

  default VectorCursor delegateMakeVectorCursorToMaker(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      boolean descending,
      int vectorSize,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setFilter(filter)
                                                     .setInterval(interval)
                                                     .setVirtualColumns(virtualColumns)
                                                     .isDescending(descending)
                                                     .setQueryContext(
                                                         QueryContext.of(
                                                             ImmutableMap.of(QueryContexts.VECTOR_SIZE_KEY, vectorSize)
                                                         )
                                                     )
                                                     .setQueryMetrics(queryMetrics)
                                                     .build();
    return asCursorMaker(buildSpec).makeVectorCursor();
  }
}
