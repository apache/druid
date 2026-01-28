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

package org.apache.druid.segment.projections;

import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.RemapColumnSelectorFactory;
import org.apache.druid.segment.vector.RemapVectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import java.util.Map;

/**
 * Represents a projection of some base table available to use to build a {@link org.apache.druid.segment.CursorHolder}
 * by a {@link org.apache.druid.segment.CursorFactory}.
 * <p>
 * Projections are a type of invisible materialized view stored inside of a {@link org.apache.druid.segment.Segment}
 * which can be automatically used if they match the {@link CursorBuildSpec} argument passed to
 * {@link org.apache.druid.segment.CursorFactory#makeCursorHolder(CursorBuildSpec)}.
 * <p>
 * In the most basic sense, a projection consists of:
 *    - the actual underlying projection rows ({@link #rowSelector})
 *    - a mapping of {@link CursorBuildSpec} columns to underlying projection columns ({@link #remapColumns})
 *    - and a modified {@link CursorBuildSpec} ({@link #cursorBuildSpec})
 * <p>
 * The {@link #getRowSelector()} and {@link #getCursorBuildSpec()} methods can be used by a
 * {@link org.apache.druid.segment.CursorFactory} to build a {@link org.apache.druid.segment.CursorHolder} for the
 * projection instead of the base table, and {@link #wrapColumnSelectorFactory(ColumnSelectorFactory)} and
 * {@link #wrapVectorColumnSelectorFactory(VectorColumnSelectorFactory)} can be used to decorate the selector factories
 * constructed by that {@link org.apache.druid.segment.CursorHolder} whenever it builds a
 * {@link org.apache.druid.segment.Cursor} or {@link org.apache.druid.segment.vector.VectorCursor} to ensure that all
 * the selectors needed to satisfy the original {@link CursorBuildSpec} are available at the correct names.
 * 
 * @see org.apache.druid.segment.QueryableIndexCursorFactory#makeCursorHolder(CursorBuildSpec)
 * @see org.apache.druid.segment.incremental.IncrementalIndexCursorFactory#makeCursorHolder(CursorBuildSpec)
 */
public class QueryableProjection<T>
{
  private final CursorBuildSpec cursorBuildSpec;
  private final Map<String, String> remapColumns;
  private final T rowSelector;

  public QueryableProjection(
      CursorBuildSpec cursorBuildSpec,
      Map<String, String> remapColumns,
      T rowSelector
  )
  {
    this.cursorBuildSpec = cursorBuildSpec;
    this.remapColumns = remapColumns;
    this.rowSelector = rowSelector;
  }

  /**
   * The original {@link CursorBuildSpec} of a query can be modified if a projection matches the query, such as removing
   * virtual columns which have already been pre-computed.
   */
  public CursorBuildSpec getCursorBuildSpec()
  {
    return cursorBuildSpec;
  }

  /**
   * The projection can contain pre-computed virtual columns or pre-aggregated aggregation columns. At query time,
   * these are remapped to match the desired names for all equivalent components of the {@link CursorBuildSpec}.
   * <p>
   * For example, if the original {@link CursorBuildSpec} has a sum aggregator named 'sum_x' which takes a field 'x'
   * as input, and an equivalent sum aggregation exists on the projection with the name 'xsum' built from the base table
   * column 'x', the wrapped column selector factory will make 'xsum' available as 'sum_x', allowing the query to
   * use the combining aggregator instead of processing the base table for column 'x'.
   */
  public ColumnSelectorFactory wrapColumnSelectorFactory(ColumnSelectorFactory selectorFactory)
  {
    return new RemapColumnSelectorFactory(
        selectorFactory,
        remapColumns
    );
  }

  /**
   * The projection can contain pre-computed virtual columns or pre-aggregated aggregation columns. At query time,
   * these are remapped to match the desired names for all equivalent components of the {@link CursorBuildSpec}
   * <p>
   * For example, if the original {@link CursorBuildSpec} has a sum aggregator named 'sum_x' which takes a field 'x'
   * as input, and an equivalent sum aggregation exists on the projection with the name 'xsum' built from the base table
   * column 'x', the wrapped column selector factory will make 'xsum' available as 'sum_x', allowing the query to
   * use the combining aggregator instead of processing the base table for column 'x'.
   */
  public VectorColumnSelectorFactory wrapVectorColumnSelectorFactory(VectorColumnSelectorFactory selectorFactory)
  {
    return new RemapVectorColumnSelectorFactory(selectorFactory, remapColumns);
  }

  /**
   * Backing storage for the rows of the projection as is appropriate for the type of
   * {@link org.apache.druid.segment.CursorFactory}
   */
  public T getRowSelector()
  {
    return rowSelector;
  }
}
