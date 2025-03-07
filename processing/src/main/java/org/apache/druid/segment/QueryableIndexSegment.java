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

import org.apache.druid.common.semantic.SemanticCreator;
import org.apache.druid.common.semantic.SemanticUtils;
import org.apache.druid.query.rowsandcols.concrete.QueryableIndexRowsAndColumns;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.Function;

/**
 *
 */
public class QueryableIndexSegment implements Segment
{
  private static final Map<Class<?>, Function<QueryableIndexSegment, ?>> AS_MAP = SemanticUtils
      .makeAsMap(QueryableIndexSegment.class);

  private final QueryableIndex index;
  private final QueryableIndexCursorFactory cursorFactory;
  private final TimeBoundaryInspector timeBoundaryInspector;
  private final SegmentId segmentId;

  public QueryableIndexSegment(QueryableIndex index, final SegmentId segmentId)
  {
    this.index = index;
    this.timeBoundaryInspector = QueryableIndexTimeBoundaryInspector.create(index);
    this.cursorFactory = new QueryableIndexCursorFactory(index, timeBoundaryInspector);
    this.segmentId = segmentId;
  }

  @Override
  public SegmentId getId()
  {
    return segmentId;
  }

  @Override
  public Interval getDataInterval()
  {
    return index.getDataInterval();
  }

  @Override
  public QueryableIndex asQueryableIndex()
  {
    return index;
  }

  @Override
  public CursorFactory asCursorFactory()
  {
    return cursorFactory;
  }

  @Override
  public void close()
  {
    // this is kinda nasty because it actually unmaps the files and stuff too
    index.close();
  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public <T> T as(@Nonnull Class<T> clazz)
  {
    final Function<QueryableIndexSegment, ?> fn = AS_MAP.get(clazz);
    if (fn != null) {
      final T fnApply = (T) fn.apply(this);
      if (fnApply != null) {
        return fnApply;
      }
    }

    if (TimeBoundaryInspector.class.equals(clazz)) {
      return (T) timeBoundaryInspector;
    } else if (Metadata.class.equals(clazz)) {
      return (T) index.getMetadata();
    } else if (PhysicalSegmentInspector.class.equals(clazz)) {
      return (T) new QueryableIndexPhysicalSegmentInspector(index);
    } else if (TopNOptimizationInspector.class.equals(clazz)) {
      return (T) new SimpleTopNOptimizationInspector(true);
    }

    return Segment.super.as(clazz);
  }

  @SemanticCreator
  @SuppressWarnings("unused")
  public CloseableShapeshifter toCloseableShapeshifter()
  {
    return new QueryableIndexRowsAndColumns(index);
  }
}
