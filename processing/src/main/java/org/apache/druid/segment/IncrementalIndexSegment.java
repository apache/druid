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

import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.segment.incremental.IncrementalIndexPhysicalSegmentInspector;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;

/**
 */
public class IncrementalIndexSegment implements Segment
{
  private final IncrementalIndex index;
  private final SegmentId segmentId;

  public IncrementalIndexSegment(IncrementalIndex index, SegmentId segmentId)
  {
    this.index = index;
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
    return index.getInterval();
  }

  @Override
  public QueryableIndex asQueryableIndex()
  {
    return null;
  }

  @Override
  public CursorFactory asCursorFactory()
  {
    return new IncrementalIndexCursorFactory(index);
  }

  @Nullable
  @Override
  public <T> T as(final Class<T> clazz)
  {
    if (TimeBoundaryInspector.class.equals(clazz)) {
      return (T) new IncrementalIndexTimeBoundaryInspector(index);
    } else if (MaxIngestedEventTimeInspector.class.equals(clazz)) {
      return (T) new IncrementalIndexMaxIngestedEventTimeInspector(index);
    } else if (Metadata.class.equals(clazz)) {
      return (T) index.getMetadata();
    } else if (PhysicalSegmentInspector.class.equals(clazz)) {
      return (T) new IncrementalIndexPhysicalSegmentInspector(index);
    } else if (TopNOptimizationInspector.class.equals(clazz)) {
      return (T) new SimpleTopNOptimizationInspector(true);
    } else {
      return Segment.super.as(clazz);
    }
  }

  @Override
  public void close()
  {
    index.close();
  }
}
