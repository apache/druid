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

package org.apache.druid.indexing.firehose;

import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A WindowedSegment represents a segment plus the list of intervals inside it which contribute to a timeline.
 * If the list of intervals is null, the entire segment contributes to the timeline.
 *
 * This class is intended for in-memory use; WindowedSegmentId is better for serializing in specs.
 */
public class WindowedSegment
{
  private final DataSegment segment;
  @Nullable
  private final List<Interval> intervals;

  public WindowedSegment(
      DataSegment segment,
      @Nullable List<Interval> intervals
  )
  {
    this.segment = segment;
    this.intervals = intervals;
    // FIXME validate that intervals are sorted, or sort them now?
  }

  public DataSegment getSegment()
  {
    return segment;
  }

  public List<Interval> getIntervals()
  {
    if (intervals != null) {
      return intervals;
    }
    return Collections.singletonList(segment.getInterval());
  }

  public boolean hasExplicitIntervals()
  {
    return intervals != null;
  }

  public WindowedSegmentId toWindowedSegmentId()
  {
    return new WindowedSegmentId(segment.getId().toString(), intervals);
  }
}
