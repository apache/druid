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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A WindowedSegment represents a segment plus the list of intervals inside it which contribute to a timeline.
 * If the list of intervals is null, the entire segment contributes to the timeline.
 *
 * This class is intended for serialization in specs. The class WindowedSegment is similar but contains
 * a full DataSegment instead of just an ID.
 */
public class WindowedSegmentId
{
  // This is of the form used by SegmentId.
  private final String segmentId;
  @Nullable
  private final List<Interval> intervals;

  @JsonCreator
  public WindowedSegmentId(
      @JsonProperty("segmentId") String segmentId,
      @JsonProperty("intervals") @Nullable List<Interval> intervals
  )
  {
    this.segmentId = segmentId;
    this.intervals = intervals;
    // FIXME validate that intervals are sorted, or sort them now?
  }

  @JsonProperty
  public String getSegmentId()
  {
    return segmentId;
  }

  @JsonProperty
  @Nullable
  public List<Interval> getIntervals()
  {
    return intervals;
  }
}
