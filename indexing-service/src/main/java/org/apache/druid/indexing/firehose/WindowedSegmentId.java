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
import com.google.common.base.Preconditions;
import org.joda.time.Interval;

import java.util.List;

/**
 * A WindowedSegment represents a segment plus the list of intervals inside it which contribute to a timeline.
 * <p>
 * This class is intended for serialization in specs.
 */
public class WindowedSegmentId
{
  // This is of the form used by SegmentId.
  private final String segmentId;
  private final List<Interval> intervals;

  @JsonCreator
  public WindowedSegmentId(
      @JsonProperty("segmentId") String segmentId,
      @JsonProperty("intervals") List<Interval> intervals
  )
  {
    this.segmentId = Preconditions.checkNotNull(segmentId, "null segmentId");
    this.intervals = Preconditions.checkNotNull(intervals, "null intervals");
  }

  @JsonProperty
  public String getSegmentId()
  {
    return segmentId;
  }

  @JsonProperty
  public List<Interval> getIntervals()
  {
    return intervals;
  }
}
