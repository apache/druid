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

package org.apache.druid.extensions.watermarking.gaps;

import org.apache.druid.extensions.timeline.metadata.TimelineSegment;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;

public abstract class FilteredSegmentGapDetector extends GapDetector
{
  protected DateTime gapStart;

  public FilteredSegmentGapDetector(String datasource, Interval interval)
  {
    super(datasource, interval);
  }

  @Override
  public void reset()
  {
    this.cursorTimestamp = interval.getStart();
    this.gaps = new ArrayList<>();
  }

  @Override
  public boolean advance(TimelineSegment segmentDetails)
  {
    if (gapStart != null) {
      if (isValid(segmentDetails)) {
        // if in the 'gap' state and we hit a valid segment, close the gap
        gaps.add(new Interval(gapStart, segmentDetails.getInterval().getStart()));
        gapStart = null;
      }
    } else {
      if (!isValid(segmentDetails)) {
        // if this segment is invalid, we start a gap from the previous cursorTimestamp (end point of previous segment)
        gapStart = DateTimes.utc(cursorTimestamp.getMillis());
      } else if (!isContiguous(segmentDetails)) {
        // or make a 'short' gap if two valid segments are not contiguous
        gaps.add(new Interval(cursorTimestamp, segmentDetails.getInterval().getStart()));
      }
    }
    cursorTimestamp = segmentDetails.getInterval().getEnd();
    return true;
  }

  protected boolean isValid(TimelineSegment segment)
  {
    return true;
  }
}
