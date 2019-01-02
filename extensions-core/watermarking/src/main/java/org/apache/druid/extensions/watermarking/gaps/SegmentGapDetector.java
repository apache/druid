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
import org.joda.time.Interval;

public class SegmentGapDetector extends GapDetector
{
  public static final String GAP_TYPE = "data";

  public SegmentGapDetector(String datasource, Interval interval)
  {
    super(datasource, interval);
  }

  @Override
  public boolean advance(TimelineSegment segmentDetails)
  {
    if (!isContiguous(segmentDetails)) {
      gaps.add(new Interval(cursorTimestamp, segmentDetails.getInterval().getStart()));
    }
    cursorTimestamp = segmentDetails.getInterval().getEnd();
    return true;
  }

  @Override
  public String getType()
  {
    return GAP_TYPE;
  }
}
