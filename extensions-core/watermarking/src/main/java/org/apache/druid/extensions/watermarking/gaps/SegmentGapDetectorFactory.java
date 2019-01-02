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

import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.joda.time.Interval;

import java.util.List;

public class SegmentGapDetectorFactory implements GapDetectorFactory
{
  @Override
  public String getGapType()
  {
    return SegmentGapDetector.GAP_TYPE;
  }

  @Override
  public GapDetector build(
      String dataSource,
      Interval interval,
      List<TimelineObjectHolder<String, ServerSelector>> segments,
      boolean isCompleteTimeline
  )
  {
    if (segments.size() > 1) {
      Interval timelineInterval = new Interval(
          segments.get(0).getInterval().getStart(),
          segments.get(segments.size() - 1).getInterval().getEnd()
      );
      return new SegmentGapDetector(dataSource, timelineInterval);
    }
    return null;
  }
}
