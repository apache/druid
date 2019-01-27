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

package org.apache.druid.client;

import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DataSegmentInterner
{

  private final ConcurrentMap<SegmentId, DataSegment> knownSegments = new ConcurrentHashMap<>();

  /**
   * This method will return a reference to already existing segment, if an equal and better segment exists.
   * A segment is considered better if it has more attributes set. A segment gets updated when
   * it moves from realtime to historical, it learns what its `size` and `dimensions`,
   * so size is a good indicator, if a segment has more attributes set.
   */
  public DataSegment replaceWithBetterSegmentIfPresent(DataSegment segment)
  {
    final DataSegment alreadyExistingSegment = knownSegments.get(segment.getId());
    if (alreadyExistingSegment == null) {
      knownSegments.put(segment.getId(), segment);
      return segment;
    }

    if (alreadyExistingSegment.trueEquals(segment)) {
      return alreadyExistingSegment;
    } else {
      if (alreadyExistingSegment.equals(segment) && alreadyExistingSegment.getSize() > segment.getSize()) {
        return alreadyExistingSegment;
      } else {
        return segment;
      }
    }
  }

}
