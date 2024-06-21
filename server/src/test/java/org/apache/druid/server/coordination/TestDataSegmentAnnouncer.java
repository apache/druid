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

package org.apache.druid.server.coordination;

import org.apache.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.List;

/**
 * A test data segment announcer that tracks the state of all segment announcements and unannouncements.
 */
public class TestDataSegmentAnnouncer extends NoopDataSegmentAnnouncer
{
  private final List<DataSegment> observedSegments;

  TestDataSegmentAnnouncer()
  {
    this.observedSegments = new ArrayList<>();
  }

  @Override
  public void announceSegment(DataSegment segment)
  {
    observedSegments.add(segment);
  }

  @Override
  public void unannounceSegment(DataSegment segment)
  {
    observedSegments.remove(segment);
  }

  @Override
  public void announceSegments(Iterable<DataSegment> segments)
  {
    for (DataSegment segment : segments) {
      observedSegments.add(segment);
    }
  }

  @Override
  public void unannounceSegments(Iterable<DataSegment> segments)
  {
    for (DataSegment segment : segments) {
      observedSegments.remove(segment);
    }
  }

  public List<DataSegment> getObservedSegments()
  {
    return observedSegments;
  }
}
