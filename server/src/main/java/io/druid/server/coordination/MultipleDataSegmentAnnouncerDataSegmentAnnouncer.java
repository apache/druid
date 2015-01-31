/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.coordination;

import io.druid.timeline.DataSegment;

import java.io.IOException;

/**
 * This class has the greatest name ever
 */
public class MultipleDataSegmentAnnouncerDataSegmentAnnouncer implements DataSegmentAnnouncer
{
  private final Iterable<DataSegmentAnnouncer> dataSegmentAnnouncers;

  public MultipleDataSegmentAnnouncerDataSegmentAnnouncer(
      Iterable<DataSegmentAnnouncer> dataSegmentAnnouncers
  )
  {
    this.dataSegmentAnnouncers = dataSegmentAnnouncers;
  }

  @Override
  public void announceSegment(DataSegment segment) throws IOException
  {
    for (DataSegmentAnnouncer dataSegmentAnnouncer : dataSegmentAnnouncers) {
      dataSegmentAnnouncer.announceSegment(segment);
    }
  }

  @Override
  public void unannounceSegment(DataSegment segment) throws IOException
  {
    for (DataSegmentAnnouncer dataSegmentAnnouncer : dataSegmentAnnouncers) {
      dataSegmentAnnouncer.unannounceSegment(segment);
    }
  }

  @Override
  public void announceSegments(Iterable<DataSegment> segments) throws IOException
  {
    for (DataSegmentAnnouncer dataSegmentAnnouncer : dataSegmentAnnouncers) {
      dataSegmentAnnouncer.announceSegments(segments);
    }
  }

  @Override
  public void unannounceSegments(Iterable<DataSegment> segments) throws IOException
  {
    for (DataSegmentAnnouncer dataSegmentAnnouncer : dataSegmentAnnouncers) {
      dataSegmentAnnouncer.unannounceSegments(segments);
    }
  }
}
