/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.curator.announcement.Announcer;
import com.metamx.druid.initialization.ZkPathsConfig;

import java.io.IOException;

/**
 * This class has the greatest name ever
 */
public class MultipleDataSegmentAnnouncerDataSegmentAnnouncer implements DataSegmentAnnouncer
{
  private final Iterable<AbstractDataSegmentAnnouncer> dataSegmentAnnouncers;

  public MultipleDataSegmentAnnouncerDataSegmentAnnouncer(
      Iterable<AbstractDataSegmentAnnouncer> dataSegmentAnnouncers
  )
  {
    this.dataSegmentAnnouncers = dataSegmentAnnouncers;
  }

  @LifecycleStart
  public void start()
  {
    for (AbstractDataSegmentAnnouncer dataSegmentAnnouncer : dataSegmentAnnouncers) {
      dataSegmentAnnouncer.start();
    }
  }

  @LifecycleStop
  public void stop()
  {
    for (AbstractDataSegmentAnnouncer dataSegmentAnnouncer : dataSegmentAnnouncers) {
      dataSegmentAnnouncer.stop();
    }
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
