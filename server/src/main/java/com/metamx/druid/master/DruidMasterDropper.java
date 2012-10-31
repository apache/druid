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

package com.metamx.druid.master;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.metamx.common.guava.Comparators;
import com.metamx.common.logger.Logger;
import com.metamx.druid.TimelineObjectHolder;
import com.metamx.druid.VersionedIntervalTimeline;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;

/**
 */
public class DruidMasterDropper implements DruidMasterHelper
{
  private static final Logger log = new Logger(DruidMasterDropper.class);

  private final DruidMaster master;

  public DruidMasterDropper(
      DruidMaster master
  )
  {
    this.master = master;
  }

  @Override
  public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
  {
    Set<DataSegment> availableSegments = params.getAvailableSegments();
    Collection<DruidServer> servicedData = params.getHistoricalServers();
    int droppedCount = 0;
    int deletedCount = 0;

    // Drop segments that are not needed
    for (DruidServer server : servicedData) {
      for (DruidDataSource dataSource : server.getDataSources()) {
        for (DataSegment segment : dataSource.getSegments()) {
          if (!availableSegments.contains(segment)) {
            LoadQueuePeon queuePeon = params.getLoadManagementPeons().get(server.getName());

            if (!queuePeon.getSegmentsToDrop().contains(segment)) {
              queuePeon.dropSegment(segment, new LoadPeonCallback()
              {
                @Override
                protected void execute()
                {
                  return;
                }
              });
              ++droppedCount;
            }
          }
        }
      }
    }

    // Delete segments that are old
    // Unservice old partitions if we've had enough time to make sure we aren't flapping with old data
    if (System.currentTimeMillis() - params.getStartTime() > params.getMillisToWaitBeforeDeleting()) {
      Map<String, VersionedIntervalTimeline<String, DataSegment>> timelines = Maps.newHashMap();
      for (DruidServer server : servicedData) {
        for (DruidDataSource dataSource : server.getDataSources()) {
          VersionedIntervalTimeline<String, DataSegment> timeline = timelines.get(dataSource.getName());
          if (timeline == null) {
            timeline = new VersionedIntervalTimeline<String, DataSegment>(Comparators.comparable());
            timelines.put(dataSource.getName(), timeline);
          }

          for (DataSegment segment : dataSource.getSegments()) {
            timeline.add(
                segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment)
            );
          }
        }
      }

      for (VersionedIntervalTimeline<String, DataSegment> timeline : timelines.values()) {
        for (TimelineObjectHolder<String, DataSegment> holder : timeline.findOvershadowed()) {
          for (DataSegment dataSegment : holder.getObject().payloads()) {
            log.info("Deleting[%s].", dataSegment);
            removeSegment(dataSegment);
            ++deletedCount;
          }
        }
      }
    }

    return params.buildFromExisting()
                 .withMessage(String.format("Dropped %,d segments from %,d servers", droppedCount, servicedData.size()))
                 .withMessage(String.format("Deleted %,d segments", deletedCount))
                 .withDroppedCount(droppedCount)
                 .withDeletedCount(deletedCount)
                 .build();
  }

  private void removeSegment(DataSegment segment)
  {
    log.info("Removing Segment[%s]", segment);
    master.removeSegment(segment);
  }
}
