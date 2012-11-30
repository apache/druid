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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.common.guava.Comparators;
import com.metamx.common.logger.Logger;
import com.metamx.druid.TimelineObjectHolder;
import com.metamx.druid.VersionedIntervalTimeline;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.collect.CountingMap;
import com.metamx.druid.master.rules.Rule;
import com.metamx.druid.master.stats.DropStat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

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
    final CountingMap<String> droppedCounts = new CountingMap<String>();
    int deletedCount = 0;

    Set<DataSegment> availableSegments = params.getAvailableSegments();
    DruidCluster cluster = params.getDruidCluster();

    // Drop segments that are not needed
    for (MinMaxPriorityQueue<ServerHolder> serverHolders : cluster.getSortedServersByTier()) {
      for (ServerHolder serverHolder : serverHolders) {
        DruidServer server = serverHolder.getServer();

        for (DruidDataSource dataSource : server.getDataSources()) {
          for (DataSegment segment : dataSource.getSegments()) {
            if (!availableSegments.contains(segment)) {
              LoadQueuePeon queuePeon = params.getLoadManagementPeons().get(server.getName());

              if (!queuePeon.getSegmentsToDrop().contains(segment)) {
                queuePeon.dropSegment(
                    segment, new LoadPeonCallback()
                {
                  @Override
                  protected void execute()
                  {
                  }
                }
                );
                droppedCounts.add(server.getTier(), 1);
              }
            }
          }
        }
      }
    }

    // Delete segments that are old
    // Unservice old partitions if we've had enough time to make sure we aren't flapping with old data
    if (System.currentTimeMillis() - params.getStartTime() > params.getMillisToWaitBeforeDeleting()) {
      Map<String, VersionedIntervalTimeline<String, DataSegment>> timelines = Maps.newHashMap();

      for (MinMaxPriorityQueue<ServerHolder> serverHolders : cluster.getSortedServersByTier()) {
        for (ServerHolder serverHolder : serverHolders) {
          DruidServer server = serverHolder.getServer();

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
      }

      for (VersionedIntervalTimeline<String, DataSegment> timeline : timelines.values()) {
        for (TimelineObjectHolder<String, DataSegment> holder : timeline.findOvershadowed()) {
          for (DataSegment dataSegment : holder.getObject().payloads()) {
            log.info("Deleting[%s].", dataSegment);
            master.removeSegment(dataSegment);
            ++deletedCount;
          }
        }
      }

      for (DataSegment segment : availableSegments) {
        Rule rule = params.getSegmentRuleLookup().lookup(segment.getIdentifier());
        DropStat stat = rule.runDrop(master, params, segment);
        deletedCount += stat.getDeletedCount();
        if (stat.getDroppedCount() != null) {
          droppedCounts.putAll(stat.getDroppedCount());
        }
      }
    }

    List<String> dropMsgs = Lists.newArrayList();
    for (Map.Entry<String, AtomicLong> entry : droppedCounts.entrySet()) {
      dropMsgs.add(
          String.format(
              "[%s] : Dropped %s segments among %,d servers",
              entry.getKey(), droppedCounts.get(entry.getKey()), cluster.get(entry.getKey()).size()
          )
      );
    }

    return params.buildFromExisting()
                 .withMessages(dropMsgs)
                 .withMessage(String.format("Deleted %,d segments", deletedCount))
                 .withDroppedCount(droppedCounts)
                 .withDeletedCount(deletedCount)
                 .build();

  }
}
