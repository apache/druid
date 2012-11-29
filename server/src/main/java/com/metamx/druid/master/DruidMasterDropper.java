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
import com.metamx.druid.master.rules.DropRule;
import com.metamx.druid.master.rules.LoadRule;
import com.metamx.druid.master.rules.Rule;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
    final Map<String, Integer> droppedCounts = Maps.newHashMap();
    int deletedCount = 0;

    Set<DataSegment> availableSegments = params.getAvailableSegments();
    Map<String, MinMaxPriorityQueue<ServerHolder>> servicedData = params.getHistoricalServers();

    // Drop segments that are not needed
    for (MinMaxPriorityQueue<ServerHolder> serverHolders : servicedData.values()) {
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
                droppedCounts.put(
                    server.getSubType(),
                    droppedCounts.get(server.getSubType()) == null ? 1 :
                    droppedCounts.get(server.getSubType()) + 1
                );
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

      for (MinMaxPriorityQueue<ServerHolder> serverHolders : servicedData.values()) {
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
            removeSegment(dataSegment);
            ++deletedCount;
          }
        }
      }

      // Remove extra replicants only if we have enough total copies of a segment in the cluster
      for (DataSegment segment : params.getAvailableSegments()) {
        Rule rule = params.getSegmentRules().get(segment.getIdentifier());

        if (rule instanceof DropRule) {
          removeSegment(segment);
          ++deletedCount;
        } else if (rule instanceof LoadRule) {
          LoadRule loadRule = (LoadRule) rule;
          Map<String, Integer> replicants = params.getSegmentsInCluster().get(segment.getIdentifier());

          if (replicants == null) {
            continue;
          }

          int totalExpectedReplicantCount = loadRule.getReplicationFactor();
          int totalActualReplicantCount = 0;
          for (Map.Entry<String, Integer> replicantEntry : replicants.entrySet()) {
            totalActualReplicantCount += replicantEntry.getValue();
          }

          // For all given node types
          for (Map.Entry<String, Integer> replicantEntry : replicants.entrySet()) {
            int actualReplicantCount = replicantEntry.getValue();
            int expectedReplicantCount = replicantEntry.getKey().equalsIgnoreCase(loadRule.getNodeType())
                                         ? totalExpectedReplicantCount
                                         : 0;

            MinMaxPriorityQueue<ServerHolder> serverQueue = params.getHistoricalServers().get(replicantEntry.getKey());
            if (serverQueue == null) {
              log.warn("No holders found for nodeType[%s]", replicantEntry.getKey());
              continue;
            }

            List<ServerHolder> droppedServers = Lists.newArrayList();
            while (actualReplicantCount > expectedReplicantCount &&
                totalActualReplicantCount > totalExpectedReplicantCount) {
              ServerHolder holder = serverQueue.pollLast();
              if (holder == null) {
                log.warn("Wtf, holder was null?  Do I have no servers[%s]?", serverQueue);
                continue;
              }

              holder.getPeon().dropSegment(
                  segment,
                  new LoadPeonCallback()
                  {
                    @Override
                    protected void execute()
                    {
                    }
                  }
              );
              droppedServers.add(holder);
              --actualReplicantCount;
              --totalActualReplicantCount;
              droppedCounts.put(
                  holder.getServer().getSubType(),
                  droppedCounts.get(holder.getServer().getSubType()) == null ? 1 :
                  droppedCounts.get(holder.getServer().getSubType()) + 1
              );
            }
            serverQueue.addAll(droppedServers);
          }
        }
      }
    }

    List<String> dropMsgs = Lists.newArrayList();
    for (Map.Entry<String, Integer> entry : droppedCounts.entrySet()) {
      dropMsgs.add(
          String.format(
              "[%s] : Dropped %,d segments among %,d servers",
              entry.getKey(), droppedCounts.get(entry.getKey()), servicedData.get(entry.getKey()).size()
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

  private void removeSegment(DataSegment segment)
  {
    log.info("Removing Segment[%s]", segment);
    master.removeSegment(segment);
  }
}
