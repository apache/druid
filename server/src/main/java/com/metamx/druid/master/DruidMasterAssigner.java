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
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.collect.CountingMap;
import com.metamx.druid.master.rules.Rule;
import com.metamx.druid.master.stats.AssignStat;
import com.metamx.emitter.EmittingLogger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class DruidMasterAssigner implements DruidMasterHelper
{
  private static final EmittingLogger log = new EmittingLogger(DruidMasterAssigner.class);

  private final DruidMaster master;

  public DruidMasterAssigner(
      DruidMaster master
  )
  {
    this.master = master;
  }

  public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
  {
    int unassignedCount = 0;
    long unassignedSize = 0;
    CountingMap<String> assignedCounts = new CountingMap<String>();

    DruidCluster cluster = params.getDruidCluster();

    if (cluster.isEmpty()) {
      log.warn("Uh... I have no servers. Not assigning anything...");
      return params;
    }

    // Assign unserviced segments to servers in order of most available space
    for (DataSegment segment : params.getAvailableSegments()) {
      Rule rule = params.getSegmentRuleLookup().lookup(segment.getIdentifier());
      AssignStat stat = rule.runAssign(params, segment);
      unassignedCount += stat.getUnassignedCount();
      unassignedSize += stat.getUnassignedSize();
      if (stat.getAssignedCount() != null) {
        assignedCounts.add(stat.getAssignedCount().lhs, stat.getAssignedCount().rhs);
      }
    }
    master.decrementRemovedSegmentsLifetime();

    List<String> assignmentMsgs = Lists.newArrayList();
    for (Map.Entry<String, AtomicLong> entry : assignedCounts.entrySet()) {
      if (cluster.get(entry.getKey()) != null) {
        assignmentMsgs.add(
            String.format(
                "[%s] : Assigned %s segments among %,d servers",
                entry.getKey(), assignedCounts.get(entry.getKey()), cluster.get(entry.getKey()).size()
            )
        );
      }
    }

    return params.buildFromExisting()
                 .withMessages(assignmentMsgs)
                 .withAssignedCount(assignedCounts)
                 .withUnassignedCount(unassignedCount)
                 .withUnassignedSize(unassignedSize)
                 .build();
  }
}
