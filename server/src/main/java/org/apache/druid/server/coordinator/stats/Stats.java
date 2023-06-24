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

package org.apache.druid.server.coordinator.stats;

/**
 * List of Coordinator stats.
 */
public class Stats
{
  public static class Segments
  {
    // Decisions taken in a run
    public static final CoordinatorStat ASSIGNED
        = new CoordinatorStat("assigned", "segment/assigned/count", CoordinatorStat.Level.INFO);
    public static final CoordinatorStat DROPPED
        = new CoordinatorStat("dropped", "segment/dropped/count", CoordinatorStat.Level.INFO);
    public static final CoordinatorStat DELETED
        = new CoordinatorStat("deleted", "segment/deleted/count", CoordinatorStat.Level.INFO);
    public static final CoordinatorStat MOVED
        = new CoordinatorStat("moved", "segment/moved/count");

    // Skipped decisions in a run
    public static final CoordinatorStat ASSIGN_SKIPPED
        = new CoordinatorStat("assignSkip", "segment/assignSkipped/count");
    public static final CoordinatorStat DROP_SKIPPED
        = new CoordinatorStat("dropSkip", "segment/dropSkipped/count");
    public static final CoordinatorStat MOVE_SKIPPED
        = new CoordinatorStat("moveSkip", "segment/moveSkipped/count");

    // Current state of segments of a datasource
    public static final CoordinatorStat USED
        = new CoordinatorStat("usedSegments", "segment/count");
    public static final CoordinatorStat USED_BYTES
        = new CoordinatorStat("usedSegmentBytes", "segment/size");
    public static final CoordinatorStat UNDER_REPLICATED
        = new CoordinatorStat("underreplicated", "segment/underReplicated/count");
    public static final CoordinatorStat UNAVAILABLE
        = new CoordinatorStat("unavailable", "segment/unavailable/count");
    public static final CoordinatorStat UNNEEDED
        = new CoordinatorStat("unneeded", "segment/unneeded/count");
    public static final CoordinatorStat OVERSHADOWED
        = new CoordinatorStat("overshadowed", "segment/overshadowed/count");
  }

  public static class SegmentQueue
  {
    public static final CoordinatorStat NUM_TO_LOAD
        = new CoordinatorStat("numToLoad", "segment/loadQueue/count");
    public static final CoordinatorStat BYTES_TO_LOAD
        = new CoordinatorStat("bytesToLoad", "segment/loadQueue/size");
    public static final CoordinatorStat NUM_TO_DROP
        = new CoordinatorStat("numToDrop", "segment/dropQueue/count");

    public static final CoordinatorStat ASSIGNED_ACTIONS
        = new CoordinatorStat("assignedActions", "segment/loadQueue/assigned");
    public static final CoordinatorStat COMPLETED_ACTIONS
        = new CoordinatorStat("successActions", "segment/loadQueue/success");
    public static final CoordinatorStat FAILED_ACTIONS
        = new CoordinatorStat("failedActions", "segment/loadQueue/failed", CoordinatorStat.Level.ERROR);
    public static final CoordinatorStat CANCELLED_ACTIONS
        = new CoordinatorStat("cancelledActions", "segment/loadQueue/cancelled");
  }

  public static class Tier
  {
    public static final CoordinatorStat REQUIRED_CAPACITY
        = new CoordinatorStat("reqdCap", "tier/required/capacity");
    public static final CoordinatorStat TOTAL_CAPACITY
        = new CoordinatorStat("totalCap", "tier/total/capacity");
    public static final CoordinatorStat REPLICATION_FACTOR
        = new CoordinatorStat("maxRepFactor", "tier/replication/factor");
    public static final CoordinatorStat HISTORICAL_COUNT
        = new CoordinatorStat("numHistorical", "tier/historical/count");
  }

  public static class Compaction
  {
    public static final CoordinatorStat SUBMITTED_TASKS
        = new CoordinatorStat("compactTasks", "compact/task/count");
    public static final CoordinatorStat MAX_SLOTS
        = new CoordinatorStat("compactMaxSlots", "compactTask/maxSlot/count");
    public static final CoordinatorStat AVAILABLE_SLOTS
        = new CoordinatorStat("compactAvlSlots", "compactTask/availableSlot/count");

    public static final CoordinatorStat PENDING_BYTES
        = new CoordinatorStat("compactPendingBytes", "segment/waitCompact/bytes");
    public static final CoordinatorStat COMPACTED_BYTES
        = new CoordinatorStat("compactedBytes", "segment/compacted/bytes");
    public static final CoordinatorStat SKIPPED_BYTES
        = new CoordinatorStat("compactSkipBytes", "segment/skipCompact/bytes");

    public static final CoordinatorStat PENDING_SEGMENTS
        = new CoordinatorStat("compactPendingSeg", "segment/waitCompact/count");
    public static final CoordinatorStat COMPACTED_SEGMENTS
        = new CoordinatorStat("compactedSeg", "segment/compacted/count");
    public static final CoordinatorStat SKIPPED_SEGMENTS
        = new CoordinatorStat("compactSkipSeg", "segment/skipCompact/count");

    public static final CoordinatorStat PENDING_INTERVALS
        = new CoordinatorStat("compactPendingIntv", "interval/waitCompact/count");
    public static final CoordinatorStat COMPACTED_INTERVALS
        = new CoordinatorStat("compactedIntv", "interval/compacted/count");
    public static final CoordinatorStat SKIPPED_INTERVALS
        = new CoordinatorStat("compactSkipIntv", "interval/skipCompact/count");
  }

  public static class CoordinatorRun
  {
    public static final CoordinatorStat DUTY_RUN_TIME
        = new CoordinatorStat("dutyRunTime", "coordinator/time");
    public static final CoordinatorStat GROUP_RUN_TIME
        = new CoordinatorStat("groupRunTime", "coordinator/global/time");
  }

  public static class Balancer
  {
    public static final CoordinatorStat COMPUTATION_ERRORS
        = new CoordinatorStat("costComputeError", "segment/balancer/compute/error");
    public static final CoordinatorStat COMPUTATION_TIME
        = new CoordinatorStat("costComputeTime", "segment/balancer/compute/time");
    public static final CoordinatorStat COMPUTATION_COUNT
        = new CoordinatorStat("costComputeCount", "segment/balancer/compute/count");
  }
}
