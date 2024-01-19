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
        = CoordinatorStat.toDebugAndEmit("assigned", "segment/assigned/count");
    public static final CoordinatorStat DROPPED
        = CoordinatorStat.toDebugAndEmit("dropped", "segment/dropped/count");
    public static final CoordinatorStat DELETED
        = CoordinatorStat.toLogAndEmit("deleted", "segment/deleted/count", CoordinatorStat.Level.INFO);
    public static final CoordinatorStat MOVED
        = CoordinatorStat.toDebugAndEmit("moved", "segment/moved/count");

    // Skipped decisions in a run
    public static final CoordinatorStat ASSIGN_SKIPPED
        = CoordinatorStat.toDebugAndEmit("assignSkip", "segment/assignSkipped/count");
    public static final CoordinatorStat DROP_SKIPPED
        = CoordinatorStat.toDebugAndEmit("dropSkip", "segment/dropSkipped/count");
    public static final CoordinatorStat MOVE_SKIPPED
        = CoordinatorStat.toDebugAndEmit("moveSkip", "segment/moveSkipped/count");

    // Current state of segments of a datasource
    public static final CoordinatorStat USED
        = CoordinatorStat.toDebugAndEmit("usedSegments", "segment/count");
    public static final CoordinatorStat USED_BYTES
        = CoordinatorStat.toDebugAndEmit("usedSegmentBytes", "segment/size");
    public static final CoordinatorStat UNDER_REPLICATED
        = CoordinatorStat.toDebugAndEmit("underreplicated", "segment/underReplicated/count");
    public static final CoordinatorStat UNAVAILABLE
        = CoordinatorStat.toDebugAndEmit("unavailable", "segment/unavailable/count");
    public static final CoordinatorStat UNNEEDED
        = CoordinatorStat.toDebugAndEmit("unneeded", "segment/unneeded/count");
    public static final CoordinatorStat OVERSHADOWED
        = CoordinatorStat.toDebugAndEmit("overshadowed", "segment/overshadowed/count");
    public static final CoordinatorStat UNNEEDED_ETERNITY_TOMBSTONE
        = CoordinatorStat.toDebugAndEmit("unneededEternityTombstone", "segment/unneededEternityTombstone/count");
  }

  public static class SegmentQueue
  {
    public static final CoordinatorStat NUM_TO_LOAD
        = CoordinatorStat.toDebugAndEmit("numToLoad", "segment/loadQueue/count");
    public static final CoordinatorStat BYTES_TO_LOAD
        = CoordinatorStat.toDebugAndEmit("bytesToLoad", "segment/loadQueue/size");
    public static final CoordinatorStat NUM_TO_DROP
        = CoordinatorStat.toDebugAndEmit("numToDrop", "segment/dropQueue/count");

    public static final CoordinatorStat ASSIGNED_ACTIONS
        = CoordinatorStat.toDebugAndEmit("assignedActions", "segment/loadQueue/assigned");
    public static final CoordinatorStat COMPLETED_ACTIONS
        = CoordinatorStat.toDebugAndEmit("successActions", "segment/loadQueue/success");
    public static final CoordinatorStat FAILED_ACTIONS
        = CoordinatorStat.toLogAndEmit("failedActions", "segment/loadQueue/failed", CoordinatorStat.Level.ERROR);
    public static final CoordinatorStat CANCELLED_ACTIONS
        = CoordinatorStat.toDebugAndEmit("cancelledActions", "segment/loadQueue/cancelled");
  }

  public static class Tier
  {
    public static final CoordinatorStat REQUIRED_CAPACITY
        = CoordinatorStat.toDebugAndEmit("reqdCap", "tier/required/capacity");
    public static final CoordinatorStat TOTAL_CAPACITY
        = CoordinatorStat.toDebugAndEmit("totalCap", "tier/total/capacity");
    public static final CoordinatorStat REPLICATION_FACTOR
        = CoordinatorStat.toDebugAndEmit("maxRepFactor", "tier/replication/factor");
    public static final CoordinatorStat HISTORICAL_COUNT
        = CoordinatorStat.toDebugAndEmit("numHistorical", "tier/historical/count");
  }

  public static class Compaction
  {
    public static final CoordinatorStat SUBMITTED_TASKS
        = CoordinatorStat.toDebugAndEmit("compactTasks", "compact/task/count");
    public static final CoordinatorStat MAX_SLOTS
        = CoordinatorStat.toDebugAndEmit("compactMaxSlots", "compactTask/maxSlot/count");
    public static final CoordinatorStat AVAILABLE_SLOTS
        = CoordinatorStat.toDebugAndEmit("compactAvlSlots", "compactTask/availableSlot/count");

    public static final CoordinatorStat PENDING_BYTES
        = CoordinatorStat.toDebugAndEmit("compactPendingBytes", "segment/waitCompact/bytes");
    public static final CoordinatorStat COMPACTED_BYTES
        = CoordinatorStat.toDebugAndEmit("compactedBytes", "segment/compacted/bytes");
    public static final CoordinatorStat SKIPPED_BYTES
        = CoordinatorStat.toDebugAndEmit("compactSkipBytes", "segment/skipCompact/bytes");

    public static final CoordinatorStat PENDING_SEGMENTS
        = CoordinatorStat.toDebugAndEmit("compactPendingSeg", "segment/waitCompact/count");
    public static final CoordinatorStat COMPACTED_SEGMENTS
        = CoordinatorStat.toDebugAndEmit("compactedSeg", "segment/compacted/count");
    public static final CoordinatorStat SKIPPED_SEGMENTS
        = CoordinatorStat.toDebugAndEmit("compactSkipSeg", "segment/skipCompact/count");

    public static final CoordinatorStat PENDING_INTERVALS
        = CoordinatorStat.toDebugAndEmit("compactPendingIntv", "interval/waitCompact/count");
    public static final CoordinatorStat COMPACTED_INTERVALS
        = CoordinatorStat.toDebugAndEmit("compactedIntv", "interval/compacted/count");
    public static final CoordinatorStat SKIPPED_INTERVALS
        = CoordinatorStat.toDebugAndEmit("compactSkipIntv", "interval/skipCompact/count");
  }

  public static class CoordinatorRun
  {
    public static final CoordinatorStat DUTY_RUN_TIME
        = CoordinatorStat.toDebugAndEmit("dutyRunTime", "coordinator/time");
    public static final CoordinatorStat GROUP_RUN_TIME
        = CoordinatorStat.toDebugAndEmit("groupRunTime", "coordinator/global/time");
  }

  public static class Kill
  {
    public static final CoordinatorStat COMPACTION_CONFIGS
        = CoordinatorStat.toDebugAndEmit("killedCompactConfigs", "metadata/kill/compaction/count");
    public static final CoordinatorStat SUPERVISOR_SPECS
        = CoordinatorStat.toDebugAndEmit("killedSupervisorSpecs", "metadata/kill/supervisor/count");
    public static final CoordinatorStat RULES
        = CoordinatorStat.toDebugAndEmit("killedRules", "metadata/kill/rule/count");
    public static final CoordinatorStat AUDIT_LOGS
        = CoordinatorStat.toDebugAndEmit("killedAuditLogs", "metadata/kill/audit/count");
    public static final CoordinatorStat DATASOURCES
        = CoordinatorStat.toDebugAndEmit("killedDatasources", "metadata/kill/datasource/count");
    public static final CoordinatorStat AVAILABLE_SLOTS
        = CoordinatorStat.toDebugAndEmit("killAvailSlots", "killTask/availableSlot/count");
    public static final CoordinatorStat MAX_SLOTS
        = CoordinatorStat.toDebugAndEmit("killMaxSlots", "killTask/maxSlot/count");
    public static final CoordinatorStat SUBMITTED_TASKS
        = CoordinatorStat.toDebugAndEmit("killTasks", "kill/task/count");
    public static final CoordinatorStat PENDING_SEGMENTS
        = CoordinatorStat.toDebugAndEmit("killPendingSegs", "kill/pendingSegments/count");
  }

  public static class Balancer
  {
    public static final CoordinatorStat COMPUTATION_ERRORS = CoordinatorStat.toLogAndEmit(
        "costComputeError",
        "segment/balancer/compute/error",
        CoordinatorStat.Level.ERROR
    );
    public static final CoordinatorStat COMPUTATION_TIME = CoordinatorStat.toDebugOnly("costComputeTime");
    public static final CoordinatorStat COMPUTATION_COUNT = CoordinatorStat.toDebugOnly("costComputeCount");
  }
}
