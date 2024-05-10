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

package org.apache.druid.server.coordinator;

import org.apache.druid.server.stats.DruidStat;

/**
 * List of Coordinator stats.
 */
public class Stats
{
  public static class Segments
  {
    // Decisions taken in a run
    public static final DruidStat ASSIGNED
        = DruidStat.toDebugAndEmit("assigned", "segment/assigned/count");
    public static final DruidStat DROPPED
        = DruidStat.toDebugAndEmit("dropped", "segment/dropped/count");
    public static final DruidStat DELETED
        = DruidStat.toLogAndEmit("deleted", "segment/deleted/count", DruidStat.Level.INFO);
    public static final DruidStat MOVED
        = DruidStat.toDebugAndEmit("moved", "segment/moved/count");

    // Skipped decisions in a run
    public static final DruidStat ASSIGN_SKIPPED
        = DruidStat.toDebugAndEmit("assignSkip", "segment/assignSkipped/count");
    public static final DruidStat DROP_SKIPPED
        = DruidStat.toDebugAndEmit("dropSkip", "segment/dropSkipped/count");
    public static final DruidStat MOVE_SKIPPED
        = DruidStat.toDebugAndEmit("moveSkip", "segment/moveSkipped/count");

    // Current state of segments of a datasource
    public static final DruidStat USED
        = DruidStat.toDebugAndEmit("usedSegments", "segment/count");
    public static final DruidStat USED_BYTES
        = DruidStat.toDebugAndEmit("usedSegmentBytes", "segment/size");
    public static final DruidStat UNDER_REPLICATED
        = DruidStat.toDebugAndEmit("underreplicated", "segment/underReplicated/count");
    public static final DruidStat UNAVAILABLE
        = DruidStat.toDebugAndEmit("unavailable", "segment/unavailable/count");
    public static final DruidStat DEEP_STORAGE_ONLY
            = DruidStat.toDebugAndEmit("deepStorageOnly", "segment/availableDeepStorageOnly/count");
    public static final DruidStat UNNEEDED
        = DruidStat.toDebugAndEmit("unneeded", "segment/unneeded/count");
    public static final DruidStat OVERSHADOWED
        = DruidStat.toDebugAndEmit("overshadowed", "segment/overshadowed/count");
    public static final DruidStat UNNEEDED_ETERNITY_TOMBSTONE
        = DruidStat.toDebugAndEmit("unneededEternityTombstone", "segment/unneededEternityTombstone/count");
  }

  public static class SegmentQueue
  {
    public static final DruidStat NUM_TO_LOAD
        = DruidStat.toDebugAndEmit("numToLoad", "segment/loadQueue/count");
    public static final DruidStat BYTES_TO_LOAD
        = DruidStat.toDebugAndEmit("bytesToLoad", "segment/loadQueue/size");
    public static final DruidStat NUM_TO_DROP
        = DruidStat.toDebugAndEmit("numToDrop", "segment/dropQueue/count");

    public static final DruidStat ASSIGNED_ACTIONS
        = DruidStat.toDebugAndEmit("assignedActions", "segment/loadQueue/assigned");
    public static final DruidStat COMPLETED_ACTIONS
        = DruidStat.toDebugAndEmit("successActions", "segment/loadQueue/success");
    public static final DruidStat FAILED_ACTIONS
        = DruidStat.toLogAndEmit("failedActions", "segment/loadQueue/failed", DruidStat.Level.ERROR);
    public static final DruidStat CANCELLED_ACTIONS
        = DruidStat.toDebugAndEmit("cancelledActions", "segment/loadQueue/cancelled");
  }

  public static class Tier
  {
    public static final DruidStat REQUIRED_CAPACITY
        = DruidStat.toDebugAndEmit("reqdCap", "tier/required/capacity");
    public static final DruidStat TOTAL_CAPACITY
        = DruidStat.toDebugAndEmit("totalCap", "tier/total/capacity");
    public static final DruidStat REPLICATION_FACTOR
        = DruidStat.toDebugAndEmit("maxRepFactor", "tier/replication/factor");
    public static final DruidStat HISTORICAL_COUNT
        = DruidStat.toDebugAndEmit("numHistorical", "tier/historical/count");
  }

  public static class Compaction
  {
    public static final DruidStat SUBMITTED_TASKS
        = DruidStat.toDebugAndEmit("compactTasks", "compact/task/count");
    public static final DruidStat MAX_SLOTS
        = DruidStat.toDebugAndEmit("compactMaxSlots", "compactTask/maxSlot/count");
    public static final DruidStat AVAILABLE_SLOTS
        = DruidStat.toDebugAndEmit("compactAvlSlots", "compactTask/availableSlot/count");

    public static final DruidStat PENDING_BYTES
        = DruidStat.toDebugAndEmit("compactPendingBytes", "segment/waitCompact/bytes");
    public static final DruidStat COMPACTED_BYTES
        = DruidStat.toDebugAndEmit("compactedBytes", "segment/compacted/bytes");
    public static final DruidStat SKIPPED_BYTES
        = DruidStat.toDebugAndEmit("compactSkipBytes", "segment/skipCompact/bytes");

    public static final DruidStat PENDING_SEGMENTS
        = DruidStat.toDebugAndEmit("compactPendingSeg", "segment/waitCompact/count");
    public static final DruidStat COMPACTED_SEGMENTS
        = DruidStat.toDebugAndEmit("compactedSeg", "segment/compacted/count");
    public static final DruidStat SKIPPED_SEGMENTS
        = DruidStat.toDebugAndEmit("compactSkipSeg", "segment/skipCompact/count");

    public static final DruidStat PENDING_INTERVALS
        = DruidStat.toDebugAndEmit("compactPendingIntv", "interval/waitCompact/count");
    public static final DruidStat COMPACTED_INTERVALS
        = DruidStat.toDebugAndEmit("compactedIntv", "interval/compacted/count");
    public static final DruidStat SKIPPED_INTERVALS
        = DruidStat.toDebugAndEmit("compactSkipIntv", "interval/skipCompact/count");
  }

  public static class CoordinatorRun
  {
    public static final DruidStat DUTY_RUN_TIME
        = DruidStat.toDebugAndEmit("dutyRunTime", "coordinator/time");
    public static final DruidStat GROUP_RUN_TIME
        = DruidStat.toDebugAndEmit("groupRunTime", "coordinator/global/time");
  }

  public static class Kill
  {
    public static final DruidStat COMPACTION_CONFIGS
        = DruidStat.toDebugAndEmit("killedCompactConfigs", "metadata/kill/compaction/count");
    public static final DruidStat SUPERVISOR_SPECS
        = DruidStat.toDebugAndEmit("killedSupervisorSpecs", "metadata/kill/supervisor/count");
    public static final DruidStat RULES
        = DruidStat.toDebugAndEmit("killedRules", "metadata/kill/rule/count");
    public static final DruidStat SEGMENT_SCHEMA
        = DruidStat.toDebugAndEmit("killSchemas", "metadata/kill/segmentSchema/count");
    public static final DruidStat AUDIT_LOGS
        = DruidStat.toDebugAndEmit("killedAuditLogs", "metadata/kill/audit/count");
    public static final DruidStat DATASOURCES
        = DruidStat.toDebugAndEmit("killedDatasources", "metadata/kill/datasource/count");
    public static final DruidStat AVAILABLE_SLOTS
        = DruidStat.toDebugAndEmit("killAvailSlots", "killTask/availableSlot/count");
    public static final DruidStat MAX_SLOTS
        = DruidStat.toDebugAndEmit("killMaxSlots", "killTask/maxSlot/count");
    public static final DruidStat SUBMITTED_TASKS
        = DruidStat.toDebugAndEmit("killTasks", "kill/task/count");
    public static final DruidStat ELIGIBLE_UNUSED_SEGMENTS
        = DruidStat.toDebugAndEmit("killEligibleUnusedSegs", "kill/eligibleUnusedSegments/count");
    public static final DruidStat PENDING_SEGMENTS
        = DruidStat.toDebugAndEmit("killPendingSegs", "kill/pendingSegments/count");
  }

  public static class Balancer
  {
    public static final DruidStat COMPUTATION_ERRORS = DruidStat.toLogAndEmit(
        "costComputeError",
        "segment/balancer/compute/error",
        DruidStat.Level.ERROR
    );
    public static final DruidStat COMPUTATION_TIME = DruidStat.toDebugOnly("costComputeTime");
    public static final DruidStat COMPUTATION_COUNT = DruidStat.toDebugOnly("costComputeCount");
  }
}
