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

public class Stats
{
  public static class Segments
  {
    // Decisions taken in a run
    public static final CoordinatorStat ASSIGNED
        = new CoordinatorStat("assigned", "segment/assigned/count");
    public static final CoordinatorStat DROPPED
        = new CoordinatorStat("dropped", "segment/dropped/count");
    public static final CoordinatorStat DELETED
        = new CoordinatorStat("deleted", "segment/deleted/count");
    public static final CoordinatorStat MOVED
        = new CoordinatorStat("moved", "segment/moved/count");
    public static final CoordinatorStat UNMOVED
        = new CoordinatorStat("unmoved", "segment/unmoved/count");

    public static final CoordinatorStat ASSIGN_SKIPPED = new CoordinatorStat("assignSkip");
    public static final CoordinatorStat DROP_SKIPPED = new CoordinatorStat("dropSkip");
    public static final CoordinatorStat THROTTLED_REPLICAS
        = new CoordinatorStat("throttled", "segment/replica/throttled");

    public static final CoordinatorStat ASSIGNED_BROADCAST
        = new CoordinatorStat("assignBroad", "segment/assigned/broadcast");
    public static final CoordinatorStat DROPPED_BROADCAST
        = new CoordinatorStat("dropBroad", "segment/dropped/broadcast");

    // Current cluster state
    public static final CoordinatorStat COUNT = new CoordinatorStat("segCount", "segment/count");
    public static final CoordinatorStat SIZE = new CoordinatorStat("segSize", "segment/size");

    public static final CoordinatorStat UNDER_REPLICATED
        = new CoordinatorStat("underreplicated", "segment/underReplicated/count");
    public static final CoordinatorStat UNAVAILABLE
        = new CoordinatorStat("unavailable", "segment/unavailable/count");
    public static final CoordinatorStat UNNEEDED
        = new CoordinatorStat("unneeded", "segment/unneeded/count");
    public static final CoordinatorStat OVERSHADOWED
        = new CoordinatorStat("overshadowed", "segment/overshadowed/count");


    // Load/drop queue stats
    public static final CoordinatorStat NUM_TO_LOAD
        = new CoordinatorStat("numToLoad", "segment/loadQueue/count");
    public static final CoordinatorStat BYTES_TO_LOAD
        = new CoordinatorStat("bytesToLoad", "segment/loadQueue/size");
    public static final CoordinatorStat FAILED_LOADS
        = new CoordinatorStat("failedLoad", "segment/loadQueue/failed");
    public static final CoordinatorStat CANCELLED_LOADS
        = new CoordinatorStat("cancelLoad", "segment/loadQueue/cancelled");
    public static final CoordinatorStat CANCELLED_DROPS
        = new CoordinatorStat("cancelDrop");
    public static final CoordinatorStat CANCELLED_MOVES
        = new CoordinatorStat("cancelMove");
    public static final CoordinatorStat FAILED_MOVES
        = new CoordinatorStat("failedMove", "segment/move/failed");
    public static final CoordinatorStat NUM_TO_DROP
        = new CoordinatorStat("numToDrop", "segment/dropQueue/count");

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

  public static class Run
  {
    public static final CoordinatorStat DUTY_TIME
        = new CoordinatorStat("dutyRunTime", "coordinator/time");
    public static final CoordinatorStat TOTAL_TIME
        = new CoordinatorStat("totalRunTime", "coordinator/global/time");
  }

  public static class Balancer
  {
    public static final CoordinatorStat RAW_COST
        = new CoordinatorStat("initialCost", "segment/cost/raw");
    public static final CoordinatorStat NORMALIZATION_COST
        = new CoordinatorStat("initialCost", "segment/cost/normalization");
    public static final CoordinatorStat NORMALIZED_COST_X_1000
        = new CoordinatorStat("initialCost", "segment/cost/normalized");
  }
}
