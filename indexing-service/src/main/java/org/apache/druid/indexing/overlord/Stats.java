package org.apache.druid.indexing.overlord;

import org.apache.druid.server.coordinator.stats.CoordinatorStat;

/**
 * Task-level stats emitted as metrics.
 */
public class Stats
{
  public static class TaskQueue {
    public static final CoordinatorStat STATUS_UPDATES_IN_QUEUE
        = CoordinatorStat.toDebugAndEmit("queuedStatusUpdates", "task/status/queue/count");
    public static final CoordinatorStat HANDLED_STATUS_UPDATES
        = CoordinatorStat.toDebugAndEmit("handledStatusUpdates", "task/status/updated/count");
  }
}
