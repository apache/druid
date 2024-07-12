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

package org.apache.druid.indexing.overlord;

import org.apache.druid.server.stats.DruidStat;

/**
 * Task-level stats emitted as metrics.
 */
public class Stats
{
  public static class TaskQueue
  {
    public static final DruidStat STATUS_UPDATES_IN_QUEUE
        = DruidStat.toDebugAndEmit("queuedStatusUpdates", "task/status/queue/count");
    public static final DruidStat HANDLED_STATUS_UPDATES
        = DruidStat.toDebugAndEmit("handledStatusUpdates", "task/status/updated/count");
  }

  public static class TaskCount
  {
    public static final DruidStat SUCCESSFUL
        = DruidStat.toDebugAndEmit("successfulTasks", "task/success/count");
    public static final DruidStat FAILED
        = DruidStat.toDebugAndEmit("failedTasks", "task/failed/count");
    public static final DruidStat RUNNING
        = DruidStat.toDebugAndEmit("runningTasks", "task/running/count");
    public static final DruidStat PENDING
        = DruidStat.toDebugAndEmit("pendingTasks", "task/pending/count");
    public static final DruidStat WAITING
        = DruidStat.toDebugAndEmit("waitingTasks", "task/waiting/count");
  }
}
