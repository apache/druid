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

package org.apache.druid.indexing.common;

/**
 * Metric names and dimension values for the re-announcement of pending segments upgraded by a concurrent REPLACE.
 * <ul>
 *   <li>the task action emits {@link #PERSISTED} (how many upgrades a commit persisted),</li>
 *   <li>the supervisor emits {@link #NOTIFIED}, {@link #UNMATCHED} and {@link #SEND_FAILED} as it fans requests out,</li>
 *   <li>the streaming task emits {@link #ANNOUNCED} and {@link #SKIPPED} (with a {@code reason}) as it applies them.</li>
 * </ul>
 * Two reconciliations bound the visibility gap:
 * <ul>
 *   <li>per segment, {@link #UNMATCHED} counts upgrades that reached no running task (delayed until handoff);</li>
 *   <li>per task, {@link #NOTIFIED} should equal {@link #ANNOUNCED} + {@link #SKIPPED} + {@link #SEND_FAILED},
 *       since every notified task either announces, skips, or fails to receive the request. A shortfall means a
 *       notification was silently dropped.</li>
 * </ul>
 */
public class SegmentUpgradeMetrics
{
  /** Number of upgraded pending segments a REPLACE commit persisted and handed to the supervisor. Task-action dims. */
  public static final String PERSISTED = "ingest/realtime/segmentUpgrade/persisted";

  /** A notification was sent to a running task (once per task). Supervisor dims plus {@code taskId}. */
  public static final String NOTIFIED = "ingest/realtime/segmentUpgrade/notified";

  /** A record matched no running task and will not be re-announced until handoff. Supervisor dims. */
  public static final String UNMATCHED = "ingest/realtime/segmentUpgrade/unmatched";

  /** An upgrade request failed to reach a task over the wire. Supervisor dims plus {@code taskId}. */
  public static final String SEND_FAILED = "ingest/realtime/segmentUpgrade/sendFailed";

  /** A task announced an upgraded segment under the new version. Task dims. */
  public static final String ANNOUNCED = "ingest/realtime/segmentUpgrade/announced";

  /**
   * A task received an upgrade request but did not announce it. The {@code reason} dimension carries
   * {@link org.apache.druid.segment.realtime.appenderator.StreamAppenderator.PendingSegmentUpgradeResult#getReason()}.
   * Task dims.
   */
  public static final String SKIPPED = "ingest/realtime/segmentUpgrade/skipped";

  private SegmentUpgradeMetrics()
  {
  }
}
