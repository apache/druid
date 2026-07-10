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
 *   <li>the task action emits {@link #COUNT} (how many upgrades a commit produced),</li>
 *   <li>the supervisor emits {@link #NOTIFIED}, {@link #UNMATCHED} and {@link #SEND_FAILED} as it fans requests out,</li>
 *   <li>the streaming task emits {@link #ANNOUNCED} and {@link #SKIPPED} (with a {@code reason}) as it applies them.</li>
 * </ul>
 * Comparing {@link #COUNT} (created) against {@link #ANNOUNCED} (applied) per dataSource quantifies the visibility gap;
 * {@link #UNMATCHED}, {@link #SEND_FAILED} and {@link #SKIPPED} attribute where a lost upgrade dropped out.
 */
public class SegmentUpgradeMetrics
{
  /** Number of upgraded pending segments a REPLACE commit created and handed to the supervisor. Task-action dims. */
  public static final String COUNT = "ingest/segmentUpgrade/count";

  /** A record was delivered to at least one running task. Supervisor dims. */
  public static final String NOTIFIED = "ingest/segmentUpgrade/notified";

  /** A record matched no running task and will not be re-announced until handoff. Supervisor dims. */
  public static final String UNMATCHED = "ingest/segmentUpgrade/unmatched";

  /** An upgrade request failed to reach a task over the wire. Supervisor dims plus {@code taskId}. */
  public static final String SEND_FAILED = "ingest/segmentUpgrade/sendFailed";

  /** A task announced an upgraded segment under the new version. Task dims. */
  public static final String ANNOUNCED = "ingest/segmentUpgrade/announced";

  /** A task received an upgrade request but did not announce it; see the {@code reason} dimension. Task dims. */
  public static final String SKIPPED = "ingest/segmentUpgrade/skipped";

  // Values for the DruidMetrics.REASON dimension on SKIPPED.

  /** The task holds no pending segment matching upgradedFromSegmentId (request targeted the wrong task). */
  public static final String REASON_UNKNOWN_BASE = "unknownBase";
  /** The base sink is gone even though this task once held it. */
  public static final String REASON_NO_SINK = "base sink already dropped";
  /** The base sink is being dropped (handoff in progress); the durable path re-announces at the new version. */
  public static final String REASON_DROPPING = "dropping base sink";

  private SegmentUpgradeMetrics()
  {
  }
}
