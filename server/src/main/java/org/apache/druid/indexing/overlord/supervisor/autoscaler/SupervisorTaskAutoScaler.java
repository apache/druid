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

package org.apache.druid.indexing.overlord.supervisor.autoscaler;

/**
 * Task-count auto-scaler driven by a streaming supervisor.
 * <p>
 * Scaler return-value contract for {@link #computeTaskCountForRollover()} and any
 * implementation-specific scale-action method:
 * <ul>
 *   <li>{@code -1} — error case: metrics unavailable or an answer cannot be computed. The
 *       supervisor will skip scaling and emit a failure metric.</li>
 *   <li>Otherwise — the scaler's preferred task count, <i>unclamped</i> by configured min/max
 *       bounds. The supervisor is responsible for clamping to {@code [taskCountMin, taskCountMax]}
 *       (and any partition-count ceiling) and for deciding whether to actually scale. Returning
 *       the current task count is a valid "stay put" signal and still lets the supervisor clamp
 *       an out-of-bounds current value back into range.</li>
 * </ul>
 */
public interface SupervisorTaskAutoScaler
{
  void start();
  void stop();
  void reset();

  /**
   * Preferred task count when a task group is rolling over, allowing a non-disruptive scale-down.
   * Called by the supervisor when tasks are ending their duration. See the type-level Javadoc for
   * the return-value contract.
   */
  default int computeTaskCountForRollover()
  {
    return -1;
  }
}
