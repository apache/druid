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
 * Return-value contract for {@link #computeTaskCountForRollover()} and any implementation-specific
 * scale-action method:
 * <ul>
 *   <li>{@link #CANNOT_COMPUTE} when no preferred count can be computed.</li>
 *   <li>Otherwise, a preferred task count {@code >= 1}, unclamped by min/max bounds. The supervisor
 *       clamps to {@code [taskCountMin, taskCountMax]} and decides whether to scale. Scaling to
 *       zero (idle) is the supervisor's responsibility, not the autoscaler's.</li>
 * </ul>
 */
public interface SupervisorTaskAutoScaler
{
  /** Sentinel for "no preferred task count available". The supervisor will skip scaling. */
  int CANNOT_COMPUTE = -1;

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
    return CANNOT_COMPUTE;
  }
}
