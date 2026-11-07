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

/**
 * Deployment mode of the Overlord in the current process.
 *
 * <p>The mode is bound by {@code CliOverlord#getModules(boolean)}, which already knows how
 * the process was started: as the {@code overlord} service, or with the Overlord modules
 * loaded into a Coordinator by {@code CliCoordinator}.
 *
 * <p>Overlord code must use this binding rather than {@code druid.coordinator.asOverlord.enabled}
 * to find out how it is deployed. That property is meant for the Coordinator only, and a
 * standalone Overlord may still see it set to {@code true}, for example when it shares a
 * {@code runtime.properties} file with the Coordinator.
 *
 * <p>Components that the Overlord shares with the Coordinator in the same process use the mode
 * to decide who owns their lifecycle:
 *
 * <ul>
 *   <li>{@link DruidOverlord} starts and stops the segment metadata cache only when {@link #STANDALONE}.</li>
 *   <li>{@code OverlordCompactionScheduler} polls segments itself only when {@link #STANDALONE}.</li>
 * </ul>
 */
public enum OverlordMode
{
  /**
   * The Overlord runs as its own service, started by {@code CliOverlord}. It owns the lifecycle
   * of every component it uses.
   */
  STANDALONE,

  /**
   * The Overlord runs inside a Coordinator process ({@code druid.coordinator.asOverlord.enabled=true}
   * on the Coordinator). Components shared with the Coordinator, such as the segment metadata cache
   * and segment polling, are started and stopped by the Coordinator.
   */
  EMBEDDED_IN_COORDINATOR
}
