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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Immutable class which represents the current status of a single clone server.
 *
 * @param segmentsPendingSync Number of segments that are already loaded on the
 *                            source server but are yet to be loaded on the target server.
 */
public record ServerCloneStatus(
    @JsonProperty("sourceServer") String sourceServer,
    @JsonProperty("targetServer") String targetServer,
    @JsonProperty("state") State state,
    @JsonProperty("segmentLoadsRemaining") long segmentLoadsRemaining,
    @JsonProperty("segmentDropsRemaining") long segmentDropsRemaining,
    @JsonProperty("segmentsPendingSync") long segmentsPendingSync,
    @JsonProperty("percentPendingSync") double percentPendingSync,
    @JsonProperty("bytesToLoad") long bytesToLoad
)
{
  /**
   * Create a {@link ServerCloneStatus} where the current status is unknown as the target server is missing.
   */
  public static ServerCloneStatus unknown(String sourceServer, String targetServer)
  {
    return new ServerCloneStatus(sourceServer, targetServer, State.TARGET_SERVER_MISSING, -1, -1, -1, -1, -1);
  }

  /**
   * Enum determining the status of the cloning process.
   */
  public enum State
  {
    /**
     * The source server is missing from the current cluster view. The clone is continuing to load segments based on the
     * last seen state of the source cluster.
     */
    SOURCE_SERVER_MISSING,
    /**
     * The target server is missing from the current cluster view.
     */
    TARGET_SERVER_MISSING,
    /**
     * Segments are loaded or being loaded. The counts give a better view of the progress.
     */
    IN_PROGRESS,
    /**
     * Clone server has caught up with the source server. This typically means
     * that nearly all (based on configured tolerance) the segments currently
     * present on the source server have also been loaded on the target server,
     * and that segments in the load queue of the source server are also present
     * in the load queue of the target server.
     */
    SYNCED
  }
}
