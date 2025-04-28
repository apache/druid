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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Immutable class which represents the current status of a single clone server.
 */
public class ServerCloneStatus
{
  private final String sourceServer;
  private final String targetServer;
  private final State state;
  private final long segmentLoadsRemaining;
  private final long segmentDropsRemaining;
  private final long bytesToLoad;

  @JsonCreator
  public ServerCloneStatus(
      @JsonProperty("sourceServer") String sourceServer,
      @JsonProperty("targetServer") String targetServer,
      @JsonProperty("state") State state,
      @JsonProperty("segmentLoadsRemaining") long segmentLoadsRemaining,
      @JsonProperty("segmentDropsRemaining") long segmentDropsRemaining,
      @JsonProperty("bytesToLoad") long bytesToLoad
  )
  {
    this.sourceServer = sourceServer;
    this.targetServer = targetServer;
    this.state = state;
    this.segmentLoadsRemaining = segmentLoadsRemaining;
    this.segmentDropsRemaining = segmentDropsRemaining;
    this.bytesToLoad = bytesToLoad;
  }

  @JsonProperty
  public String getSourceServer()
  {
    return sourceServer;
  }

  @JsonProperty
  public String getTargetServer()
  {
    return targetServer;
  }

  @JsonProperty
  public long getSegmentLoadsRemaining()
  {
    return segmentLoadsRemaining;
  }

  @JsonProperty
  public long getSegmentDropsRemaining()
  {
    return segmentDropsRemaining;
  }

  @JsonProperty
  public long getBytesToLoad()
  {
    return bytesToLoad;
  }

  @JsonProperty
  public State getState()
  {
    return state;
  }

  /**
   * Create a {@link ServerCloneStatus} where the current status is unknown as the target server is missing.
   */
  public static ServerCloneStatus unknown(String sourceServer, String targetServer)
  {
    return new ServerCloneStatus(sourceServer, targetServer, State.TARGET_SERVER_MISSING, -1, -1, -1);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ServerCloneStatus that = (ServerCloneStatus) o;
    return segmentLoadsRemaining == that.segmentLoadsRemaining
           && segmentDropsRemaining == that.segmentDropsRemaining
           && bytesToLoad == that.bytesToLoad
           && Objects.equals(sourceServer, that.sourceServer)
           && Objects.equals(targetServer, that.targetServer)
           && state == that.state;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        sourceServer,
        targetServer,
        state,
        segmentLoadsRemaining,
        segmentDropsRemaining,
        bytesToLoad
    );
  }

  @Override
  public String toString()
  {
    return "ServerCloneStatus{" +
           "sourceServer='" + sourceServer + '\'' +
           ", targetServer='" + targetServer + '\'' +
           ", state=" + state +
           ", segmentLoadsRemaining=" + segmentLoadsRemaining +
           ", segmentDropsRemaining=" + segmentDropsRemaining +
           ", bytesToLoad=" + bytesToLoad +
           '}';
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
    IN_PROGRESS
  }
}
