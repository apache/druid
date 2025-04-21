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

public class ServerCloneStatus
{
  private final String sourceServer;
  private final State state;
  private final long segmentLoadsRemaining;
  private final long segmentDropsRemaining;
  private final long bytesRemaining;

  @JsonCreator
  public ServerCloneStatus(
      @JsonProperty("sourceServer") String sourceServer,
      @JsonProperty("state") State state,
      @JsonProperty("segmentLoadsRemaining") long segmentLoadsRemaining,
      @JsonProperty("segmentDropsRemaining") long segmentDropsRemaining,
      @JsonProperty("bytesRemaining") long bytesRemaining
  )
  {
    this.sourceServer = sourceServer;
    this.state = state;
    this.segmentLoadsRemaining = segmentLoadsRemaining;
    this.segmentDropsRemaining = segmentDropsRemaining;
    this.bytesRemaining = bytesRemaining;
  }

  @JsonProperty
  public String getSourceServer()
  {
    return sourceServer;
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
  public long getBytesRemaining()
  {
    return bytesRemaining;
  }

  @JsonProperty
  public State getState()
  {
    return state;
  }

  public static ServerCloneStatus unknown(String sourceServer)
  {
    return new ServerCloneStatus(sourceServer, State.TARGET_SERVER_MISSING, 0, 0, 0);
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
           && bytesRemaining == that.bytesRemaining
           && Objects.equals(sourceServer, that.sourceServer)
           && state == that.state;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(sourceServer, state, segmentLoadsRemaining, segmentDropsRemaining, bytesRemaining);
  }

  @Override
  public String toString()
  {
    return "CloneStatusMetrics{" +
           "sourceServer='" + sourceServer + '\'' +
           ", state=" + state +
           ", segmentLoadsRemaining=" + segmentLoadsRemaining +
           ", segmentDropsRemaining=" + segmentDropsRemaining +
           ", bytesRemaining=" + bytesRemaining +
           '}';
  }

  /**
   * Enum determining the status of the cloning process.
   */
  public enum State
  {
    SOURCE_SERVER_MISSING,
    TARGET_SERVER_MISSING,
    LOADING
  }
}
