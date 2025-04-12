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

public class CloneStatusMetrics
{
  private final String sourceServer;
  private final long segmentLoadsRemaining;
  private final long segmenetsDropsRemaining;
  private final long bytesRemaining;
  private final long segmentChangesInLastRun;

  @JsonCreator
  public CloneStatusMetrics(
      @JsonProperty("sourceServer") String sourceServer,
      @JsonProperty("segmentLoadsRemaining") long segmentLoadsRemaining,
      @JsonProperty("segmenetsDropsRemaining") long segmenetsDropsRemaining,
      @JsonProperty("bytesRemaining") long bytesRemaining,
      @JsonProperty("segmentChangesInLastRun") long segmentChangesInLastRun
  )
  {
    this.sourceServer = sourceServer;
    this.segmentLoadsRemaining = segmentLoadsRemaining;
    this.segmenetsDropsRemaining = segmenetsDropsRemaining;
    this.bytesRemaining = bytesRemaining;
    this.segmentChangesInLastRun = segmentChangesInLastRun;
  }

  @JsonProperty("sourceServer")
  public String getSourceServer()
  {
    return sourceServer;
  }

  @JsonProperty("segmentLoadsRemaining")
  public long getSegmentLoadsRemaining()
  {
    return segmentLoadsRemaining;
  }

  @JsonProperty("segmenetsDropsRemaining")
  public long getSegmenetsDropsRemaining()
  {
    return segmenetsDropsRemaining;
  }

  @JsonProperty("bytesRemaining")
  public long getBytesRemaining()
  {
    return bytesRemaining;
  }

  @JsonProperty("segmentChangesInLastRun")
  public long getSegmentChangesInLastRun()
  {
    return segmentChangesInLastRun;
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
    CloneStatusMetrics that = (CloneStatusMetrics) o;
    return segmentLoadsRemaining == that.segmentLoadsRemaining
           && segmenetsDropsRemaining == that.segmenetsDropsRemaining
           && bytesRemaining == that.bytesRemaining
           && segmentChangesInLastRun == that.segmentChangesInLastRun
           && Objects.equals(sourceServer, that.sourceServer);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        sourceServer,
        segmentLoadsRemaining,
        segmenetsDropsRemaining,
        bytesRemaining,
        segmentChangesInLastRun
    );
  }

  @Override
  public String toString()
  {
    return "CloneStatusMetrics{" +
           "sourceServer='" + sourceServer + '\'' +
           ", segmentLoadsRemaining=" + segmentLoadsRemaining +
           ", segmenetsDropsRemaining=" + segmenetsDropsRemaining +
           ", bytesRemaining=" + bytesRemaining +
           ", segmentChangesInLastRun=" + segmentChangesInLastRun +
           '}';
  }
}
