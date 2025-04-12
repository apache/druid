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
  private final Status status;
  private final long segmentLoadsRemaining;
  private final long segmenetsDropsRemaining;
  private final long bytesRemaining;

  @JsonCreator
  public CloneStatusMetrics(
      @JsonProperty("sourceServer") String sourceServer,
      @JsonProperty("status") Status status,
      @JsonProperty("segmentLoadsRemaining") long segmentLoadsRemaining,
      @JsonProperty("segmentDropsRemaining") long segmenetsDropsRemaining,
      @JsonProperty("bytesRemaining") long bytesRemaining
  )
  {
    this.sourceServer = sourceServer;
    this.status = status;
    this.segmentLoadsRemaining = segmentLoadsRemaining;
    this.segmenetsDropsRemaining = segmenetsDropsRemaining;
    this.bytesRemaining = bytesRemaining;
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

  @JsonProperty("segmentDropsRemaining")
  public long getSegmenetsDropsRemaining()
  {
    return segmenetsDropsRemaining;
  }

  @JsonProperty("bytesRemaining")
  public long getBytesRemaining()
  {
    return bytesRemaining;
  }

  @JsonProperty("status")
  public Status getStatus()
  {
    return status;
  }

  public static CloneStatusMetrics unknown(String sourceServer)
  {
    return new CloneStatusMetrics(sourceServer, Status.TARGET_SERVER_MISSING, 0, 0, 0);
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
           && Objects.equals(sourceServer, that.sourceServer)
           && status == that.status;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(sourceServer, status, segmentLoadsRemaining, segmenetsDropsRemaining, bytesRemaining);
  }

  @Override
  public String toString()
  {
    return "CloneStatusMetrics{" +
           "sourceServer='" + sourceServer + '\'' +
           ", status=" + status +
           ", segmentLoadsRemaining=" + segmentLoadsRemaining +
           ", segmenetsDropsRemaining=" + segmenetsDropsRemaining +
           ", bytesRemaining=" + bytesRemaining +
           '}';
  }

  public enum Status
  {
    SOURCE_SERVER_MISSING,
    TARGET_SERVER_MISSING,
    LOADING
  }
}
