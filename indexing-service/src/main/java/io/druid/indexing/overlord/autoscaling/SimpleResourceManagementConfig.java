/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.overlord.autoscaling;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Period;

/**
 */
public class SimpleResourceManagementConfig
{
  @JsonProperty
  private Period workerIdleTimeout = new Period("PT10m");

  @JsonProperty
  private Period maxScalingDuration = new Period("PT15M");

  @JsonProperty
  private int numEventsToTrack = 50;

  @JsonProperty
  private Period pendingTaskTimeout = new Period("PT30s");

  @JsonProperty
  private String workerVersion = null;

  @JsonProperty
  private int workerPort = 8080;

  public Period getWorkerIdleTimeout()
  {
    return workerIdleTimeout;
  }

  public SimpleResourceManagementConfig setWorkerIdleTimeout(Period workerIdleTimeout)
  {
    this.workerIdleTimeout = workerIdleTimeout;
    return this;
  }

  public Period getMaxScalingDuration()
  {
    return maxScalingDuration;
  }

  public SimpleResourceManagementConfig setMaxScalingDuration(Period maxScalingDuration)
  {
    this.maxScalingDuration = maxScalingDuration;
    return this;
  }

  public int getNumEventsToTrack()
  {
    return numEventsToTrack;
  }

  public SimpleResourceManagementConfig setNumEventsToTrack(int numEventsToTrack)
  {
    this.numEventsToTrack = numEventsToTrack;
    return this;
  }

  public Period getPendingTaskTimeout()
  {
    return pendingTaskTimeout;
  }

  public SimpleResourceManagementConfig setPendingTaskTimeout(Period pendingTaskTimeout)
  {
    this.pendingTaskTimeout = pendingTaskTimeout;
    return this;
  }

  public String getWorkerVersion()
  {
    return workerVersion;
  }

  public SimpleResourceManagementConfig setWorkerVersion(String workerVersion)
  {
    this.workerVersion = workerVersion;
    return this;
  }

  public int getWorkerPort()
  {
    return workerPort;
  }

  public SimpleResourceManagementConfig setWorkerPort(int workerPort)
  {
    this.workerPort = workerPort;
    return this;
  }
}
