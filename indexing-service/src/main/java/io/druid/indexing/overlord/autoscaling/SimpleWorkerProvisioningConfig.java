/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord.autoscaling;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Period;

/**
 */
public class SimpleWorkerProvisioningConfig
{
  @JsonProperty
  private Period workerIdleTimeout = new Period("PT90m");

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

  public SimpleWorkerProvisioningConfig setWorkerIdleTimeout(Period workerIdleTimeout)
  {
    this.workerIdleTimeout = workerIdleTimeout;
    return this;
  }

  public Period getMaxScalingDuration()
  {
    return maxScalingDuration;
  }

  public SimpleWorkerProvisioningConfig setMaxScalingDuration(Period maxScalingDuration)
  {
    this.maxScalingDuration = maxScalingDuration;
    return this;
  }

  public int getNumEventsToTrack()
  {
    return numEventsToTrack;
  }

  public SimpleWorkerProvisioningConfig setNumEventsToTrack(int numEventsToTrack)
  {
    this.numEventsToTrack = numEventsToTrack;
    return this;
  }

  public Period getPendingTaskTimeout()
  {
    return pendingTaskTimeout;
  }

  public SimpleWorkerProvisioningConfig setPendingTaskTimeout(Period pendingTaskTimeout)
  {
    this.pendingTaskTimeout = pendingTaskTimeout;
    return this;
  }

  public String getWorkerVersion()
  {
    return workerVersion;
  }

  public SimpleWorkerProvisioningConfig setWorkerVersion(String workerVersion)
  {
    this.workerVersion = workerVersion;
    return this;
  }

  // Do not use this if possible. Assuming all workers will have the same port is bad for containers.
  public int getWorkerPort()
  {
    return workerPort;
  }

  public SimpleWorkerProvisioningConfig setWorkerPort(int workerPort)
  {
    this.workerPort = workerPort;
    return this;
  }
}
