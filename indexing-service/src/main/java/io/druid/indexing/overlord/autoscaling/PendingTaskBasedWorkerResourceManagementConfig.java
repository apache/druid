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
public class PendingTaskBasedWorkerResourceManagementConfig extends SimpleWorkerResourceManagementConfig
{
  @JsonProperty
  private int maxScalingStep = 10;


  public int getMaxScalingStep()
  {
    return maxScalingStep;
  }

  public PendingTaskBasedWorkerResourceManagementConfig setMaxScalingStep(int maxScalingStep)
  {
    this.maxScalingStep = maxScalingStep;
    return this;
  }

  public PendingTaskBasedWorkerResourceManagementConfig setWorkerIdleTimeout(Period workerIdleTimeout)
  {
    super.setWorkerIdleTimeout(workerIdleTimeout);
    return this;
  }

  public PendingTaskBasedWorkerResourceManagementConfig setMaxScalingDuration(Period maxScalingDuration)
  {
    super.setMaxScalingDuration(maxScalingDuration);
    return this;
  }

  public PendingTaskBasedWorkerResourceManagementConfig setNumEventsToTrack(int numEventsToTrack)
  {
    super.setNumEventsToTrack(numEventsToTrack);
    return this;
  }

  public PendingTaskBasedWorkerResourceManagementConfig setWorkerVersion(String workerVersion)
  {
    super.setWorkerVersion(workerVersion);
    return this;
  }

  public PendingTaskBasedWorkerResourceManagementConfig setPendingTaskTimeout(Period pendingTaskTimeout)
  {
    super.setPendingTaskTimeout(pendingTaskTimeout);
    return this;
  }

}
