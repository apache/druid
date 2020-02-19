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

package org.apache.druid.indexing.overlord.autoscaling;

import org.joda.time.Period;

public class CategorizedProvisioningConfig extends PendingTaskBasedWorkerProvisioningConfig
{
  @Override
  public CategorizedProvisioningConfig setMaxScalingStep(int maxScalingStep)
  {
    super.setMaxScalingStep(maxScalingStep);
    return this;
  }

  @Override
  public CategorizedProvisioningConfig setWorkerIdleTimeout(Period workerIdleTimeout)
  {
    super.setWorkerIdleTimeout(workerIdleTimeout);
    return this;
  }

  @Override
  public CategorizedProvisioningConfig setMaxScalingDuration(Period maxScalingDuration)
  {
    super.setMaxScalingDuration(maxScalingDuration);
    return this;
  }

  @Override
  public CategorizedProvisioningConfig setNumEventsToTrack(int numEventsToTrack)
  {
    super.setNumEventsToTrack(numEventsToTrack);
    return this;
  }

  @Override
  public CategorizedProvisioningConfig setWorkerVersion(String workerVersion)
  {
    super.setWorkerVersion(workerVersion);
    return this;
  }

  @Override
  public CategorizedProvisioningConfig setPendingTaskTimeout(Period pendingTaskTimeout)
  {
    super.setPendingTaskTimeout(pendingTaskTimeout);
    return this;
  }

  @Override
  public CategorizedProvisioningConfig setWorkerPort(int workerPort)
  {
    super.setWorkerPort(workerPort);
    return this;
  }
}
