/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.validation.constraints.Min;

public class TierLocalTaskRunnerConfig extends ForkingTaskRunnerConfig
{
  /**
   * This is the time (in ms) that the forking task runner should allow the task to softly shutdown before trying to forcibly kill it.
   */
  @JsonProperty
  @Min(0)
  private long softShutdownTimeLimit = 30_000L;

  /**
   * The time (in ms) to wait to receive a heartbeat before terminating
   */
  @JsonProperty
  @Min(0)
  private long heartbeatTimeLimit = 300_000L;

  /**
   * Time (in ms) to wait for network timeout when sending heartbeats to tasks.
   * WARNING: Tasks are polled serially, and only local tasks are polled by this runner, so this value should be
   * low and significantly lower than heartbeatTimeLimit / maxHeartbeatRetries
   * Failure to send a heartbeat will simply log the error and wait until the next round of heartbeat attempts.
   */
  @JsonProperty
  @Min(0)
  private long heartbeatLocalNetworkTimeout = 100L;

  /**
   * Maximum count of retries to send a heartbeat to a local worker. Specifically, the delay between post attempts
   * follows this formula: heartbeatTimeLimit / maxHeartbeatRetries
   */

  @JsonProperty
  @Min(0)
  private long maxHeartbeatRetries = 10L;

  public TierLocalTaskRunnerConfig setMaxHeartbeatRetries(long retries)
  {
    Preconditions.checkArgument(retries >= 0, "retries too small");
    maxHeartbeatRetries = retries;
    return this;
  }

  public long getMaxHeartbeatRetries()
  {
    return maxHeartbeatRetries;
  }

  public long getDelayBetweenHeartbeatBatches()
  {
    return getHeartbeatTimeLimit() / getMaxHeartbeatRetries();
  }

  public TierLocalTaskRunnerConfig setHeartbeatLocalNetworkTimeout(long timeout)
  {
    Preconditions.checkArgument(timeout >= 0, "timeout too small");
    heartbeatLocalNetworkTimeout = timeout;
    return this;
  }

  public long getHeartbeatLocalNetworkTimeout()
  {
    return heartbeatLocalNetworkTimeout;
  }

  public TierLocalTaskRunnerConfig setSoftShutdownTimeLimit(long softShutdownTimeLimit)
  {
    this.softShutdownTimeLimit = softShutdownTimeLimit;
    return this;
  }

  public long getSoftShutdownTimeLimit()
  {
    return softShutdownTimeLimit;
  }

  public TierLocalTaskRunnerConfig setHeartbeatTimeLimit(long heartbeatTimeLimit)
  {
    this.heartbeatTimeLimit = heartbeatTimeLimit;
    return this;
  }

  public long getHeartbeatTimeLimit()
  {
    return heartbeatTimeLimit;
  }
}
