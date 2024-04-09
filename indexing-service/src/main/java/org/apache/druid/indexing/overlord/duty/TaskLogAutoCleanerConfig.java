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

package org.apache.druid.indexing.overlord.duty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.Configs;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Config for periodic cleanup of stale task logs from metadata store and deep
 * storage.
 */
public class TaskLogAutoCleanerConfig
{
  @JsonProperty
  private final boolean enabled;

  @JsonProperty
  private final long initialDelay;

  @JsonProperty
  private final long delay;

  @JsonProperty
  private final long durationToRetain;

  /**
   * Config for Task logs auto-cleaner.
   * All time-related parameters should be in milliseconds.
   */
  @JsonCreator
  public TaskLogAutoCleanerConfig(
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("initialDelay") Long initialDelay,
      @JsonProperty("delay") Long delay,
      @JsonProperty("durationToRetain") Long durationToRetain
  )
  {
    if (enabled) {
      Preconditions.checkNotNull(durationToRetain, "'durationToRetain' must be provided.");
    }

    this.enabled = enabled;
    this.initialDelay = Configs.valueOrDefault(
        initialDelay,
        60000 + ThreadLocalRandom.current().nextInt(4 * 60000)
    );
    this.delay = Configs.valueOrDefault(delay, TimeUnit.HOURS.toMillis(6));
    this.durationToRetain = Configs.valueOrDefault(durationToRetain, Long.MAX_VALUE);

    Preconditions.checkArgument(this.initialDelay > 0, "'initialDelay' must be greater than 0.");
    Preconditions.checkArgument(this.delay > 0, "'delay' must be greater than 0.");
    Preconditions.checkArgument(this.durationToRetain > 0, "'durationToRetain' must be greater than 0.");
  }

  public boolean isEnabled()
  {
    return enabled;
  }

  public long getInitialDelay()
  {
    return initialDelay;
  }

  public long getDelay()
  {
    return delay;
  }

  public long getDurationToRetain()
  {
    return durationToRetain;
  }

  @Override
  public String toString()
  {
    return "TaskLogAutoCleanerConfig{" +
           "enabled=" + enabled +
           ", initialDelay=" + initialDelay +
           ", delay=" + delay +
           ", durationToRetain=" + durationToRetain +
           '}';
  }
}
