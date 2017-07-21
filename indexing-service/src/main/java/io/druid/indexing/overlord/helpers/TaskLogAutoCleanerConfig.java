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

package io.druid.indexing.overlord.helpers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Random;

/**
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

  @JsonCreator
  public TaskLogAutoCleanerConfig(
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("initialDelay") Long initialDelay,
      @JsonProperty("delay") Long delay,
      @JsonProperty("durationToRetain") Long durationToRetain
  )
  {
    if (enabled) {
      Preconditions.checkNotNull(durationToRetain, "durationToRetain must be provided.");
    }

    this.enabled = enabled;
    this.initialDelay = initialDelay == null ? 60000 + new Random().nextInt(4*60000) : initialDelay.longValue();
    this.delay = delay == null ? 6*60*60*1000 : delay.longValue();
    this.durationToRetain = durationToRetain == null ? Long.MAX_VALUE : durationToRetain.longValue();

    Preconditions.checkArgument(this.initialDelay > 0, "initialDelay must be > 0.");
    Preconditions.checkArgument(this.delay > 0, "delay must be > 0.");
    Preconditions.checkArgument(this.durationToRetain > 0, "durationToRetain must be > 0.");
  }

  public boolean isEnabled()
  {
    return this.enabled;
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
