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

package org.apache.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.joda.time.Period;

import javax.annotation.Nullable;

/**
 * Config for {@code UnusedSegmentKiller}.
 */
public class UnusedSegmentKillerConfig
{
  @JsonProperty("enabled")
  private final boolean enabled;

  @JsonProperty("bufferPeriod")
  private final Period bufferPeriod;

  @JsonCreator
  public UnusedSegmentKillerConfig(
      @JsonProperty("enabled") @Nullable Boolean enabled,
      @JsonProperty("bufferPeriod") @Nullable Period bufferPeriod
  )
  {
    this.enabled = Configs.valueOrDefault(enabled, false);
    this.bufferPeriod = Configs.valueOrDefault(bufferPeriod, Period.days(90));
  }

  /**
   * Period for which segments are retained even after being marked as unused.
   * If this returns null, segments are never killed by the {@code UnusedSegmentKiller}
   * but they might still be killed by the Coordinator.
   */
  public Period getBufferPeriod()
  {
    return bufferPeriod;
  }

  public boolean isEnabled()
  {
    return enabled;
  }
}
