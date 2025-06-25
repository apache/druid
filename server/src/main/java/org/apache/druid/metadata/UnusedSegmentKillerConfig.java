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
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.Period;

import javax.annotation.Nullable;

/**
 * Config for {@code UnusedSegmentKiller}. This is used only by the Overlord.
 * Enabling this config on the Coordinator or other services has no effect.
 */
public class UnusedSegmentKillerConfig
{
  private static final Logger log = new Logger(UnusedSegmentKillerConfig.class);

  @JsonProperty("enabled")
  private final boolean enabled;

  @JsonProperty("bufferPeriod")
  private final Period bufferPeriod;

  @JsonProperty("dutyPeriod")
  private final Period dutyPeriod;

  @JsonCreator
  public UnusedSegmentKillerConfig(
      @JsonProperty("enabled") @Nullable Boolean enabled,
      @JsonProperty("bufferPeriod") @Nullable Period bufferPeriod,
      @JsonProperty("dutyPeriod") @Nullable Period dutyPeriod
  )
  {
    this.enabled = Configs.valueOrDefault(enabled, false);
    this.bufferPeriod = Configs.valueOrDefault(bufferPeriod, Period.days(30));

    if (dutyPeriod == null) {
      this.dutyPeriod = Period.hours(1);
    } else {
      log.warn(
          "The config 'druid.manager.segments.killUnused.dutyPeriod'"
          + " is for testing only and should not be set in production clusters"
          + " as it may have unintended side-effects."
      );
      this.dutyPeriod = dutyPeriod;
    }
  }

  /**
   * Period for which segments are retained even after being marked as unused.
   */
  public Period getBufferPeriod()
  {
    return bufferPeriod;
  }

  /**
   * Period describing the frequency at which the unused segment killer duty
   * should be run. This config is for testing only and should not be used in
   * production clusters.
   */
  public Period getDutyPeriod()
  {
    return dutyPeriod;
  }

  public boolean isEnabled()
  {
    return enabled;
  }
}
