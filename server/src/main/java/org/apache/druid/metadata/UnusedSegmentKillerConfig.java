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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;
import org.joda.time.Period;

import javax.annotation.Nullable;

/**
 * Config for {@code UnusedSegmentKiller}. This is used only by the Overlord.
 * Enabling this config on the Coordinator or other services has no effect.
 */
public class UnusedSegmentKillerConfig
{
  private static final Logger log = new Logger(UnusedSegmentKillerConfig.class);

  /**
   * Maximum number of segments that should be killed in a single run of the duty.
   * A value of 200k typically causes the search query to finish within ~5s and
   * allows the kill queue to finish processing in about (200 * 30s) = 100 minutes,
   * since a batch of 1000 segments takes about 30s to be processed.
   */
  public static final int DEFAULT_MAX_SEGMENTS_TO_KILL = 200_000;

  public static final Period DEFAULT_BUFFER_PERIOD = Period.days(30);

  /**
   * Grace period as used in {@link #getMaxUpdatedTimeOfKillableSegment()}.
   * A grace period of 1 hour is adequate to allow any ongoing segment update
   * operations to finish.
   */
  public static final Period GRACE_PERIOD = Period.hours(1);

  @JsonProperty("enabled")
  private final boolean enabled;

  @JsonProperty("bufferPeriod")
  private final Period bufferPeriod;

  @JsonProperty("dutyPeriod")
  private final Period dutyPeriod;

  @JsonProperty("maxSegmentsToKill")
  private final Integer maxSegmentsToKill;

  @JsonCreator
  public UnusedSegmentKillerConfig(
      @JsonProperty("enabled") @Nullable Boolean enabled,
      @JsonProperty("bufferPeriod") @Nullable Period bufferPeriod,
      @JsonProperty("dutyPeriod") @Nullable Period dutyPeriod,
      @JsonProperty("maxSegmentsToKill") @Nullable Integer maxSegmentsToKill
  )
  {
    this.enabled = Configs.valueOrDefault(enabled, false);
    this.bufferPeriod = Configs.valueOrDefault(bufferPeriod, DEFAULT_BUFFER_PERIOD);
    this.maxSegmentsToKill = Configs.valueOrDefault(maxSegmentsToKill, DEFAULT_MAX_SEGMENTS_TO_KILL);

    if (this.maxSegmentsToKill > DEFAULT_MAX_SEGMENTS_TO_KILL) {
      log.warn(
          "Setting a high value[%d] for 'druid.manager.segments.killUnused.maxSegmentsToKill'."
          + " This may slow down the segment killer and/or put undue strain on the metadata store.",
          this.maxSegmentsToKill
      );
    } else {
      InvalidInput.conditionalException(
          this.maxSegmentsToKill > 0,
          "'druid.manager.segments.killUnused.maxSegmentsToKill' must be greater than zero"
      );
    }

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
   * Default value is {@link #DEFAULT_BUFFER_PERIOD}.
   */
  public Period getBufferPeriod()
  {
    return bufferPeriod;
  }

  /**
   * Maximum value for the updated time of a segment that makes it eligible for
   * kill. A segment becomes eligible if it has been unused for at least the
   * {@link #getBufferPeriod()}. After this period, the segment cannot be marked
   * as used again anymore. Since marking a non-overshadowed segment as used can
   * be a slow operation (due to the requirement to build the entire timeline
   * and then identify non-overshadowed segments), a {@link #GRACE_PERIOD} is
   * added to the buffer period. This helps avoid any unexpected behaviour in
   * case a slow update operation is started right at the boundary of the buffer
   * period, and a kill task is launched right after.
   */
  public DateTime getMaxUpdatedTimeOfKillableSegment()
  {
    return DateTimes.nowUtc().minus(bufferPeriod.plus(GRACE_PERIOD));
  }

  /**
   * Period dictating the frequency at which the unused segment killer duty
   * should be run. This config is for testing only and SHOULD NOT be used in
   * production clusters.
   */
  public Period getDutyPeriod()
  {
    return dutyPeriod;
  }

  /**
   * Maximum number of segments to kill in a single run of the duty. Default
   * value is {@link #DEFAULT_MAX_SEGMENTS_TO_KILL}.
   */
  public int getMaxSegmentsToKill()
  {
    return maxSegmentsToKill;
  }

  public boolean isEnabled()
  {
    return enabled;
  }
}
