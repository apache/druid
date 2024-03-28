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

package org.apache.druid.server.coordinator.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.joda.time.Duration;

public class CoordinatorKillConfigs
{
  public static CoordinatorKillConfigs STANDARD
      = new CoordinatorKillConfigs(null, null, null, null, null, null, null, null, null, null, null, null);

  @JsonProperty("supervisor")
  private final MetadataCleanupConfig supervisors;

  @JsonProperty("audit")
  private final MetadataCleanupConfig audit;

  @JsonProperty("datasource")
  private final MetadataCleanupConfig datasource;

  @JsonProperty("rule")
  private final MetadataCleanupConfig rules;

  @JsonProperty("compaction")
  private final MetadataCleanupConfig compaction;

  @JsonProperty("pendingSegments")
  private final MetadataCleanupConfig pendingSegments;

  private final KillUnusedSegmentsConfig unusedSegments;

  // Raw configs for killing unused segments
  // These have been added as fields just to keep JsonConfigurator happy
  @JsonProperty("on")
  private final Boolean killUnusedEnabled;

  @JsonProperty("period")
  private final Duration killUnusedPeriod;

  @JsonProperty("durationToRetain")
  private final Duration killUnusedDurationToRetain;

  @JsonProperty("ignoreDurationToRetain")
  private final Boolean killUnusedIgnoreDurationToRetain;

  @JsonProperty("bufferPeriod")
  private final Duration killUnusedBufferPeriod;

  @JsonProperty("maxSegments")
  private final Integer killUnusedMaxSegments;

  @JsonCreator
  public CoordinatorKillConfigs(
      @JsonProperty("pendingSegments") MetadataCleanupConfig pendingSegments,
      @JsonProperty("supervisor") MetadataCleanupConfig supervisors,
      @JsonProperty("audit") MetadataCleanupConfig audit,
      @JsonProperty("datasource") MetadataCleanupConfig datasource,
      @JsonProperty("rule") MetadataCleanupConfig rules,
      @JsonProperty("compaction") MetadataCleanupConfig compaction,
      // Configs for cleanup of unused segments
      @JsonProperty("on") Boolean killUnusedEnabled,
      @JsonProperty("period") Duration killUnusedPeriod,
      @JsonProperty("durationToRetain") Duration killUnusedDurationToRetain,
      @JsonProperty("ignoreDurationToRetain") Boolean killUnusedIgnoreDurationToRetain,
      @JsonProperty("bufferPeriod") Duration killUnusedBufferPeriod,
      @JsonProperty("maxSegments") Integer killUnusedMaxSegments
  )
  {
    this.pendingSegments = Configs.valueOrDefault(pendingSegments, MetadataCleanupConfig.STANDARD);
    this.supervisors = Configs.valueOrDefault(supervisors, MetadataCleanupConfig.STANDARD);
    this.audit = Configs.valueOrDefault(audit, MetadataCleanupConfig.STANDARD);
    this.datasource = Configs.valueOrDefault(datasource, MetadataCleanupConfig.STANDARD);
    this.rules = Configs.valueOrDefault(rules, MetadataCleanupConfig.STANDARD);
    this.compaction = Configs.valueOrDefault(compaction, MetadataCleanupConfig.STANDARD);

    this.killUnusedEnabled = killUnusedEnabled;
    this.killUnusedPeriod = killUnusedPeriod;
    this.killUnusedDurationToRetain = killUnusedDurationToRetain;
    this.killUnusedBufferPeriod = killUnusedBufferPeriod;
    this.killUnusedIgnoreDurationToRetain = killUnusedIgnoreDurationToRetain;
    this.killUnusedMaxSegments = killUnusedMaxSegments;

    this.unusedSegments = createUnusedSegmentsConfig();
  }

  private KillUnusedSegmentsConfig createUnusedSegmentsConfig()
  {
    return new KillUnusedSegmentsConfig(
        killUnusedEnabled,
        killUnusedPeriod,
        killUnusedDurationToRetain,
        killUnusedIgnoreDurationToRetain,
        killUnusedBufferPeriod,
        killUnusedMaxSegments
    );
  }

  public MetadataCleanupConfig audit()
  {
    return audit;
  }

  public MetadataCleanupConfig datasource()
  {
    return datasource;
  }

  public MetadataCleanupConfig rules()
  {
    return rules;
  }

  public MetadataCleanupConfig compaction()
  {
    return compaction;
  }

  public MetadataCleanupConfig pendingSegments()
  {
    return pendingSegments;
  }

  public MetadataCleanupConfig supervisors()
  {
    return supervisors;
  }

  public KillUnusedSegmentsConfig unusedSegments()
  {
    return unusedSegments;
  }
}
