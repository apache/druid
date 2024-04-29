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
  public static CoordinatorKillConfigs DEFAULT
      = new CoordinatorKillConfigs(null, null, null, null, null, null, null, null, null, null, null, null, null);

  @JsonProperty("supervisor")
  private final MetadataCleanupConfig supervisors;

  @JsonProperty("audit")
  private final MetadataCleanupConfig auditLogs;

  @JsonProperty("datasource")
  private final MetadataCleanupConfig datasources;

  @JsonProperty("rule")
  private final MetadataCleanupConfig rules;

  @JsonProperty("compaction")
  private final MetadataCleanupConfig compactionConfigs;

  @JsonProperty("pendingSegments")
  private final MetadataCleanupConfig pendingSegments;

  @JsonProperty("segmentSchema")
  private final MetadataCleanupConfig segmentSchemas;

  // Raw configs for killing unused segments
  // These have been added as fields to make JsonConfigurator happy
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
      @JsonProperty("audit") MetadataCleanupConfig auditLogs,
      @JsonProperty("datasource") MetadataCleanupConfig datasources,
      @JsonProperty("rule") MetadataCleanupConfig rules,
      @JsonProperty("compaction") MetadataCleanupConfig compactionConfigs,
      @JsonProperty("segmentSchema") MetadataCleanupConfig segmentSchemas,
      // Configs for cleanup of unused segments
      @JsonProperty("on") Boolean killUnusedEnabled,
      @JsonProperty("period") Duration killUnusedPeriod,
      @JsonProperty("durationToRetain") Duration killUnusedDurationToRetain,
      @JsonProperty("ignoreDurationToRetain") Boolean killUnusedIgnoreDurationToRetain,
      @JsonProperty("bufferPeriod") Duration killUnusedBufferPeriod,
      @JsonProperty("maxSegments") Integer killUnusedMaxSegments
  )
  {
    this.pendingSegments = Configs.valueOrDefault(pendingSegments, MetadataCleanupConfig.DEFAULT);
    this.supervisors = Configs.valueOrDefault(supervisors, MetadataCleanupConfig.DEFAULT);
    this.auditLogs = Configs.valueOrDefault(auditLogs, MetadataCleanupConfig.DEFAULT);
    this.datasources = Configs.valueOrDefault(datasources, MetadataCleanupConfig.DEFAULT);
    this.rules = Configs.valueOrDefault(rules, MetadataCleanupConfig.DEFAULT);
    this.compactionConfigs = Configs.valueOrDefault(compactionConfigs, MetadataCleanupConfig.DEFAULT);
    this.segmentSchemas = Configs.valueOrDefault(segmentSchemas, MetadataCleanupConfig.DEFAULT);

    this.killUnusedEnabled = killUnusedEnabled;
    this.killUnusedPeriod = killUnusedPeriod;
    this.killUnusedDurationToRetain = killUnusedDurationToRetain;
    this.killUnusedBufferPeriod = killUnusedBufferPeriod;
    this.killUnusedIgnoreDurationToRetain = killUnusedIgnoreDurationToRetain;
    this.killUnusedMaxSegments = killUnusedMaxSegments;
  }

  public MetadataCleanupConfig auditLogs()
  {
    return auditLogs;
  }

  public MetadataCleanupConfig datasources()
  {
    return datasources;
  }

  public MetadataCleanupConfig rules()
  {
    return rules;
  }

  public MetadataCleanupConfig compactionConfigs()
  {
    return compactionConfigs;
  }

  public MetadataCleanupConfig pendingSegments()
  {
    return pendingSegments;
  }

  public MetadataCleanupConfig supervisors()
  {
    return supervisors;
  }

  public MetadataCleanupConfig segmentSchemas()
  {
    return segmentSchemas;
  }

  /**
   * Creates a KillUnusedSegmentsConfig. This config is initialized lazily as
   * it uses the indexingPeriod as the default cleanup period.
   */
  public KillUnusedSegmentsConfig unusedSegments(Duration indexingPeriod)
  {
    return new KillUnusedSegmentsConfig(
        killUnusedEnabled,
        Configs.valueOrDefault(killUnusedPeriod, indexingPeriod),
        killUnusedDurationToRetain,
        killUnusedIgnoreDurationToRetain,
        killUnusedBufferPeriod,
        killUnusedMaxSegments
    );
  }
}
