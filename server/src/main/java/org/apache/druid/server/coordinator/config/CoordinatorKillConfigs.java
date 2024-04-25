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
      = new CoordinatorKillConfigs(null, null, null, null, null, null, null, null, null, null, null, null, null);

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

  @JsonProperty("segmentSchema")
  private final MetadataCleanupConfig segmentSchema;

  // Raw configs for killing unused segments
  // These have been added as fields because KillUnusedSegmentsConfig is initialized lazily
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
      @JsonProperty("segmentSchema") MetadataCleanupConfig segmentSchema,
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
    this.audit = Configs.valueOrDefault(audit, MetadataCleanupConfig.DEFAULT);
    this.datasource = Configs.valueOrDefault(datasource, MetadataCleanupConfig.DEFAULT);
    this.rules = Configs.valueOrDefault(rules, MetadataCleanupConfig.DEFAULT);
    this.compaction = Configs.valueOrDefault(compaction, MetadataCleanupConfig.DEFAULT);
    this.segmentSchema = Configs.valueOrDefault(segmentSchema, MetadataCleanupConfig.DEFAULT);

    this.killUnusedEnabled = killUnusedEnabled;
    this.killUnusedPeriod = killUnusedPeriod;
    this.killUnusedDurationToRetain = killUnusedDurationToRetain;
    this.killUnusedBufferPeriod = killUnusedBufferPeriod;
    this.killUnusedIgnoreDurationToRetain = killUnusedIgnoreDurationToRetain;
    this.killUnusedMaxSegments = killUnusedMaxSegments;
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

  public MetadataCleanupConfig segmentSchema()
  {
    return segmentSchema;
  }

  /**
   * Creates a KillUnusedSegmentsConfig.
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
