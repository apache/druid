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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.server.compaction.CompactionCandidateSearchPolicy;
import org.apache.druid.server.compaction.NewestSegmentFirstPolicy;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Cluster-level compaction configs.
 * All fields of this class are nullable. A non-null value denotes that the
 * corresponding field has been explicitly specified.
 */
public class ClusterCompactionConfig
{
  private static final CompactionCandidateSearchPolicy DEFAULT_COMPACTION_POLICY
      = new NewestSegmentFirstPolicy(null);

  private final double compactionTaskSlotRatio;
  private final int maxCompactionTaskSlots;
  private final boolean useSupervisors;
  private final CompactionEngine engine;
  private final CompactionCandidateSearchPolicy compactionPolicy;
  /**
   * Whether to persist last compaction state directly in segments for backwards compatibility.
   * <p>
   * In a future release this option will be removed and last compaction state will no longer be persisted in segments.
   * Instead, it will only be stored in the metadata store with a fingerprint id that segments will reference. Some
   * operators may want to disable this behavior early to begin saving space in segment metadatastore table entries.
   */
  private final boolean legacyPersistLastCompactionStateInSegments;

  @JsonCreator
  public ClusterCompactionConfig(
      @JsonProperty("compactionTaskSlotRatio") @Nullable Double compactionTaskSlotRatio,
      @JsonProperty("maxCompactionTaskSlots") @Nullable Integer maxCompactionTaskSlots,
      @JsonProperty("compactionPolicy") @Nullable CompactionCandidateSearchPolicy compactionPolicy,
      @JsonProperty("useSupervisors") @Nullable Boolean useSupervisors,
      @JsonProperty("engine") @Nullable CompactionEngine engine,
      @JsonProperty("legacyPersistLastCompactionStateInSegments") Boolean legacyPersistLastCompactionStateInSegments
  )
  {
    this.compactionTaskSlotRatio = Configs.valueOrDefault(compactionTaskSlotRatio, 0.1);
    this.maxCompactionTaskSlots = Configs.valueOrDefault(maxCompactionTaskSlots, Integer.MAX_VALUE);
    this.compactionPolicy = Configs.valueOrDefault(compactionPolicy, DEFAULT_COMPACTION_POLICY);
    this.engine = Configs.valueOrDefault(engine, CompactionEngine.NATIVE);
    this.useSupervisors = Configs.valueOrDefault(useSupervisors, false);
    this.legacyPersistLastCompactionStateInSegments = Configs.valueOrDefault(
        legacyPersistLastCompactionStateInSegments,
        true
    );

    if (!this.useSupervisors && this.engine == CompactionEngine.MSQ) {
      throw InvalidInput.exception("MSQ Compaction engine can be used only with compaction supervisors.");
    }
  }

  @JsonProperty
  public double getCompactionTaskSlotRatio()
  {
    return compactionTaskSlotRatio;
  }

  @JsonProperty
  public int getMaxCompactionTaskSlots()
  {
    return maxCompactionTaskSlots;
  }

  @JsonProperty
  public CompactionCandidateSearchPolicy getCompactionPolicy()
  {
    return compactionPolicy;
  }

  @JsonProperty
  public boolean isUseSupervisors()
  {
    return useSupervisors;
  }

  @JsonProperty
  public CompactionEngine getEngine()
  {
    return engine;
  }

  @JsonProperty
  public boolean isLegacyPersistLastCompactionStateInSegments()
  {
    return legacyPersistLastCompactionStateInSegments;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClusterCompactionConfig that = (ClusterCompactionConfig) o;
    return Objects.equals(compactionTaskSlotRatio, that.compactionTaskSlotRatio)
           && Objects.equals(maxCompactionTaskSlots, that.maxCompactionTaskSlots)
           && Objects.equals(compactionPolicy, that.compactionPolicy)
           && Objects.equals(useSupervisors, that.useSupervisors)
           && Objects.equals(engine, that.engine)
           && Objects.equals(legacyPersistLastCompactionStateInSegments, that.legacyPersistLastCompactionStateInSegments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        compactionTaskSlotRatio,
        maxCompactionTaskSlots,
        compactionPolicy,
        useSupervisors,
        engine,
        legacyPersistLastCompactionStateInSegments
    );
  }

  @Override
  public String toString()
  {
    return "ClusterCompactionConfig{" +
           "compactionTaskSlotRatio=" + compactionTaskSlotRatio +
           ", maxCompactionTaskSlots=" + maxCompactionTaskSlots +
           ", useSupervisors=" + useSupervisors +
           ", engine=" + engine +
           ", compactionPolicy=" + compactionPolicy +
           ", legacyPersistLastCompactionStateInSegments=" + legacyPersistLastCompactionStateInSegments +
           '}';
  }
}
