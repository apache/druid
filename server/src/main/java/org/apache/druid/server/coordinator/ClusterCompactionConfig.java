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
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.server.compaction.CompactionCandidateSearchPolicy;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Cluster-level compaction configs.
 * All fields of this class are nullable. A non-null value denotes that the
 * corresponding field has been explicitly specified.
 */
public class ClusterCompactionConfig
{
  private final Double compactionTaskSlotRatio;
  private final Integer maxCompactionTaskSlots;
  private final Boolean useSupervisors;
  private final CompactionEngine engine;
  private final CompactionCandidateSearchPolicy compactionPolicy;

  @JsonCreator
  public ClusterCompactionConfig(
      @JsonProperty("compactionTaskSlotRatio") @Nullable Double compactionTaskSlotRatio,
      @JsonProperty("maxCompactionTaskSlots") @Nullable Integer maxCompactionTaskSlots,
      @JsonProperty("compactionPolicy") @Nullable CompactionCandidateSearchPolicy compactionPolicy,
      @JsonProperty("useSupervisors") @Nullable Boolean useSupervisors,
      @JsonProperty("engine") @Nullable CompactionEngine engine
  )
  {
    this.compactionTaskSlotRatio = compactionTaskSlotRatio;
    this.maxCompactionTaskSlots = maxCompactionTaskSlots;
    this.compactionPolicy = compactionPolicy;
    this.engine = engine;
    this.useSupervisors = useSupervisors;
  }

  @Nullable
  @JsonProperty
  public Double getCompactionTaskSlotRatio()
  {
    return compactionTaskSlotRatio;
  }

  @Nullable
  @JsonProperty
  public Integer getMaxCompactionTaskSlots()
  {
    return maxCompactionTaskSlots;
  }

  @Nullable
  @JsonProperty
  public CompactionCandidateSearchPolicy getCompactionPolicy()
  {
    return compactionPolicy;
  }

  @Nullable
  @JsonProperty
  public Boolean getUseSupervisors()
  {
    return useSupervisors;
  }

  @Nullable
  @JsonProperty
  public CompactionEngine getEngine()
  {
    return engine;
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
           && Objects.equals(engine, that.engine);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        compactionTaskSlotRatio,
        maxCompactionTaskSlots,
        compactionPolicy,
        useSupervisors,
        engine
    );
  }
}
