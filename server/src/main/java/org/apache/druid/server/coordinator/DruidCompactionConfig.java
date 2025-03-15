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
import com.google.common.base.Optional;
import org.apache.druid.common.config.Configs;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.server.compaction.CompactionCandidateSearchPolicy;
import org.apache.druid.server.compaction.NewestSegmentFirstPolicy;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DruidCompactionConfig
{
  public static final String CONFIG_KEY = "coordinator.compaction.config";

  private static final CompactionCandidateSearchPolicy DEFAULT_COMPACTION_POLICY
      = new NewestSegmentFirstPolicy(null);
  private static final DruidCompactionConfig EMPTY_INSTANCE
      = new DruidCompactionConfig(List.of(), null, null, null, null, null);

  private final List<DataSourceCompactionConfig> compactionConfigs;
  private final double compactionTaskSlotRatio;
  private final int maxCompactionTaskSlots;
  private final boolean useSupervisors;
  private final CompactionEngine engine;
  private final CompactionCandidateSearchPolicy compactionPolicy;

  public DruidCompactionConfig withDatasourceConfigs(
      List<DataSourceCompactionConfig> compactionConfigs
  )
  {
    return new DruidCompactionConfig(
        compactionConfigs,
        compactionTaskSlotRatio,
        maxCompactionTaskSlots,
        compactionPolicy,
        useSupervisors,
        engine
    );
  }

  /**
   * Creates a copy of this {@link DruidCompactionConfig} by updating the non-null
   * fields provided in the {@link ClusterCompactionConfig}.
   */
  public DruidCompactionConfig withClusterConfig(
      ClusterCompactionConfig update
  )
  {
    return new DruidCompactionConfig(
        this.compactionConfigs,
        Configs.valueOrDefault(update.getCompactionTaskSlotRatio(), compactionTaskSlotRatio),
        Configs.valueOrDefault(update.getMaxCompactionTaskSlots(), maxCompactionTaskSlots),
        Configs.valueOrDefault(update.getCompactionPolicy(), compactionPolicy),
        Configs.valueOrDefault(update.getUseSupervisors(), useSupervisors),
        Configs.valueOrDefault(update.getEngine(), engine)
    );
  }

  public DruidCompactionConfig withDatasourceConfig(DataSourceCompactionConfig dataSourceConfig)
  {
    final Map<String, DataSourceCompactionConfig> configs = dataSourceToCompactionConfigMap();
    configs.put(dataSourceConfig.getDataSource(), dataSourceConfig);
    return withDatasourceConfigs(new ArrayList<>(configs.values()));
  }

  public static DruidCompactionConfig empty()
  {
    return EMPTY_INSTANCE;
  }

  @JsonCreator
  public DruidCompactionConfig(
      @JsonProperty("compactionConfigs") List<DataSourceCompactionConfig> compactionConfigs,
      @JsonProperty("compactionTaskSlotRatio") @Nullable Double compactionTaskSlotRatio,
      @JsonProperty("maxCompactionTaskSlots") @Nullable Integer maxCompactionTaskSlots,
      @JsonProperty("compactionPolicy") @Nullable CompactionCandidateSearchPolicy compactionPolicy,
      @JsonProperty("useSupervisors") @Nullable Boolean useSupervisors,
      @JsonProperty("engine") @Nullable CompactionEngine engine
  )
  {
    this.compactionConfigs = Configs.valueOrDefault(compactionConfigs, Collections.emptyList());
    this.compactionTaskSlotRatio = Configs.valueOrDefault(compactionTaskSlotRatio, 0.1);
    this.maxCompactionTaskSlots = Configs.valueOrDefault(maxCompactionTaskSlots, Integer.MAX_VALUE);
    this.compactionPolicy = Configs.valueOrDefault(compactionPolicy, DEFAULT_COMPACTION_POLICY);
    this.engine = Configs.valueOrDefault(engine, CompactionEngine.NATIVE);
    this.useSupervisors = Configs.valueOrDefault(useSupervisors, false);

    if (!this.useSupervisors && engine == CompactionEngine.MSQ) {
      throw InvalidInput.exception("MSQ Compaction engine can be used only with compaction supervisors.");
    }
  }

  @JsonProperty
  public List<DataSourceCompactionConfig> getCompactionConfigs()
  {
    return compactionConfigs;
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
  public boolean isUseSupervisors()
  {
    return useSupervisors;
  }

  @JsonProperty
  public CompactionEngine getEngine()
  {
    return engine;
  }

  // Null-safe getters not used for serialization
  public ClusterCompactionConfig clusterConfig()
  {
    return new ClusterCompactionConfig(
        compactionTaskSlotRatio,
        maxCompactionTaskSlots,
        compactionPolicy,
        useSupervisors,
        engine
    );
  }

  public Map<String, DataSourceCompactionConfig> dataSourceToCompactionConfigMap()
  {
    return getCompactionConfigs().stream().collect(
        Collectors.toMap(DataSourceCompactionConfig::getDataSource, Function.identity())
    );
  }

  public Optional<DataSourceCompactionConfig> findConfigForDatasource(String dataSource)
  {
    for (DataSourceCompactionConfig dataSourceConfig : getCompactionConfigs()) {
      if (dataSource.equals(dataSourceConfig.getDataSource())) {
        return Optional.of(dataSourceConfig);
      }
    }
    return Optional.absent();
  }

  @JsonProperty
  public CompactionCandidateSearchPolicy getCompactionPolicy()
  {
    return compactionPolicy;
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
    DruidCompactionConfig that = (DruidCompactionConfig) o;
    return Double.compare(that.compactionTaskSlotRatio, compactionTaskSlotRatio) == 0 &&
           maxCompactionTaskSlots == that.maxCompactionTaskSlots &&
           useSupervisors == that.useSupervisors &&
           Objects.equals(compactionPolicy, that.compactionPolicy) &&
           Objects.equals(engine, that.engine) &&
           Objects.equals(compactionConfigs, that.compactionConfigs);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        compactionConfigs,
        compactionTaskSlotRatio,
        maxCompactionTaskSlots,
        compactionPolicy,
        useSupervisors,
        engine
    );
  }

  @Override
  public String toString()
  {
    return "CoordinatorCompactionConfig{" +
           "compactionConfigs=" + compactionConfigs +
           ", compactionTaskSlotRatio=" + compactionTaskSlotRatio +
           ", maxCompactionTaskSlots=" + maxCompactionTaskSlots +
           ", compactionPolicy=" + compactionPolicy +
           ", useSupervisors=" + useSupervisors +
           ", engine=" + engine +
           '}';
  }
}
