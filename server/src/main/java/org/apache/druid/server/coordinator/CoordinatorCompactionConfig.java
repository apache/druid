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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class CoordinatorCompactionConfig
{
  public static final String CONFIG_KEY = "coordinator.compaction.config";

  private static final double DEFAULT_COMPACTION_TASK_RATIO = 0.1;
  private static final int DEFAILT_MAX_COMPACTION_TASK_SLOTS = Integer.MAX_VALUE;
  private static final boolean DEFAULT_USE_AUTO_SCALE_SLOTS = false;

  private final List<DataSourceCompactionConfig> compactionConfigs;
  private final double compactionTaskSlotRatio;
  private final int maxCompactionTaskSlots;
  private final boolean useAutoScaleSlots;

  public static CoordinatorCompactionConfig from(
      CoordinatorCompactionConfig baseConfig,
      List<DataSourceCompactionConfig> compactionConfigs
  )
  {
    return new CoordinatorCompactionConfig(
        compactionConfigs,
        baseConfig.compactionTaskSlotRatio,
        baseConfig.maxCompactionTaskSlots,
        baseConfig.useAutoScaleSlots
    );
  }

  public static CoordinatorCompactionConfig from(
      CoordinatorCompactionConfig baseConfig,
      @Nullable Double compactionTaskSlotRatio,
      @Nullable Integer maxCompactionTaskSlots,
      @Nullable Boolean useAutoScaleSlots
  )
  {
    return new CoordinatorCompactionConfig(
        baseConfig.compactionConfigs,
        compactionTaskSlotRatio == null ? baseConfig.compactionTaskSlotRatio : compactionTaskSlotRatio,
        maxCompactionTaskSlots == null ? baseConfig.maxCompactionTaskSlots : maxCompactionTaskSlots,
        useAutoScaleSlots == null ? baseConfig.useAutoScaleSlots : useAutoScaleSlots
    );
  }

  public static CoordinatorCompactionConfig from(List<DataSourceCompactionConfig> compactionConfigs)
  {
    return new CoordinatorCompactionConfig(compactionConfigs, null, null, null);
  }

  public static CoordinatorCompactionConfig empty()
  {
    return new CoordinatorCompactionConfig(ImmutableList.of(), null, null, null);
  }

  public static AtomicReference<CoordinatorCompactionConfig> watch(final JacksonConfigManager configManager)
  {
    return configManager.watch(
        CoordinatorCompactionConfig.CONFIG_KEY,
        CoordinatorCompactionConfig.class,
        CoordinatorCompactionConfig.empty()
    );
  }

  public static byte[] getConfigInByteFromDb(final MetadataStorageConnector connector, MetadataStorageTablesConfig config)
  {
    return connector.lookup(
        config.getConfigTable(),
        "name",
        "payload",
        CoordinatorCompactionConfig.CONFIG_KEY
    );
  }

  public static CoordinatorCompactionConfig convertByteToConfig(final JacksonConfigManager configManager, byte[] configInByte)
  {
    return configManager.convertByteToConfig(configInByte, CoordinatorCompactionConfig.class, CoordinatorCompactionConfig.empty());
  }

  @Nonnull
  public static CoordinatorCompactionConfig current(final JacksonConfigManager configManager)
  {
    return Preconditions.checkNotNull(watch(configManager).get(), "Got null config from watcher?!");
  }

  @JsonCreator
  public CoordinatorCompactionConfig(
      @JsonProperty("compactionConfigs") List<DataSourceCompactionConfig> compactionConfigs,
      @JsonProperty("compactionTaskSlotRatio") @Nullable Double compactionTaskSlotRatio,
      @JsonProperty("maxCompactionTaskSlots") @Nullable Integer maxCompactionTaskSlots,
      @JsonProperty("useAutoScaleSlots") @Nullable Boolean useAutoScaleSlots
  )
  {
    this.compactionConfigs = compactionConfigs;
    this.compactionTaskSlotRatio = compactionTaskSlotRatio == null ?
                                   DEFAULT_COMPACTION_TASK_RATIO :
                                   compactionTaskSlotRatio;
    this.maxCompactionTaskSlots = maxCompactionTaskSlots == null ?
                                  DEFAILT_MAX_COMPACTION_TASK_SLOTS :
                                  maxCompactionTaskSlots;
    this.useAutoScaleSlots = useAutoScaleSlots == null ?
                             DEFAULT_USE_AUTO_SCALE_SLOTS :
                             useAutoScaleSlots;
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
  public boolean isUseAutoScaleSlots()
  {
    return useAutoScaleSlots;
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
    CoordinatorCompactionConfig that = (CoordinatorCompactionConfig) o;
    return Double.compare(that.compactionTaskSlotRatio, compactionTaskSlotRatio) == 0 &&
           maxCompactionTaskSlots == that.maxCompactionTaskSlots &&
           useAutoScaleSlots == that.useAutoScaleSlots &&
           Objects.equals(compactionConfigs, that.compactionConfigs);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(compactionConfigs, compactionTaskSlotRatio, maxCompactionTaskSlots, useAutoScaleSlots);
  }

  @Override
  public String toString()
  {
    return "CoordinatorCompactionConfig{" +
           "compactionConfigs=" + compactionConfigs +
           ", compactionTaskSlotRatio=" + compactionTaskSlotRatio +
           ", maxCompactionTaskSlots=" + maxCompactionTaskSlots +
           ", useAutoScaleSlots=" + useAutoScaleSlots +
           '}';
  }
}
