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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.audit.AuditInfo;
import org.joda.time.DateTime;

/**
 * A DTO containing audit information for compaction config for a datasource.
 */
public class DataSourceCompactionConfigAuditEntry
{
  private final GlobalCompactionConfig globalConfig;
  private final DataSourceCompactionConfig compactionConfig;
  private final AuditInfo auditInfo;
  private final DateTime auditTime;

  @JsonCreator
  public DataSourceCompactionConfigAuditEntry(
      @JsonProperty("globalConfig") GlobalCompactionConfig globalConfig,
      @JsonProperty("compactionConfig") DataSourceCompactionConfig compactionConfig,
      @JsonProperty("auditInfo") AuditInfo auditInfo,
      @JsonProperty("auditTime") DateTime auditTime
  )
  {
    this.globalConfig = globalConfig;
    this.compactionConfig = compactionConfig;
    this.auditInfo = auditInfo;
    this.auditTime = auditTime;
  }

  @JsonProperty
  public GlobalCompactionConfig getGlobalConfig()
  {
    return globalConfig;
  }

  @JsonProperty
  public DataSourceCompactionConfig getCompactionConfig()
  {
    return compactionConfig;
  }

  @JsonProperty
  public AuditInfo getAuditInfo()
  {
    return auditInfo;
  }

  @JsonProperty
  public DateTime getAuditTime()
  {
    return auditTime;
  }

  /**
   * A DTO containing compaction config for that affects the entire cluster.
   */
  public static class GlobalCompactionConfig
  {
    private final double compactionTaskSlotRatio;
    private final int maxCompactionTaskSlots;
    private final boolean useAutoScaleSlots;

    @JsonCreator
    public GlobalCompactionConfig(
        @JsonProperty("compactionTaskSlotRatio")
        double compactionTaskSlotRatio,
        @JsonProperty("maxCompactionTaskSlots") int maxCompactionTaskSlots,
        @JsonProperty("useAutoScaleSlots") boolean useAutoScaleSlots
    )
    {
      this.compactionTaskSlotRatio = compactionTaskSlotRatio;
      this.maxCompactionTaskSlots = maxCompactionTaskSlots;
      this.useAutoScaleSlots = useAutoScaleSlots;
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

    @JsonIgnore
    public boolean hasSameConfig(CoordinatorCompactionConfig coordinatorCompactionConfig)
    {
      return useAutoScaleSlots == coordinatorCompactionConfig.isUseAutoScaleSlots() &&
             compactionTaskSlotRatio == coordinatorCompactionConfig.getCompactionTaskSlotRatio() &&
             coordinatorCompactionConfig.getMaxCompactionTaskSlots() == maxCompactionTaskSlots;
    }
  }
}
