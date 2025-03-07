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
import org.apache.druid.audit.AuditInfo;
import org.joda.time.DateTime;

import java.util.Objects;

/**
 * A DTO containing audit information for compaction config for a datasource.
 */
public class DataSourceCompactionConfigAuditEntry
{
  private final ClusterCompactionConfig globalConfig;
  private final DataSourceCompactionConfig compactionConfig;
  private final AuditInfo auditInfo;
  private final DateTime auditTime;

  @JsonCreator
  public DataSourceCompactionConfigAuditEntry(
      @JsonProperty("globalConfig") ClusterCompactionConfig globalConfig,
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
  public ClusterCompactionConfig getGlobalConfig()
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

  public boolean hasSameConfig(DataSourceCompactionConfigAuditEntry other)
  {
    return Objects.equals(this.compactionConfig, other.compactionConfig)
        && Objects.equals(this.globalConfig, other.globalConfig);
  }
}
