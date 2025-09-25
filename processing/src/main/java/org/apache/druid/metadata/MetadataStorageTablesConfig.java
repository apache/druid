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
import org.apache.druid.java.util.common.StringUtils;

/**
 */
public class MetadataStorageTablesConfig
{
  public static final String PROPERTY_BASE = "druid.metadata.storage.tables";

  public static MetadataStorageTablesConfig fromBase(String base)
  {
    return new MetadataStorageTablesConfig(base, null, null, null, null, null, null, null, null, null, null, null, null);
  }

  private static final String DEFAULT_BASE = "druid";

  @JsonProperty("base")
  private final String base;

  @JsonProperty("dataSource")
  private final String dataSourceTable;

  @JsonProperty("pendingSegments")
  private final String pendingSegmentsTable;

  @JsonProperty("segments")
  private final String segmentsTable;

  @JsonProperty("upgradeSegments")
  private final String upgradeSegmentsTable;

  @JsonProperty("rules")
  private final String rulesTable;

  @JsonProperty("config")
  private final String configTable;

  @JsonProperty("tasks")
  private final String tasksTable;

  @JsonProperty("taskLock")
  private final String taskLockTable;

  @JsonProperty("audit")
  private final String auditTable;

  @JsonProperty("supervisors")
  private final String supervisorTable;

  @JsonProperty("segmentSchemas")
  private final String segmentSchemasTable;

  @JsonProperty("useShortIndexNames")
  private final boolean useShortIndexNames;

  @JsonCreator
  public MetadataStorageTablesConfig(
      @JsonProperty("base") String base,
      @JsonProperty("dataSource") String dataSourceTable,
      @JsonProperty("pendingSegments") String pendingSegmentsTable,
      @JsonProperty("segments") String segmentsTable,
      @JsonProperty("rules") String rulesTable,
      @JsonProperty("config") String configTable,
      @JsonProperty("tasks") String tasksTable,
      @JsonProperty("taskLock") String taskLockTable,
      @JsonProperty("audit") String auditTable,
      @JsonProperty("supervisors") String supervisorTable,
      @JsonProperty("upgradeSegments") String upgradeSegmentsTable,
      @JsonProperty("segmentSchemas") String segmentSchemasTable,
      @JsonProperty("useShortIndexNames") Boolean useShortIndexNames
  )
  {
    this.base = (base == null) ? DEFAULT_BASE : base;
    this.dataSourceTable = makeTableName(dataSourceTable, "dataSource");
    this.pendingSegmentsTable = makeTableName(pendingSegmentsTable, "pendingSegments");
    this.segmentsTable = makeTableName(segmentsTable, "segments");
    this.upgradeSegmentsTable = makeTableName(upgradeSegmentsTable, "upgradeSegments");
    this.rulesTable = makeTableName(rulesTable, "rules");
    this.configTable = makeTableName(configTable, "config");

    this.tasksTable = makeTableName(tasksTable, "tasks");
    this.taskLockTable = makeTableName(taskLockTable, "tasklocks");
    this.auditTable = makeTableName(auditTable, "audit");
    this.supervisorTable = makeTableName(supervisorTable, "supervisors");
    this.segmentSchemasTable = makeTableName(segmentSchemasTable, "segmentSchemas");
    this.useShortIndexNames = Configs.valueOrDefault(useShortIndexNames, false);
  }

  private String makeTableName(String explicitTableName, String defaultSuffix)
  {
    if (explicitTableName == null) {
      if (base == null) {
        return null;
      }
      return StringUtils.format("%s_%s", base, defaultSuffix);
    }

    return explicitTableName;
  }

  public String getBase()
  {
    return base;
  }

  public String getDataSourceTable()
  {
    return dataSourceTable;
  }

  public String getPendingSegmentsTable()
  {
    return pendingSegmentsTable;
  }

  public String getSegmentsTable()
  {
    return segmentsTable;
  }

  public String getUpgradeSegmentsTable()
  {
    return upgradeSegmentsTable;
  }

  public String getRulesTable()
  {
    return rulesTable;
  }

  public String getConfigTable()
  {
    return configTable;
  }

  public String getAuditTable()
  {
    return auditTable;
  }

  public String getSupervisorTable()
  {
    return supervisorTable;
  }

  public String getTasksTable()
  {
    return tasksTable;
  }

  public String getTaskLockTable()
  {
    return taskLockTable;
  }

  public String getSegmentSchemasTable()
  {
    return segmentSchemasTable;
  }

  public boolean isUseShortIndexNames()
  {
    return useShortIndexNames;
  }
}
