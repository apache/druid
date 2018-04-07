/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import io.druid.java.util.common.StringUtils;

import java.util.Map;

/**
 */
public class MetadataStorageTablesConfig
{
  public static MetadataStorageTablesConfig fromBase(String base)
  {
    return new MetadataStorageTablesConfig(base, null, null, null, null, null, null, null, null, null, null);
  }

  public static final String TASK_ENTRY_TYPE = "task";

  private static final String DEFAULT_BASE = "druid";

  private final Map<String, String> entryTables = Maps.newHashMap();
  private final Map<String, String> logTables = Maps.newHashMap();
  private final Map<String, String> lockTables = Maps.newHashMap();

  @JsonProperty("base")
  private final String base;

  @JsonProperty("dataSource")
  private final String dataSourceTable;

  @JsonProperty("pendingSegments")
  private final String pendingSegmentsTable;

  @JsonProperty("segments")
  private final String segmentsTable;

  @JsonProperty("rules")
  private final String rulesTable;

  @JsonProperty("config")
  private final String configTable;

  @JsonProperty("tasks")
  private final String tasksTable;

  @JsonProperty("taskLog")
  private final String taskLogTable;

  @JsonProperty("taskLock")
  private final String taskLockTable;

  @JsonProperty("audit")
  private final String auditTable;

  @JsonProperty("supervisors")
  private final String supervisorTable;

  @JsonCreator
  public MetadataStorageTablesConfig(
      @JsonProperty("base") String base,
      @JsonProperty("dataSource") String dataSourceTable,
      @JsonProperty("pendingSegments") String pendingSegmentsTable,
      @JsonProperty("segments") String segmentsTable,
      @JsonProperty("rules") String rulesTable,
      @JsonProperty("config") String configTable,
      @JsonProperty("tasks") String tasksTable,
      @JsonProperty("taskLog") String taskLogTable,
      @JsonProperty("taskLock") String taskLockTable,
      @JsonProperty("audit") String auditTable,
      @JsonProperty("supervisors") String supervisorTable
  )
  {
    this.base = (base == null) ? DEFAULT_BASE : base;
    this.dataSourceTable = makeTableName(dataSourceTable, "dataSource");
    this.pendingSegmentsTable = makeTableName(pendingSegmentsTable, "pendingSegments");
    this.segmentsTable = makeTableName(segmentsTable, "segments");
    this.rulesTable = makeTableName(rulesTable, "rules");
    this.configTable = makeTableName(configTable, "config");

    this.tasksTable = makeTableName(tasksTable, "tasks");
    this.taskLogTable = makeTableName(taskLogTable, "tasklogs");
    this.taskLockTable = makeTableName(taskLockTable, "tasklocks");
    entryTables.put(TASK_ENTRY_TYPE, this.tasksTable);
    logTables.put(TASK_ENTRY_TYPE, this.taskLogTable);
    lockTables.put(TASK_ENTRY_TYPE, this.taskLockTable);
    this.auditTable = makeTableName(auditTable, "audit");
    this.supervisorTable = makeTableName(supervisorTable, "supervisors");
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

  public String getRulesTable()
  {
    return rulesTable;
  }

  public String getConfigTable()
  {
    return configTable;
  }

  public String getEntryTable(final String entryType)
  {
    return entryTables.get(entryType);
  }

  public String getLogTable(final String entryType)
  {
    return logTables.get(entryType);
  }

  public String getLockTable(final String entryType)
  {
    return lockTables.get(entryType);
  }

  public String getTaskEntryType()
  {
    return TASK_ENTRY_TYPE;
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

  public String getTaskLogTable()
  {
    return taskLogTable;
  }

  public String getTaskLockTable()
  {
    return taskLockTable;
  }
}
