package com.metamx.druid.db;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

/**
 */
public class DbTablesConfig
{
  public static DbTablesConfig fromBase(String base)
  {
    return new DbTablesConfig(base, null, null, null, null, null, null);
  }

  @JsonProperty
  @NotNull
  private final String base;

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

  @JsonCreator
  public DbTablesConfig(
      @JsonProperty("base") String base,
      @JsonProperty("segments") String segmentsTable,
      @JsonProperty("rules") String rulesTable,
      @JsonProperty("config") String configTable,
      @JsonProperty("tasks") String tasksTable,
      @JsonProperty("taskLog") String taskLogTable,
      @JsonProperty("taskLock") String taskLockTable
  )
  {
    this.base = base;
    this.segmentsTable = makeTableName(segmentsTable, "segments");
    this.rulesTable = makeTableName(rulesTable, "rules");
    this.configTable = makeTableName(configTable, "config");
    this.tasksTable = makeTableName(tasksTable, "tasks");
    this.taskLogTable = makeTableName(taskLogTable, "task_log");
    this.taskLockTable = makeTableName(taskLockTable, "task_lock");
  }

  private String makeTableName(String explicitTableName, String defaultSuffix)
  {
    if (explicitTableName == null) {
      if (base == null) {
        return null;
      }
      return String.format("%s_%s", base, defaultSuffix);
    }

    return explicitTableName;
  }

  public String getBase()
  {
    return base;
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