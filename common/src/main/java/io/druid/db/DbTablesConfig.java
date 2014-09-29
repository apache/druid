/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.db;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class DbTablesConfig
{
  public static DbTablesConfig fromBase(String base)
  {
    return new DbTablesConfig(base, null, null, null, null, null, null);
  }

  private static String defaultBase = "druid";

  @JsonProperty("base")
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
    this.base = (base == null) ? defaultBase : base;
    this.segmentsTable = makeTableName(segmentsTable, "segments");
    this.rulesTable = makeTableName(rulesTable, "rules");
    this.configTable = makeTableName(configTable, "config");
    this.tasksTable = makeTableName(tasksTable, "tasks");
    this.taskLogTable = makeTableName(taskLogTable, "tasklogs");
    this.taskLockTable = makeTableName(taskLockTable, "tasklocks");
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
