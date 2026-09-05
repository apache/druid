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

package org.apache.druid.indexing.overlord.task;

import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.indexing.overlord.http.TaskStateLookup;
import org.apache.druid.metadata.TaskStorageQueryFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.system.SystemTableNotLeaderException;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;

import java.util.List;

/** Native row supplier for {@code sys.tasks}. */
public class TasksTableDataProvider implements SystemTableDataProvider
{
  private static final List<SystemTablePushdownFilter> METADATA_STORAGE_PUSHDOWN_FILTERS = List.of(
      new SystemTablePushdownFilter("task_id", "id"),
      new SystemTablePushdownFilter("group_id", null),
      new SystemTablePushdownFilter("type", null),
      new SystemTablePushdownFilter("datasource", null),
      new SystemTablePushdownFilter("created_time", "created_date"),
      new SystemTablePushdownFilter("status", null)
  );
  private static final List<SystemTablePushdownFilter> STATUS_PUSHDOWN_FILTERS = List.of(
      new SystemTablePushdownFilter("status", null)
  );

  private final TaskQueryTool taskQueryTool;
  private final TaskMaster taskMaster;

  @Inject
  public TasksTableDataProvider(
      final TaskQueryTool taskQueryTool,
      final TaskMaster taskMaster
  )
  {
    this.taskQueryTool = taskQueryTool;
    this.taskMaster = taskMaster;
  }

  @Override
  public List<SystemTablePushdownFilter> getPushdownFilters()
  {
    return taskQueryTool.supportsTaskStatusQueryFilter()
           ? METADATA_STORAGE_PUSHDOWN_FILTERS
           : STATUS_PUSHDOWN_FILTERS;
  }

  @Override
  public Iterable<Object[]> getRows(
      final List<DimFilter> filters,
      final AuthenticationResult internalAuthenticationResult
  )
  {
    if (!taskMaster.isHalfOrFullLeader()) {
      throw new SystemTableNotLeaderException("overlord");
    }
    final List<TaskStatusPlus> tasks = taskQueryTool.getTaskStatusPlusList(
        TaskStateLookup.ALL,
        new TaskStorageQueryFilter(filters)
    );
    return Iterables.transform(tasks, TasksTableDataProvider::taskToRow);
  }

  private static Object[] taskToRow(final TaskStatusPlus task)
  {
    return new Object[]{
        task.getId(),
        task.getGroupId(),
        task.getType(),
        task.getDataSource(),
        task.getCreatedTime() == null ? null : task.getCreatedTime().toString(),
        task.getQueueInsertionTime() == null ? null : task.getQueueInsertionTime().toString(),
        task.getStatusCode() == null ? null : task.getStatusCode().toString(),
        task.getRunnerStatusCode() == null ? null : task.getRunnerStatusCode().toString(),
        task.getDuration() == null ? 0L : task.getDuration(),
        task.getLocation().getLocation(),
        task.getLocation().getHost(),
        (long) task.getLocation().getPort(),
        (long) task.getLocation().getTlsPort(),
        task.getErrorMsg()
    };
  }
}
