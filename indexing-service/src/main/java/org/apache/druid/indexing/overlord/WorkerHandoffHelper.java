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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * Groups streaming tasks running on one Middle Manager and queues early handoff
 * through the existing supervisor notice path.
 */
public final class WorkerHandoffHelper
{
  public static final String REASON_MISSING_TASK_GROUP_ID = "missing_taskGroupId";
  public static final String REASON_MISSING_SUPERVISOR_ID = "missing_supervisorId";
  public static final String REASON_SUPERVISOR_NOT_FOUND = "supervisor_not_found";
  public static final String REASON_HANDOFF_UNSUPPORTED = "handoff_unsupported";

  private static final Logger LOG = new Logger(WorkerHandoffHelper.class);

  private WorkerHandoffHelper()
  {
  }

  public static Optional<ImmutableWorkerInfo> findWorker(
      Collection<ImmutableWorkerInfo> workers,
      String host
  )
  {
    return Iterables.tryFind(
        workers,
        entry -> entry.getWorker().getHost().equals(host)
    );
  }

  /**
   * Groups seekable-stream tasks that can be handed off. Compact, kill, and batch
   * tasks are omitted from both {@link Grouping#getTaskGroups()} and {@link Grouping#getSkipped()}.
   */
  public static Grouping groupTaskGroups(
      Collection<String> runningTaskIds,
      Function<String, Optional<Task>> taskLookup
  )
  {
    final Map<String, Set<Integer>> grouped = new LinkedHashMap<>();
    final Map<String, List<String>> taskIdsBySupervisor = new LinkedHashMap<>();
    final List<SkippedTask> skipped = new ArrayList<>();
    for (String taskId : runningTaskIds) {
      if (taskId == null || taskId.isEmpty()) {
        continue;
      }
      final Optional<Task> taskOptional = taskLookup.apply(taskId);
      if (!taskOptional.isPresent()) {
        continue;
      }
      final Task task = taskOptional.get();
      if (!(task instanceof SeekableStreamIndexTask<?, ?, ?>)) {
        continue;
      }
      final SeekableStreamIndexTask<?, ?, ?> streamTask = (SeekableStreamIndexTask<?, ?, ?>) task;
      final Integer taskGroupId = streamTask.getIOConfig().getTaskGroupId();
      if (taskGroupId == null) {
        LOG.info("Skipping task [%s]: streaming task has no taskGroupId", taskId);
        skipped.add(new SkippedTask(taskId, REASON_MISSING_TASK_GROUP_ID));
        continue;
      }
      final String supervisorId = streamTask.getSupervisorId();
      if (supervisorId == null || supervisorId.isEmpty()) {
        LOG.info("Skipping task [%s]: streaming task has no supervisorId", taskId);
        skipped.add(new SkippedTask(taskId, REASON_MISSING_SUPERVISOR_ID));
        continue;
      }
      grouped.computeIfAbsent(supervisorId, ignored -> new TreeSet<>()).add(taskGroupId);
      taskIdsBySupervisor.computeIfAbsent(supervisorId, ignored -> new ArrayList<>()).add(taskId);
    }
    final Map<String, List<Integer>> taskGroups = new LinkedHashMap<>();
    for (Map.Entry<String, Set<Integer>> entry : grouped.entrySet()) {
      taskGroups.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }
    return new Grouping(taskGroups, taskIdsBySupervisor, skipped);
  }

  public static Result submitHandoffs(
      SupervisorManager supervisorManager,
      Grouping grouping
  )
  {
    final List<SkippedTask> skipped = new ArrayList<>(grouping.getSkipped());
    if (grouping.getTaskGroups().isEmpty()) {
      return new Result(Collections.emptyMap(), skipped);
    }
    final Map<String, List<Integer>> accepted = new LinkedHashMap<>();
    for (Map.Entry<String, List<Integer>> entry : grouping.getTaskGroups().entrySet()) {
      final String supervisorId = entry.getKey();
      final List<Integer> taskGroupIds = entry.getValue();
      try {
        if (supervisorManager.handoffTaskGroupsEarly(supervisorId, taskGroupIds)) {
          accepted.put(supervisorId, taskGroupIds);
        } else {
          LOG.info(
              "Supervisor [%s] was not found; skipping early handoff for task groups [%s]",
              supervisorId,
              taskGroupIds
          );
          addSkippedForSupervisor(skipped, grouping, supervisorId, REASON_SUPERVISOR_NOT_FOUND);
        }
      }
      catch (UnsupportedOperationException | DruidException e) {
        LOG.warn(
            e,
            "Supervisor [%s] does not support early handoff; skipping task groups [%s]",
            supervisorId,
            taskGroupIds
        );
        addSkippedForSupervisor(skipped, grouping, supervisorId, REASON_HANDOFF_UNSUPPORTED);
      }
    }
    return new Result(accepted, skipped);
  }

  private static void addSkippedForSupervisor(
      List<SkippedTask> skipped,
      Grouping grouping,
      String supervisorId,
      String reason
  )
  {
    final List<String> taskIds = grouping.getTaskIdsBySupervisor().getOrDefault(
        supervisorId,
        Collections.emptyList()
    );
    for (String taskId : taskIds) {
      if (taskId == null || taskId.isEmpty()) {
        continue;
      }
      skipped.add(new SkippedTask(taskId, reason));
    }
  }

  public static final class SkippedTask
  {
    private final String id;
    private final String reason;

    @JsonCreator
    public SkippedTask(
        @JsonProperty("id") final String id,
        @JsonProperty("reason") final String reason
    )
    {
      this.id = id;
      this.reason = reason;
    }

    @JsonProperty
    public String getId()
    {
      return id;
    }

    @JsonProperty
    public String getReason()
    {
      return reason;
    }

    @Override
    public boolean equals(final Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final SkippedTask that = (SkippedTask) o;
      return Objects.equals(id, that.id) && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(id, reason);
    }
  }

  public static final class Grouping
  {
    private final Map<String, List<Integer>> taskGroups;
    private final Map<String, List<String>> taskIdsBySupervisor;
    private final List<SkippedTask> skipped;

    public Grouping(
        final Map<String, List<Integer>> taskGroups,
        final Map<String, List<String>> taskIdsBySupervisor,
        final List<SkippedTask> skipped
    )
    {
      this.taskGroups = taskGroups;
      this.taskIdsBySupervisor = taskIdsBySupervisor;
      this.skipped = skipped;
    }

    public Map<String, List<Integer>> getTaskGroups()
    {
      return taskGroups;
    }

    public Map<String, List<String>> getTaskIdsBySupervisor()
    {
      return taskIdsBySupervisor;
    }

    public List<SkippedTask> getSkipped()
    {
      return skipped;
    }
  }

  public static final class Result
  {
    private final Map<String, List<Integer>> taskGroups;
    private final List<SkippedTask> skipped;

    public Result(
        final Map<String, List<Integer>> taskGroups,
        final List<SkippedTask> skipped
    )
    {
      this.taskGroups = taskGroups;
      this.skipped = skipped;
    }

    public Map<String, List<Integer>> getTaskGroups()
    {
      return taskGroups;
    }

    public List<SkippedTask> getSkipped()
    {
      return skipped;
    }
  }
}
