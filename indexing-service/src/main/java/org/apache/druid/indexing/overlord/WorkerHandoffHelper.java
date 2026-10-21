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

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
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
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * Groups streaming tasks running on one Middle Manager and queues early handoff
 * through the existing supervisor notice path.
 */
public final class WorkerHandoffHelper
{
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

  public static Map<String, List<Integer>> groupTaskGroups(
      Collection<String> runningTaskIds,
      Function<String, Optional<Task>> taskLookup
  )
  {
    final Map<String, Set<Integer>> grouped = new LinkedHashMap<>();
    for (String taskId : runningTaskIds) {
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
        continue;
      }
      final String supervisorId = streamTask.getSupervisorId();
      if (supervisorId == null || supervisorId.isEmpty()) {
        LOG.info("Skipping task [%s]: streaming task has no supervisorId", taskId);
        continue;
      }
      grouped.computeIfAbsent(supervisorId, ignored -> new TreeSet<>()).add(taskGroupId);
    }
    final Map<String, List<Integer>> result = new LinkedHashMap<>();
    for (Map.Entry<String, Set<Integer>> entry : grouped.entrySet()) {
      result.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }
    return result;
  }

  public static Map<String, List<Integer>> submitHandoffs(
      SupervisorManager supervisorManager,
      Map<String, List<Integer>> taskGroupsBySupervisor
  )
  {
    if (taskGroupsBySupervisor.isEmpty()) {
      return Collections.emptyMap();
    }
    final Map<String, List<Integer>> accepted = new LinkedHashMap<>();
    for (Map.Entry<String, List<Integer>> entry : taskGroupsBySupervisor.entrySet()) {
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
        }
      }
      catch (Exception e) {
        LOG.warn(
            e,
            "Supervisor [%s] does not support early handoff; skipping task groups [%s]",
            supervisorId,
            taskGroupIds
        );
      }
    }
    return accepted;
  }
}
