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

package io.druid.indexing.overlord.autoscaling;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ImmutableWorkerInfo;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.TasksAndWorkers;
import io.druid.indexing.overlord.WorkerTaskRunner;
import io.druid.indexing.overlord.config.WorkerTaskRunnerConfig;
import io.druid.indexing.worker.Worker;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class TasksAndWorkersFilteredByIp implements TasksAndWorkers
{
  private final WorkerTaskRunner delegate;
  private final String ipPrefix;
  private final Predicate<ImmutableWorkerInfo> ipFilter;
  private final Predicate<Task> taskPredicate;

  TasksAndWorkersFilteredByIp(WorkerTaskRunner delegate, final String ipPrefix, Predicate<Task> taskPredicate)
  {
    this.delegate = delegate;
    this.ipPrefix = Preconditions.checkNotNull(ipPrefix);
    this.ipFilter = new Predicate<ImmutableWorkerInfo>()
    {
      @Override
      public boolean apply(@Nullable ImmutableWorkerInfo info)
      {
        return info != null && info.getWorker().getIp().startsWith(ipPrefix);
      }
    };
    this.taskPredicate = taskPredicate;
  }

  private Collection<Worker> filter(Collection<Worker> workers)
  {
    Collection<Worker> filtered = new ArrayList<>(workers.size());
    for (Worker worker : workers) {
      if (worker.getIp().startsWith(ipPrefix)) {
        filtered.add(worker);
      }
    }
    return filtered;
  }

  @Override
  public Collection<ImmutableWorkerInfo> getWorkers()
  {
    return Lists.newArrayList(Collections2.filter(delegate.getWorkers(), ipFilter));
  }

  @Override
  public Collection<Worker> getLazyWorkers()
  {
    return filter(delegate.getLazyWorkers());
  }

  @Override
  public Collection<Worker> markWorkersLazy(final Predicate<ImmutableWorkerInfo> isLazyWorker, int maxWorkers)
  {
    return filter(delegate.markWorkersLazy(Predicates.and(isLazyWorker, ipFilter), maxWorkers));
  }

  @Override
  public WorkerTaskRunnerConfig getConfig()
  {
    return delegate.getConfig();
  }

  @Override
  public Collection<Task> getPendingTaskPayloads()
  {
    return ImmutableList.copyOf(Collections2.filter(delegate.getPendingTaskPayloads(), taskPredicate));
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
  {
    Collection<? extends TaskRunnerWorkItem> pendingTasks = delegate.getPendingTasks();
    Set<String> pendingTaskIds = new HashSet<>();
    for (Task task : getPendingTaskPayloads()) {
      pendingTaskIds.add(task.getId());
    }
    ImmutableList.Builder<TaskRunnerWorkItem> filtered = ImmutableList.builder();
    for (TaskRunnerWorkItem taskRunnerWorkItem : pendingTasks) {
      if (pendingTaskIds.contains(taskRunnerWorkItem.getTaskId())) {
        filtered.add(taskRunnerWorkItem);
      }
    }
    return filtered.build();
  }
}
