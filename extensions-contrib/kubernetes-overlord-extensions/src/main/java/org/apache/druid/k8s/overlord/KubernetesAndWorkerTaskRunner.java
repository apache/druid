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

package org.apache.druid.k8s.overlord;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.vavr.collection.Iterator;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.indexing.overlord.config.WorkerTaskRunnerConfig;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.tasklogs.TaskLogStreamer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Mixed mode task runner that can run tasks on either Kubernetes or workers based on KubernetesAndWorkerTaskRunnerConfig.
 * This task runner is always aware of task runner running on either system.
 */
public class KubernetesAndWorkerTaskRunner implements TaskLogStreamer, WorkerTaskRunner
{
  private final KubernetesTaskRunner kubernetesTaskRunner;
  private final WorkerTaskRunner workerTaskRunner;
  private final KubernetesAndWorkerTaskRunnerConfig kubernetesAndWorkerTaskRunnerConfig;

  public KubernetesAndWorkerTaskRunner(
      KubernetesTaskRunner kubernetesTaskRunner,
      WorkerTaskRunner workerTaskRunner,
      KubernetesAndWorkerTaskRunnerConfig kubernetesAndWorkerTaskRunnerConfig
  )
  {
    this.kubernetesTaskRunner = kubernetesTaskRunner;
    this.workerTaskRunner = workerTaskRunner;
    this.kubernetesAndWorkerTaskRunnerConfig = kubernetesAndWorkerTaskRunnerConfig;
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    return Iterator.concat(kubernetesTaskRunner.restore(), workerTaskRunner.restore()).collect(Collectors.toList());
  }

  @Override
  @LifecycleStart
  public void start()
  {
  }

  @Override
  public void registerListener(TaskRunnerListener listener, Executor executor)
  {
    kubernetesTaskRunner.registerListener(listener, executor);
    workerTaskRunner.registerListener(listener, executor);
  }

  @Override
  public void unregisterListener(String listenerId)
  {
    kubernetesTaskRunner.unregisterListener(listenerId);
    workerTaskRunner.unregisterListener(listenerId);
  }

  @Override
  public ListenableFuture<TaskStatus> run(Task task)
  {
    if (kubernetesAndWorkerTaskRunnerConfig.isSendAllTasksToWorkerTaskRunner()) {
      return workerTaskRunner.run(task);
    } else {
      return kubernetesTaskRunner.run(task);
    }
  }

  @Override
  public void shutdown(String taskid, String reason)
  {
    // Technically this is a no-op for tasks a runner does't know about.
    workerTaskRunner.shutdown(taskid, reason);
    kubernetesTaskRunner.shutdown(taskid, reason);
  }

  @Override
  @LifecycleStop
  public void stop()
  {
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getRunningTasks()
  {
    return Iterator.concat(kubernetesTaskRunner.getRunningTasks(), workerTaskRunner.getRunningTasks()).collect(Collectors.toList());
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
  {
    return Iterator.concat(kubernetesTaskRunner.getPendingTasks(), workerTaskRunner.getPendingTasks()).collect(Collectors.toList());

  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
  {
    return Iterator.concat(kubernetesTaskRunner.getKnownTasks(), workerTaskRunner.getKnownTasks()).collect(Collectors.toList());

  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return workerTaskRunner.getScalingStats();
  }

  @Override
  public Map<String, Long> getTotalTaskSlotCount()
  {
    Map<String, Long> taskSlotCounts = new HashMap<>();
    taskSlotCounts.putAll(kubernetesTaskRunner.getTotalTaskSlotCount());
    taskSlotCounts.putAll(workerTaskRunner.getTotalTaskSlotCount());
    return taskSlotCounts;
  }

  @Override
  public Map<String, Long> getIdleTaskSlotCount()
  {
    Map<String, Long> taskSlotCounts = new HashMap<>();
    taskSlotCounts.putAll(kubernetesTaskRunner.getIdleTaskSlotCount());
    taskSlotCounts.putAll(workerTaskRunner.getIdleTaskSlotCount());
    return taskSlotCounts;
  }

  @Override
  public Map<String, Long> getUsedTaskSlotCount()
  {
    Map<String, Long> taskSlotCounts = new HashMap<>();
    taskSlotCounts.putAll(kubernetesTaskRunner.getUsedTaskSlotCount());
    taskSlotCounts.putAll(workerTaskRunner.getUsedTaskSlotCount());
    return taskSlotCounts;
  }

  @Override
  public Map<String, Long> getLazyTaskSlotCount()
  {
    Map<String, Long> taskSlotCounts = new HashMap<>();
    taskSlotCounts.putAll(kubernetesTaskRunner.getLazyTaskSlotCount());
    taskSlotCounts.putAll(workerTaskRunner.getLazyTaskSlotCount());
    return taskSlotCounts;
  }

  @Override
  public Map<String, Long> getBlacklistedTaskSlotCount()
  {
    Map<String, Long> taskSlotCounts = new HashMap<>();
    taskSlotCounts.putAll(kubernetesTaskRunner.getBlacklistedTaskSlotCount());
    taskSlotCounts.putAll(workerTaskRunner.getBlacklistedTaskSlotCount());
    return taskSlotCounts;
  }

  @Override
  public Collection<ImmutableWorkerInfo> getWorkers()
  {
    return workerTaskRunner.getWorkers();
  }

  @Override
  public Collection<Worker> getLazyWorkers()
  {
    return workerTaskRunner.getLazyWorkers();
  }

  @Override
  public Collection<Worker> markWorkersLazy(Predicate<ImmutableWorkerInfo> isLazyWorker, int maxWorkers)
  {
    return workerTaskRunner.markWorkersLazy(isLazyWorker, maxWorkers);
  }

  @Override
  public WorkerTaskRunnerConfig getConfig()
  {
    return workerTaskRunner.getConfig();
  }

  @Override
  public Collection<Task> getPendingTaskPayloads()
  {
    return workerTaskRunner.getPendingTaskPayloads();
  }

  @Override
  public Optional<InputStream> streamTaskLog(String taskid, long offset) throws IOException
  {
    Optional<InputStream> kubernetesTaskLog = kubernetesTaskRunner.streamTaskLog(taskid, offset);
    if (kubernetesTaskLog.isPresent()) {
      return kubernetesTaskLog;
    } else if (workerTaskRunner instanceof TaskLogStreamer) {
      return ((TaskLogStreamer) workerTaskRunner).streamTaskLog(taskid, offset);
    }
    return Optional.absent();
  }

  @Nullable
  @Override
  public RunnerTaskState getRunnerTaskState(String taskId)
  {
    RunnerTaskState runnerTaskState = kubernetesTaskRunner.getRunnerTaskState(taskId);
    if (runnerTaskState == null) {
      return workerTaskRunner.getRunnerTaskState(taskId);
    }

    return runnerTaskState;
  }

  @Override
  public List<TaskRunner> getSubTaskRunners()
  {
    return ImmutableList.of(kubernetesTaskRunner, workerTaskRunner);
  }

  @Override
  public int getTotalCapacity()
  {
    return kubernetesTaskRunner.getTotalCapacity() + workerTaskRunner.getTotalCapacity();
  }

  @Override
  public int getUsedCapacity()
  {
    return kubernetesTaskRunner.getUsedCapacity() + workerTaskRunner.getUsedCapacity();
  }
}
