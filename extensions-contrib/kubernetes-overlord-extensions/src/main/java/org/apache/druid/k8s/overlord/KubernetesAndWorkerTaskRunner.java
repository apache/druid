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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.indexing.overlord.config.WorkerTaskRunnerConfig;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.k8s.overlord.runnerstrategy.RunnerStrategy;
import org.apache.druid.tasklogs.TaskLogStreamer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Mixed mode task runner that can run tasks on either Kubernetes or workers based on KubernetesAndWorkerTaskRunnerConfig.
 * This task runner is always aware of task runner running on either system.
 */
public class KubernetesAndWorkerTaskRunner implements TaskLogStreamer, WorkerTaskRunner
{
  private final KubernetesTaskRunner kubernetesTaskRunner;
  private final WorkerTaskRunner workerTaskRunner;
  private final RunnerStrategy runnerStrategy;

  public KubernetesAndWorkerTaskRunner(
      KubernetesTaskRunner kubernetesTaskRunner,
      WorkerTaskRunner workerTaskRunner,
      RunnerStrategy runnerStrategy
  )
  {
    this.kubernetesTaskRunner = kubernetesTaskRunner;
    this.workerTaskRunner = workerTaskRunner;
    this.runnerStrategy = runnerStrategy;
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    return Lists.newArrayList(Iterables.concat(kubernetesTaskRunner.restore(), workerTaskRunner.restore()));
  }

  @Override
  @LifecycleStart
  public void start()
  {
    kubernetesTaskRunner.start();
    workerTaskRunner.start();
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
    RunnerStrategy.RunnerType runnerType = runnerStrategy.getRunnerTypeForTask(task);
    if (RunnerStrategy.RunnerType.WORKER_RUNNER_TYPE.equals(runnerType)) {
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
    kubernetesTaskRunner.stop();
    workerTaskRunner.stop();
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getRunningTasks()
  {
    return Lists.newArrayList(Iterables.concat(kubernetesTaskRunner.getRunningTasks(), workerTaskRunner.getRunningTasks()));
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
  {
    return Lists.newArrayList(Iterables.concat(kubernetesTaskRunner.getPendingTasks(), workerTaskRunner.getPendingTasks()));

  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
  {
    return Lists.newArrayList(Iterables.concat(kubernetesTaskRunner.getKnownTasks(), workerTaskRunner.getKnownTasks()));

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
    Map<String, Long> taskSlotCounts = new HashMap<>(workerTaskRunner.getIdleTaskSlotCount());
    kubernetesTaskRunner.getIdleTaskSlotCount().forEach((tier, count) -> taskSlotCounts.merge(tier, count, Long::sum));
    return taskSlotCounts;
  }

  @Override
  public Map<String, Long> getUsedTaskSlotCount()
  {
    Map<String, Long> taskSlotCounts = new HashMap<>(workerTaskRunner.getUsedTaskSlotCount());
    kubernetesTaskRunner.getUsedTaskSlotCount().forEach((tier, count) -> taskSlotCounts.merge(tier, count, Long::sum));
    return taskSlotCounts;
  }

  @Override
  public Map<String, Long> getLazyTaskSlotCount()
  {
    Map<String, Long> taskSlotCounts = new HashMap<>(workerTaskRunner.getLazyTaskSlotCount());
    kubernetesTaskRunner.getLazyTaskSlotCount().forEach((tier, count) -> taskSlotCounts.merge(tier, count, Long::sum));
    return taskSlotCounts;
  }

  @Override
  public Map<String, Long> getBlacklistedTaskSlotCount()
  {
    Map<String, Long> taskSlotCounts = new HashMap<>(workerTaskRunner.getBlacklistedTaskSlotCount());
    kubernetesTaskRunner.getBlacklistedTaskSlotCount().forEach((tier, count) -> taskSlotCounts.merge(tier, count, Long::sum));
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

  @Override
  public TaskLocation getTaskLocation(String taskId)
  {
    TaskLocation taskLocation = kubernetesTaskRunner.getTaskLocation(taskId);
    if (taskLocation == null || taskLocation.equals(TaskLocation.unknown())) {
      return workerTaskRunner.getTaskLocation(taskId);
    }
    return taskLocation;
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
  public int getTotalCapacity()
  {
    int k8sCapacity = kubernetesTaskRunner.getTotalCapacity();
    int workerCapacity = workerTaskRunner.getTotalCapacity();
    if (k8sCapacity == -1 && workerCapacity == -1) {
      return -1;
    }
    return Math.max(0, k8sCapacity) + Math.max(0, workerCapacity);
  }

  @Override
  public int getUsedCapacity()
  {
    int k8sCapacity = kubernetesTaskRunner.getUsedCapacity();
    int workerCapacity = workerTaskRunner.getUsedCapacity();
    if (k8sCapacity == -1 && workerCapacity == -1) {
      return -1;
    }
    return Math.max(0, k8sCapacity) + Math.max(0, workerCapacity);
  }

  // Worker task runners do not implement these methods
  @Override
  public void updateStatus(Task task, TaskStatus status)
  {
    kubernetesTaskRunner.updateStatus(task, status);
  }

  @Override
  public void updateLocation(Task task, TaskLocation location)
  {
    kubernetesTaskRunner.updateLocation(task, location);
  }
}
