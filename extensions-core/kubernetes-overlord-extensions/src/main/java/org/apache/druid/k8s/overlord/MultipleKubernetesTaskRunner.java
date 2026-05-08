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


import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.tasklogs.TaskLogStreamer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

public class MultipleKubernetesTaskRunner implements TaskLogStreamer, TaskRunner
{
  private static final Logger log = new Logger(MultipleKubernetesTaskRunner.class);

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = RoundRobinSelector.class)
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = "roundrobin", value = RoundRobinSelector.class),
      @JsonSubTypes.Type(name = "random", value = RandomSelector.class),
      @JsonSubTypes.Type(name = "leastTask", value = LeastTaskSelector.class)
  })
  public interface KubernetesClusterSelector
  {
    MultipleKubernetesTaskRunnerDelegate next(List<MultipleKubernetesTaskRunnerDelegate> taskRunners);
  }

  public static class RoundRobinSelector implements KubernetesClusterSelector
  {
    private int currentIndex = 0;

    @Override
    public synchronized MultipleKubernetesTaskRunnerDelegate next(List<MultipleKubernetesTaskRunnerDelegate> taskRunners)
    {
      MultipleKubernetesTaskRunnerDelegate runner = taskRunners.get(currentIndex);
      currentIndex = (currentIndex + 1) % taskRunners.size();
      return runner;
    }
  }

  public static class RandomSelector implements KubernetesClusterSelector
  {
    @Override
    public MultipleKubernetesTaskRunnerDelegate next(List<MultipleKubernetesTaskRunnerDelegate> taskRunners)
    {
      int nextIndex = ThreadLocalRandom.current().nextInt(taskRunners.size());
      return taskRunners.get(nextIndex);
    }
  }

  public static class LeastTaskSelector implements KubernetesClusterSelector
  {
    @Override
    public MultipleKubernetesTaskRunnerDelegate next(List<MultipleKubernetesTaskRunnerDelegate> taskRunners)
    {
      int minTasks = taskRunners.stream()
                                .mapToInt(runner -> runner.getRunner().getKnownTasks().size())
                                .min()
                                .orElse(0);

      List<MultipleKubernetesTaskRunnerDelegate> runners = taskRunners.stream()
                                                      .filter(runner -> runner.getRunner().getKnownTasks().size() == minTasks)
                                                      .collect(Collectors.toList());
      if (runners.isEmpty()) {
        // fallback to random selection
        return taskRunners.get(ThreadLocalRandom.current().nextInt(taskRunners.size()));
      }

      if (runners.size() == 1) {
        return runners.get(0);
      }

      // Use random to avoid deterministic selection among multiple least-loaded runners
      int index = ThreadLocalRandom.current().nextInt(runners.size());
      return runners.get(index);
    }
  }

  private final KubernetesClusterSelector clusterSelector;
  private final List<MultipleKubernetesTaskRunnerDelegate> taskRunners;
  private final KubernetesTaskRunnerConfig config;
  private final ThreadPoolExecutor executor;
  private final Object schedulerLock = new Object();

  /**
   * ONLY for test cases to use
   */
  @VisibleForTesting
  MultipleKubernetesTaskRunner(
      KubernetesTaskRunnerConfig config,
      KubernetesClusterSelector clusterSelector,
      List<MultipleKubernetesTaskRunnerDelegate> taskRunners
  )
  {
    this(
        config,
        clusterSelector,
        taskRunners,
        null  // No shared executor, each runner uses its own executor
    );
  }

  public MultipleKubernetesTaskRunner(
      KubernetesTaskRunnerConfig config,
      KubernetesClusterSelector clusterSelector,
      List<MultipleKubernetesTaskRunnerDelegate> taskRunners,
      ThreadPoolExecutor executor
  )
  {
    Preconditions.checkState(
        !taskRunners.stream().allMatch(MultipleKubernetesTaskRunnerDelegate::isDisabled),
        "At least one task runner must be enabled"
    );

    this.config = config;
    this.clusterSelector = clusterSelector;
    this.taskRunners = taskRunners;
    this.executor = executor;
  }

  @VisibleForTesting
  List<MultipleKubernetesTaskRunnerDelegate> getTaskRunners()
  {
    return taskRunners;
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    return ImmutableList.of();
  }

  @Override
  @LifecycleStart
  public void start()
  {
    log.info("Starting MultipleKubernetesTaskRunner with capacity [%d]...", config.getCapacity());

    // Start all underlying runners
    for (MultipleKubernetesTaskRunnerDelegate taskRunner : taskRunners) {
      taskRunner.getRunner().start();
    }

    log.info("MultipleKubernetesTaskRunner started.");
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    log.info("Stopping MultipleKubernetesTaskRunner...");

    // Stop all underlying runners and their associated Kubernetes clients
    for (MultipleKubernetesTaskRunnerDelegate delegate : taskRunners) {
      delegate.close();
    }

    // Shutdown shared executor if present
    if (executor != null) {
      executor.shutdownNow();
    }

    log.info("MultipleKubernetesTaskRunner stopped.");
  }

  @Override
  public void registerListener(TaskRunnerListener listener, Executor executor)
  {
    for (MultipleKubernetesTaskRunnerDelegate taskRunner : taskRunners) {
      taskRunner.getRunner().registerListener(listener, executor);
    }
  }

  @Override
  public void unregisterListener(String listenerId)
  {
    for (MultipleKubernetesTaskRunnerDelegate taskRunner : taskRunners) {
      taskRunner.getRunner().unregisterListener(listenerId);
    }
  }

  @Override
  public ListenableFuture<TaskStatus> run(Task task)
  {
    synchronized (this.schedulerLock) {
      // Check if task already exists in any underlying runner (for restart scenarios)
      for (MultipleKubernetesTaskRunnerDelegate taskRunner : this.taskRunners) {
        RunnerTaskState taskState = taskRunner.getRunner().getRunnerTaskState(task.getId());
        if (taskState != null) {
          log.info(
              "Task [%s] found in runner [%s] under state [%s] (PoolSize = [%d], Queued = [%d]) delegating directly",
              task.getId(),
              taskRunner.getK8sCluster(),
              taskState,
              executor == null ? 0 : executor.getPoolSize(),
              executor == null ? 0 : executor.getQueue().size()
          );
          return taskRunner.getRunner().run(task);
        }
      }

      // Find enabled runners
      List<MultipleKubernetesTaskRunnerDelegate> candidates = taskRunners.stream()
                                                         .filter(runner -> !runner.isDisabled())
                                                         .collect(Collectors.toList());
      if (candidates.isEmpty()) {
        // A defensive check, should not happen due to constructor check
        throw new RE("No enabled runners available");
      }

      // Select runner using strategy
      // Capacity control is handled by the shared thread pool executor
      MultipleKubernetesTaskRunnerDelegate selected = clusterSelector.next(candidates);
      KubernetesTaskRunner selectedRunner = selected.getRunner();
      log.info(
          "Submitting task [%s] to runner [%s]",
          task.getId(),
          selected.getK8sCluster()
      );

      // Add k8s cluster info to task context so that users can use selector based strategy to match a pod template.
      // See SelectorBasedPodTemplateSelectStrategy to learn more.
      if (selected.getK8sCluster() != null) {
        //noinspection unchecked
        Map<String, String> tags = new LinkedHashMap<>((Map<String, String>) task.getContext().getOrDefault(DruidMetrics.TAGS, ImmutableMap.of()));
        tags.put("k8s_cluster", selected.getK8sCluster());
        task.getContext().put(DruidMetrics.TAGS, tags);
      }

      return selectedRunner.run(task);
    }
  }

  @Override
  public void shutdown(String taskid, String reason)
  {
    log.info("Shutdown request for task [%s]: %s", taskid, reason);

    // Delegate shutdown to all underlying runners
    for (MultipleKubernetesTaskRunnerDelegate taskRunner : taskRunners) {
      taskRunner.getRunner().shutdown(taskid, reason);
    }
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getRunningTasks()
  {
    return this.taskRunners.stream()
                           .flatMap((runner) -> runner.getRunner().getRunningTasks().stream())
                           .collect(Collectors.toList());
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
  {
    // Return pending tasks from underlying runners
    // (our local pending queue will eventually be submitted to those runners)
    return this.taskRunners.stream()
                           .flatMap((runner) -> runner.getRunner().getPendingTasks().stream())
                           .collect(Collectors.toList());
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
  {
    return this.taskRunners.stream()
                           .flatMap((runner) -> runner.getRunner().getKnownTasks().stream())
                           .collect(Collectors.toList());
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.absent();
  }

  @Override
  public Map<String, Long> getTotalTaskSlotCount()
  {
    return ImmutableMap.of(KubernetesTaskRunner.WORKER_CATEGORY, (long) this.config.getCapacity());
  }

  @Override
  public Map<String, Long> getIdleTaskSlotCount()
  {
    return ImmutableMap.of(KubernetesTaskRunner.WORKER_CATEGORY, (long) Math.max(0, this.config.getCapacity() - getUsedCapacity()));
  }

  @Override
  public Map<String, Long> getUsedTaskSlotCount()
  {
    return ImmutableMap.of(KubernetesTaskRunner.WORKER_CATEGORY, (long) Math.min(config.getCapacity(), getUsedCapacity()));
  }

  @Override
  public Map<String, Long> getLazyTaskSlotCount()
  {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Long> getBlacklistedTaskSlotCount()
  {
    return Collections.emptyMap();
  }

  @Override
  public int getTotalCapacity()
  {
    return config.getCapacity();
  }

  @Override
  public int getUsedCapacity()
  {
    return this.taskRunners.stream()
                           .mapToInt((runner) -> runner.getRunner().getKnownTasks().size())
                           .sum();
  }

  @Override
  public TaskLocation getTaskLocation(String taskId)
  {
    for (MultipleKubernetesTaskRunnerDelegate taskRunner : taskRunners) {
      TaskLocation location = taskRunner.getRunner().getTaskLocation(taskId);
      if (location != null && !TaskLocation.unknown().equals(location)) {
        return location;
      }
    }
    return TaskLocation.unknown();
  }

  @Nullable
  @Override
  public RunnerTaskState getRunnerTaskState(String taskId)
  {
    for (MultipleKubernetesTaskRunnerDelegate taskRunner : taskRunners) {
      RunnerTaskState state = taskRunner.getRunner().getRunnerTaskState(taskId);
      if (state != null) {
        return state;
      }
    }
    return null;
  }


  @Override
  public Optional<InputStream> streamTaskLog(String taskid, long offset)
  {
    for (MultipleKubernetesTaskRunnerDelegate taskRunner : taskRunners) {
      if (taskRunner.getRunner().getRunnerTaskState(taskid) != null) {
        // Found the task runner, then stream the task log on this runner
        return taskRunner.getRunner().streamTaskLog(taskid, offset);
      }
    }
    return Optional.absent();
  }

  @Override
  public Optional<InputStream> streamTaskReports(String taskid) throws IOException
  {
    for (final MultipleKubernetesTaskRunnerDelegate taskRunner : taskRunners) {
      if (taskRunner.getRunner().getRunnerTaskState(taskid) != null) {
        return taskRunner.getRunner().streamTaskReports(taskid);
      }
    }
    return Optional.absent();
  }
}
