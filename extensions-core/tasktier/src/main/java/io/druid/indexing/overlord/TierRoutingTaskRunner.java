/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.autoscaling.ScalingStats;
import io.druid.indexing.overlord.autoscaling.TierRoutingManagementStrategy;
import io.druid.indexing.overlord.routing.TierRouteConfig;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;
import io.druid.tasklogs.TaskLogStreamer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
 * Be a proxy/mux to multiple task runners.
 * Currently this is not very smart about task IDs, favoring a shotgun approach to things which require a taskID.
 */
public class TierRoutingTaskRunner implements TaskRunner, TaskLogStreamer
{
  private static final Logger LOG = new Logger(TierRoutingTaskRunner.class);
  private final ConcurrentMap<String, TaskRunner> runnerMap = new ConcurrentHashMap<>();
  final TierRoutingManagementStrategy managementStrategy;

  @Inject
  public TierRoutingTaskRunner(
      Supplier<TierRouteConfig> configSupplier,
      ScheduledExecutorFactory managementExecutorServiceFactory
  )
  {
    managementStrategy = new TierRoutingManagementStrategy(configSupplier, managementExecutorServiceFactory);
  }

  public ConcurrentMap<String, TaskRunner> getRunnerMap()
  {
    return runnerMap;
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    // As per RemoteTaskRunner
    return ImmutableList.of();
  }

  @Override
  public void registerListener(TaskRunnerListener listener, Executor executor)
  {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void unregisterListener(String listenerId)
  {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public ListenableFuture<TaskStatus> run(Task task)
  {
    final TaskRunner runner = managementStrategy.getRunner(task);
    if (runner == null) {
      throw new IAE("tier for task [%s] not found", task.getId());
    }
    return managementStrategy.getRunner(task).run(task);
  }

  @Override
  public void shutdown(String taskid)
  {
    for (TaskRunner taskRunner : runnerMap.values()) {
      try {
        taskRunner.shutdown(taskid);
      }
      catch (Exception e) {
        LOG.error(e, "Error shutting down task [%s]", taskid);
      }
    }
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getRunningTasks()
  {
    final Collection<TaskRunnerWorkItem> items = new ArrayList<>();
    for (TaskRunner runner : runnerMap.values()) {
      try {
        items.addAll(runner.getRunningTasks());
      }
      catch (Exception e) {
        LOG.error(e, "Error fetching running tasks");
      }
    }
    return items;
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
  {
    final Collection<TaskRunnerWorkItem> items = new ArrayList<>();
    for (TaskRunner runner : runnerMap.values()) {
      try {
        items.addAll(runner.getPendingTasks());
      }
      catch (Exception e) {
        LOG.error(e, "Error fetching pending tasks");
      }
    }
    return items;
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
  {
    final Collection<TaskRunnerWorkItem> items = new ArrayList<>();
    for (TaskRunner runner : runnerMap.values()) {
      try {
        items.addAll(runner.getKnownTasks());
      }
      catch (Exception e) {
        LOG.error(e, "Error fetching known tasks");
      }
    }
    return items;
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.fromNullable(managementStrategy.getStats());
  }

  @Override
  @LifecycleStart
  public void start()
  {
    managementStrategy.startManagement(this);
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    managementStrategy.stopManagement();
  }

  @Override
  public Optional<ByteSource> streamTaskLog(String taskid, long offset) throws IOException
  {
    // TODO: smarter
    for (TaskRunner runner : runnerMap.values()) {
      if (!(runner instanceof TaskLogStreamer)) {
        LOG.debug("[%s] is not a task log streamer", runner.getClass().getCanonicalName());
        continue;
      }
      try {
        Optional<ByteSource> maybeLog = ((TaskLogStreamer) runner).streamTaskLog(taskid, offset);
        if (maybeLog.isPresent()) {
          return maybeLog;
        }
      }
      catch (Exception e) {
        LOG.error(e, "Error fetching log for [%s]", runner);
      }
    }
    LOG.info("Could not find any runners who knew about [%s]", taskid);
    return Optional.absent();
  }
}
