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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SimpleTaskRunner implements TaskRunner
{
  private static final Logger log = new Logger(TestTaskRunner.class);
  private static final Duration T_PENDING_TO_RUNNING = Duration.standardSeconds(2);
  private static final Duration T_SHUTDOWN_ACK = Duration.millis(8);
  private static final Duration T_SHUTDOWN_COMPLETE = Duration.standardSeconds(2);
  @GuardedBy("knownTasks")
  private final Map<String, TestTaskRunnerWorkItem> knownTasks = new HashMap<>();
  private final ScheduledExecutorService exec = ScheduledExecutors.fixed(8, "TaskQueueScaleTest-%s");
  private int capacity;

  public SimpleTaskRunner(final int capacity)
  {
    this.capacity = capacity;
  }

  public SimpleTaskRunner()
  {
    capacity = -1;
  }

  static void threadSleep(Duration duration)
  {
    try {
      Thread.sleep(duration.getMillis());
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void start()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListenableFuture<TaskStatus> run(Task task)
  {
    // Production task runners generally do not take a long time to execute "run", but may take a long time to
    // go from "running" to "pending".
    synchronized (knownTasks) {
      final TestTaskRunnerWorkItem item = knownTasks.computeIfAbsent(
          task.getId(),
          id -> new TestTaskRunnerWorkItem(
              id,
              task.getType()
          )
      );
      exec.schedule(
          () -> {
            try {
              synchronized (knownTasks) {
                final TestTaskRunnerWorkItem item2 = knownTasks.get(task.getId());
                if (item2.getState() == RunnerTaskState.PENDING) {
                  knownTasks.put(task.getId(), item2.withState(RunnerTaskState.RUNNING));
                }
              }

              exec.schedule(
                  () -> {
                    try {
                      final TestTaskRunnerWorkItem item2;
                      synchronized (knownTasks) {
                        item2 = knownTasks.get(task.getId());
                        knownTasks.put(task.getId(), item2.withState(RunnerTaskState.NONE));
                      }
                      if (item2 != null) {
                        item2.setResult(TaskStatus.success(task.getId()));
                      }
                    }
                    catch (Throwable e) {
                      log.error(e, "Error in scheduled executor");
                    }
                  },
                  ((NoopTask) task).getRunTime(),
                  TimeUnit.MILLISECONDS
              );
            }
            catch (Throwable e) {
              log.error(e, "Error in scheduled executor");
            }
          },
          T_PENDING_TO_RUNNING.getMillis(),
          TimeUnit.MILLISECONDS
      );

      return item.getResult();
    }
  }

  @Override
  public void shutdown(String taskid, String reason)
  {
    // Production task runners take a long time to execute "shutdown" if the task is currently running.
    synchronized (knownTasks) {
      if (!knownTasks.containsKey(taskid)) {
        return;
      }
    }

    threadSleep(T_SHUTDOWN_ACK);

    final TestTaskRunnerWorkItem existingTask;
    synchronized (knownTasks) {
      existingTask = knownTasks.get(taskid);
    }
    if (!existingTask.getResult().isDone()) {
      exec.schedule(() -> {
        existingTask.setResult(TaskStatus.failure("taskId", "stopped"));
        synchronized (knownTasks) {
          knownTasks.remove(taskid);
        }
      }, T_SHUTDOWN_COMPLETE.getMillis(), TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void registerListener(TaskRunnerListener listener, Executor executor)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unregisterListener(String listenerId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    // Do nothing, and return null. (TaskQueue doesn't use the return value.)
    return null;
  }

  @Override
  public void stop()
  {
    exec.shutdownNow();
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getRunningTasks()
  {
    synchronized (knownTasks) {
      return knownTasks.values()
                       .stream()
                       .filter(item -> item.getState() == RunnerTaskState.RUNNING)
                       .collect(Collectors.toList());
    }
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
  {
    synchronized (knownTasks) {
      return knownTasks.values()
                       .stream()
                       .filter(item -> item.getState() == RunnerTaskState.PENDING)
                       .collect(Collectors.toList());
    }
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
  {
    synchronized (knownTasks) {
      return ImmutableList.copyOf(knownTasks.values());
    }
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Long> getTotalTaskSlotCount()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Long> getIdleTaskSlotCount()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Long> getUsedTaskSlotCount()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Long> getLazyTaskSlotCount()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Long> getBlacklistedTaskSlotCount()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getTotalCapacity()
  {
    return capacity;
  }

  public static class TestTaskRunnerWorkItem extends TaskRunnerWorkItem
  {
    private final RunnerTaskState state;
    private final String type;

    public TestTaskRunnerWorkItem(final String taskId, final String type)
    {
      this(taskId, SettableFuture.create(), RunnerTaskState.PENDING, type);
    }


    private TestTaskRunnerWorkItem(
        final String taskId,
        final ListenableFuture<TaskStatus> result,
        final RunnerTaskState state,
        final String type
    )
    {
      super(taskId, result);
      this.state = state;
      this.type = type;
    }

    public RunnerTaskState getState()
    {
      return state;
    }

    @Override
    public TaskLocation getLocation()
    {
      return TaskLocation.unknown();
    }

    @Nullable
    @Override
    public String getTaskType()
    {
      return type;
    }


    @Override
    public String getDataSource()
    {
      throw new UnsupportedOperationException();
    }

    public void setResult(final TaskStatus result)
    {
      ((SettableFuture<TaskStatus>) getResult()).set(result);

      // possibly a parallel shutdown request was issued during the
      // shutdown time; ignore it
    }

    public TestTaskRunnerWorkItem withState(final RunnerTaskState newState)
    {
      return new TestTaskRunnerWorkItem(getTaskId(), getResult(), newState, getTaskType());
    }
  }
}
