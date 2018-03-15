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

package io.druid.indexing.common.task;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.client.indexing.TaskStatus;
import io.druid.client.indexing.TaskStatusResponse;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.logger.Logger;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Responsible for submitting tasks, monitoring task statuses, resubmitting failed tasks, and returning the final task
 * status.
 */
public class TaskMonitor
{
  private static final Logger log = new Logger(TaskMonitor.class);

  private final ScheduledExecutorService taskStatusChecker = Execs.scheduledSingleThreaded(("task-monitor-%d"));

  private final ConcurrentMap<String, MonitorEntry> taskFutureMap = new ConcurrentHashMap<>();

  // overlord client
  private final AtomicInteger numRunningTasks = new AtomicInteger();

  private final IndexingServiceClient indexingServiceClient;
  private final int maxRetry;

  private volatile boolean running = false;

  TaskMonitor(IndexingServiceClient indexingServiceClient, int maxRetry)
  {
    this.indexingServiceClient = Preconditions.checkNotNull(indexingServiceClient, "indexingServiceClient");
    this.maxRetry = maxRetry;
  }

  public void start(long taskStatusCheckingPeriod)
  {
    running = true;
    log.info("Starting taskMonitor");
    // NOTE: This polling can be improved to event-driven processing by registering TaskRunnerListener to TaskRunner.
    // That listener should be able to send the events reported to TaskRunner to this TaskMonitor.
    taskStatusChecker.scheduleAtFixedRate(
        () -> {
          try {
            final Iterator<Entry<String, MonitorEntry>> iterator = taskFutureMap.entrySet().iterator();
            while (iterator.hasNext()) {
              final Entry<String, MonitorEntry> entry = iterator.next();
              final String taskId = entry.getKey();
              final MonitorEntry monitorEntry = entry.getValue();
              final TaskStatusResponse taskStatusResponse = indexingServiceClient.getTaskStatus(taskId);
              if (taskStatusResponse != null) {
                final TaskStatus taskStatus = taskStatusResponse.getStatus();
                switch (taskStatus.getStatusCode()) {
                  case SUCCESS:
                    numRunningTasks.decrementAndGet();
                    iterator.remove();
                    monitorEntry.setLastStatus(taskStatus);
                    break;
                  case FAILED:
                    numRunningTasks.decrementAndGet();
                    log.warn("task[%s] failed!", taskId);
                    if (monitorEntry.numRetry < maxRetry) {
                      log.info(
                          "We still have chnaces[%d/%d]. Retrying task[%s]",
                          monitorEntry.numRetry,
                          maxRetry,
                          taskId
                      );
                      retry(monitorEntry.task);
                      monitorEntry.incrementNumRetry();
                    } else {
                      log.error(
                          "task[%s] failed after [%d] retries",
                          taskId,
                          monitorEntry.numRetry
                      );
                      iterator.remove();
                      monitorEntry.setLastStatus(taskStatus);
                    }
                    break;
                  default:
                    // do nothing
                }
              }
            }
          }
          catch (Throwable t) {
            log.error(t, "Error while monitoring");
          }
        },
        taskStatusCheckingPeriod,
        taskStatusCheckingPeriod,
        TimeUnit.MILLISECONDS
    );
  }

  public void stop()
  {
    running = false;
    taskStatusChecker.shutdownNow();
    log.info("Stopped taskMonitor");
  }

  public ListenableFuture<TaskStatus> submit(Task task)
  {
    if (!running) {
      return Futures.immediateFailedFuture(new ISE("TaskMonitore is not running"));
    }
    log.info("Submitting a new task[%s]", task.getId());
    final String taskId = indexingServiceClient.runTask(task);

    numRunningTasks.incrementAndGet();

    final SettableFuture<TaskStatus> taskFuture = SettableFuture.create();
    taskFutureMap.put(taskId, new MonitorEntry(task, taskFuture));
    return taskFuture;
  }

  private void retry(Task task)
  {
    if (running) {
      indexingServiceClient.runTask(task);
      numRunningTasks.incrementAndGet();
    }
  }

  public void killAll()
  {
    taskFutureMap.keySet().forEach(indexingServiceClient::killTask);
    taskFutureMap.clear();
  }

  public int getNumRunningTasks()
  {
    return numRunningTasks.intValue();
  }

  private static class MonitorEntry
  {
    private final Task task;
    private final SettableFuture<TaskStatus> future;

    private int numRetry;

    MonitorEntry(Task task, SettableFuture<TaskStatus> future)
    {
      this.task = task;
      this.future = future;
    }

    void setLastStatus(TaskStatus taskStatus)
    {
      future.set(taskStatus);
    }

    void incrementNumRetry()
    {
      numRetry++;
    }
  }
}
