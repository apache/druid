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

package org.apache.druid.indexing.overlord.hrtr;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.TaskRunnerUtils;
import org.apache.druid.indexing.overlord.config.HttpRemoteTaskRunnerConfig;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.WorkerHistoryItem;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.coordination.ChangeRequestHttpSyncer;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 */
public class WorkerHolder
{
  private static final EmittingLogger log = new EmittingLogger(WorkerHolder.class);

  public static final TypeReference<ChangeRequestsSnapshot<WorkerHistoryItem>> WORKER_SYNC_RESP_TYPE_REF = new TypeReference<ChangeRequestsSnapshot<WorkerHistoryItem>>()
  {
  };


  private final Worker worker;
  private Worker disabledWorker;

  protected final AtomicBoolean disabled;

  // Known list of tasks running/completed on this worker.
  protected final AtomicReference<Map<String, TaskAnnouncement>> tasksSnapshotRef;

  private final AtomicReference<DateTime> lastCompletedTaskTime = new AtomicReference<>(DateTimes.nowUtc());
  private final AtomicReference<DateTime> blacklistedUntil = new AtomicReference<>();
  private final AtomicInteger continuouslyFailedTasksCount = new AtomicInteger(0);

  private final ChangeRequestHttpSyncer<WorkerHistoryItem> syncer;

  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final HttpRemoteTaskRunnerConfig config;

  private final Listener listener;

  public WorkerHolder(
      ObjectMapper smileMapper,
      HttpClient httpClient,
      HttpRemoteTaskRunnerConfig config,
      ScheduledExecutorService workersSyncExec,
      Listener listener,
      Worker worker,
      List<TaskAnnouncement> knownAnnouncements
  )
  {
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.config = config;
    this.listener = listener;
    this.worker = worker;
    //worker holder is created disabled and gets enabled after first sync success.
    this.disabled = new AtomicBoolean(true);

    this.syncer = new ChangeRequestHttpSyncer<>(
        smileMapper,
        httpClient,
        workersSyncExec,
        TaskRunnerUtils.makeWorkerURL(worker, "/"),
        "/druid-internal/v1/worker",
        WORKER_SYNC_RESP_TYPE_REF,
        config.getSyncRequestTimeout().toStandardDuration().getMillis(),
        config.getServerUnstabilityTimeout().toStandardDuration().getMillis(),
        createSyncListener()
    );

    ConcurrentMap<String, TaskAnnouncement> announcements = new ConcurrentHashMap<>();
    if (knownAnnouncements != null) {
      knownAnnouncements.forEach(e -> announcements.put(e.getTaskId(), e));
    }
    tasksSnapshotRef = new AtomicReference<>(announcements);
  }

  public Worker getWorker()
  {
    return worker;
  }

  private Map<String, TaskAnnouncement> getRunningTasks()
  {
    return tasksSnapshotRef.get().entrySet().stream().filter(
        e -> e.getValue().getTaskStatus().isRunnable()
    ).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
  }

  private int getCurrCapacityUsed()
  {
    int currCapacity = 0;
    for (TaskAnnouncement taskAnnouncement : getRunningTasks().values()) {
      currCapacity += taskAnnouncement.getTaskResource().getRequiredCapacity();
    }
    return currCapacity;
  }

  private Set<String> getAvailabilityGroups()
  {
    Set<String> retVal = new HashSet<>();
    for (TaskAnnouncement taskAnnouncement : getRunningTasks().values()) {
      retVal.add(taskAnnouncement.getTaskResource().getAvailabilityGroup());
    }
    return retVal;
  }

  public DateTime getBlacklistedUntil()
  {
    return blacklistedUntil.get();
  }

  public void setLastCompletedTaskTime(DateTime completedTaskTime)
  {
    lastCompletedTaskTime.set(completedTaskTime);
  }

  public void setBlacklistedUntil(DateTime blacklistedUntil)
  {
    this.blacklistedUntil.set(blacklistedUntil);
  }

  public ImmutableWorkerInfo toImmutable()
  {
    Worker w = worker;
    if (disabled.get()) {
      if (disabledWorker == null) {
        disabledWorker = new Worker(
            worker.getScheme(),
            worker.getHost(),
            worker.getIp(),
            worker.getCapacity(),
            "",
            worker.getCategory()
        );
      }
      w = disabledWorker;
    }

    return new ImmutableWorkerInfo(
        w,
        getCurrCapacityUsed(),
        getAvailabilityGroups(),
        getRunningTasks().keySet(),
        lastCompletedTaskTime.get(),
        blacklistedUntil.get()
    );
  }

  public int getContinuouslyFailedTasksCount()
  {
    return continuouslyFailedTasksCount.get();
  }

  public void resetContinuouslyFailedTasksCount()
  {
    this.continuouslyFailedTasksCount.set(0);
  }

  public void incrementContinuouslyFailedTasksCount()
  {
    this.continuouslyFailedTasksCount.incrementAndGet();
  }

  public boolean assignTask(Task task)
  {
    if (disabled.get()) {
      log.info(
          "Received task[%s] assignment on worker[%s] when worker is disabled.",
          task.getId(),
          worker.getHost()
      );
      return false;
    }

    URL url = TaskRunnerUtils.makeWorkerURL(worker, "/druid-internal/v1/worker/assignTask");
    int numTries = config.getAssignRequestMaxRetries();

    try {
      return RetryUtils.retry(
          () -> {
            try {
              final StatusResponseHolder response = httpClient.go(
                  new Request(HttpMethod.POST, url)
                      .addHeader(HttpHeaders.Names.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
                      .setContent(smileMapper.writeValueAsBytes(task)),
                  StatusResponseHandler.getInstance(),
                  config.getAssignRequestHttpTimeout().toStandardDuration()
              ).get();

              if (response.getStatus().getCode() == 200) {
                return true;
              } else {
                throw new RE(
                    "Failed to assign task[%s] to worker[%s]. Response Code[%s] and Message[%s]. Retrying...",
                    task.getId(),
                    worker.getHost(),
                    response.getStatus().getCode(),
                    response.getContent()
                );
              }
            }
            catch (ExecutionException ex) {
              throw new RE(
                  ex,
                  "Request to assign task[%s] to worker[%s] failed. Retrying...",
                  task.getId(),
                  worker.getHost()
              );
            }
          },
          e -> !(e instanceof InterruptedException),
          numTries
      );
    }
    catch (Exception ex) {
      log.info("Not sure whether task[%s] was successfully assigned to worker[%s].", task.getId(), worker.getHost());
      return true;
    }
  }

  public void shutdownTask(String taskId)
  {
    final URL url = TaskRunnerUtils.makeWorkerURL(worker, "/druid/worker/v1/task/%s/shutdown", taskId);

    try {
      RetryUtils.retry(
          () -> {
            try {
              final StatusResponseHolder response = httpClient.go(
                  new Request(HttpMethod.POST, url),
                  StatusResponseHandler.getInstance(),
                  config.getShutdownRequestHttpTimeout().toStandardDuration()
              ).get();

              if (response.getStatus().getCode() == 200) {
                log.info(
                    "Sent shutdown message to worker: %s, status %s, response: %s",
                    worker.getHost(),
                    response.getStatus(),
                    response.getContent()
                );
                return null;
              } else {
                throw new RE("Attempt to shutdown task[%s] on worker[%s] failed.", taskId, worker.getHost());
              }
            }
            catch (ExecutionException e) {
              throw new RE(e, "Error in handling post to [%s] for task [%s]", worker.getHost(), taskId);
            }
          },
          e -> !(e instanceof InterruptedException),
          config.getShutdownRequestMaxRetries()
      );
    }
    catch (Exception ex) {
      if (ex instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }

      log.error("Failed to shutdown task[%s] on worker[%s] failed.", taskId, worker.getHost());
    }
  }

  public void start()
  {
    syncer.start();
  }

  public void stop()
  {
    syncer.stop();
  }

  public void waitForInitialization() throws InterruptedException
  {
    if (!syncer.awaitInitialization(3 * syncer.getServerHttpTimeout(), TimeUnit.MILLISECONDS)) {
      throw new RE("Failed to sync with worker[%s].", worker.getHost());
    }
  }

  public boolean isInitialized()
  {
    try {
      return syncer.awaitInitialization(1, TimeUnit.MILLISECONDS);
    }
    catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  public boolean isEnabled()
  {
    return !disabled.get();
  }

  public ChangeRequestHttpSyncer<WorkerHistoryItem> getUnderlyingSyncer()
  {
    return syncer;
  }

  public ChangeRequestHttpSyncer.Listener<WorkerHistoryItem> createSyncListener()
  {
    return new ChangeRequestHttpSyncer.Listener<WorkerHistoryItem>()
    {
      @Override
      public void fullSync(List<WorkerHistoryItem> changes)
      {
        ConcurrentMap<String, TaskAnnouncement> newSnapshot = new ConcurrentHashMap<>();

        List<TaskAnnouncement> delta = new ArrayList<>();
        boolean isWorkerDisabled = disabled.get();

        for (WorkerHistoryItem change : changes) {
          if (change instanceof WorkerHistoryItem.TaskUpdate) {
            TaskAnnouncement announcement = ((WorkerHistoryItem.TaskUpdate) change).getTaskAnnouncement();
            newSnapshot.put(announcement.getTaskId(), announcement);
            delta.add(announcement);
          } else if (change instanceof WorkerHistoryItem.Metadata) {
            isWorkerDisabled = ((WorkerHistoryItem.Metadata) change).isDisabled();
          } else {
            log.makeAlert(
                "Got unknown sync update[%s] from worker[%s]. Ignored.",
                change.getClass().getName(),
                worker.getHost()
            ).emit();
          }
        }

        for (TaskAnnouncement announcement : tasksSnapshotRef.get().values()) {
          if (!newSnapshot.containsKey(announcement.getTaskId()) && !announcement.getTaskStatus()
                                                                                 .isComplete()) {
            log.warn(
                "task[%s] in state[%s] suddenly disappeared on worker[%s]. failing it.",
                announcement.getTaskId(),
                announcement.getStatus(),
                worker.getHost()
            );
            delta.add(TaskAnnouncement.create(
                announcement.getTaskId(),
                announcement.getTaskType(),
                announcement.getTaskResource(),
                TaskStatus.failure(announcement.getTaskId()),
                announcement.getTaskLocation(),
                announcement.getTaskDataSource()
            ));
          }
        }

        tasksSnapshotRef.set(newSnapshot);

        notifyListener(delta, isWorkerDisabled);
      }

      @Override
      public void deltaSync(List<WorkerHistoryItem> changes)
      {
        List<TaskAnnouncement> delta = new ArrayList<>();
        boolean isWorkerDisabled = disabled.get();

        for (WorkerHistoryItem change : changes) {
          if (change instanceof WorkerHistoryItem.TaskUpdate) {
            TaskAnnouncement announcement = ((WorkerHistoryItem.TaskUpdate) change).getTaskAnnouncement();
            tasksSnapshotRef.get().put(announcement.getTaskId(), announcement);
            delta.add(announcement);
          } else if (change instanceof WorkerHistoryItem.TaskRemoval) {
            String taskId = ((WorkerHistoryItem.TaskRemoval) change).getTaskId();
            TaskAnnouncement announcement = tasksSnapshotRef.get().remove(taskId);
            if (announcement != null && !announcement.getTaskStatus().isComplete()) {
              log.warn(
                  "task[%s] in state[%s] suddenly disappeared on worker[%s]. failing it.",
                  announcement.getTaskId(),
                  announcement.getStatus(),
                  worker.getHost()
              );
              delta.add(TaskAnnouncement.create(
                  announcement.getTaskId(),
                  announcement.getTaskType(),
                  announcement.getTaskResource(),
                  TaskStatus.failure(announcement.getTaskId()),
                  announcement.getTaskLocation(),
                  announcement.getTaskDataSource()
              ));
            }
          } else if (change instanceof WorkerHistoryItem.Metadata) {
            isWorkerDisabled = ((WorkerHistoryItem.Metadata) change).isDisabled();
          } else {
            log.makeAlert(
                "Got unknown sync update[%s] from worker[%s]. Ignored.",
                change.getClass().getName(),
                worker.getHost()
            ).emit();
          }
        }

        notifyListener(delta, isWorkerDisabled);
      }

      private void notifyListener(List<TaskAnnouncement> announcements, boolean isWorkerDisabled)
      {
        for (TaskAnnouncement announcement : announcements) {
          try {
            listener.taskAddedOrUpdated(announcement, WorkerHolder.this);
          }
          catch (Exception ex) {
            log.error(
                ex,
                "Unknown exception while updating task[%s] state from worker[%s].",
                announcement.getTaskId(),
                worker.getHost()
            );
          }
        }

        if (isWorkerDisabled != disabled.get()) {
          disabled.set(isWorkerDisabled);
          log.info("Worker[%s] disabled set to [%s].", worker.getHost(), isWorkerDisabled);
        }
      }
    };
  }

  public interface Listener
  {
    void taskAddedOrUpdated(TaskAnnouncement announcement, WorkerHolder workerHolder);
  }
}
