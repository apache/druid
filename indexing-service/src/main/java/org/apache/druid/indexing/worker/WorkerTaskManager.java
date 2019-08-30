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

package org.apache.druid.indexing.worker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class manages the list of tasks assigned to this worker.
 * <p>
 * It persists the list of assigned and completed tasks on disk. assigned task from disk is deleted as soon as it
 * starts running and completed task on disk is deleted based on a periodic schedule where overlord is asked for
 * active tasks to see which completed tasks are safe to delete.
 */
public abstract class WorkerTaskManager
{
  private static final EmittingLogger log = new EmittingLogger(WorkerTaskManager.class);

  private final ObjectMapper jsonMapper;
  private final TaskRunner taskRunner;
  private final ExecutorService exec;

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private final ConcurrentMap<String, Task> assignedTasks = new ConcurrentHashMap<>();

  // ZK_CLEANUP_TODO : these are marked protected to be used in subclass WorkerTaskMonitor that updates ZK.
  // should be marked private alongwith WorkerTaskMonitor removal.
  protected final ConcurrentMap<String, TaskDetails> runningTasks = new ConcurrentHashMap<>();
  protected final ConcurrentMap<String, TaskAnnouncement> completedTasks = new ConcurrentHashMap<>();

  private final ChangeRequestHistory<WorkerHistoryItem> changeHistory = new ChangeRequestHistory<>();

  //synchronizes access to "running", "completed" and "changeHistory"
  protected final Object lock = new Object();

  private final TaskConfig taskConfig;

  private final ScheduledExecutorService completedTasksCleanupExecutor;

  private final AtomicBoolean disabled = new AtomicBoolean(false);

  private final DruidLeaderClient overlordClient;

  @Inject
  public WorkerTaskManager(
      ObjectMapper jsonMapper,
      TaskRunner taskRunner,
      TaskConfig taskConfig,
      @IndexingService DruidLeaderClient overlordClient
  )
  {
    this.jsonMapper = jsonMapper;
    this.taskRunner = taskRunner;
    this.taskConfig = taskConfig;
    this.exec = Execs.singleThreaded("WorkerTaskManager-NoticeHandler");
    this.completedTasksCleanupExecutor = Execs.scheduledSingleThreaded("WorkerTaskManager-CompletedTasksCleaner");
    this.overlordClient = overlordClient;
  }

  @LifecycleStart
  public void start() throws Exception
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    synchronized (lock) {
      try {
        log.info("Starting...");
        cleanupAndMakeTmpTaskDir();
        registerLocationListener();
        restoreRestorableTasks();
        initAssignedTasks();
        initCompletedTasks();
        scheduleCompletedTasksCleanup();
        lifecycleLock.started();
        log.info("Started.");
      }
      catch (Exception e) {
        log.makeAlert(e, "Exception starting WorkerTaskManager.").emit();
        throw e;
      }
      finally {
        lifecycleLock.exitStart();
      }
    }
  }

  @LifecycleStop
  public void stop() throws Exception
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }

    synchronized (lock) {
      try {
        // When stopping, the task status should not be communicated to the overlord, so the listener and exec
        // are shut down before the taskRunner is stopped.
        taskRunner.unregisterListener("WorkerTaskManager");
        exec.shutdownNow();
        taskRunner.stop();
        log.info("Stopped WorkerTaskManager.");
      }
      catch (Exception e) {
        log.makeAlert(e, "Exception stopping WorkerTaskManager")
           .emit();
      }
    }
  }

  public Map<String, TaskAnnouncement> getCompletedTasks()
  {
    return completedTasks;
  }

  private void submitNoticeToExec(Notice notice)
  {
    exec.execute(
        () -> {
          try {
            notice.handle();
          }
          catch (Exception e) {
            if (e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }

            log.makeAlert(e, "Failed to handle notice")
               .addData("noticeClass", notice.getClass().getSimpleName())
               .addData("noticeTaskId", notice.getTaskId())
               .emit();
          }
        }
    );
  }

  private void restoreRestorableTasks()
  {
    final List<Pair<Task, ListenableFuture<TaskStatus>>> restored = taskRunner.restore();
    for (Pair<Task, ListenableFuture<TaskStatus>> pair : restored) {
      addRunningTask(pair.lhs, pair.rhs);
    }
  }

  private void registerLocationListener()
  {
    taskRunner.registerListener(
        new TaskRunnerListener()
        {
          @Override
          public String getListenerId()
          {
            return "WorkerTaskManager";
          }

          @Override
          public void locationChanged(final String taskId, final TaskLocation newLocation)
          {
            submitNoticeToExec(new LocationNotice(taskId, newLocation));
          }

          @Override
          public void statusChanged(final String taskId, final TaskStatus status)
          {
            // do nothing
          }
        },
        Execs.directExecutor()
    );
  }

  private void addRunningTask(final Task task, final ListenableFuture<TaskStatus> future)
  {
    runningTasks.put(task.getId(), new TaskDetails(task));
    Futures.addCallback(
        future,
        new FutureCallback<TaskStatus>()
        {
          @Override
          public void onSuccess(TaskStatus result)
          {
            submitNoticeToExec(new StatusNotice(task, result));
          }

          @Override
          public void onFailure(Throwable t)
          {
            submitNoticeToExec(new StatusNotice(task, TaskStatus.failure(task.getId())));
          }
        }
    );
  }

  public void assignTask(Task task)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.SECONDS), "not started");

    synchronized (lock) {
      if (assignedTasks.containsKey(task.getId())
          || runningTasks.containsKey(task.getId())
          || completedTasks.containsKey(task.getId())) {
        log.info("Assign task[%s] request ignored because it exists already.", task.getId());
        return;
      }

      try {
        FileUtils.writeAtomically(new File(getAssignedTaskDir(), task.getId()), getTmpTaskDir(),
            os -> {
            jsonMapper.writeValue(os, task);
            return null;
          }
        );
        assignedTasks.put(task.getId(), task);
      }
      catch (IOException ex) {
        log.error(ex, "Error while trying to persist assigned task[%s]", task.getId());
        throw new ISE("Assign Task[%s] Request failed because [%s].", task.getId(), ex.getMessage());
      }

      changeHistory.addChangeRequest(
          new WorkerHistoryItem.TaskUpdate(
              TaskAnnouncement.create(
                  task,
                  TaskStatus.running(task.getId()),
                  TaskLocation.unknown()
              )
          )
      );
    }

    submitNoticeToExec(new RunNotice(task));
  }

  private File getTmpTaskDir()
  {
    return new File(taskConfig.getBaseTaskDir(), "workerTaskManagerTmp");
  }

  private void cleanupAndMakeTmpTaskDir()
  {
    File tmpDir = getTmpTaskDir();
    tmpDir.mkdirs();
    if (!tmpDir.isDirectory()) {
      throw new ISE("Tmp Tasks Dir [%s] does not exist/not-a-directory.", tmpDir);
    }

    // Delete any tmp files left out from before due to jvm crash.
    try {
      org.apache.commons.io.FileUtils.cleanDirectory(tmpDir);
    }
    catch (IOException ex) {
      log.warn("Failed to cleanup tmp dir [%s].", tmpDir.getAbsolutePath());
    }
  }

  public File getAssignedTaskDir()
  {
    return new File(taskConfig.getBaseTaskDir(), "assignedTasks");
  }

  private void initAssignedTasks()
  {
    File assignedTaskDir = getAssignedTaskDir();

    log.info("Looking for any previously assigned tasks on disk[%s].", assignedTaskDir);

    assignedTaskDir.mkdirs();

    if (!assignedTaskDir.isDirectory()) {
      throw new ISE("Assigned Tasks Dir [%s] does not exist/not-a-directory.", assignedTaskDir);
    }

    for (File taskFile : assignedTaskDir.listFiles()) {
      try {
        String taskId = taskFile.getName();
        Task task = jsonMapper.readValue(taskFile, Task.class);
        if (taskId.equals(task.getId())) {
          assignedTasks.put(taskId, task);
          log.info("Found assigned task[%s].", taskId);
        } else {
          throw new ISE("WTF! Corrupted assigned task on disk[%s].", taskFile.getAbsoluteFile());
        }
      }
      catch (IOException ex) {
        log.error(ex, "Failed to read assigned task from disk at [%s]. Ignored.", taskFile.getAbsoluteFile());
      }
    }

    for (Task task : assignedTasks.values()) {
      submitNoticeToExec(new RunNotice(task));
    }
  }

  private void cleanupAssignedTask(Task task)
  {
    assignedTasks.remove(task.getId());
    File taskFile = new File(getAssignedTaskDir(), task.getId());
    try {
      Files.delete(taskFile.toPath());
    }
    catch (IOException ex) {
      log.error(ex, "Failed to delete assigned task from disk at [%s].", taskFile);
    }
  }

  public ListenableFuture<ChangeRequestsSnapshot<WorkerHistoryItem>> getChangesSince(ChangeRequestHistory.Counter counter)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.SECONDS), "not started");

    if (counter.getCounter() < 0) {
      synchronized (lock) {

        List<WorkerHistoryItem> items = new ArrayList<>();
        items.add(new WorkerHistoryItem.Metadata(disabled.get()));

        for (Task task : assignedTasks.values()) {
          items.add(
              new WorkerHistoryItem.TaskUpdate(
                  TaskAnnouncement.create(
                      task,
                      TaskStatus.running(task.getId()),
                      TaskLocation.unknown()
                  )
              )
          );
        }

        for (TaskDetails details : runningTasks.values()) {
          items.add(
              new WorkerHistoryItem.TaskUpdate(
                  TaskAnnouncement.create(
                      details.task,
                      details.status,
                      details.location
                  )
              )
          );
        }

        for (TaskAnnouncement taskAnnouncement : completedTasks.values()) {
          items.add(new WorkerHistoryItem.TaskUpdate(taskAnnouncement));
        }

        SettableFuture<ChangeRequestsSnapshot<WorkerHistoryItem>> future = SettableFuture.create();
        future.set(ChangeRequestsSnapshot.success(changeHistory.getLastCounter(), Lists.newArrayList(items)));
        return future;
      }
    } else {
      return changeHistory.getRequestsSince(counter);
    }
  }

  public File getCompletedTaskDir()
  {
    return new File(taskConfig.getBaseTaskDir(), "completedTasks");
  }

  private void moveFromRunningToCompleted(String taskId, TaskAnnouncement taskAnnouncement)
  {
    synchronized (lock) {
      runningTasks.remove(taskId);
      completedTasks.put(taskId, taskAnnouncement);

      try {
        FileUtils.writeAtomically(new File(getCompletedTaskDir(), taskId), getTmpTaskDir(),
            os -> {
            jsonMapper.writeValue(os, taskAnnouncement);
            return null;
          }
        );
      }
      catch (IOException ex) {
        log.error(ex, "Error while trying to persist completed task[%s] announcement.", taskId);
        throw new ISE("Persisting completed task[%s] announcement failed because [%s].", taskId, ex.getMessage());
      }
    }
  }

  private void initCompletedTasks()
  {
    File completedTaskDir = getCompletedTaskDir();
    log.info("Looking for any previously completed tasks on disk[%s].", completedTaskDir);

    completedTaskDir.mkdirs();

    if (!completedTaskDir.isDirectory()) {
      throw new ISE("Completed Tasks Dir [%s] does not exist/not-a-directory.", completedTaskDir);
    }

    for (File taskFile : completedTaskDir.listFiles()) {
      try {
        String taskId = taskFile.getName();
        TaskAnnouncement taskAnnouncement = jsonMapper.readValue(taskFile, TaskAnnouncement.class);
        if (taskId.equals(taskAnnouncement.getTaskId())) {
          completedTasks.put(taskId, taskAnnouncement);
          log.info("Found completed task[%s] with status[%s].", taskId, taskAnnouncement.getStatus());
        } else {
          throw new ISE("WTF! Corrupted completed task on disk[%s].", taskFile.getAbsoluteFile());
        }
      }
      catch (IOException ex) {
        log.error(ex, "Failed to read completed task from disk at [%s]. Ignored.", taskFile.getAbsoluteFile());
      }
    }
  }

  private void scheduleCompletedTasksCleanup()
  {
    completedTasksCleanupExecutor.scheduleAtFixedRate(
        () -> {
          try {
            if (completedTasks.isEmpty()) {
              log.debug("Skipping completed tasks cleanup. Its empty.");
              return;
            }

            ImmutableSet<String> taskIds = ImmutableSet.copyOf(completedTasks.keySet());
            Map<String, TaskStatus> taskStatusesFromOverlord = null;

            try {
              StringFullResponseHolder fullResponseHolder = overlordClient.go(
                  overlordClient.makeRequest(HttpMethod.POST, "/druid/indexer/v1/taskStatus")
                                .setContent(jsonMapper.writeValueAsBytes(taskIds))
                                .addHeader(HttpHeaders.Names.ACCEPT, MediaType.APPLICATION_JSON)
                                .addHeader(HttpHeaders.Names.CONTENT_TYPE, MediaType.APPLICATION_JSON)

              );
              if (fullResponseHolder.getStatus().getCode() == 200) {
                String responseContent = fullResponseHolder.getContent();
                taskStatusesFromOverlord = jsonMapper.readValue(
                    responseContent,
                    new TypeReference<Map<String, TaskStatus>>()
                    {
                    }
                );
                log.debug("Received completed task status response [%s].", responseContent);
              } else if (fullResponseHolder.getStatus().getCode() == 404) {
                // NOTE: this is to support backward compatibility, when overlord doesn't have "activeTasks" endpoint.
                // this if clause should be removed in a future release.
                log.debug("Deleting all completed tasks. Overlord appears to be running on older version.");
                taskStatusesFromOverlord = ImmutableMap.of();
              } else {
                log.info(
                    "Got non-success code[%s] from overlord while getting active tasks. will retry on next scheduled run.",
                    fullResponseHolder.getStatus().getCode()
                );
              }
            }
            catch (Exception ex) {
              log.info(ex, "Exception while getting active tasks from overlord. will retry on next scheduled run.");

              if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
              }
            }

            if (taskStatusesFromOverlord == null) {
              return;
            }

            for (String taskId : taskIds) {
              TaskStatus status = taskStatusesFromOverlord.get(taskId);
              if (status == null || status.isComplete()) {

                log.info(
                    "Deleting completed task[%s] information, overlord task status[%s].",
                    taskId,
                    status == null ? "unknown" : status.getStatusCode()
                );

                completedTasks.remove(taskId);
                File taskFile = new File(getCompletedTaskDir(), taskId);
                try {
                  Files.deleteIfExists(taskFile.toPath());
                  changeHistory.addChangeRequest(new WorkerHistoryItem.TaskRemoval(taskId));
                }
                catch (IOException ex) {
                  log.error(ex, "Failed to delete completed task from disk [%s].", taskFile);
                }

              }
            }
          }
          catch (Throwable th) {
            log.error(th, "WTF! Got unknown exception while running the scheduled cleanup.");
          }
        },
        1,
        5,
        TimeUnit.MINUTES
    );
  }

  public void workerEnabled()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.SECONDS), "not started");

    if (disabled.compareAndSet(true, false)) {
      changeHistory.addChangeRequest(new WorkerHistoryItem.Metadata(false));
    }
  }

  public void workerDisabled()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.SECONDS), "not started");

    if (disabled.compareAndSet(false, true)) {
      changeHistory.addChangeRequest(new WorkerHistoryItem.Metadata(true));
    }
  }

  private static class TaskDetails
  {
    private final Task task;
    private final long startTime;
    private TaskStatus status;
    private TaskLocation location;

    public TaskDetails(Task task)
    {
      this.task = task;
      this.startTime = System.currentTimeMillis();
      this.status = TaskStatus.running(task.getId());
      this.location = TaskLocation.unknown();
    }
  }

  private interface Notice
  {
    String getTaskId();

    void handle();
  }

  private class RunNotice implements Notice
  {
    private final Task task;

    public RunNotice(Task task)
    {
      this.task = task;
    }

    @Override
    public String getTaskId()
    {
      return task.getId();
    }

    @Override
    public void handle()
    {
      TaskAnnouncement announcement;
      synchronized (lock) {
        if (runningTasks.containsKey(task.getId()) || completedTasks.containsKey(task.getId())) {
          log.warn(
              "Got run notice for task [%s] that I am already running or completed...",
              task.getId()
          );

          taskStarted(task.getId());
          return;
        }

        final ListenableFuture<TaskStatus> future = taskRunner.run(task);
        addRunningTask(task, future);

        announcement = TaskAnnouncement.create(
            task,
            TaskStatus.running(task.getId()),
            TaskLocation.unknown()
        );

        changeHistory.addChangeRequest(new WorkerHistoryItem.TaskUpdate(announcement));

        cleanupAssignedTask(task);
        log.info("Task[%s] started.", task.getId());
      }

      taskAnnouncementChanged(announcement);
      taskStarted(task.getId());
    }
  }

  private class StatusNotice implements Notice
  {
    private final Task task;
    private final TaskStatus status;

    public StatusNotice(Task task, TaskStatus status)
    {
      this.task = task;
      this.status = status;
    }

    @Override
    public String getTaskId()
    {
      return task.getId();
    }

    @Override
    public void handle()
    {
      synchronized (lock) {
        final TaskDetails details = runningTasks.get(task.getId());

        if (details == null) {
          log.warn("Got status notice for task [%s] that isn't running...", task.getId());
          return;
        }

        if (!status.isComplete()) {
          log.warn(
              "WTF?! Got status notice for task [%s] that isn't complete (status = [%s])...",
              task.getId(),
              status.getStatusCode()
          );
          return;
        }

        details.status = status.withDuration(System.currentTimeMillis() - details.startTime);

        TaskAnnouncement latest = TaskAnnouncement.create(
            details.task,
            details.status,
            details.location
        );

        moveFromRunningToCompleted(task.getId(), latest);

        changeHistory.addChangeRequest(new WorkerHistoryItem.TaskUpdate(latest));
        taskAnnouncementChanged(latest);
        log.info(
            "Job's finished. Completed [%s] with status [%s]",
            task.getId(),
            status.getStatusCode()
        );
      }
    }
  }

  private class LocationNotice implements Notice
  {
    private final String taskId;
    private final TaskLocation location;

    public LocationNotice(String taskId, TaskLocation location)
    {
      this.taskId = taskId;
      this.location = location;
    }

    @Override
    public String getTaskId()
    {
      return taskId;
    }

    @Override
    public void handle()
    {
      synchronized (lock) {
        final TaskDetails details = runningTasks.get(taskId);

        if (details == null) {
          log.warn("Got location notice for task [%s] that isn't running...", taskId);
          return;
        }

        if (!Objects.equals(details.location, location)) {
          details.location = location;

          TaskAnnouncement latest = TaskAnnouncement.create(
              details.task,
              details.status,
              details.location
          );

          changeHistory.addChangeRequest(new WorkerHistoryItem.TaskUpdate(latest));
          taskAnnouncementChanged(latest);
        }
      }
    }
  }

  // ZK_CLEANUP_TODO :
  //Note: Following abstract methods exist only to support WorkerTaskMonitor that
  //watches task assignments and updates task statuses inside Zookeeper. When the transition to HTTP is complete
  //in Overlord as well as MiddleManagers then WorkerTaskMonitor should be deleted, this class should no longer be abstract
  //and the methods below should be removed.
  protected abstract void taskStarted(String taskId);

  protected abstract void taskAnnouncementChanged(TaskAnnouncement announcement);
}
