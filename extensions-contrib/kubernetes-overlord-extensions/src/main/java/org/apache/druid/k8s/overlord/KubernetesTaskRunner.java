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
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.netty.util.SuppressForbidden;
import org.apache.commons.io.FileUtils;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerUtils;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.apache.druid.k8s.overlord.common.DruidK8sConstants;
import org.apache.druid.k8s.overlord.common.JobResponse;
import org.apache.druid.k8s.overlord.common.K8sTaskId;
import org.apache.druid.k8s.overlord.common.KubernetesPeonClient;
import org.apache.druid.k8s.overlord.common.KubernetesResourceNotFoundException;
import org.apache.druid.k8s.overlord.common.PeonPhase;
import org.apache.druid.k8s.overlord.common.TaskAdapter;
import org.apache.druid.tasklogs.TaskLogPusher;
import org.apache.druid.tasklogs.TaskLogStreamer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs tasks as k8s jobs using the "internal peon" verb.
 * One additional feature of this class is that kubernetes is the source of truth, so if you launch a task
 * shutdown druid, bring up druid, the task will keep running and the state will be updated when the cluster
 * comes back.  Thus while no tasks are technically restorable, all tasks once launched will run in isolation to the
 * extent possible without requiring the overlord consistently up during their lifetime.
 */

public class KubernetesTaskRunner implements TaskLogStreamer, TaskRunner
{

  private static final EmittingLogger log = new EmittingLogger(KubernetesTaskRunner.class);
  private final CopyOnWriteArrayList<Pair<TaskRunnerListener, Executor>> listeners = new CopyOnWriteArrayList<>();

  // to cleanup old jobs that might not have been deleted.
  private final ScheduledExecutorService cleanupExecutor;

  protected final ConcurrentHashMap<String, K8sWorkItem> tasks = new ConcurrentHashMap<>();
  protected final TaskAdapter adapter;

  private final KubernetesTaskRunnerConfig k8sConfig;
  private final TaskQueueConfig taskQueueConfig;
  private final TaskLogPusher taskLogPusher;
  private final ListeningExecutorService exec;
  private final KubernetesPeonClient client;
  private final HttpClient httpClient;


  public KubernetesTaskRunner(
      TaskAdapter adapter,
      KubernetesTaskRunnerConfig k8sConfig,
      TaskQueueConfig taskQueueConfig,
      TaskLogPusher taskLogPusher,
      KubernetesPeonClient client,
      HttpClient httpClient
  )
  {
    this.adapter = adapter;
    this.k8sConfig = k8sConfig;
    this.taskQueueConfig = taskQueueConfig;
    this.taskLogPusher = taskLogPusher;
    this.client = client;
    this.httpClient = httpClient;
    this.cleanupExecutor = Executors.newScheduledThreadPool(1);
    this.exec = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(taskQueueConfig.getMaxSize(), "k8s-task-runner-%d")
    );
    Preconditions.checkArgument(
        taskQueueConfig.getMaxSize() < Integer.MAX_VALUE,
        "The task queue bounds how many concurrent k8s tasks you can have"
    );
  }


  @Override
  public Optional<InputStream> streamTaskLog(String taskid, long offset)
  {
    return client.getPeonLogs(new K8sTaskId(taskid));
  }

  @Override
  public ListenableFuture<TaskStatus> run(Task task)
  {
    synchronized (tasks) {
      tasks.computeIfAbsent(
          task.getId(), k -> new K8sWorkItem(
              client,
              task,
              exec.submit(() -> {
                K8sTaskId k8sTaskId = new K8sTaskId(task);
                try {
                  JobResponse completedPhase;
                  Optional<Job> existingJob = client.jobExists(k8sTaskId);
                  if (!existingJob.isPresent()) {
                    Job job = adapter.fromTask(task);
                    log.info("Job created %s and ready to launch", k8sTaskId);
                    Pod peonPod = client.launchJobAndWaitForStart(
                        job,
                        KubernetesTaskRunnerConfig.toMilliseconds(k8sConfig.k8sjobLaunchTimeout),
                        TimeUnit.MILLISECONDS
                    );
                    log.info("Job %s launched in k8s", k8sTaskId);
                    completedPhase = monitorJob(peonPod, k8sTaskId);
                  } else {
                    Job job = existingJob.get();
                    if (job.getStatus().getActive() == null) {
                      if (job.getStatus().getSucceeded() != null) {
                        completedPhase = new JobResponse(job, PeonPhase.SUCCEEDED);
                      } else {
                        completedPhase = new JobResponse(job, PeonPhase.FAILED);
                      }
                    } else {
                      // the job is active lets monitor it
                      completedPhase = monitorJob(k8sTaskId);
                    }
                  }
                  TaskStatus status;
                  if (PeonPhase.SUCCEEDED.equals(completedPhase.getPhase())) {
                    status = TaskStatus.success(task.getId());
                  } else if (completedPhase.getJob() == null) {
                    status = TaskStatus.failure(
                        task.getId(),
                        "K8s Job for task disappeared before completion: " + k8sTaskId
                    );
                  } else {
                    status = TaskStatus.failure(
                        task.getId(),
                        "Task failed: " + k8sTaskId
                    );
                  }
                  if (completedPhase.getJobDuration().isPresent()) {
                    status = status.withDuration(completedPhase.getJobDuration().get());
                  }
                  updateStatus(task, status);
                  return status;
                }
                catch (Exception e) {
                  log.error(e, "Error with task: %s", k8sTaskId);
                  throw e;
                }
                finally {
                  // publish task logs
                  Path log = Files.createTempFile(task.getId(), "log");
                  try {
                    Optional<InputStream> logStream = client.getPeonLogs(new K8sTaskId(task.getId()));
                    if (logStream.isPresent()) {
                      FileUtils.copyInputStreamToFile(logStream.get(), log.toFile());
                    }
                    taskLogPusher.pushTaskLog(task.getId(), log.toFile());
                  }
                  finally {
                    Files.deleteIfExists(log);
                  }
                  client.cleanUpJob(new K8sTaskId(task.getId()));
                  synchronized (tasks) {
                    tasks.remove(task.getId());
                  }
                }
              })
          ));
      return tasks.get(task.getId()).getResult();
    }
  }

  JobResponse monitorJob(K8sTaskId k8sTaskId)
  {
    return monitorJob(client.getMainJobPod(k8sTaskId), k8sTaskId);
  }

  JobResponse monitorJob(Pod peonPod, K8sTaskId k8sTaskId)
  {
    if (peonPod == null) {
      throw new ISE("Error in k8s launching peon pod for task %s", k8sTaskId);
    }
    return client.waitForJobCompletion(
        k8sTaskId,
        KubernetesTaskRunnerConfig.toMilliseconds(k8sConfig.maxTaskDuration),
        TimeUnit.MILLISECONDS
    );
  }

  @Override
  public void updateStatus(Task task, TaskStatus status)
  {
    log.info("Updating task: %s with status %s", task.getId(), status);
    TaskRunnerUtils.notifyStatusChanged(listeners, task.getId(), status);
  }

  @Override
  public void updateLocation(Task task, TaskLocation location)
  {
    TaskRunnerUtils.notifyLocationChanged(listeners, task.getId(), location);
  }

  @Override
  public void shutdown(String taskid, String reason)
  {
    client.cleanUpJob(new K8sTaskId(taskid));
  }

  @Override
  public Optional<InputStream> streamTaskReports(String taskid) throws IOException
  {
    final K8sWorkItem workItem = tasks.get(taskid);
    if (workItem == null) {
      return Optional.absent();
    }

    final TaskLocation taskLocation = workItem.getLocation();

    if (TaskLocation.unknown().equals(taskLocation)) {
      // No location known for this task. It may have not been assigned one yet.
      return Optional.absent();
    }

    final URL url = TaskRunnerUtils.makeTaskLocationURL(
        taskLocation,
        "/druid/worker/v1/chat/%s/liveReports",
        taskid
    );

    try {
      return Optional.of(httpClient.go(
          new Request(HttpMethod.GET, url),
          new InputStreamResponseHandler()
      ).get());
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    catch (ExecutionException e) {
      // Unwrap if possible
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    return ImmutableList.of();
  }

  @Override
  public void start()
  {
    cleanupExecutor.scheduleAtFixedRate(
        () ->
            client.cleanCompletedJobsOlderThan(
                KubernetesTaskRunnerConfig.toMilliseconds(k8sConfig.taskCleanupDelay),
                TimeUnit.MILLISECONDS
            ),
        1,
        KubernetesTaskRunnerConfig.toMilliseconds(k8sConfig.taskCleanupInterval),
        TimeUnit.MILLISECONDS
    );
    log.debug("Started cleanup executor for jobs older than %s....", k8sConfig.taskCleanupDelay);
  }


  @Override
  public void stop()
  {
    log.info("Stopping KubernetesTaskRunner");
    cleanupExecutor.shutdownNow();
    log.info("Stopped KubernetesTaskRunner");
  }

  @Override
  public Map<String, Long> getTotalTaskSlotCount()
  {
    return ImmutableMap.of("taskQueue", (long) taskQueueConfig.getMaxSize());
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
  {
    List<TaskRunnerWorkItem> result = new ArrayList<>();
    for (Pod existingTask : client.listPeonPods()) {
      try {
        Task task = adapter.toTask(existingTask);
        ListenableFuture<TaskStatus> future = run(task);
        result.add(new K8sWorkItem(
            client,
            task,
            future,
            DateTimes.of(existingTask.getMetadata().getCreationTimestamp())
        ));
      }
      catch (IOException e) {
        log.error("Error deserializing task from pod: " + existingTask.getMetadata().getName());
      }

    }
    return result;
  }


  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.absent();
  }

  @Override
  public Map<String, Long> getIdleTaskSlotCount()
  {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Long> getUsedTaskSlotCount()
  {
    return Collections.emptyMap();
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
  public boolean isK8sTaskRunner()
  {
    return true;
  }

  @Override
  public void unregisterListener(String listenerId)
  {
    for (Pair<TaskRunnerListener, Executor> pair : listeners) {
      if (pair.lhs != null && pair.lhs.getListenerId().equals(listenerId)) {
        listeners.remove(pair);
        log.debug("Unregistered listener [%s]", listenerId);
        return;
      }
    }
  }

  @Override
  public void registerListener(TaskRunnerListener listener, Executor executor)
  {
    for (Pair<TaskRunnerListener, Executor> pair : listeners) {
      if (pair.lhs != null && pair.lhs.getListenerId().equals(listener.getListenerId())) {
        throw new ISE("Listener [%s] already registered", listener.getListenerId());
      }
    }

    final Pair<TaskRunnerListener, Executor> listenerPair = Pair.of(listener, executor);
    log.debug("Registered listener [%s]", listener.getListenerId());
    listeners.add(listenerPair);
  }

  @Override
  @SuppressForbidden(reason = "Sets#newHashSet")
  public Collection<TaskRunnerWorkItem> getRunningTasks()
  {
    List<TaskRunnerWorkItem> result = new ArrayList<>();
    for (Pod existingTask : client.listPeonPods(Sets.newHashSet(PeonPhase.RUNNING))) {
      try {
        Task task = adapter.toTask(existingTask);
        ListenableFuture<TaskStatus> future = run(task);
        result.add(new K8sWorkItem(
            client,
            task,
            future,
            DateTime.parse(existingTask.getMetadata().getCreationTimestamp())
        ));
      }
      catch (IOException e) {
        log.error("Error deserializing task from pod: " + existingTask.getMetadata().getName());
      }
    }
    return result;
  }

  @Override
  public Collection<TaskRunnerWorkItem> getPendingTasks()
  {
    // the task queue limits concurrent tasks, we fire off to k8s right away
    // thus nothing is really "pending"
    return new ArrayList<>();
  }

  @Nullable
  @Override
  public RunnerTaskState getRunnerTaskState(String taskId)
  {
    Pod item = client.getMainJobPod(new K8sTaskId(taskId));
    if (item == null) {
      return null;
    } else {
      PeonPhase phase = PeonPhase.getPhaseFor(item);
      switch (phase) {
        case PENDING:
          return RunnerTaskState.PENDING;
        case RUNNING:
          return RunnerTaskState.RUNNING;
        default:
          return RunnerTaskState.WAITING;
      }
    }
  }

  public static class K8sWorkItem extends TaskRunnerWorkItem
  {
    private final Task task;
    private KubernetesPeonClient client;

    public K8sWorkItem(KubernetesPeonClient client, Task task, ListenableFuture<TaskStatus> statusFuture)
    {
      super(task.getId(), statusFuture);
      this.task = task;
      this.client = client;
    }

    public K8sWorkItem(
        KubernetesPeonClient client,
        Task task,
        ListenableFuture<TaskStatus> statusFuture,
        DateTime createdTime
    )
    {
      super(task.getId(), statusFuture, createdTime, createdTime);
      this.task = task;
      this.client = client;
    }

    @Override
    public TaskLocation getLocation()
    {
      K8sTaskId taskId = new K8sTaskId(task.getId());
      try {
        Pod mainPod = client.getMainJobPod(new K8sTaskId(task.getId()));
        if (mainPod.getStatus() == null || mainPod.getStatus().getPodIP() == null) {
          return TaskLocation.unknown();
        }
        boolean tlsEnabled = Boolean.parseBoolean(
            mainPod.getMetadata()
                   .getAnnotations()
                   .getOrDefault(DruidK8sConstants.TLS_ENABLED, "false"));
        return TaskLocation.create(
            mainPod.getStatus().getPodIP(),
            DruidK8sConstants.PORT,
            DruidK8sConstants.TLS_PORT,
            tlsEnabled
        );
      }
      catch (KubernetesResourceNotFoundException e) {
        log.debug(e, "Error getting task location for task %s", taskId);
        return TaskLocation.unknown();
      }
      catch (Exception e) {
        log.error(e, "Error getting task location for task %s", taskId);
        return TaskLocation.unknown();
      }
    }

    @Override
    public String getTaskType()
    {
      return task.getType();
    }

    @Override
    public String getDataSource()
    {
      return task.getDataSource();
    }
  }

}
