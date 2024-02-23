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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerUtils;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.apache.druid.k8s.overlord.common.KubernetesPeonClient;
import org.apache.druid.k8s.overlord.taskadapter.TaskAdapter;
import org.apache.druid.tasklogs.TaskLogStreamer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
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
import java.util.stream.Collectors;

/**
 * Runs tasks as k8s jobs using the "internal peon" verb.
 * The KubernetesTaskRunner runs tasks by transforming the task spec into a K8s Job spec based
 * on the TaskAdapter it is configured with. The KubernetesTaskRunner has a pool of threads
 * (configurable with the capacity configuration) to track the jobs (1 thread tracks 1 job).
 *
 * Each thread calls down to the KubernetesPeonLifecycle class to submit the Job to K8s and then
 * waits for the lifecycle class to report back with the Job's status (success/failure).
 *
 * If there are not enough threads in the thread pool to execute and wait for a job, then the
 * task is put in a queue and left in WAITING state until another task completes.
 *
 * When the KubernetesTaskRunner comes up it attempts to restore its internal mapping of tasks
 * from Kubernetes by listing running jobs and calling join on each job, which spawns a thread to
 * wait for the fabric8 client library to report back, similar to what happens when a new
 * job is run.
 */
public class KubernetesTaskRunner implements TaskLogStreamer, TaskRunner
{
  private static final EmittingLogger log = new EmittingLogger(KubernetesTaskRunner.class);
  private final CopyOnWriteArrayList<Pair<TaskRunnerListener, Executor>> listeners = new CopyOnWriteArrayList<>();

  // to cleanup old jobs that might not have been deleted.
  private final ScheduledExecutorService cleanupExecutor;

  protected final ConcurrentHashMap<String, KubernetesWorkItem> tasks = new ConcurrentHashMap<>();
  protected final TaskAdapter adapter;

  private final KubernetesPeonClient client;
  private final KubernetesTaskRunnerConfig config;
  private final ListeningExecutorService exec;
  private final HttpClient httpClient;
  private final PeonLifecycleFactory peonLifecycleFactory;
  private final ServiceEmitter emitter;
  // currently worker categories aren't supported, so it's hardcoded.
  protected static final String WORKER_CATEGORY = "_k8s_worker_category";

  public KubernetesTaskRunner(
      TaskAdapter adapter,
      KubernetesTaskRunnerConfig config,
      KubernetesPeonClient client,
      HttpClient httpClient,
      PeonLifecycleFactory peonLifecycleFactory,
      ServiceEmitter emitter
  )
  {
    this.adapter = adapter;
    this.config = config;
    this.client = client;
    this.httpClient = httpClient;
    this.peonLifecycleFactory = peonLifecycleFactory;
    this.cleanupExecutor = Executors.newScheduledThreadPool(1);
    this.exec = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(config.getCapacity(), "k8s-task-runner-%d")
    );
    this.emitter = emitter;
  }

  @Override
  public Optional<InputStream> streamTaskLog(String taskid, long offset)
  {
    KubernetesWorkItem workItem = tasks.get(taskid);
    if (workItem == null) {
      return Optional.absent();
    }
    return workItem.streamTaskLogs();
  }

  @Override
  public ListenableFuture<TaskStatus> run(Task task)
  {
    synchronized (tasks) {
      return tasks.computeIfAbsent(task.getId(), k -> new KubernetesWorkItem(task, exec.submit(() -> runTask(task))))
                  .getResult();
    }
  }

  protected ListenableFuture<TaskStatus> joinAsync(Task task)
  {
    synchronized (tasks) {
      return tasks.computeIfAbsent(task.getId(), k -> new KubernetesWorkItem(task, exec.submit(() -> joinTask(task))))
                  .getResult();
    }
  }

  private TaskStatus runTask(Task task)
  {
    return doTask(task, true);
  }

  private TaskStatus joinTask(Task task)
  {
    return doTask(task, false);
  }

  @VisibleForTesting
  protected TaskStatus doTask(Task task, boolean run)
  {
    try {
      KubernetesPeonLifecycle peonLifecycle = peonLifecycleFactory.build(
          task,
          this::emitTaskStateMetrics
      );

      synchronized (tasks) {
        KubernetesWorkItem workItem = tasks.get(task.getId());

        if (workItem == null) {
          throw new ISE("Task [%s] has been shut down", task.getId());
        }

        workItem.setKubernetesPeonLifecycle(peonLifecycle);
      }

      TaskStatus taskStatus;
      if (run) {
        taskStatus = peonLifecycle.run(
            adapter.fromTask(task),
            config.getTaskLaunchTimeout().toStandardDuration().getMillis(),
            config.getTaskTimeout().toStandardDuration().getMillis(),
            adapter.shouldUseDeepStorageForTaskPayload(task)
        );
      } else {
        taskStatus = peonLifecycle.join(
            config.getTaskTimeout().toStandardDuration().getMillis()
        );
      }

      updateStatus(task, taskStatus);

      return taskStatus;
    }
    catch (Exception e) {
      log.error(e, "Task [%s] execution caught an exception", task.getId());
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  protected void emitTaskStateMetrics(KubernetesPeonLifecycle.State state, String taskId)
  {
    switch (state) {
      case RUNNING:
        KubernetesWorkItem workItem;
        synchronized (tasks) {
          workItem = tasks.get(taskId);
          if (workItem == null) {
            log.warn("Task [%s] disappeared. This could happen if the task was canceled or if it crashed.", taskId);
            return;
          }
        }
        ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
        IndexTaskUtils.setTaskDimensions(metricBuilder, workItem.getTask());
        emitter.emit(
            metricBuilder.setMetric(
                "task/pending/time",
                new Duration(workItem.getCreatedTime(), DateTimes.nowUtc()).getMillis()
            )
        );
      default:
        // ignore other state transition now
        return;
    }
  }

  @Override
  public void updateStatus(Task task, TaskStatus status)
  {
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
    log.info("Shutdown [%s] because [%s]", taskid, reason);

    KubernetesWorkItem workItem = tasks.get(taskid);

    if (workItem == null) {
      log.info("Ignoring request to cancel unknown task [%s]", taskid);
      return;
    }

    synchronized (tasks) {
      tasks.remove(taskid);
    }

    workItem.shutdown();
  }

  @Override
  public Optional<InputStream> streamTaskReports(String taskid) throws IOException
  {
    final KubernetesWorkItem workItem = tasks.get(taskid);
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
  @LifecycleStart
  public void start()
  {
    log.info("Starting K8sTaskRunner...");
    // Load tasks from previously running jobs and wait for their statuses to be updated asynchronously.
    for (Job job : client.getPeonJobs()) {
      try {
        joinAsync(adapter.toTask(job));
      }
      catch (IOException e) {
        log.error(e, "Error deserializing task from job [%s]", job.getMetadata().getName());
      }
    }
    log.info("Loaded %,d tasks from previous run", tasks.size());

    cleanupExecutor.scheduleAtFixedRate(
        () ->
            client.deleteCompletedPeonJobsOlderThan(
                config.getTaskCleanupDelay().toStandardDuration().getMillis(),
                TimeUnit.MILLISECONDS
            ),
        1,
        config.getTaskCleanupInterval().toStandardDuration().getMillis(),
        TimeUnit.MILLISECONDS
    );
    log.debug("Started cleanup executor for jobs older than %s...", config.getTaskCleanupDelay());
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    log.debug("Stopping KubernetesTaskRunner");
    cleanupExecutor.shutdownNow();
    log.debug("Stopped KubernetesTaskRunner");
  }

  @Override
  public Map<String, Long> getTotalTaskSlotCount()
  {
    return ImmutableMap.of(WORKER_CATEGORY, (long) config.getCapacity());
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
  {
    return Lists.newArrayList(tasks.values());
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.absent();
  }

  @Override
  public Map<String, Long> getIdleTaskSlotCount()
  {
    return ImmutableMap.of(WORKER_CATEGORY, (long) Math.max(0, config.getCapacity() - tasks.size()));
  }

  @Override
  public Map<String, Long> getUsedTaskSlotCount()
  {
    return ImmutableMap.of(WORKER_CATEGORY, (long) Math.min(config.getCapacity(), tasks.size()));
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
  public Collection<TaskRunnerWorkItem> getRunningTasks()
  {
    return tasks.values()
                .stream()
                .filter(KubernetesWorkItem::isRunning)
                .collect(Collectors.toList());
  }

  @Override
  public Collection<TaskRunnerWorkItem> getPendingTasks()
  {
    return tasks.values()
                .stream()
                .filter(KubernetesWorkItem::isPending)
                .collect(Collectors.toList());
  }

  @Override
  public TaskLocation getTaskLocation(String taskId)
  {
    final KubernetesWorkItem workItem = tasks.get(taskId);
    if (workItem == null) {
      return TaskLocation.unknown();
    } else {
      return workItem.getLocation();
    }
  }

  @Nullable
  @Override
  public RunnerTaskState getRunnerTaskState(String taskId)
  {
    KubernetesWorkItem workItem = tasks.get(taskId);
    if (workItem == null) {
      return null;
    }

    return workItem.getRunnerTaskState();
  }

  @Override
  public int getTotalCapacity()
  {
    return config.getCapacity();
  }

  @Override
  public int getUsedCapacity()
  {
    return tasks.size();
  }
}
