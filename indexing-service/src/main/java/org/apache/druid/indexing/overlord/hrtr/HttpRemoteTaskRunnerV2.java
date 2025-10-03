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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.WorkerNodeService;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.RemoteTaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerUtils;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningService;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningStrategy;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.indexing.overlord.config.HttpRemoteTaskRunnerConfig;
import org.apache.druid.indexing.overlord.config.WorkerTaskRunnerConfig;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.tasklogs.TaskLogStreamer;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * V2 rewrite of HttpRemoteTaskRunner with improved concurrency and priority-based task assignment.
 * 
 * Key improvements:
 * - Uses ReadWriteLock instead of giant statusLock for better concurrency
 * - Separates concerns with different locks for different data structures
 * - Implements priority-based task assignment with PriorityQueue
 * - Isolated codepaths for better throughput
 * 
 * This class manages tasks on Middle Manager nodes using HTTP endpoints:
 * 1. POST request for assigning a task
 * 2. POST request for shutting down a task 
 * 3. GET request for getting list of assigned/running/completed tasks with long poll support
 */
@ManageLifecycle
public class HttpRemoteTaskRunnerV2 implements WorkerTaskRunner, TaskLogStreamer, WorkerHolder.Listener
{
  private static final EmittingLogger log = new EmittingLogger(HttpRemoteTaskRunnerV2.class);
  private static final ObjectMapper jsonMapper = new ObjectMapper();

  // Core Configuration
  private final HttpRemoteTaskRunnerConfig config;
  private final HttpClient httpClient;
  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private final Supplier<WorkerBehaviorConfig> workerConfigRef;
  private final ProvisioningStrategy<WorkerTaskRunner> provisioningStrategy;
  private ProvisioningService provisioningService;
  private final TaskStorage taskStorage;
  private final ServiceEmitter emitter;

  // Priority-based task scheduling queue - higher priority values processed first
  private final PriorityBlockingQueue<Task> pendingTasks = new PriorityBlockingQueue<>();

  // Task structures
  private final ConcurrentHashMap<String, HttpRemoteTaskRunnerWorkItemV2> tasks = new ConcurrentHashMap<>();
  private final ReadWriteLock taskLock = new ReentrantReadWriteLock(true);

  // Worker structures ("host:port" -> WorkerHolder)
  private final ConcurrentHashMap<String, WorkerHolder> workers = new ConcurrentHashMap<>();
  private final ReadWriteLock workerLock = new ReentrantReadWriteLock(true);

  // Executors
  private final ExecutorService pendingTasksExec;
  private final ScheduledExecutorService workersSyncExec;
  private final ListeningScheduledExecutorService cleanupExec;
  
  // Lifecycle
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  
  // Listeners
  private final CopyOnWriteArrayList<Pair<TaskRunnerListener, Executor>> listeners = new CopyOnWriteArrayList<>();

  // Node discovery
  private volatile DruidNodeDiscovery.Listener nodeDiscoveryListener;

  public HttpRemoteTaskRunnerV2(
      HttpRemoteTaskRunnerConfig config,
      HttpClient httpClient,
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      ProvisioningStrategy<WorkerTaskRunner> provisioningStrategy,
      DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      TaskStorage taskStorage,
      ServiceEmitter emitter
  )
  {
    this.config = config;
    this.httpClient = httpClient;
    this.workerConfigRef = workerConfigRef;
    this.provisioningStrategy = provisioningStrategy;
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.taskStorage = taskStorage;
    this.emitter = emitter;

    this.pendingTasksExec = Execs.multiThreaded(
        config.getPendingTasksRunnerNumThreads(),
        "HttpRemoteTaskRunnerV2-pending-tasks-%d"
    );

    this.workersSyncExec = ScheduledExecutors.fixed(
        config.getWorkerSyncNumThreads(),
        "HttpRemoteTaskRunnerV2-worker-sync-%d"
    );

    this.cleanupExec = MoreExecutors.listeningDecorator(
        ScheduledExecutors.fixed(1, "HttpRemoteTaskRunnerV2-Worker-Cleanup-%d")
    );
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    return List.of();
  }

  @Override
  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      return;
    }

    try {
      log.info("Starting...");

      // Start worker discovery
      startWorkerDiscovery();

      // Start pending task execution loop
      startPendingTaskHandling();

      // Start cleanup task
      ScheduledExecutors.scheduleAtFixedRate(
          cleanupExec,
          Period.ZERO.toStandardDuration(),
          config.getWorkerBlackListCleanupPeriod().toStandardDuration(),
          this::checkAndRemoveWorkersFromBlackList
      );

      provisioningService = provisioningStrategy.makeProvisioningService(this);

      lifecycleLock.started();

      log.info("Started.");
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @Override
  public void registerListener(TaskRunnerListener listener, Executor executor)
  {
    for (Pair<TaskRunnerListener, Executor> pair : listeners) {
      if (pair.lhs.getListenerId().equals(listener.getListenerId())) {
        throw new ISE("Listener [%s] already registered", listener.getListenerId());
      }
    }

    final Pair<TaskRunnerListener, Executor> listenerPair = Pair.of(listener, executor);

    taskLock.writeLock().lock();
    try {
      for (Map.Entry<String, HttpRemoteTaskRunnerWorkItemV2> entry : tasks.entrySet()) {
        if (entry.getValue().getState() == HttpRemoteTaskRunnerWorkItemV2.State.RUNNING) {
          TaskRunnerUtils.notifyLocationChanged(
              ImmutableList.of(listenerPair),
              entry.getKey(),
              entry.getValue().getLocation()
          );
        }
      }

      log.info("Registered listener [%s]", listener.getListenerId());
      listeners.add(listenerPair);
    } finally {
      taskLock.writeLock().unlock();
    }
  }

  @Override
  public void unregisterListener(String listenerId)
  {
    for (Pair<TaskRunnerListener, Executor> pair : listeners) {
      if (pair.lhs.getListenerId().equals(listenerId)) {
        listeners.remove(pair);
        log.info("Unregistered listener [%s]", listenerId);
        return;
      }
    }
  }

  @Override
  public ListenableFuture<TaskStatus> run(Task task)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS), "TaskRunner not started.");

    AtomicReference<ListenableFuture<TaskStatus>> taskFuture = new AtomicReference<>();

    taskLock.readLock().lock();
    try {
      tasks.compute(task.getId(), (id, entry) -> {
        // Task already exists, but in case it was discovered from a worker on start()
        // and TaskAnnouncement does not have Task instance, add it.
        if (entry != null) {
          if (entry.getTask() == null) {
            entry.setTask(task);
          }
        } else {
          entry = new HttpRemoteTaskRunnerWorkItemV2(
              task.getId(),
              null,
              null,
              task,
              task.getType(),
              HttpRemoteTaskRunnerWorkItemV2.State.PENDING
           );
           pendingTasks.offer(task); // TODO: make Task::getPriority + time-based order the priority
         }

        taskFuture.set(entry.getResult());
        return entry;
      });
    } finally {
      taskLock.readLock().unlock();
    }
    return taskFuture.get();
  }

  @Override
  public void shutdown(String taskId, String reason)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS), "TaskRunner not started.");

    AtomicReference<WorkerHolder> workerHolderRunningTask = new AtomicReference<>();

    workerLock.readLock().lock();
    taskLock.readLock().lock();
    try {
      tasks.compute(taskId, (key, entry) -> {
        if (entry != null) {
          switch (entry.getState()) {
            case RUNNING:
              final WorkerHolder workerHolder = workers.get(entry.getWorker().getHost());
              if (workerHolder == null) {
                log.info("Can't shutdown! No worker running task[%s].", taskId);
              } else {
                workerHolderRunningTask.set(workerHolder);
              }
              break;
            case COMPLETE:
              log.info("Can't shutdown! Task[%s] already complete.", taskId);
              entry = null; // remove task entry from map
              break;
            default:
              // TODO: are there any other states here that we can handle better?
              log.info("Task[%s] in state[%s]. Skipping shutdown.", taskId, entry.getState());
              break;
          }
        } else {
          log.info("Received shutdown task[%s], but can't find it. Ignored.", taskId);
        }
        return entry;
      });
    } finally {
      taskLock.readLock().unlock();
      workerLock.readLock().unlock();
    }

    if (workerHolderRunningTask.get() != null) {
      log.debug(
          "Got shutdown request for task[%s]. Asking worker[%s] to kill it.",
          taskId,
          workerHolderRunningTask.get().getWorker().getHost()
      );
      workerHolderRunningTask.get().shutdownTask(taskId);
    }
  }

  @Override
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("Can't stop TaskRunner.");
    }

    try {
      log.info("Stopping HttpRemoteTaskRunnerV2");

      // Stop worker discovery
      if (nodeDiscoveryListener != null) {
        DruidNodeDiscovery druidNodeDiscovery = druidNodeDiscoveryProvider.getForService(WorkerNodeService.DISCOVERY_SERVICE_KEY);
        druidNodeDiscovery.removeListener(nodeDiscoveryListener);
      }

      // Shutdown executors
      pendingTasksExec.shutdown();
      workersSyncExec.shutdown();
      cleanupExec.shutdown();

      // Stop all worker holders
      workerLock.writeLock().lock();
      try {
        for (WorkerHolder workerHolder : workers.values()) {
          workerHolder.stop();
        }
        workers.clear();
      } finally {
        workerLock.writeLock().unlock();
      }

      lifecycleLock.exitStop();

      log.info("HttpRemoteTaskRunnerV2 stopped");
    }
    catch (Exception e) {
      log.error(e, "Error stopping HttpRemoteTaskRunnerV2");
      throw new RuntimeException(e);
    }
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getRunningTasks()
  {
    taskLock.readLock().lock();
    try {
      return tasks.values().stream()
          .filter(workItem -> workItem.getState() == HttpRemoteTaskRunnerWorkItemV2.State.RUNNING)
          .collect(Collectors.toList());
    } finally {
      taskLock.readLock().unlock();
    }
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
  {
    taskLock.readLock().lock();
    try {
      return tasks.values().stream()
          .filter(workItem -> workItem.getState().isPending())
          .collect(Collectors.toList());
    } finally {
      taskLock.readLock().unlock();
    }
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
  {
    taskLock.readLock().lock();
    try {
      return List.copyOf(tasks.values());
    } finally {
      taskLock.readLock().unlock();
    }
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.fromNullable(provisioningService != null ? provisioningService.getStats() : null);
  }

  @Override
  public Map<String, Long> getTotalTaskSlotCount()
  {
    workerLock.readLock().lock();
    try {
      long totalCapacity = workers.values().stream()
          .filter(WorkerHolder::isInitialized)
          .mapToLong(workerHolder -> workerHolder.getWorker().getCapacity())
          .sum();
      return Map.of("total", totalCapacity);
    } finally {
      workerLock.readLock().unlock();
    }
  }

  @Override
  public Map<String, Long> getIdleTaskSlotCount()
  {
    workerLock.readLock().lock();
    taskLock.readLock().lock();
    try {
      long idleCapacity = workers.values().stream()
          .filter(WorkerHolder::isInitialized)
          .filter(WorkerHolder::isEnabled)
          .filter(w -> isWorkerInState(w, WorkerHolder.WorkerState.READY))
          .mapToLong(workerHolder -> Math.max(0,
              workerHolder.getWorker().getCapacity() - getRunningTasksOnWorker(workerHolder.getWorker().getHost())))
          .sum();
      return Map.of("idle", idleCapacity);
    } finally {
      taskLock.readLock().unlock();
      workerLock.readLock().unlock();
    }
  }

  @Override
  public Map<String, Long> getUsedTaskSlotCount()
  {
    workerLock.readLock().lock();
    taskLock.readLock().lock();
    try {
      long usedCapacity = workers.values().stream()
          .filter(WorkerHolder::isInitialized)
          .mapToLong(workerHolder -> getRunningTasksOnWorker(workerHolder.getWorker().getHost()))
          .sum();
      return Map.of("used", usedCapacity);
    } finally {
      taskLock.readLock().unlock();
      workerLock.readLock().unlock();
    }
  }

  @Override
  public Map<String, Long> getLazyTaskSlotCount()
  {
    workerLock.readLock().lock();
    try {
      long lazyCapacity = workers.values().stream()
          .filter(w -> isWorkerInState(w, WorkerHolder.WorkerState.LAZY))
          .mapToLong(workerHolder -> workerHolder.getWorker().getCapacity())
          .sum();
      return Map.of("lazy", lazyCapacity);
    } finally {
      workerLock.readLock().unlock();
    }
  }

  @Override
  public Map<String, Long> getBlacklistedTaskSlotCount()
  {
    workerLock.writeLock().lock();
    try {
      long blacklistedCapacity = workers.values().stream()
          .filter(w -> isWorkerInState(w, WorkerHolder.WorkerState.BLACKLISTED))
          .mapToLong(workerHolder -> workerHolder.getWorker().getCapacity())
          .sum();
      return Map.of("blacklisted", blacklistedCapacity);
    } finally {
      workerLock.writeLock().unlock();
    }
  }

  @Override
  public Collection<ImmutableWorkerInfo> getWorkers()
  {
    workerLock.readLock().lock();
    try {
      return workers.values().stream()
          .filter(workerHolder -> workerHolder.isInitialized())
          .map(WorkerHolder::toImmutable)
          .collect(Collectors.toList());
    } finally {
      workerLock.readLock().unlock();
    }
  }

  @Override
  public Collection<Worker> getLazyWorkers()
  {
    workerLock.readLock().lock();
    try {
      return workers.values().stream()
          .filter(workerHolder -> isWorkerInState(workerHolder, WorkerHolder.WorkerState.LAZY))
          .map(WorkerHolder::getWorker)
          .collect(Collectors.toList());
    } finally {
      workerLock.readLock().unlock();
    }
  }

  @Override
  public Collection<Worker> markWorkersLazy(Predicate<ImmutableWorkerInfo> isLazyWorker, int maxLazyWorkers)
  {
    workerLock.writeLock().lock();
    try {
      long currentLazyCount = workers.values().stream()
          .filter(w -> isWorkerInState(w, WorkerHolder.WorkerState.LAZY))
          .count();

      if (currentLazyCount >= maxLazyWorkers) {
        return getLazyWorkersUnsafe();
      }

      int additionalLazyNeeded = (int) (maxLazyWorkers - currentLazyCount);
      int marked = 0;

      for (WorkerHolder workerHolder : workers.values()) {
        if (marked >= additionalLazyNeeded) {
          break;
        }

        if (isWorkerInState(workerHolder, WorkerHolder.WorkerState.READY) &&
            isLazyWorker.apply(workerHolder.toImmutable()) &&
            isWorkerOkForMarkingLazy(workerHolder)) {
          
          setWorkerState(workerHolder, WorkerHolder.WorkerState.LAZY);
          marked++;
          log.info("Marked worker[%s] as lazy", workerHolder.getWorker().getHost());
        }
      }

      return getLazyWorkersUnsafe();
    } finally {
      workerLock.writeLock().unlock();
    }
  }

  @Override
  public WorkerTaskRunnerConfig getConfig()
  {
    return config;
  }

  @Override
  public Collection<Task> getPendingTaskPayloads()
  {
    // PriorityBlockingQueue is thread-safe for iteration
    return List.copyOf(pendingTasks);
  }

  @Override
  public void taskAddedOrUpdated(TaskAnnouncement announcement, WorkerHolder workerHolder)
  {
    final String taskId = announcement.getTaskId();
    final Worker worker = workerHolder.getWorker();

    log.debug(
        "Worker[%s] wrote status[%s] for task[%s] on location[%s].",
        worker.getHost(),
        announcement.getTaskStatus().getStatusCode(),
        taskId,
        announcement.getTaskLocation()
    );

    final AtomicReference<HttpRemoteTaskRunnerWorkItemV2> taskItem = new AtomicReference<>();
    final AtomicBoolean shouldShutdownTask = new AtomicBoolean(false);
    final AtomicBoolean isTaskCompleted = new AtomicBoolean(false);

    taskLock.readLock().lock();
    try {
      tasks.compute(taskId, (key, entry) -> {
        if (entry == null) {
          Optional<TaskStatus> knownStatusInStorage = taskStorage.getStatus(taskId);

          if (knownStatusInStorage.isPresent()) {
            switch (knownStatusInStorage.get().getStatusCode()) {
              case RUNNING:
                entry = new HttpRemoteTaskRunnerWorkItemV2(
                    taskId,
                    worker,
                    TaskLocation.unknown(),
                    null,
                    announcement.getTaskType(),
                    HttpRemoteTaskRunnerWorkItemV2.State.RUNNING
                );
                break;
              case SUCCESS:
              case FAILED:
                if (!announcement.getTaskStatus().isComplete()) {
                  log.info(
                      "Worker[%s] reported status for completed, known from taskStorage, task[%s]. Ignored.",
                      worker.getHost(),
                      taskId
                  );
                }
                break;
              default:
                log.makeAlert(
                    "Found unrecognized state[%s] of task[%s] in taskStorage. Notification[%s] from worker[%s] is ignored.",
                    knownStatusInStorage.get().getStatusCode(),
                    taskId,
                    announcement,
                    worker.getHost()
                ).emit();
            }
          } else {
            log.warn(
                "Worker[%s] reported status[%s] for unknown task[%s]. Ignored.",
                worker.getHost(),
                announcement.getStatus(),
                taskId
            );
          }
        }

        if (entry == null) {
          if (!announcement.getTaskStatus().isComplete()) {
            shouldShutdownTask.set(true);
          }
        } else {
          switch (announcement.getTaskStatus().getStatusCode()) {
            case RUNNING:
              switch (entry.getState()) {
                case PENDING:
                  // TODO: maybe emit a task/waiting/time metric here?
                case PENDING_WORKER_ASSIGN:
                  entry.setWorker(worker);
                  entry.setState(HttpRemoteTaskRunnerWorkItemV2.State.RUNNING);
                  log.info("Task[%s] started RUNNING on worker[%s].", taskId, worker.getHost());

                  final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
                  IndexTaskUtils.setTaskDimensions(metricBuilder, entry.getTask());
                  emitter.emit(metricBuilder.setMetric(
                      "task/pending/time",
                      new Duration(entry.getCreatedTime(), DateTimes.nowUtc()).getMillis())
                  );
                case RUNNING:
                  if (worker.getHost().equals(entry.getWorker().getHost())) {
                    if (!announcement.getTaskLocation().equals(entry.getLocation())) {
                      log.info(
                          "Task[%s] location changed on worker[%s]. new location[%s].",
                          taskId,
                          worker.getHost(),
                          announcement.getTaskLocation()
                      );
                      entry.setLocation(announcement.getTaskLocation());
                      TaskRunnerUtils.notifyLocationChanged(listeners, taskId, announcement.getTaskLocation());
                    }
                  } else {
                    log.warn(
                        "Found worker[%s] running task[%s] which is being run by another worker[%s]. Notification ignored.",
                        worker.getHost(),
                        taskId,
                        entry.getWorker().getHost()
                    );
                    shouldShutdownTask.set(true);
                  }
                  break;
                case COMPLETE:
                  log.warn(
                      "Worker[%s] reported status for completed task[%s]. Ignored.",
                      worker.getHost(),
                      taskId
                  );
                  shouldShutdownTask.set(true);
                  break;
                default:
                  log.makeAlert(
                      "Found unrecognized state[%s] of task[%s]. Notification[%s] from worker[%s] is ignored.",
                      entry.getState(),
                      taskId,
                      announcement,
                      worker.getHost()
                  ).emit();
              }
              break;
            case FAILED:
            case SUCCESS:
              switch (entry.getState()) {
                case PENDING:
                case PENDING_WORKER_ASSIGN:
                  entry.setWorker(worker);
                  entry.setState(HttpRemoteTaskRunnerWorkItemV2.State.RUNNING);
                  log.info("Task[%s] finished on worker[%s].", taskId, worker.getHost());
                  // fall through
                case RUNNING:
                  if (worker.getHost().equals(entry.getWorker().getHost())) {
                    if (!announcement.getTaskLocation().equals(entry.getLocation())) {
                      log.info(
                          "Task[%s] location changed on worker[%s]. new location[%s].",
                          taskId,
                          worker.getHost(),
                          announcement.getTaskLocation()
                      );
                      entry.setLocation(announcement.getTaskLocation());
                      TaskRunnerUtils.notifyLocationChanged(listeners, taskId, announcement.getTaskLocation());
                    }

                    isTaskCompleted.set(true);
                  } else {
                    log.warn(
                        "Worker[%s] reported completed task[%s] which is being run by another worker[%s]. Notification ignored.",
                        worker.getHost(),
                        taskId,
                        entry.getWorker().getHost()
                    );
                  }
                  break;
                case COMPLETE:
                  // this can happen when a worker is restarted and reports its list of completed tasks again.
                  break;
                default:
                  log.makeAlert(
                      "Found unrecognized state[%s] of task[%s]. Notification[%s] from worker[%s] is ignored.",
                      entry.getState(),
                      taskId,
                      announcement,
                      worker.getHost()
                  ).emit();
              }
              break;
            default:
              log.makeAlert(
                  "Worker[%s] reported unrecognized state[%s] for task[%s].",
                  worker.getHost(),
                  announcement.getTaskStatus().getStatusCode(),
                  taskId
              ).emit();
          }
        }

        taskItem.set(entry);
        return entry;
      });
    }
    finally {
      taskLock.readLock().unlock();
    }

    if (isTaskCompleted.get()) {
      taskComplete(taskItem.get(), workerHolder, announcement.getTaskStatus());
    }

    if (shouldShutdownTask.get()) {
      log.warn("Killing task[%s] on worker[%s].", taskId, worker.getHost());
      workerHolder.shutdownTask(taskId);
    }

    // TODO: setup signalling
  }

  private void taskComplete(HttpRemoteTaskRunnerWorkItemV2 taskItem, WorkerHolder workerHolder, TaskStatus taskStatus)
  {
    Preconditions.checkNotNull(taskItem, "taskRunnerWorkItem");
    Preconditions.checkNotNull(taskStatus, "taskStatus");
    if (workerHolder != null) {
      log.info(
          "Worker[%s] completed task[%s] with status[%s]",
          workerHolder.getWorker().getHost(),
          taskStatus.getId(),
          taskStatus.getStatusCode()
      );
      // Worker is done with this task
      workerHolder.setLastCompletedTaskTime(DateTimes.nowUtc());
    }

    if (taskItem.getResult().isDone()) {
      // This is not the first complete event.
      try {
        TaskState lastKnownState = taskItem.getResult().get().getStatusCode();
        if (taskStatus.getStatusCode() != lastKnownState) {
          log.warn(
              "The state of the new task complete event is different from its last known state. "
              + "New state[%s], last known state[%s]",
              taskStatus.getStatusCode(),
              lastKnownState
          );
        }
      }
      catch (InterruptedException e) {
        log.warn(e, "Interrupted while getting the last known task status.");
        Thread.currentThread().interrupt();
      }
      catch (ExecutionException e) {
        // This case should not really happen.
        log.warn(e, "Failed to get the last known task status. Ignoring this failure.");
      }
    } else {
      // Notify interested parties
      taskItem.setResult(taskStatus);
      TaskRunnerUtils.notifyStatusChanged(listeners, taskStatus.getId(), taskStatus);

      // Update success/failure counters, Blacklist node if there are too many failures.
      if (workerHolder != null) {
        blacklistWorkerIfNeeded(taskStatus, workerHolder);
      }
    }
  }

  private void blacklistWorkerIfNeeded(TaskStatus taskStatus, WorkerHolder workerHolder) {
    workerLock.readLock().lock();
    try {
      workers.compute(workerHolder.getWorker().getHost(), (host, workerEntry) -> {
        if (workerEntry == null) {
          log.error("Asked to potentially blacklist unknown worker [%s].", workerHolder.getWorker().getHost());
        } else {
          if (taskStatus.isSuccess()) {
            workerEntry.resetContinuouslyFailedTasksCount();
            workerEntry.setBlacklistedUntil(null);
            workerEntry.setWorkerState(WorkerHolder.WorkerState.READY);
            log.info("Worker[%s] removed from blacklist because task[%s] finished with SUCCESS", taskStatus.getId(), workerHolder.getWorker());
          } else if (taskStatus.isFailure()) {
            workerEntry.incrementContinuouslyFailedTasksCount();
          }

          if (workerEntry.getContinuouslyFailedTasksCount() > config.getMaxRetriesBeforeBlacklist() && workers.size() <= workers.size() * (config.getMaxPercentageBlacklistWorkers() / 100.0) - 1) {
            workerEntry.setBlacklistedUntil(DateTimes.nowUtc().plus(config.getWorkerBlackListBackoffTime()));
            workerEntry.setWorkerState(WorkerHolder.WorkerState.BLACKLISTED);
            log.info(
                "Blacklisting worker[%s] until [%s] after [%,d] failed tasks in a row.",
                workerEntry.getWorker(),
                workerEntry.getBlacklistedUntil(),
                workerEntry.getContinuouslyFailedTasksCount()
            );
          }
        }
        return workerEntry;
      });
    } finally {
      workerLock.readLock().unlock();
    }
  }

  // Helper methods for worker state management
  private boolean isWorkerInState(WorkerHolder workerHolder, WorkerHolder.WorkerState state)
  {
    return workerHolder.getWorkerState() == state;
  }

  private void setWorkerState(WorkerHolder workerHolder, WorkerHolder.WorkerState state)
  {
    workerHolder.setWorkerState(state);
  }

  private Collection<Worker> getLazyWorkersUnsafe()
  {
    // Called while holding write lock
    return workers.values().stream()
        .filter(w -> isWorkerInState(w, WorkerHolder.WorkerState.LAZY))
        .map(WorkerHolder::getWorker)
        .collect(Collectors.toList());
  }

  private boolean isWorkerOkForMarkingLazy(WorkerHolder workerHolder)
  {
    String workerHost = workerHolder.getWorker().getHost();
    
    // Check if worker has any running tasks
    taskLock.readLock().lock();
    try {
      for (HttpRemoteTaskRunnerWorkItemV2 workItem : tasks.values()) {
        if (workItem.getWorker() != null && 
            workerHost.equals(workItem.getWorker().getHost()) && 
            workItem.getState() == HttpRemoteTaskRunnerWorkItemV2.State.RUNNING) {
          return false;
        }
      }
    } finally {
      taskLock.readLock().unlock();
    }
    
    return true;
  }

  private void checkAndRemoveWorkersFromBlackList()
  {
    workerLock.writeLock().lock();
    try {
      workers.values().stream()
          .filter(w -> isWorkerInState(w, WorkerHolder.WorkerState.BLACKLISTED))
          .forEach(workerHolder -> {
            // TODO: Implement blacklist timeout logic
            if (shouldRemoveFromBlacklist(workerHolder)) {
              setWorkerState(workerHolder, WorkerHolder.WorkerState.READY);
              log.info("Removed worker[%s] from blacklist", workerHolder.getWorker().getHost());
            }
          });
    } finally {
      workerLock.writeLock().unlock();
    }
  }

  private boolean shouldRemoveFromBlacklist(WorkerHolder workerHolder)
  {
    // TODO: Implement proper blacklist timeout logic
    return false;
  }

  private long getRunningTasksOnWorker(String workerHost)
  {
    // Called while holding taskLock.readLock()
    return tasks.values().stream()
        .filter(workItem -> workItem.getWorker() != null)
        .filter(workItem -> workerHost.equals(workItem.getWorker().getHost()))
        .filter(workItem -> workItem.getState() == HttpRemoteTaskRunnerWorkItemV2.State.RUNNING)
        .count();
  }

  private void startWorkerDiscovery()
  {
    DruidNodeDiscovery druidNodeDiscovery = druidNodeDiscoveryProvider.getForService(WorkerNodeService.DISCOVERY_SERVICE_KEY);
    
    this.nodeDiscoveryListener = new DruidNodeDiscovery.Listener()
    {
      @Override
      public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
      {
        nodes.forEach(node -> addWorker(toWorker(node)));
      }

      @Override
      public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
      {
        nodes.forEach(node -> removeWorker(toWorker(node)));
      }

      @Override
      public void nodeViewInitialized()
      {
        log.info("Worker discovery view initialized with [%d] workers", workers.size());
      }

      @Override
      public void nodeViewInitializedTimedOut()
      {
        log.warn("Worker discovery view initialization timed out");
      }
    };
    
    druidNodeDiscovery.registerListener(nodeDiscoveryListener);
  }

  private Worker toWorker(DiscoveryDruidNode node)
  {
    final WorkerNodeService workerNodeService = node.getService(WorkerNodeService.DISCOVERY_SERVICE_KEY, WorkerNodeService.class);
    if (workerNodeService == null) {
      // this shouldn't typically happen, but just in case it does, make a dummy worker to allow the callbacks to
      // continue since addWorker/removeWorker only need worker.getHost()
      return new Worker(
          node.getDruidNode().getServiceScheme(),
          node.getDruidNode().getHostAndPortToUse(),
          null,
          0,
          "",
          WorkerConfig.DEFAULT_CATEGORY
      );
    }
    return new Worker(
        node.getDruidNode().getServiceScheme(),
        node.getDruidNode().getHostAndPortToUse(),
        workerNodeService.getIp(),
        workerNodeService.getCapacity(),
        workerNodeService.getVersion(),
        workerNodeService.getCategory()
    );
  }

  private void addWorker(Worker worker)
  {
    log.info("Worker[%s] appeared", worker.getHost());
    
    workerLock.readLock().lock();
    taskLock.writeLock().lock();
    try {
        workers.compute(worker.getHost(), (host, workerEntry) -> {
        if (workerEntry != null) {
          log.warn("Worker[%s] already exists. Not adding again.", worker.getHost());
        } else {
          List<TaskAnnouncement> expectedAnnouncements = new ArrayList<>();
          // It might be a worker that existed before, temporarily went away and then came back. We might have a set of
          // tasks that we think are running on this worker. Provide that information to WorkerHolder that
          // manages the task syncing with that worker.
          for (Map.Entry<String, HttpRemoteTaskRunnerWorkItemV2> e : tasks.entrySet()) {
            if (e.getValue().getState() == HttpRemoteTaskRunnerWorkItemV2.State.RUNNING) {
              Worker w = e.getValue().getWorker();
              if (w != null && w.getHost().equals(worker.getHost()) && e.getValue().getTask() != null) {
                expectedAnnouncements.add(
                    TaskAnnouncement.create(
                        e.getValue().getTask(),
                        TaskStatus.running(e.getKey()),
                        e.getValue().getLocation()
                    )
                );
              }
            }
          }
          workerEntry = createWorkerHolder(
              jsonMapper,
              httpClient,
              config,
              workersSyncExec,
              this,
              worker,
              expectedAnnouncements
          );
          workerEntry.start();
        }
        return workerEntry;
      });
      // TODO: SIGNAL
    }
    finally {
      taskLock.writeLock().unlock();
      workerLock.readLock().unlock();
    }
  }

  private void removeWorker(Worker worker)
  {
    log.info("Worker[%s] disappeared", worker.getHost());
    
    workerLock.readLock().lock();
    try {
      WorkerHolder workerHolder = workers.remove(worker.getHost());
      if (workerHolder != null) {
        workerHolder.stop();
      }
    }
    finally {
      workerLock.readLock().unlock();
    }
  }

  private void startPendingTaskHandling()
  {
    for (int i = 0; i < config.getPendingTasksRunnerNumThreads(); i++) {
      pendingTasksExec.submit(this::pendingTasksExecutionLoop);
    }
  }

  private void pendingTasksExecutionLoop()
  {
    while (!Thread.interrupted() && lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
      try {
        // Block until a task is available
        Task task = pendingTasks.poll(1, TimeUnit.MINUTES);
        
        if (task == null) {
          continue; // Timeout, check conditions and continue
        }

        String taskId = task.getId();
        
        // Check if task still exists and is pending
        taskLock.readLock().lock();
        HttpRemoteTaskRunnerWorkItemV2 workItem;
        try {
          workItem = tasks.get(taskId);
          if (workItem == null || !workItem.getState().isPending()) {
            log.debug("Task[%s] is no longer PENDING, removed and skipped.", taskId);
            continue;
          }
        } finally {
          taskLock.readLock().unlock();
        }

        // Find worker to run task
        ImmutableWorkerInfo workerInfo = findWorkerToRunTask(task);
        
        if (workerInfo == null) {
          // No worker available, put task back and wait
          log.debug("No worker available for task[%s], putting back in queue", taskId);
          pendingTasks.offer(task);

          // TODO: setup signalling
          continue;
        }

        // Try to assign task to worker
        if (assignTaskToWorker(task, workerInfo)) {
          log.info("Successfully assigned task[%s] to worker[%s]", taskId, workerInfo.getWorker().getHost());
        } else {
          // Assignment failed, add back to queue
          log.warn("Failed to assign task[%s] to worker[%s], adding back to queue", taskId, workerInfo.getWorker().getHost());
          pendingTasks.offer(task);
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      catch (Exception e) {
        log.error(e, "Error in pending tasks execution loop");
      }
    }
    
    log.info("Pending tasks execution loop exited");
  }

  private ImmutableWorkerInfo findWorkerToRunTask(Task task)
  {
    WorkerBehaviorConfig workerConfig = workerConfigRef.get();
    if (workerConfig == null || workerConfig.getSelectStrategy() == null) {
      return null; // No strategy configured
    }

    return workerConfig.getSelectStrategy().findWorkerForTask(
        config,
        getWorkersEligibleToRunTasks(),
        task
    );
  }

  private ImmutableMap<String, ImmutableWorkerInfo> getWorkersEligibleToRunTasks()
  {
    workerLock.readLock().lock();
    try {
      return workers.entrySet().stream()
          .filter(entry -> {
            WorkerHolder workerHolder = entry.getValue();
            return workerHolder.isInitialized() &&
                   workerHolder.isEnabled() &&
                   isWorkerInState(workerHolder, WorkerHolder.WorkerState.READY);
          })
          .collect(ImmutableMap.toImmutableMap(
              Map.Entry::getKey,
              entry -> entry.getValue().toImmutable()
          ));
    } finally {
      workerLock.readLock().unlock();
    }
  }

  private boolean assignTaskToWorker(Task task, ImmutableWorkerInfo workerInfo)
  {
    String taskId = task.getId();
    String workerHost = workerInfo.getWorker().getHost();

    log.info("Assigning task[%s] to worker[%s]", taskId, workerHost);

    // Get worker holder with write lock to prevent race conditions
    WorkerHolder workerHolder;
    workerLock.writeLock().lock();
    try {
      workerHolder = workers.get(workerHost);
      if (workerHolder == null || !isWorkerInState(workerHolder, WorkerHolder.WorkerState.READY)) {
        log.info("Not assigning task[%s] to unavailable worker[%s]", taskId, workerHost);
        return false;
      }

      // Mark worker as having unacknowledged task
      setWorkerState(workerHolder, WorkerHolder.WorkerState.PENDING_ASSIGN);
    }
    finally {
      workerLock.writeLock().unlock();
    }

    try {
      // Update work item state
      taskLock.readLock().lock();
      try {
        tasks.compute(taskId, (key, entry) -> {
          if (entry != null) {
            entry.setState(HttpRemoteTaskRunnerWorkItemV2.State.PENDING_WORKER_ASSIGN);
            entry.setWorker(workerInfo.getWorker());
          }
          return entry;
        });
      } finally {
        taskLock.readLock().unlock();
      }

      // Attempt HTTP task assignment
      boolean success = workerHolder.assignTask(task);
      if (success) {
        // Task assignment successful, worker will transition state when acknowledged
        return true;
      } else {
        log.warn("Failed to assign task[%s] to worker[%s] via HTTP", taskId, workerHost);
        return false;
      }
    }
    finally {
      // TODO: SIGNAL READY
    }
  }

  @Override
  public void stateChanged(boolean enabled, WorkerHolder workerHolder)
  {
    log.info("Worker[%s] state changed to enabled=[%s]", workerHolder.getWorker().getHost(), enabled);
    
    if (enabled) {
      // TODO: SIGNAL
    }
  }

  @Override
  public Optional<InputStream> streamTaskLog(String taskId, long offset) throws IOException
  {
    HttpRemoteTaskRunnerWorkItemV2 workItem;
    taskLock.readLock().lock();
    try {
      workItem = tasks.get(taskId);
    } finally {
      taskLock.readLock().unlock();
    }

    if (workItem == null || workItem.getWorker() == null) {
      return Optional.absent();
    }

    workerLock.readLock().lock();
    try {
      WorkerHolder workerHolder = workers.get(workItem.getWorker().getHost());
      if (workerHolder == null) {
        return Optional.absent();
      }

      return workerHolder.streamTaskLog(taskId, offset);
    } finally {
      workerLock.readLock().unlock();
    }
  }

  @Override
  public Optional<InputStream> streamTaskReports(String taskId) throws IOException
  {
    HttpRemoteTaskRunnerWorkItemV2 workItem;
    taskLock.readLock().lock();
    try {
      workItem = tasks.get(taskId);
    } finally {
      taskLock.readLock().unlock();
    }

    if (workItem == null || workItem.getWorker() == null) {
      return Optional.absent();
    }

    workerLock.readLock().lock();
    try {
      WorkerHolder workerHolder = workers.get(workItem.getWorker().getHost());
      if (workerHolder == null) {
        return Optional.absent();
      }

      return workerHolder.streamTaskReports(taskId);
    } finally {
      workerLock.readLock().unlock();
    }
  }

  @Override
  public Optional<InputStream> streamTaskStatus(String taskId) throws IOException
  {
    HttpRemoteTaskRunnerWorkItemV2 workItem;
    taskLock.readLock().lock();
    try {
      workItem = tasks.get(taskId);
      if (workItem == null || workItem.getState() == HttpRemoteTaskRunnerWorkItemV2.State.COMPLETE) {
        return Optional.absent();
      }
    } finally {
      taskLock.readLock().unlock();
    }

    if (workItem.getWorker() == null) {
      return Optional.absent();
    }

    workerLock.readLock().lock();
    try {
      WorkerHolder workerHolder = workers.get(workItem.getWorker().getHost());
      if (workerHolder == null) {
        return Optional.absent();
      }

      return workerHolder.streamTaskStatus(taskId);
    } finally {
      workerLock.readLock().unlock();
    }
  }

  /**
   * Work item for HttpRemoteTaskRunnerV2
   */
  public static class HttpRemoteTaskRunnerWorkItemV2 extends RemoteTaskRunnerWorkItem
  {
    public enum State
    {
      PENDING(0, true, RunnerTaskState.PENDING),
      PENDING_WORKER_ASSIGN(1, true, RunnerTaskState.PENDING),
      RUNNING(2, false, RunnerTaskState.RUNNING),
      COMPLETE(3, false, RunnerTaskState.NONE);

      private final int index;
      private final boolean isPending;
      private final RunnerTaskState runnerTaskState;

      State(int index, boolean isPending, RunnerTaskState runnerTaskState)
      {
        this.index = index;
        this.isPending = isPending;
        this.runnerTaskState = runnerTaskState;
      }

      public boolean isPending()
      {
        return isPending;
      }

      public RunnerTaskState toRunnerTaskState()
      {
        return runnerTaskState;
      }
    }

    private Task task;
    private volatile State state;
    private volatile boolean shutdownRequested = false;
    private final SettableFuture<TaskStatus> result = SettableFuture.create();

    public HttpRemoteTaskRunnerWorkItemV2(
        String taskId,
        Worker worker,
        TaskLocation location,
        @Nullable Task task,
        String taskType,
        State state
    )
    {
      super(taskId, task == null ? null : task.getType(), worker, location, task == null ? null : task.getDataSource());
      this.state = state;
      this.task = task;
    }

    public Task getTask()
    {
      return task;
    }

    public void setTask(Task task)
    {
      this.task = task;
    }

    public State getState()
    {
      return state;
    }

    public void setState(State state)
    {
      this.state = state;
    }

    public boolean isShutdownRequested()
    {
      return shutdownRequested;
    }

    public void setShutdownRequested(boolean shutdownRequested)
    {
      this.shutdownRequested = shutdownRequested;
    }

    public void updateFromAnnouncement(TaskAnnouncement announcement)
    {
      // Update state based on task announcement
      TaskStatus status = announcement.getTaskStatus();

      if (status.isRunnable()) {
        setState(State.RUNNING);
      } else if (status.isSuccess() || status.isFailure()) {
        setState(State.COMPLETE);
        result.set(status);
      }

      setLocation(announcement.getTaskLocation());
    }
  }
 }

/*
  tasks:
    - use concurrent hashmap for the data structure, helps protect against concurrent reads/writes
    - accesses (read/write) which need key-level atomicity use read lock
    - accesses (read/write) which need full, map-level atomicity use write lock
  workers:
    - use concurrent hashmap for the data structure, helps protect against concurrent reads/writes
    - accesses (read/write) which need key-level atomicity use read lock
    - accesses (read/write) which need full, map-level atomicity use write lock
    - add a "state" enum to WorkerHolder which indicates the status of the worker in the workers set.
       READY
       PENDING_ASSIGN
       LAZY
       BLACKLISTED
    - use this to remove the workersWithUnacknowledgedTask/lazy/blacklisted sets management headache.
*/
