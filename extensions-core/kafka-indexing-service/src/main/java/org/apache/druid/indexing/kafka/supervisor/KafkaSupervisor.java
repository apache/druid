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

package org.apache.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.RealtimeIndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.kafka.KafkaDataSourceMetadata;
import org.apache.druid.indexing.kafka.KafkaIOConfig;
import org.apache.druid.indexing.kafka.KafkaIndexTask;
import org.apache.druid.indexing.kafka.KafkaIndexTaskClient;
import org.apache.druid.indexing.kafka.KafkaIndexTaskClientFactory;
import org.apache.druid.indexing.kafka.KafkaPartitions;
import org.apache.druid.indexing.kafka.KafkaTuningConfig;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Supervisor responsible for managing the KafkaIndexTasks for a single dataSource. At a high level, the class accepts a
 * {@link KafkaSupervisorSpec} which includes the Kafka topic and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the Kafka topic's partitions
 * and the list of running indexing tasks and ensures that all partitions are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * Kafka offsets.
 */
public class KafkaSupervisor implements Supervisor
{
  private static final EmittingLogger log = new EmittingLogger(KafkaSupervisor.class);
  private static final long MAX_RUN_FREQUENCY_MILLIS = 1000; // prevent us from running too often in response to events
  private static final long NOT_SET = -1;
  private static final long MINIMUM_FUTURE_TIMEOUT_IN_SECONDS = 120;
  private static final long MINIMUM_GET_OFFSET_PERIOD_MILLIS = 5000;
  private static final long INITIAL_GET_OFFSET_DELAY_MILLIS = 15000;
  private static final long INITIAL_EMIT_LAG_METRIC_DELAY_MILLIS = 25000;
  private static final CopyOnWriteArrayList EMPTY_LIST = Lists.newCopyOnWriteArrayList();

  public static final String IS_INCREMENTAL_HANDOFF_SUPPORTED = "IS_INCREMENTAL_HANDOFF_SUPPORTED";

  // Internal data structures
  // --------------------------------------------------------

  /**
   * A TaskGroup is the main data structure used by KafkaSupervisor to organize and monitor Kafka partitions and
   * indexing tasks. All the tasks in a TaskGroup should always be doing the same thing (reading the same partitions and
   * starting from the same offset) and if [replicas] is configured to be 1, a TaskGroup will contain a single task (the
   * exception being if the supervisor started up and discovered and adopted some already running tasks). At any given
   * time, there should only be up to a maximum of [taskCount] actively-reading task groups (tracked in the [taskGroups]
   * map) + zero or more pending-completion task groups (tracked in [pendingCompletionTaskGroups]).
   */
  private class TaskGroup
  {
    final int groupId;

    // This specifies the partitions and starting offsets for this task group. It is set on group creation from the data
    // in [partitionGroups] and never changes during the lifetime of this task group, which will live until a task in
    // this task group has completed successfully, at which point this will be destroyed and a new task group will be
    // created with new starting offsets. This allows us to create replacement tasks for failed tasks that process the
    // same offsets, even if the values in [partitionGroups] has been changed.
    final ImmutableMap<Integer, Long> partitionOffsets;

    final ConcurrentHashMap<String, TaskData> tasks = new ConcurrentHashMap<>();
    final Optional<DateTime> minimumMessageTime;
    final Optional<DateTime> maximumMessageTime;
    DateTime completionTimeout; // is set after signalTasksToFinish(); if not done by timeout, take corrective action
    final TreeMap<Integer, Map<Integer, Long>> sequenceOffsets = new TreeMap<>();
    final String baseSequenceName;

    TaskGroup(
        int groupId,
        ImmutableMap<Integer, Long> partitionOffsets,
        Optional<DateTime> minimumMessageTime,
        Optional<DateTime> maximumMessageTime
    )
    {
      this.groupId = groupId;
      this.partitionOffsets = partitionOffsets;
      this.minimumMessageTime = minimumMessageTime;
      this.maximumMessageTime = maximumMessageTime;
      this.sequenceOffsets.put(0, partitionOffsets);
      this.baseSequenceName = generateSequenceName(partitionOffsets, minimumMessageTime, maximumMessageTime);
    }

    int addNewCheckpoint(Map<Integer, Long> checkpoint)
    {
      sequenceOffsets.put(sequenceOffsets.lastKey() + 1, checkpoint);
      return sequenceOffsets.lastKey();
    }

    Set<String> taskIds()
    {
      return tasks.keySet();
    }
  }

  private static class TaskData
  {
    @Nullable
    volatile TaskStatus status;
    @Nullable
    volatile DateTime startTime;
    volatile Map<Integer, Long> currentOffsets = new HashMap<>();

    @Override
    public String toString()
    {
      return "TaskData{" +
             "status=" + status +
             ", startTime=" + startTime +
             ", currentOffsets=" + currentOffsets +
             '}';
    }
  }

  // Map<{group ID}, {actively reading task group}>; see documentation for TaskGroup class
  private final ConcurrentHashMap<Integer, TaskGroup> taskGroups = new ConcurrentHashMap<>();

  // After telling a taskGroup to stop reading and begin publishing a segment, it is moved from [taskGroups] to here so
  // we can monitor its status while we queue new tasks to read the next range of offsets. This is a list since we could
  // have multiple sets of tasks publishing at once if time-to-publish > taskDuration.
  // Map<{group ID}, List<{pending completion task groups}>>
  private final ConcurrentHashMap<Integer, CopyOnWriteArrayList<TaskGroup>> pendingCompletionTaskGroups = new ConcurrentHashMap<>();

  // The starting offset for a new partition in [partitionGroups] is initially set to NOT_SET. When a new task group
  // is created and is assigned partitions, if the offset in [partitionGroups] is NOT_SET it will take the starting
  // offset value from the metadata store, and if it can't find it there, from Kafka. Once a task begins
  // publishing, the offset in partitionGroups will be updated to the ending offset of the publishing-but-not-yet-
  // completed task, which will cause the next set of tasks to begin reading from where the previous task left
  // off. If that previous task now fails, we will set the offset in [partitionGroups] back to NOT_SET which will
  // cause successive tasks to again grab their starting offset from metadata store. This mechanism allows us to
  // start up successive tasks without waiting for the previous tasks to succeed and still be able to handle task
  // failures during publishing.
  // Map<{group ID}, Map<{partition ID}, {startingOffset}>>
  private final ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Long>> partitionGroups = new ConcurrentHashMap<>();
  // --------------------------------------------------------

  private final TaskStorage taskStorage;
  private final TaskMaster taskMaster;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final KafkaIndexTaskClient taskClient;
  private final ObjectMapper sortingMapper;
  private final KafkaSupervisorSpec spec;
  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private final String dataSource;
  private final KafkaSupervisorIOConfig ioConfig;
  private final KafkaSupervisorTuningConfig tuningConfig;
  private final KafkaTuningConfig taskTuningConfig;
  private final String supervisorId;
  private final TaskInfoProvider taskInfoProvider;
  private final long futureTimeoutInSeconds; // how long to wait for async operations to complete
  private final RowIngestionMetersFactory rowIngestionMetersFactory;

  private final ExecutorService exec;
  private final ScheduledExecutorService scheduledExec;
  private final ScheduledExecutorService reportingExec;
  private final ListeningExecutorService workerExec;
  private final BlockingQueue<Notice> notices = new LinkedBlockingDeque<>();
  private final Object stopLock = new Object();
  private final Object stateChangeLock = new Object();
  private final Object consumerLock = new Object();

  private boolean listenerRegistered = false;
  private long lastRunTime;

  private volatile DateTime firstRunTime;
  private volatile KafkaConsumer consumer;
  private volatile boolean started = false;
  private volatile boolean stopped = false;
  private volatile Map<Integer, Long> latestOffsetsFromKafka;
  private volatile DateTime offsetsLastUpdated;

  public KafkaSupervisor(
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final KafkaIndexTaskClientFactory taskClientFactory,
      final ObjectMapper mapper,
      final KafkaSupervisorSpec spec,
      final RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.sortingMapper = mapper.copy().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    this.spec = spec;
    this.emitter = spec.getEmitter();
    this.monitorSchedulerConfig = spec.getMonitorSchedulerConfig();
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;

    this.dataSource = spec.getDataSchema().getDataSource();
    this.ioConfig = spec.getIoConfig();
    this.tuningConfig = spec.getTuningConfig();
    this.taskTuningConfig = KafkaTuningConfig.copyOf(this.tuningConfig);
    this.supervisorId = StringUtils.format("KafkaSupervisor-%s", dataSource);
    this.exec = Execs.singleThreaded(supervisorId);
    this.scheduledExec = Execs.scheduledSingleThreaded(supervisorId + "-Scheduler-%d");
    this.reportingExec = Execs.scheduledSingleThreaded(supervisorId + "-Reporting-%d");

    int workerThreads = (this.tuningConfig.getWorkerThreads() != null
                         ? this.tuningConfig.getWorkerThreads()
                         : Math.min(10, this.ioConfig.getTaskCount()));
    this.workerExec = MoreExecutors.listeningDecorator(Execs.multiThreaded(workerThreads, supervisorId + "-Worker-%d"));
    log.info("Created worker pool with [%d] threads for dataSource [%s]", workerThreads, this.dataSource);

    this.taskInfoProvider = new TaskInfoProvider()
    {
      @Override
      public TaskLocation getTaskLocation(final String id)
      {
        Preconditions.checkNotNull(id, "id");
        Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
        if (taskRunner.isPresent()) {
          Optional<? extends TaskRunnerWorkItem> item = Iterables.tryFind(
              taskRunner.get().getRunningTasks(),
              (Predicate<TaskRunnerWorkItem>) taskRunnerWorkItem -> id.equals(taskRunnerWorkItem.getTaskId())
          );

          if (item.isPresent()) {
            return item.get().getLocation();
          }
        } else {
          log.error("Failed to get task runner because I'm not the leader!");
        }

        return TaskLocation.unknown();
      }

      @Override
      public Optional<TaskStatus> getTaskStatus(String id)
      {
        return taskStorage.getStatus(id);
      }
    };

    this.futureTimeoutInSeconds = Math.max(
        MINIMUM_FUTURE_TIMEOUT_IN_SECONDS,
        tuningConfig.getChatRetries() * (tuningConfig.getHttpTimeout().getStandardSeconds()
                                         + KafkaIndexTaskClient.MAX_RETRY_WAIT_SECONDS)
    );

    int chatThreads = (this.tuningConfig.getChatThreads() != null
                       ? this.tuningConfig.getChatThreads()
                       : Math.min(10, this.ioConfig.getTaskCount() * this.ioConfig.getReplicas()));
    this.taskClient = taskClientFactory.build(
        taskInfoProvider,
        dataSource,
        chatThreads,
        this.tuningConfig.getHttpTimeout(),
        this.tuningConfig.getChatRetries()
    );
    log.info(
        "Created taskClient with dataSource[%s] chatThreads[%d] httpTimeout[%s] chatRetries[%d]",
        dataSource,
        chatThreads,
        this.tuningConfig.getHttpTimeout(),
        this.tuningConfig.getChatRetries()
    );
  }

  @Override
  public void start()
  {
    synchronized (stateChangeLock) {
      Preconditions.checkState(!started, "already started");
      Preconditions.checkState(!exec.isShutdown(), "already stopped");

      try {
        consumer = getKafkaConsumer();

        exec.submit(
            () -> {
              try {
                long pollTimeout = Math.max(ioConfig.getPeriod().getMillis(), MAX_RUN_FREQUENCY_MILLIS);
                while (!Thread.currentThread().isInterrupted() && !stopped) {
                  final Notice notice = notices.poll(pollTimeout, TimeUnit.MILLISECONDS);
                  if (notice == null) {
                    continue;
                  }

                  try {
                    notice.handle();
                  }
                  catch (Throwable e) {
                    log.makeAlert(e, "KafkaSupervisor[%s] failed to handle notice", dataSource)
                       .addData("noticeClass", notice.getClass().getSimpleName())
                       .emit();
                  }
                }
              }
              catch (InterruptedException e) {
                log.info("KafkaSupervisor[%s] interrupted, exiting", dataSource);
              }
            }
        );
        firstRunTime = DateTimes.nowUtc().plus(ioConfig.getStartDelay());
        scheduledExec.scheduleAtFixedRate(
            buildRunTask(),
            ioConfig.getStartDelay().getMillis(),
            Math.max(ioConfig.getPeriod().getMillis(), MAX_RUN_FREQUENCY_MILLIS),
            TimeUnit.MILLISECONDS
        );

        reportingExec.scheduleAtFixedRate(
            updateCurrentAndLatestOffsets(),
            ioConfig.getStartDelay().getMillis() + INITIAL_GET_OFFSET_DELAY_MILLIS, // wait for tasks to start up
            Math.max(
                tuningConfig.getOffsetFetchPeriod().getMillis(), MINIMUM_GET_OFFSET_PERIOD_MILLIS
            ),
            TimeUnit.MILLISECONDS
        );

        reportingExec.scheduleAtFixedRate(
            emitLag(),
            ioConfig.getStartDelay().getMillis() + INITIAL_EMIT_LAG_METRIC_DELAY_MILLIS, // wait for tasks to start up
            monitorSchedulerConfig.getEmitterPeriod().getMillis(),
            TimeUnit.MILLISECONDS
        );

        started = true;
        log.info(
            "Started KafkaSupervisor[%s], first run in [%s], with spec: [%s]",
            dataSource,
            ioConfig.getStartDelay(),
            spec.toString()
        );
      }
      catch (Exception e) {
        if (consumer != null) {
          consumer.close();
        }
        log.makeAlert(e, "Exception starting KafkaSupervisor[%s]", dataSource)
           .emit();
        throw Throwables.propagate(e);
      }
    }
  }

  @Override
  public void stop(boolean stopGracefully)
  {
    synchronized (stateChangeLock) {
      Preconditions.checkState(started, "not started");

      log.info("Beginning shutdown of KafkaSupervisor[%s]", dataSource);

      try {
        scheduledExec.shutdownNow(); // stop recurring executions
        reportingExec.shutdownNow();

        Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
        if (taskRunner.isPresent()) {
          taskRunner.get().unregisterListener(supervisorId);
        }

        // Stopping gracefully will synchronize the end offsets of the tasks and signal them to publish, and will block
        // until the tasks have acknowledged or timed out. We want this behavior when we're explicitly shut down through
        // the API, but if we shut down for other reasons (e.g. we lose leadership) we want to just stop and leave the
        // tasks as they are.
        synchronized (stopLock) {
          if (stopGracefully) {
            log.info("Posting GracefulShutdownNotice, signalling managed tasks to complete and publish");
            notices.add(new GracefulShutdownNotice());
          } else {
            log.info("Posting ShutdownNotice");
            notices.add(new ShutdownNotice());
          }

          long shutdownTimeoutMillis = tuningConfig.getShutdownTimeout().getMillis();
          long endTime = System.currentTimeMillis() + shutdownTimeoutMillis;
          while (!stopped) {
            long sleepTime = endTime - System.currentTimeMillis();
            if (sleepTime <= 0) {
              log.info("Timed out while waiting for shutdown (timeout [%,dms])", shutdownTimeoutMillis);
              stopped = true;
              break;
            }
            stopLock.wait(sleepTime);
          }
        }
        log.info("Shutdown notice handled");

        taskClient.close();
        workerExec.shutdownNow();
        exec.shutdownNow();
        started = false;

        log.info("KafkaSupervisor[%s] has stopped", dataSource);
      }
      catch (Exception e) {
        log.makeAlert(e, "Exception stopping KafkaSupervisor[%s]", dataSource)
           .emit();
      }
    }
  }

  @Override
  public SupervisorReport getStatus()
  {
    return generateReport(true);
  }

  @Override
  public Map<String, Map<String, Object>> getStats()
  {
    try {
      return getCurrentTotalStats();
    }
    catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      log.error(ie, "getStats() interrupted.");
      throw new RuntimeException(ie);
    }
    catch (ExecutionException | TimeoutException eete) {
      throw new RuntimeException(eete);
    }
  }

  @Override
  public void reset(DataSourceMetadata dataSourceMetadata)
  {
    log.info("Posting ResetNotice");
    notices.add(new ResetNotice(dataSourceMetadata));
  }

  @Override
  public void checkpoint(
      @Nullable Integer taskGroupId,
      @Deprecated String baseSequenceName,
      DataSourceMetadata previousCheckPoint,
      DataSourceMetadata currentCheckPoint
  )
  {
    Preconditions.checkNotNull(previousCheckPoint, "previousCheckpoint");
    Preconditions.checkNotNull(currentCheckPoint, "current checkpoint cannot be null");
    Preconditions.checkArgument(
        ioConfig.getTopic().equals(((KafkaDataSourceMetadata) currentCheckPoint).getKafkaPartitions().getTopic()),
        "Supervisor topic [%s] and topic in checkpoint [%s] does not match",
        ioConfig.getTopic(),
        ((KafkaDataSourceMetadata) currentCheckPoint).getKafkaPartitions().getTopic()
    );

    log.info("Checkpointing [%s] for taskGroup [%s]", currentCheckPoint, taskGroupId);
    notices.add(
        new CheckpointNotice(
            taskGroupId,
            baseSequenceName,
            (KafkaDataSourceMetadata) previousCheckPoint,
            (KafkaDataSourceMetadata) currentCheckPoint
        )
    );
  }

  public void possiblyRegisterListener()
  {
    // getTaskRunner() sometimes fails if the task queue is still being initialized so retry later until we succeed

    if (listenerRegistered) {
      return;
    }

    Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
    if (taskRunner.isPresent()) {
      taskRunner.get().registerListener(
          new TaskRunnerListener()
          {
            @Override
            public String getListenerId()
            {
              return supervisorId;
            }

            @Override
            public void locationChanged(final String taskId, final TaskLocation newLocation)
            {
              // do nothing
            }

            @Override
            public void statusChanged(String taskId, TaskStatus status)
            {
              notices.add(new RunNotice());
            }
          }, MoreExecutors.sameThreadExecutor()
      );

      listenerRegistered = true;
    }
  }

  private interface Notice
  {
    void handle() throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException;
  }

  private class RunNotice implements Notice
  {
    @Override
    public void handle() throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException
    {
      long nowTime = System.currentTimeMillis();
      if (nowTime - lastRunTime < MAX_RUN_FREQUENCY_MILLIS) {
        return;
      }
      lastRunTime = nowTime;

      runInternal();
    }
  }

  private class GracefulShutdownNotice extends ShutdownNotice
  {
    @Override
    public void handle() throws InterruptedException, ExecutionException, TimeoutException
    {
      gracefulShutdownInternal();
      super.handle();
    }
  }

  private class ShutdownNotice implements Notice
  {
    @Override
    public void handle() throws InterruptedException, ExecutionException, TimeoutException
    {
      consumer.close();

      synchronized (stopLock) {
        stopped = true;
        stopLock.notifyAll();
      }
    }
  }

  private class ResetNotice implements Notice
  {
    final DataSourceMetadata dataSourceMetadata;

    ResetNotice(DataSourceMetadata dataSourceMetadata)
    {
      this.dataSourceMetadata = dataSourceMetadata;
    }

    @Override
    public void handle()
    {
      resetInternal(dataSourceMetadata);
    }
  }

  private class CheckpointNotice implements Notice
  {
    @Nullable
    private final Integer nullableTaskGroupId;
    @Deprecated
    private final String baseSequenceName;
    private final KafkaDataSourceMetadata previousCheckpoint;
    private final KafkaDataSourceMetadata currentCheckpoint;

    CheckpointNotice(
        @Nullable Integer nullableTaskGroupId,
        @Deprecated String baseSequenceName,
        KafkaDataSourceMetadata previousCheckpoint,
        KafkaDataSourceMetadata currentCheckpoint
    )
    {
      this.baseSequenceName = baseSequenceName;
      this.nullableTaskGroupId = nullableTaskGroupId;
      this.previousCheckpoint = previousCheckpoint;
      this.currentCheckpoint = currentCheckpoint;
    }

    @Override
    public void handle() throws ExecutionException, InterruptedException
    {
      // Find taskGroupId using taskId if it's null. It can be null while rolling update.
      final int taskGroupId;
      if (nullableTaskGroupId == null) {
        // We search taskId in taskGroups and pendingCompletionTaskGroups sequentially. This should be fine because
        // 1) a taskGroup can be moved from taskGroups to pendingCompletionTaskGroups in RunNotice
        //    (see checkTaskDuration()).
        // 2) Notices are proceesed by a single thread. So, CheckpointNotice and RunNotice cannot be processed at the
        //    same time.
        final java.util.Optional<Integer> maybeGroupId = taskGroups
            .entrySet()
            .stream()
            .filter(entry -> {
              final TaskGroup taskGroup = entry.getValue();
              return taskGroup.baseSequenceName.equals(baseSequenceName);
            })
            .findAny()
            .map(Entry::getKey);
        taskGroupId = maybeGroupId.orElse(
            pendingCompletionTaskGroups
                .entrySet()
                .stream()
                .filter(entry -> {
                  final List<TaskGroup> taskGroups = entry.getValue();
                  return taskGroups.stream().anyMatch(group -> group.baseSequenceName.equals(baseSequenceName));
                })
                .findAny()
                .orElseThrow(() -> new ISE("Cannot find taskGroup for baseSequenceName[%s]", baseSequenceName))
                .getKey()
        );
      } else {
        taskGroupId = nullableTaskGroupId;
      }

      // check for consistency
      // if already received request for this sequenceName and dataSourceMetadata combination then return
      final TaskGroup taskGroup = taskGroups.get(taskGroupId);

      if (isValidTaskGroup(taskGroupId, taskGroup)) {
        final TreeMap<Integer, Map<Integer, Long>> checkpoints = taskGroup.sequenceOffsets;

        // check validity of previousCheckpoint
        int index = checkpoints.size();
        for (int sequenceId : checkpoints.descendingKeySet()) {
          Map<Integer, Long> checkpoint = checkpoints.get(sequenceId);
          // We have already verified the topic of the current checkpoint is same with that in ioConfig.
          // See checkpoint().
          if (checkpoint.equals(previousCheckpoint.getKafkaPartitions().getPartitionOffsetMap())) {
            break;
          }
          index--;
        }
        if (index == 0) {
          throw new ISE("No such previous checkpoint [%s] found", previousCheckpoint);
        } else if (index < checkpoints.size()) {
          // if the found checkpoint is not the latest one then already checkpointed by a replica
          Preconditions.checkState(index == checkpoints.size() - 1, "checkpoint consistency failure");
          log.info("Already checkpointed with offsets [%s]", checkpoints.lastEntry().getValue());
          return;
        }
        final Map<Integer, Long> newCheckpoint = checkpointTaskGroup(taskGroup, false).get();
        taskGroup.addNewCheckpoint(newCheckpoint);
        log.info("Handled checkpoint notice, new checkpoint is [%s] for taskGroup [%s]", newCheckpoint, taskGroupId);
      }
    }

    private boolean isValidTaskGroup(int taskGroupId, @Nullable TaskGroup taskGroup)
    {
      if (taskGroup == null) {
        // taskGroup might be in pendingCompletionTaskGroups or partitionGroups
        if (pendingCompletionTaskGroups.containsKey(taskGroupId)) {
          log.warn(
              "Ignoring checkpoint request because taskGroup[%d] has already stopped indexing and is waiting for "
              + "publishing segments",
              taskGroupId
          );
          return false;
        } else if (partitionGroups.containsKey(taskGroupId)) {
          log.warn("Ignoring checkpoint request because taskGroup[%d] is inactive", taskGroupId);
          return false;
        } else {
          throw new ISE("WTH?! cannot find taskGroup [%s] among all taskGroups [%s]", taskGroupId, taskGroups);
        }
      }

      return true;
    }
  }

  @VisibleForTesting
  void resetInternal(DataSourceMetadata dataSourceMetadata)
  {
    if (dataSourceMetadata == null) {
      // Reset everything
      boolean result = indexerMetadataStorageCoordinator.deleteDataSourceMetadata(dataSource);
      log.info("Reset dataSource[%s] - dataSource metadata entry deleted? [%s]", dataSource, result);
      taskGroups.values().forEach(this::killTasksInGroup);
      taskGroups.clear();
      partitionGroups.clear();
    } else if (!(dataSourceMetadata instanceof KafkaDataSourceMetadata)) {
      throw new IAE("Expected KafkaDataSourceMetadata but found instance of [%s]", dataSourceMetadata.getClass());
    } else {
      // Reset only the partitions in dataSourceMetadata if it has not been reset yet
      final KafkaDataSourceMetadata resetKafkaMetadata = (KafkaDataSourceMetadata) dataSourceMetadata;

      if (resetKafkaMetadata.getKafkaPartitions().getTopic().equals(ioConfig.getTopic())) {
        // metadata can be null
        final DataSourceMetadata metadata = indexerMetadataStorageCoordinator.getDataSourceMetadata(dataSource);
        if (metadata != null && !(metadata instanceof KafkaDataSourceMetadata)) {
          throw new IAE(
              "Expected KafkaDataSourceMetadata from metadata store but found instance of [%s]",
              metadata.getClass()
          );
        }
        final KafkaDataSourceMetadata currentMetadata = (KafkaDataSourceMetadata) metadata;

        // defend against consecutive reset requests from replicas
        // as well as the case where the metadata store do not have an entry for the reset partitions
        boolean doReset = false;
        for (Entry<Integer, Long> resetPartitionOffset : resetKafkaMetadata.getKafkaPartitions()
                                                                               .getPartitionOffsetMap()
                                                                               .entrySet()) {
          final Long partitionOffsetInMetadataStore = currentMetadata == null
                                                      ? null
                                                      : currentMetadata.getKafkaPartitions()
                                                                       .getPartitionOffsetMap()
                                                                       .get(resetPartitionOffset.getKey());
          final TaskGroup partitionTaskGroup = taskGroups.get(
              getTaskGroupIdForPartition(resetPartitionOffset.getKey())
          );
          final boolean isSameOffset = partitionTaskGroup != null
                                       && partitionTaskGroup.partitionOffsets.get(resetPartitionOffset.getKey())
                                                                             .equals(resetPartitionOffset.getValue());
          if (partitionOffsetInMetadataStore != null || isSameOffset) {
            doReset = true;
            break;
          }
        }

        if (!doReset) {
          log.info("Ignoring duplicate reset request [%s]", dataSourceMetadata);
          return;
        }

        boolean metadataUpdateSuccess = false;
        if (currentMetadata == null) {
          metadataUpdateSuccess = true;
        } else {
          final DataSourceMetadata newMetadata = currentMetadata.minus(resetKafkaMetadata);
          try {
            metadataUpdateSuccess = indexerMetadataStorageCoordinator.resetDataSourceMetadata(dataSource, newMetadata);
          }
          catch (IOException e) {
            log.error("Resetting DataSourceMetadata failed [%s]", e.getMessage());
            Throwables.propagate(e);
          }
        }
        if (metadataUpdateSuccess) {
          resetKafkaMetadata.getKafkaPartitions().getPartitionOffsetMap().keySet().forEach(partition -> {
            final int groupId = getTaskGroupIdForPartition(partition);
            killTaskGroupForPartitions(ImmutableSet.of(partition));
            taskGroups.remove(groupId);
            partitionGroups.get(groupId).replaceAll((partitionId, offset) -> NOT_SET);
          });
        } else {
          throw new ISE("Unable to reset metadata");
        }
      } else {
        log.warn(
            "Reset metadata topic [%s] and supervisor's topic [%s] do not match",
            resetKafkaMetadata.getKafkaPartitions().getTopic(),
            ioConfig.getTopic()
        );
      }
    }
  }

  private void killTaskGroupForPartitions(Set<Integer> partitions)
  {
    for (Integer partition : partitions) {
      killTasksInGroup(taskGroups.get(getTaskGroupIdForPartition(partition)));
    }
  }

  private void killTasksInGroup(TaskGroup taskGroup)
  {
    if (taskGroup != null) {
      for (String taskId : taskGroup.tasks.keySet()) {
        log.info("Killing task [%s] in the task group", taskId);
        killTask(taskId);
      }
    }
  }

  @VisibleForTesting
  void gracefulShutdownInternal() throws ExecutionException, InterruptedException, TimeoutException
  {
    // Prepare for shutdown by 1) killing all tasks that haven't been assigned to a worker yet, and 2) causing all
    // running tasks to begin publishing by setting their startTime to a very long time ago so that the logic in
    // checkTaskDuration() will be triggered. This is better than just telling these tasks to publish whatever they
    // have, as replicas that are supposed to publish the same segment may not have read the same set of offsets.
    for (TaskGroup taskGroup : taskGroups.values()) {
      for (Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
        if (taskInfoProvider.getTaskLocation(entry.getKey()).equals(TaskLocation.unknown())) {
          killTask(entry.getKey());
        } else {
          entry.getValue().startTime = DateTimes.EPOCH;
        }
      }
    }

    checkTaskDuration();
  }

  @VisibleForTesting
  void runInternal() throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException
  {
    possiblyRegisterListener();
    updatePartitionDataFromKafka();
    discoverTasks();
    updateTaskStatus();
    checkTaskDuration();
    checkPendingCompletionTasks();
    checkCurrentTaskState();

    // if supervisor is not suspended, ensure required tasks are running
    // if suspended, ensure tasks have been requested to gracefully stop
    if (!spec.isSuspended()) {
      log.info("[%s] supervisor is running.", dataSource);
      createNewTasks();
    } else {
      log.info("[%s] supervisor is suspended.", dataSource);
      gracefulShutdownInternal();
    }

    if (log.isDebugEnabled()) {
      log.debug(generateReport(true).toString());
    } else {
      log.info(generateReport(false).toString());
    }
  }

  String generateSequenceName(
      Map<Integer, Long> startPartitions,
      Optional<DateTime> minimumMessageTime,
      Optional<DateTime> maximumMessageTime
  )
  {
    StringBuilder sb = new StringBuilder();

    for (Entry<Integer, Long> entry : startPartitions.entrySet()) {
      sb.append(StringUtils.format("+%d(%d)", entry.getKey(), entry.getValue()));
    }
    String partitionOffsetStr = sb.toString().substring(1);

    String minMsgTimeStr = (minimumMessageTime.isPresent() ? String.valueOf(minimumMessageTime.get().getMillis()) : "");
    String maxMsgTimeStr = (maximumMessageTime.isPresent() ? String.valueOf(maximumMessageTime.get().getMillis()) : "");

    String dataSchema, tuningConfig;
    try {
      dataSchema = sortingMapper.writeValueAsString(spec.getDataSchema());
      tuningConfig = sortingMapper.writeValueAsString(taskTuningConfig);
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }

    String hashCode = DigestUtils.sha1Hex(dataSchema
                                          + tuningConfig
                                          + partitionOffsetStr
                                          + minMsgTimeStr
                                          + maxMsgTimeStr)
                                 .substring(0, 15);

    return Joiner.on("_").join("index_kafka", dataSource, hashCode);
  }

  private KafkaConsumer<byte[], byte[]> getKafkaConsumer()
  {
    final Properties props = new Properties();

    props.setProperty("metadata.max.age.ms", "10000");
    props.setProperty("group.id", StringUtils.format("kafka-supervisor-%s", RealtimeIndexTask.makeRandomId()));

    props.putAll(ioConfig.getConsumerProperties());

    props.setProperty("enable.auto.commit", "false");

    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  private void updatePartitionDataFromKafka()
  {
    Map<String, List<PartitionInfo>> topics;
    try {
      synchronized (consumerLock) {
        topics = consumer.listTopics(); // updates the consumer's list of partitions from the brokers
      }
    }
    catch (Exception e) { // calls to the consumer throw NPEs when the broker doesn't respond
      log.warn(
          e,
          "Unable to get partition data from Kafka for brokers [%s], are the brokers up?",
          ioConfig.getConsumerProperties().get(KafkaSupervisorIOConfig.BOOTSTRAP_SERVERS_KEY)
      );
      return;
    }

    List<PartitionInfo> partitions = topics.get(ioConfig.getTopic());
    if (partitions == null) {
      log.warn("No such topic [%s] found, list of discovered topics [%s]", ioConfig.getTopic(), topics.keySet());
    }
    int numPartitions = (partitions != null ? partitions.size() : 0);

    log.debug("Found [%d] Kafka partitions for topic [%s]", numPartitions, ioConfig.getTopic());

    for (int partition = 0; partition < numPartitions; partition++) {
      int taskGroupId = getTaskGroupIdForPartition(partition);

      ConcurrentHashMap<Integer, Long> partitionMap = partitionGroups.computeIfAbsent(
          taskGroupId,
          k -> new ConcurrentHashMap<>()
      );

      // The starting offset for a new partition in [partitionGroups] is initially set to NOT_SET; when a new task group
      // is created and is assigned partitions, if the offset in [partitionGroups] is NOT_SET it will take the starting
      // offset value from the metadata store, and if it can't find it there, from Kafka. Once a task begins
      // publishing, the offset in partitionGroups will be updated to the ending offset of the publishing-but-not-yet-
      // completed task, which will cause the next set of tasks to begin reading from where the previous task left
      // off. If that previous task now fails, we will set the offset in [partitionGroups] back to NOT_SET which will
      // cause successive tasks to again grab their starting offset from metadata store. This mechanism allows us to
      // start up successive tasks without waiting for the previous tasks to succeed and still be able to handle task
      // failures during publishing.
      if (partitionMap.putIfAbsent(partition, NOT_SET) == null) {
        log.info(
            "New partition [%d] discovered for topic [%s], added to task group [%d]",
            partition,
            ioConfig.getTopic(),
            taskGroupId
        );
      }
    }
  }

  private void discoverTasks() throws ExecutionException, InterruptedException, TimeoutException
  {
    int taskCount = 0;
    List<String> futureTaskIds = Lists.newArrayList();
    List<ListenableFuture<Boolean>> futures = Lists.newArrayList();
    List<Task> tasks = taskStorage.getActiveTasks();
    final Map<Integer, TaskGroup> taskGroupsToVerify = new HashMap<>();

    for (Task task : tasks) {
      if (!(task instanceof KafkaIndexTask) || !dataSource.equals(task.getDataSource())) {
        continue;
      }

      taskCount++;
      final KafkaIndexTask kafkaTask = (KafkaIndexTask) task;
      final String taskId = task.getId();

      // Determine which task group this task belongs to based on one of the partitions handled by this task. If we
      // later determine that this task is actively reading, we will make sure that it matches our current partition
      // allocation (getTaskGroupIdForPartition(partition) should return the same value for every partition being read
      // by this task) and kill it if it is not compatible. If the task is instead found to be in the publishing
      // state, we will permit it to complete even if it doesn't match our current partition allocation to support
      // seamless schema migration.

      Iterator<Integer> it = kafkaTask.getIOConfig().getStartPartitions().getPartitionOffsetMap().keySet().iterator();
      final Integer taskGroupId = (it.hasNext() ? getTaskGroupIdForPartition(it.next()) : null);

      if (taskGroupId != null) {
        // check to see if we already know about this task, either in [taskGroups] or in [pendingCompletionTaskGroups]
        // and if not add it to taskGroups or pendingCompletionTaskGroups (if status = PUBLISHING)
        TaskGroup taskGroup = taskGroups.get(taskGroupId);
        if (!isTaskInPendingCompletionGroups(taskId) && (taskGroup == null || !taskGroup.tasks.containsKey(taskId))) {

          futureTaskIds.add(taskId);
          futures.add(
              Futures.transform(
                  taskClient.getStatusAsync(taskId), new Function<KafkaIndexTask.Status, Boolean>()
                  {
                    @Override
                    public Boolean apply(KafkaIndexTask.Status status)
                    {
                      try {
                        log.debug("Task [%s], status [%s]", taskId, status);
                        if (status == KafkaIndexTask.Status.PUBLISHING) {
                          kafkaTask.getIOConfig().getStartPartitions().getPartitionOffsetMap().keySet().forEach(
                              partition -> addDiscoveredTaskToPendingCompletionTaskGroups(
                                  getTaskGroupIdForPartition(partition),
                                  taskId,
                                  kafkaTask.getIOConfig()
                                           .getStartPartitions()
                                           .getPartitionOffsetMap()
                              )
                          );

                          // update partitionGroups with the publishing task's offsets (if they are greater than what is
                          // existing) so that the next tasks will start reading from where this task left off
                          Map<Integer, Long> publishingTaskEndOffsets = taskClient.getEndOffsets(taskId);

                          for (Entry<Integer, Long> entry : publishingTaskEndOffsets.entrySet()) {
                            Integer partition = entry.getKey();
                            Long offset = entry.getValue();
                            ConcurrentHashMap<Integer, Long> partitionOffsets = partitionGroups.get(
                                getTaskGroupIdForPartition(partition)
                            );

                            boolean succeeded;
                            do {
                              succeeded = true;
                              Long previousOffset = partitionOffsets.putIfAbsent(partition, offset);
                              if (previousOffset != null && previousOffset < offset) {
                                succeeded = partitionOffsets.replace(partition, previousOffset, offset);
                              }
                            } while (!succeeded);
                          }
                        } else {
                          for (Integer partition : kafkaTask.getIOConfig()
                                                            .getStartPartitions()
                                                            .getPartitionOffsetMap()
                                                            .keySet()) {
                            if (!taskGroupId.equals(getTaskGroupIdForPartition(partition))) {
                              log.warn(
                                  "Stopping task [%s] which does not match the expected partition allocation",
                                  taskId
                              );
                              try {
                                stopTask(taskId, false).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
                              }
                              catch (InterruptedException | ExecutionException | TimeoutException e) {
                                log.warn(e, "Exception while stopping task");
                              }
                              return false;
                            }
                          }
                          // make sure the task's io and tuning configs match with the supervisor config
                          // if it is current then only create corresponding taskGroup if it does not exist
                          if (!isTaskCurrent(taskGroupId, taskId)) {
                            log.info(
                                "Stopping task [%s] which does not match the expected parameters and ingestion spec",
                                taskId
                            );
                            try {
                              stopTask(taskId, false).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
                            }
                            catch (InterruptedException | ExecutionException | TimeoutException e) {
                              log.warn(e, "Exception while stopping task");
                            }
                            return false;
                          } else {
                            final TaskGroup taskGroup = taskGroups.computeIfAbsent(
                                taskGroupId,
                                k -> {
                                  log.info("Creating a new task group for taskGroupId[%d]", taskGroupId);
                                  return new TaskGroup(
                                      taskGroupId,
                                      ImmutableMap.copyOf(
                                          kafkaTask.getIOConfig().getStartPartitions().getPartitionOffsetMap()
                                      ),
                                      kafkaTask.getIOConfig().getMinimumMessageTime(),
                                      kafkaTask.getIOConfig().getMaximumMessageTime()
                                  );
                                }
                            );
                            taskGroupsToVerify.put(taskGroupId, taskGroup);
                            final TaskData prevTaskData = taskGroup.tasks.putIfAbsent(taskId, new TaskData());
                            if (prevTaskData != null) {
                              throw new ISE(
                                  "WTH? a taskData[%s] already exists for new task[%s]",
                                  prevTaskData,
                                  taskId
                              );
                            }
                          }
                        }
                        return true;
                      }
                      catch (Throwable t) {
                        log.error(t, "Something bad while discovering task [%s]", taskId);
                        return null;
                      }
                    }
                  }, workerExec
              )
          );
        }
      }
    }

    List<Boolean> results = Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
    for (int i = 0; i < results.size(); i++) {
      if (results.get(i) == null) {
        String taskId = futureTaskIds.get(i);
        log.warn("Task [%s] failed to return status, killing task", taskId);
        killTask(taskId);
      }
    }
    log.debug("Found [%d] Kafka indexing tasks for dataSource [%s]", taskCount, dataSource);

    // make sure the checkpoints are consistent with each other and with the metadata store
    verifyAndMergeCheckpoints(taskGroupsToVerify.values());
  }

  private void verifyAndMergeCheckpoints(final Collection<TaskGroup> taskGroupsToVerify)
  {
    final List<ListenableFuture<?>> futures = new ArrayList<>();
    for (TaskGroup taskGroup : taskGroupsToVerify) {
      futures.add(workerExec.submit(() -> verifyAndMergeCheckpoints(taskGroup)));
    }
    try {
      Futures.allAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
    }
    catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This method does two things -
   * 1. Makes sure the checkpoints information in the taskGroup is consistent with that of the tasks, if not kill
   * inconsistent tasks.
   * 2. truncates the checkpoints in the taskGroup corresponding to which segments have been published, so that any newly
   * created tasks for the taskGroup start indexing from after the latest published offsets.
   */
  private void verifyAndMergeCheckpoints(final TaskGroup taskGroup)
  {
    final int groupId = taskGroup.groupId;
    final List<Pair<String, TreeMap<Integer, Map<Integer, Long>>>> taskSequences = new ArrayList<>();
    final List<ListenableFuture<TreeMap<Integer, Map<Integer, Long>>>> futures = new ArrayList<>();
    final List<String> taskIds = new ArrayList<>();

    for (String taskId : taskGroup.taskIds()) {
      final ListenableFuture<TreeMap<Integer, Map<Integer, Long>>> checkpointsFuture = taskClient.getCheckpointsAsync(
          taskId,
          true
      );
      taskIds.add(taskId);
      futures.add(checkpointsFuture);
    }

    try {
      List<TreeMap<Integer, Map<Integer, Long>>> futuresResult = Futures.successfulAsList(futures)
                                                                        .get(futureTimeoutInSeconds, TimeUnit.SECONDS);

      for (int i = 0; i < futuresResult.size(); i++) {
        final TreeMap<Integer, Map<Integer, Long>> checkpoints = futuresResult.get(i);
        final String taskId = taskIds.get(i);
        if (checkpoints == null) {
          try {
            // catch the exception in failed futures
            futures.get(i).get();
          }
          catch (Exception e) {
            log.error(e, "Problem while getting checkpoints for task [%s], killing the task", taskId);
            killTask(taskId);
            taskGroup.tasks.remove(taskId);
          }
        } else if (checkpoints.isEmpty()) {
          log.warn("Ignoring task [%s], as probably it is not started running yet", taskId);
        } else {
          taskSequences.add(new Pair<>(taskId, checkpoints));
        }
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    final KafkaDataSourceMetadata latestDataSourceMetadata = (KafkaDataSourceMetadata) indexerMetadataStorageCoordinator
        .getDataSourceMetadata(dataSource);
    final boolean hasValidOffsetsFromDb = latestDataSourceMetadata != null &&
                                          latestDataSourceMetadata.getKafkaPartitions() != null &&
                                          ioConfig.getTopic().equals(
                                              latestDataSourceMetadata.getKafkaPartitions().getTopic()
                                          );
    final Map<Integer, Long> latestOffsetsFromDb;
    if (hasValidOffsetsFromDb) {
      latestOffsetsFromDb = latestDataSourceMetadata.getKafkaPartitions().getPartitionOffsetMap();
    } else {
      latestOffsetsFromDb = null;
    }

    // order tasks of this taskGroup by the latest sequenceId
    taskSequences.sort((o1, o2) -> o2.rhs.firstKey().compareTo(o1.rhs.firstKey()));

    final Set<String> tasksToKill = new HashSet<>();
    final AtomicInteger earliestConsistentSequenceId = new AtomicInteger(-1);
    int taskIndex = 0;

    while (taskIndex < taskSequences.size()) {
      TreeMap<Integer, Map<Integer, Long>> taskCheckpoints = taskSequences.get(taskIndex).rhs;
      String taskId = taskSequences.get(taskIndex).lhs;
      if (earliestConsistentSequenceId.get() == -1) {
        // find the first replica task with earliest sequenceId consistent with datasource metadata in the metadata
        // store
        if (taskCheckpoints.entrySet().stream().anyMatch(
            sequenceCheckpoint -> sequenceCheckpoint.getValue().entrySet().stream().allMatch(
                partitionOffset -> Longs.compare(
                    partitionOffset.getValue(),
                    latestOffsetsFromDb == null ?
                    partitionOffset.getValue() :
                    latestOffsetsFromDb.getOrDefault(partitionOffset.getKey(), partitionOffset.getValue())
                ) == 0) && earliestConsistentSequenceId.compareAndSet(-1, sequenceCheckpoint.getKey())) || (
                pendingCompletionTaskGroups.getOrDefault(groupId, EMPTY_LIST).size() > 0
                && earliestConsistentSequenceId.compareAndSet(-1, taskCheckpoints.firstKey()))) {
          final SortedMap<Integer, Map<Integer, Long>> latestCheckpoints = new TreeMap<>(
              taskCheckpoints.tailMap(earliestConsistentSequenceId.get())
          );
          log.info("Setting taskGroup sequences to [%s] for group [%d]", latestCheckpoints, groupId);
          taskGroup.sequenceOffsets.clear();
          taskGroup.sequenceOffsets.putAll(latestCheckpoints);
        } else {
          log.debug(
              "Adding task [%s] to kill list, checkpoints[%s], latestoffsets from DB [%s]",
              taskId,
              taskCheckpoints,
              latestOffsetsFromDb
          );
          tasksToKill.add(taskId);
        }
      } else {
        // check consistency with taskGroup sequences
        if (taskCheckpoints.get(taskGroup.sequenceOffsets.firstKey()) == null
            || !(taskCheckpoints.get(taskGroup.sequenceOffsets.firstKey())
                                .equals(taskGroup.sequenceOffsets.firstEntry().getValue()))
            || taskCheckpoints.tailMap(taskGroup.sequenceOffsets.firstKey()).size()
               != taskGroup.sequenceOffsets.size()) {
          log.debug(
              "Adding task [%s] to kill list, checkpoints[%s], taskgroup checkpoints [%s]",
              taskId,
              taskCheckpoints,
              taskGroup.sequenceOffsets
          );
          tasksToKill.add(taskId);
        }
      }
      taskIndex++;
    }

    if ((tasksToKill.size() > 0 && tasksToKill.size() == taskGroup.tasks.size()) ||
        (taskGroup.tasks.size() == 0 && pendingCompletionTaskGroups.getOrDefault(groupId, EMPTY_LIST).size() == 0)) {
      // killing all tasks or no task left in the group ?
      // clear state about the taskgroup so that get latest offset information is fetched from metadata store
      log.warn("Clearing task group [%d] information as no valid tasks left the group", groupId);
      taskGroups.remove(groupId);
      partitionGroups.get(groupId).replaceAll((partition, offset) -> NOT_SET);
    }

    taskSequences.stream().filter(taskIdSequences -> tasksToKill.contains(taskIdSequences.lhs)).forEach(
        sequenceCheckpoint -> {
          log.warn(
              "Killing task [%s], as its checkpoints [%s] are not consistent with group checkpoints[%s] or latest "
              + "persisted offsets in metadata store [%s]",
              sequenceCheckpoint.lhs,
              sequenceCheckpoint.rhs,
              taskGroup.sequenceOffsets,
              latestOffsetsFromDb
          );
          killTask(sequenceCheckpoint.lhs);
          taskGroup.tasks.remove(sequenceCheckpoint.lhs);
        }
    );
  }

  private void addDiscoveredTaskToPendingCompletionTaskGroups(
      int groupId,
      String taskId,
      Map<Integer, Long> startingPartitions
  )
  {
    final CopyOnWriteArrayList<TaskGroup> taskGroupList = pendingCompletionTaskGroups.computeIfAbsent(
        groupId,
        k -> new CopyOnWriteArrayList<>()
    );
    for (TaskGroup taskGroup : taskGroupList) {
      if (taskGroup.partitionOffsets.equals(startingPartitions)) {
        if (taskGroup.tasks.putIfAbsent(taskId, new TaskData()) == null) {
          log.info("Added discovered task [%s] to existing pending task group [%s]", taskId, groupId);
        }
        return;
      }
    }

    log.info("Creating new pending completion task group [%s] for discovered task [%s]", groupId, taskId);

    // reading the minimumMessageTime & maximumMessageTime from the publishing task and setting it here is not necessary as this task cannot
    // change to a state where it will read any more events
    TaskGroup newTaskGroup = new TaskGroup(
        groupId,
        ImmutableMap.copyOf(startingPartitions),
        Optional.absent(),
        Optional.absent()
    );

    newTaskGroup.tasks.put(taskId, new TaskData());
    newTaskGroup.completionTimeout = DateTimes.nowUtc().plus(ioConfig.getCompletionTimeout());

    taskGroupList.add(newTaskGroup);
  }

  private void updateTaskStatus() throws ExecutionException, InterruptedException, TimeoutException
  {
    final List<ListenableFuture<Boolean>> futures = Lists.newArrayList();
    final List<String> futureTaskIds = Lists.newArrayList();

    // update status (and startTime if unknown) of current tasks in taskGroups
    for (TaskGroup group : taskGroups.values()) {
      for (Entry<String, TaskData> entry : group.tasks.entrySet()) {
        final String taskId = entry.getKey();
        final TaskData taskData = entry.getValue();

        if (taskData.startTime == null) {
          futureTaskIds.add(taskId);
          futures.add(
              Futures.transform(
                  taskClient.getStartTimeAsync(taskId), new Function<DateTime, Boolean>()
                  {
                    @Nullable
                    @Override
                    public Boolean apply(@Nullable DateTime startTime)
                    {
                      if (startTime == null) {
                        return false;
                      }

                      taskData.startTime = startTime;
                      long millisRemaining = ioConfig.getTaskDuration().getMillis() -
                                             (System.currentTimeMillis() - taskData.startTime.getMillis());
                      if (millisRemaining > 0) {
                        scheduledExec.schedule(
                            buildRunTask(),
                            millisRemaining + MAX_RUN_FREQUENCY_MILLIS,
                            TimeUnit.MILLISECONDS
                        );
                      }

                      return true;
                    }
                  }, workerExec
              )
          );
        }

        taskData.status = taskStorage.getStatus(taskId).get();
      }
    }

    // update status of pending completion tasks in pendingCompletionTaskGroups
    for (List<TaskGroup> taskGroups : pendingCompletionTaskGroups.values()) {
      for (TaskGroup group : taskGroups) {
        for (Entry<String, TaskData> entry : group.tasks.entrySet()) {
          entry.getValue().status = taskStorage.getStatus(entry.getKey()).get();
        }
      }
    }

    List<Boolean> results = Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
    for (int i = 0; i < results.size(); i++) {
      // false means the task hasn't started running yet and that's okay; null means it should be running but the HTTP
      // request threw an exception so kill the task
      if (results.get(i) == null) {
        String taskId = futureTaskIds.get(i);
        log.warn("Task [%s] failed to return start time, killing task", taskId);
        killTask(taskId);
      }
    }
  }

  private void checkTaskDuration() throws InterruptedException, ExecutionException, TimeoutException
  {
    final List<ListenableFuture<Map<Integer, Long>>> futures = Lists.newArrayList();
    final List<Integer> futureGroupIds = Lists.newArrayList();

    for (Entry<Integer, TaskGroup> entry : taskGroups.entrySet()) {
      Integer groupId = entry.getKey();
      TaskGroup group = entry.getValue();

      // find the longest running task from this group
      DateTime earliestTaskStart = DateTimes.nowUtc();
      for (TaskData taskData : group.tasks.values()) {
        // startTime can be null if kafkaSupervisor is stopped gracefully before processing any runNotice
        if (taskData.startTime != null && earliestTaskStart.isAfter(taskData.startTime)) {
          earliestTaskStart = taskData.startTime;
        }
      }

      // if this task has run longer than the configured duration, signal all tasks in the group to persist
      if (earliestTaskStart.plus(ioConfig.getTaskDuration()).isBeforeNow()) {
        log.info("Task group [%d] has run for [%s]", groupId, ioConfig.getTaskDuration());
        futureGroupIds.add(groupId);
        futures.add(checkpointTaskGroup(group, true));
      }
    }

    List<Map<Integer, Long>> results = Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
    for (int j = 0; j < results.size(); j++) {
      Integer groupId = futureGroupIds.get(j);
      TaskGroup group = taskGroups.get(groupId);
      Map<Integer, Long> endOffsets = results.get(j);

      if (endOffsets != null) {
        // set a timeout and put this group in pendingCompletionTaskGroups so that it can be monitored for completion
        group.completionTimeout = DateTimes.nowUtc().plus(ioConfig.getCompletionTimeout());
        pendingCompletionTaskGroups.computeIfAbsent(groupId, k -> new CopyOnWriteArrayList<>()).add(group);

        // set endOffsets as the next startOffsets
        for (Entry<Integer, Long> entry : endOffsets.entrySet()) {
          partitionGroups.get(groupId).put(entry.getKey(), entry.getValue());
        }
      } else {
        log.warn(
            "All tasks in group [%s] failed to transition to publishing state, killing tasks [%s]",
            groupId,
            group.taskIds()
        );
        for (String id : group.taskIds()) {
          killTask(id);
        }
        // clear partitionGroups, so that latest offsets from db is used as start offsets not the stale ones
        // if tasks did some successful incremental handoffs
        partitionGroups.get(groupId).replaceAll((partition, offset) -> NOT_SET);
      }

      // remove this task group from the list of current task groups now that it has been handled
      taskGroups.remove(groupId);
    }
  }

  private ListenableFuture<Map<Integer, Long>> checkpointTaskGroup(final TaskGroup taskGroup, final boolean finalize)
  {
    if (finalize) {
      // 1) Check if any task completed (in which case we're done) and kill unassigned tasks
      Iterator<Entry<String, TaskData>> i = taskGroup.tasks.entrySet().iterator();
      while (i.hasNext()) {
        Entry<String, TaskData> taskEntry = i.next();
        String taskId = taskEntry.getKey();
        TaskData task = taskEntry.getValue();

        // task.status can be null if kafkaSupervisor is stopped gracefully before processing any runNotice.
        if (task.status != null) {
          if (task.status.isSuccess()) {
            // If any task in this group has already completed, stop the rest of the tasks in the group and return.
            // This will cause us to create a new set of tasks next cycle that will start from the offsets in
            // metadata store (which will have advanced if we succeeded in publishing and will remain the same if
            // publishing failed and we need to re-ingest)
            return Futures.transform(
                stopTasksInGroup(taskGroup),
                new Function<Object, Map<Integer, Long>>()
                {
                  @Nullable
                  @Override
                  public Map<Integer, Long> apply(@Nullable Object input)
                  {
                    return null;
                  }
                }
            );
          }

          if (task.status.isRunnable()) {
            if (taskInfoProvider.getTaskLocation(taskId).equals(TaskLocation.unknown())) {
              log.info("Killing task [%s] which hasn't been assigned to a worker", taskId);
              killTask(taskId);
              i.remove();
            }
          }
        }
      }
    }

    // 2) Pause running tasks
    final List<ListenableFuture<Map<Integer, Long>>> pauseFutures = Lists.newArrayList();
    final List<String> pauseTaskIds = ImmutableList.copyOf(taskGroup.taskIds());
    for (final String taskId : pauseTaskIds) {
      pauseFutures.add(taskClient.pauseAsync(taskId));
    }

    return Futures.transform(
        Futures.successfulAsList(pauseFutures), new Function<List<Map<Integer, Long>>, Map<Integer, Long>>()
        {
          @Nullable
          @Override
          public Map<Integer, Long> apply(List<Map<Integer, Long>> input)
          {
            // 3) Build a map of the highest offset read by any task in the group for each partition
            final Map<Integer, Long> endOffsets = new HashMap<>();
            for (int i = 0; i < input.size(); i++) {
              Map<Integer, Long> result = input.get(i);

              if (result == null || result.isEmpty()) { // kill tasks that didn't return a value
                String taskId = pauseTaskIds.get(i);
                log.warn("Task [%s] failed to respond to [pause] in a timely manner, killing task", taskId);
                killTask(taskId);
                taskGroup.tasks.remove(taskId);

              } else { // otherwise build a map of the highest offsets seen
                for (Entry<Integer, Long> offset : result.entrySet()) {
                  if (!endOffsets.containsKey(offset.getKey())
                      || endOffsets.get(offset.getKey()).compareTo(offset.getValue()) < 0) {
                    endOffsets.put(offset.getKey(), offset.getValue());
                  }
                }
              }
            }

            // 4) Set the end offsets for each task to the values from step 3 and resume the tasks. All the tasks should
            //    finish reading and start publishing within a short period, depending on how in sync the tasks were.
            final List<ListenableFuture<Boolean>> setEndOffsetFutures = Lists.newArrayList();
            final List<String> setEndOffsetTaskIds = ImmutableList.copyOf(taskGroup.taskIds());

            if (setEndOffsetTaskIds.isEmpty()) {
              log.info("All tasks in taskGroup [%d] have failed, tasks will be re-created", taskGroup.groupId);
              return null;
            }

            try {

              if (endOffsets.equals(taskGroup.sequenceOffsets.lastEntry().getValue())) {
                log.warn(
                    "Checkpoint [%s] is same as the start offsets [%s] of latest sequence for the task group [%d]",
                    endOffsets,
                    taskGroup.sequenceOffsets.lastEntry().getValue(),
                    taskGroup.groupId
                );
              }

              log.info(
                  "Setting endOffsets for tasks in taskGroup [%d] to %s and resuming",
                  taskGroup.groupId,
                  endOffsets
              );
              for (final String taskId : setEndOffsetTaskIds) {
                setEndOffsetFutures.add(taskClient.setEndOffsetsAsync(taskId, endOffsets, finalize));
              }

              List<Boolean> results = Futures.successfulAsList(setEndOffsetFutures)
                                             .get(futureTimeoutInSeconds, TimeUnit.SECONDS);
              for (int i = 0; i < results.size(); i++) {
                if (results.get(i) == null || !results.get(i)) {
                  String taskId = setEndOffsetTaskIds.get(i);
                  log.warn("Task [%s] failed to respond to [set end offsets] in a timely manner, killing task", taskId);
                  killTask(taskId);
                  taskGroup.tasks.remove(taskId);
                }
              }
            }
            catch (Exception e) {
              log.error("Something bad happened [%s]", e.getMessage());
              Throwables.propagate(e);
            }

            if (taskGroup.tasks.isEmpty()) {
              log.info("All tasks in taskGroup [%d] have failed, tasks will be re-created", taskGroup.groupId);
              return null;
            }

            return endOffsets;
          }
        }, workerExec
    );
  }

  /**
   * Monitors [pendingCompletionTaskGroups] for tasks that have completed. If any task in a task group has completed, we
   * can safely stop the rest of the tasks in that group. If a task group has exceeded its publishing timeout, then
   * we need to stop all tasks in not only that task group but also 1) any subsequent task group that is also pending
   * completion and 2) the current task group that is running, because the assumption that we have handled up to the
   * starting offset for subsequent task groups is no longer valid, and subsequent tasks would fail as soon as they
   * attempted to publish because of the contiguous range consistency check.
   */
  private void checkPendingCompletionTasks() throws ExecutionException, InterruptedException, TimeoutException
  {
    List<ListenableFuture<?>> futures = Lists.newArrayList();

    for (Entry<Integer, CopyOnWriteArrayList<TaskGroup>> pendingGroupList : pendingCompletionTaskGroups.entrySet()) {

      boolean stopTasksInTaskGroup = false;
      Integer groupId = pendingGroupList.getKey();
      CopyOnWriteArrayList<TaskGroup> taskGroupList = pendingGroupList.getValue();
      List<TaskGroup> toRemove = Lists.newArrayList();

      for (TaskGroup group : taskGroupList) {
        boolean foundSuccess = false, entireTaskGroupFailed = false;

        if (stopTasksInTaskGroup) {
          // One of the earlier groups that was handling the same partition set timed out before the segments were
          // published so stop any additional groups handling the same partition set that are pending completion.
          futures.add(stopTasksInGroup(group));
          toRemove.add(group);
          continue;
        }

        Iterator<Entry<String, TaskData>> iTask = group.tasks.entrySet().iterator();
        while (iTask.hasNext()) {
          final Entry<String, TaskData> entry = iTask.next();
          final String taskId = entry.getKey();
          final TaskData taskData = entry.getValue();

          Preconditions.checkNotNull(taskData.status, "WTH? task[%s] has a null status", taskId);

          if (taskData.status.isFailure()) {
            iTask.remove(); // remove failed task
            if (group.tasks.isEmpty()) {
              // if all tasks in the group have failed, just nuke all task groups with this partition set and restart
              entireTaskGroupFailed = true;
              break;
            }
          }

          if (taskData.status.isSuccess()) {
            // If one of the pending completion tasks was successful, stop the rest of the tasks in the group as
            // we no longer need them to publish their segment.
            log.info("Task [%s] completed successfully, stopping tasks %s", taskId, group.taskIds());
            futures.add(stopTasksInGroup(group));
            foundSuccess = true;
            toRemove.add(group); // remove the TaskGroup from the list of pending completion task groups
            break; // skip iterating the rest of the tasks in this group as they've all been stopped now
          }
        }

        if ((!foundSuccess && group.completionTimeout.isBeforeNow()) || entireTaskGroupFailed) {
          if (entireTaskGroupFailed) {
            log.warn("All tasks in group [%d] failed to publish, killing all tasks for these partitions", groupId);
          } else {
            log.makeAlert(
                "No task in [%s] for taskGroup [%d] succeeded before the completion timeout elapsed [%s]!",
                group.taskIds(),
                groupId,
                ioConfig.getCompletionTimeout()
            ).emit();
          }

          // reset partitions offsets for this task group so that they will be re-read from metadata storage
          partitionGroups.get(groupId).replaceAll((partition, offset) -> NOT_SET);
          // kill all the tasks in this pending completion group
          killTasksInGroup(group);
          // set a flag so the other pending completion groups for this set of partitions will also stop
          stopTasksInTaskGroup = true;

          // kill all the tasks in the currently reading task group and remove the bad task group
          killTasksInGroup(taskGroups.remove(groupId));
          toRemove.add(group);
        }
      }

      taskGroupList.removeAll(toRemove);
    }

    // wait for all task shutdowns to complete before returning
    Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
  }

  private void checkCurrentTaskState() throws ExecutionException, InterruptedException, TimeoutException
  {
    List<ListenableFuture<?>> futures = Lists.newArrayList();
    Iterator<Entry<Integer, TaskGroup>> iTaskGroups = taskGroups.entrySet().iterator();
    while (iTaskGroups.hasNext()) {
      Entry<Integer, TaskGroup> taskGroupEntry = iTaskGroups.next();
      Integer groupId = taskGroupEntry.getKey();
      TaskGroup taskGroup = taskGroupEntry.getValue();

      // Iterate the list of known tasks in this group and:
      //   1) Kill any tasks which are not "current" (have the partitions, starting offsets, and minimumMessageTime
      //      & maximumMessageTime (if applicable) in [taskGroups])
      //   2) Remove any tasks that have failed from the list
      //   3) If any task completed successfully, stop all the tasks in this group and move to the next group

      log.debug("Task group [%d] pre-pruning: %s", groupId, taskGroup.taskIds());

      Iterator<Entry<String, TaskData>> iTasks = taskGroup.tasks.entrySet().iterator();
      while (iTasks.hasNext()) {
        Entry<String, TaskData> task = iTasks.next();
        String taskId = task.getKey();
        TaskData taskData = task.getValue();

        // stop and remove bad tasks from the task group
        if (!isTaskCurrent(groupId, taskId)) {
          log.info("Stopping task [%s] which does not match the expected offset range and ingestion spec", taskId);
          futures.add(stopTask(taskId, false));
          iTasks.remove();
          continue;
        }

        Preconditions.checkNotNull(taskData.status, "WTH? task[%s] has a null status", taskId);

        // remove failed tasks
        if (taskData.status.isFailure()) {
          iTasks.remove();
          continue;
        }

        // check for successful tasks, and if we find one, stop all tasks in the group and remove the group so it can
        // be recreated with the next set of offsets
        if (taskData.status.isSuccess()) {
          futures.add(stopTasksInGroup(taskGroup));
          iTaskGroups.remove();
          break;
        }
      }
      log.debug("Task group [%d] post-pruning: %s", groupId, taskGroup.taskIds());
    }

    // wait for all task shutdowns to complete before returning
    Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
  }

  void createNewTasks() throws JsonProcessingException
  {
    // update the checkpoints in the taskGroup to latest ones so that new tasks do not read what is already published
    verifyAndMergeCheckpoints(
        taskGroups.values()
                  .stream()
                  .filter(taskGroup -> taskGroup.tasks.size() < ioConfig.getReplicas())
                  .collect(Collectors.toList())
    );

    // check that there is a current task group for each group of partitions in [partitionGroups]
    for (Integer groupId : partitionGroups.keySet()) {
      if (!taskGroups.containsKey(groupId)) {
        log.info("Creating new task group [%d] for partitions %s", groupId, partitionGroups.get(groupId).keySet());

        Optional<DateTime> minimumMessageTime = (ioConfig.getLateMessageRejectionPeriod().isPresent() ? Optional.of(
            DateTimes.nowUtc().minus(ioConfig.getLateMessageRejectionPeriod().get())
        ) : Optional.absent());

        Optional<DateTime> maximumMessageTime = (ioConfig.getEarlyMessageRejectionPeriod().isPresent() ? Optional.of(
            DateTimes.nowUtc().plus(ioConfig.getTaskDuration()).plus(ioConfig.getEarlyMessageRejectionPeriod().get())
        ) : Optional.absent());

        final TaskGroup taskGroup = new TaskGroup(
            groupId,
            generateStartingOffsetsForPartitionGroup(groupId),
            minimumMessageTime,
            maximumMessageTime
        );
        taskGroups.put(
            groupId,
            taskGroup
        );
      }
    }

    // iterate through all the current task groups and make sure each one has the desired number of replica tasks
    boolean createdTask = false;
    for (Entry<Integer, TaskGroup> entry : taskGroups.entrySet()) {
      TaskGroup taskGroup = entry.getValue();
      Integer groupId = entry.getKey();

      if (ioConfig.getReplicas() > taskGroup.tasks.size()) {
        log.info(
            "Number of tasks [%d] does not match configured numReplicas [%d] in task group [%d], creating more tasks",
            taskGroup.tasks.size(), ioConfig.getReplicas(), groupId
        );
        createKafkaTasksForGroup(groupId, ioConfig.getReplicas() - taskGroup.tasks.size());
        createdTask = true;
      }
    }

    if (createdTask && firstRunTime.isBeforeNow()) {
      // Schedule a run event after a short delay to update our internal data structures with the new tasks that were
      // just created. This is mainly for the benefit of the status API in situations where the run period is lengthy.
      scheduledExec.schedule(buildRunTask(), 5000, TimeUnit.MILLISECONDS);
    }
  }

  private void createKafkaTasksForGroup(int groupId, int replicas) throws JsonProcessingException
  {
    Map<Integer, Long> startPartitions = taskGroups.get(groupId).partitionOffsets;
    Map<Integer, Long> endPartitions = new HashMap<>();

    for (Integer partition : startPartitions.keySet()) {
      endPartitions.put(partition, Long.MAX_VALUE);
    }
    TaskGroup group = taskGroups.get(groupId);

    Map<String, String> consumerProperties = Maps.newHashMap(ioConfig.getConsumerProperties());
    DateTime minimumMessageTime = taskGroups.get(groupId).minimumMessageTime.orNull();
    DateTime maximumMessageTime = taskGroups.get(groupId).maximumMessageTime.orNull();

    KafkaIOConfig kafkaIOConfig = new KafkaIOConfig(
        groupId,
        group.baseSequenceName,
        new KafkaPartitions(ioConfig.getTopic(), startPartitions),
        new KafkaPartitions(ioConfig.getTopic(), endPartitions),
        consumerProperties,
        true,
        minimumMessageTime,
        maximumMessageTime,
        ioConfig.isSkipOffsetGaps()
    );

    final String checkpoints = sortingMapper.writerWithType(new TypeReference<TreeMap<Integer, Map<Integer, Long>>>()
    {
    }).writeValueAsString(taskGroups.get(groupId).sequenceOffsets);
    final Map<String, Object> context = spec.getContext() == null
                                        ? ImmutableMap.of(
        "checkpoints",
        checkpoints,
        IS_INCREMENTAL_HANDOFF_SUPPORTED,
        true
    )
                                        : ImmutableMap.<String, Object>builder()
                                            .put("checkpoints", checkpoints)
                                            .put(IS_INCREMENTAL_HANDOFF_SUPPORTED, true)
                                            .putAll(spec.getContext())
                                            .build();
    for (int i = 0; i < replicas; i++) {
      String taskId = Joiner.on("_").join(group.baseSequenceName, RealtimeIndexTask.makeRandomId());
      KafkaIndexTask indexTask = new KafkaIndexTask(
          taskId,
          new TaskResource(group.baseSequenceName, 1),
          spec.getDataSchema(),
          taskTuningConfig,
          kafkaIOConfig,
          context,
          null,
          null,
          rowIngestionMetersFactory
      );

      Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
      if (taskQueue.isPresent()) {
        try {
          taskQueue.get().add(indexTask);
        }
        catch (EntryExistsException e) {
          log.error("Tried to add task [%s] but it already exists", indexTask.getId());
        }
      } else {
        log.error("Failed to get task queue because I'm not the leader!");
      }
    }
  }

  private ImmutableMap<Integer, Long> generateStartingOffsetsForPartitionGroup(int groupId)
  {
    ImmutableMap.Builder<Integer, Long> builder = ImmutableMap.builder();
    for (Entry<Integer, Long> entry : partitionGroups.get(groupId).entrySet()) {
      Integer partition = entry.getKey();
      Long offset = entry.getValue();

      if (offset != null && offset != NOT_SET) {
        // if we are given a startingOffset (set by a previous task group which is pending completion) then use it
        builder.put(partition, offset);
      } else {
        // if we don't have a startingOffset (first run or we had some previous failures and reset the offsets) then
        // get the offset from metadata storage (if available) or Kafka (otherwise)
        builder.put(partition, getOffsetFromStorageForPartition(partition));
      }
    }
    return builder.build();
  }

  /**
   * Queries the dataSource metadata table to see if there is a previous ending offset for this partition. If it doesn't
   * find any data, it will retrieve the latest or earliest Kafka offset depending on the useEarliestOffset config.
   */
  private long getOffsetFromStorageForPartition(int partition)
  {
    long offset;
    final Map<Integer, Long> metadataOffsets = getOffsetsFromMetadataStorage();
    if (metadataOffsets.get(partition) != null) {
      offset = metadataOffsets.get(partition);
      log.debug("Getting offset [%,d] from metadata storage for partition [%d]", offset, partition);

      long latestKafkaOffset = getOffsetFromKafkaForPartition(partition, false);
      if (offset > latestKafkaOffset) {
        throw new ISE(
            "Offset in metadata storage [%,d] > latest Kafka offset [%,d] for partition[%d] dataSource[%s]. If these "
            + "messages are no longer available (perhaps you deleted and re-created your Kafka topic) you can use the "
            + "supervisor reset API to restart ingestion.",
            offset,
            latestKafkaOffset,
            partition,
            dataSource
        );
      }

    } else {
      offset = getOffsetFromKafkaForPartition(partition, ioConfig.isUseEarliestOffset());
      log.debug("Getting offset [%,d] from Kafka for partition [%d]", offset, partition);
    }

    return offset;
  }

  private Map<Integer, Long> getOffsetsFromMetadataStorage()
  {
    final DataSourceMetadata dataSourceMetadata = indexerMetadataStorageCoordinator.getDataSourceMetadata(dataSource);
    if (dataSourceMetadata instanceof KafkaDataSourceMetadata) {
      KafkaPartitions partitions = ((KafkaDataSourceMetadata) dataSourceMetadata).getKafkaPartitions();
      if (partitions != null) {
        if (!ioConfig.getTopic().equals(partitions.getTopic())) {
          log.warn(
              "Topic in metadata storage [%s] doesn't match spec topic [%s], ignoring stored offsets",
              partitions.getTopic(),
              ioConfig.getTopic()
          );
          return Collections.emptyMap();
        } else if (partitions.getPartitionOffsetMap() != null) {
          return partitions.getPartitionOffsetMap();
        }
      }
    }

    return Collections.emptyMap();
  }

  private long getOffsetFromKafkaForPartition(int partition, boolean useEarliestOffset)
  {
    synchronized (consumerLock) {
      TopicPartition topicPartition = new TopicPartition(ioConfig.getTopic(), partition);
      if (!consumer.assignment().contains(topicPartition)) {
        consumer.assign(Collections.singletonList(topicPartition));
      }

      if (useEarliestOffset) {
        consumer.seekToBeginning(Collections.singletonList(topicPartition));
      } else {
        consumer.seekToEnd(Collections.singletonList(topicPartition));
      }

      return consumer.position(topicPartition);
    }
  }

  /**
   * Compares the sequence name from the task with one generated for the task's group ID and returns false if they do
   * not match. The sequence name is generated from a hash of the dataSchema, tuningConfig, starting offsets, and the
   * minimumMessageTime or maximumMessageTime if set.
   */
  private boolean isTaskCurrent(int taskGroupId, String taskId)
  {
    Optional<Task> taskOptional = taskStorage.getTask(taskId);
    if (!taskOptional.isPresent() || !(taskOptional.get() instanceof KafkaIndexTask)) {
      return false;
    }

    String taskSequenceName = ((KafkaIndexTask) taskOptional.get()).getIOConfig().getBaseSequenceName();
    if (taskGroups.get(taskGroupId) != null) {
      return Preconditions
          .checkNotNull(taskGroups.get(taskGroupId), "null taskGroup for taskId[%s]", taskGroupId)
          .baseSequenceName
          .equals(taskSequenceName);
    } else {
      return generateSequenceName(
          ((KafkaIndexTask) taskOptional.get()).getIOConfig()
                                               .getStartPartitions()
                                               .getPartitionOffsetMap(),
          ((KafkaIndexTask) taskOptional.get()).getIOConfig().getMinimumMessageTime(),
          ((KafkaIndexTask) taskOptional.get()).getIOConfig().getMaximumMessageTime()
      ).equals(taskSequenceName);
    }
  }

  private ListenableFuture<?> stopTasksInGroup(@Nullable TaskGroup taskGroup)
  {
    if (taskGroup == null) {
      return Futures.immediateFuture(null);
    }

    final List<ListenableFuture<Void>> futures = Lists.newArrayList();
    for (Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
      final String taskId = entry.getKey();
      final TaskData taskData = entry.getValue();
      if (taskData.status == null) {
        killTask(taskId);
      } else if (!taskData.status.isComplete()) {
        futures.add(stopTask(taskId, false));
      }
    }

    return Futures.successfulAsList(futures);
  }

  private ListenableFuture<Void> stopTask(final String id, final boolean publish)
  {
    return Futures.transform(
        taskClient.stopAsync(id, publish), new Function<Boolean, Void>()
        {
          @Nullable
          @Override
          public Void apply(@Nullable Boolean result)
          {
            if (result == null || !result) {
              log.info("Task [%s] failed to stop in a timely manner, killing task", id);
              killTask(id);
            }
            return null;
          }
        }
    );
  }

  private void killTask(final String id)
  {
    Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
      taskQueue.get().shutdown(id);
    } else {
      log.error("Failed to get task queue because I'm not the leader!");
    }
  }

  protected int getTaskGroupIdForPartition(int partition)
  {
    return partition % ioConfig.getTaskCount();
  }

  private boolean isTaskInPendingCompletionGroups(String taskId)
  {
    for (List<TaskGroup> taskGroups : pendingCompletionTaskGroups.values()) {
      for (TaskGroup taskGroup : taskGroups) {
        if (taskGroup.tasks.containsKey(taskId)) {
          return true;
        }
      }
    }
    return false;
  }

  private SupervisorReport<KafkaSupervisorReportPayload> generateReport(boolean includeOffsets)
  {
    int numPartitions = partitionGroups.values().stream().mapToInt(Map::size).sum();

    Map<Integer, Long> partitionLag = getLagPerPartition(getHighestCurrentOffsets());
    final KafkaSupervisorReportPayload payload = new KafkaSupervisorReportPayload(
        dataSource,
        ioConfig.getTopic(),
        numPartitions,
        ioConfig.getReplicas(),
        ioConfig.getTaskDuration().getMillis() / 1000,
        includeOffsets ? latestOffsetsFromKafka : null,
        includeOffsets ? partitionLag : null,
        includeOffsets ? partitionLag.values().stream().mapToLong(x -> Math.max(x, 0)).sum() : null,
        includeOffsets ? offsetsLastUpdated : null,
        spec.isSuspended()
    );
    SupervisorReport<KafkaSupervisorReportPayload> report = new SupervisorReport<>(
        dataSource,
        DateTimes.nowUtc(),
        payload
    );

    List<TaskReportData> taskReports = Lists.newArrayList();

    try {
      for (TaskGroup taskGroup : taskGroups.values()) {
        for (Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
          String taskId = entry.getKey();
          @Nullable
          DateTime startTime = entry.getValue().startTime;
          Map<Integer, Long> currentOffsets = entry.getValue().currentOffsets;
          Long remainingSeconds = null;
          if (startTime != null) {
            remainingSeconds = Math.max(
                0, ioConfig.getTaskDuration().getMillis() - (System.currentTimeMillis() - startTime.getMillis())
            ) / 1000;
          }

          taskReports.add(
              new TaskReportData(
                  taskId,
                  includeOffsets ? taskGroup.partitionOffsets : null,
                  includeOffsets ? currentOffsets : null,
                  startTime,
                  remainingSeconds,
                  TaskReportData.TaskType.ACTIVE,
                  includeOffsets ? getLagPerPartition(currentOffsets) : null
              )
          );
        }
      }

      for (List<TaskGroup> taskGroups : pendingCompletionTaskGroups.values()) {
        for (TaskGroup taskGroup : taskGroups) {
          for (Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
            String taskId = entry.getKey();
            @Nullable
            DateTime startTime = entry.getValue().startTime;
            Map<Integer, Long> currentOffsets = entry.getValue().currentOffsets;
            Long remainingSeconds = null;
            if (taskGroup.completionTimeout != null) {
              remainingSeconds = Math.max(0, taskGroup.completionTimeout.getMillis() - System.currentTimeMillis())
                                 / 1000;
            }

            taskReports.add(
                new TaskReportData(
                    taskId,
                    includeOffsets ? taskGroup.partitionOffsets : null,
                    includeOffsets ? currentOffsets : null,
                    startTime,
                    remainingSeconds,
                    TaskReportData.TaskType.PUBLISHING,
                    null
                )
            );
          }
        }
      }

      taskReports.forEach(payload::addTask);
    }
    catch (Exception e) {
      log.warn(e, "Failed to generate status report");
    }

    return report;
  }

  private Runnable buildRunTask()
  {
    return () -> notices.add(new RunNotice());
  }

  private void updateLatestOffsetsFromKafka()
  {
    synchronized (consumerLock) {
      final Map<String, List<PartitionInfo>> topics = consumer.listTopics();

      if (topics == null || !topics.containsKey(ioConfig.getTopic())) {
        throw new ISE("Could not retrieve partitions for topic [%s]", ioConfig.getTopic());
      }

      final Set<TopicPartition> topicPartitions = topics.get(ioConfig.getTopic())
                                                        .stream()
                                                        .map(x -> new TopicPartition(x.topic(), x.partition()))
                                                        .collect(Collectors.toSet());
      consumer.assign(topicPartitions);
      consumer.seekToEnd(topicPartitions);

      latestOffsetsFromKafka = topicPartitions
          .stream()
          .collect(Collectors.toMap(TopicPartition::partition, consumer::position));
    }
  }

  private Map<Integer, Long> getHighestCurrentOffsets()
  {
    return taskGroups
        .values()
        .stream()
        .flatMap(taskGroup -> taskGroup.tasks.entrySet().stream())
        .flatMap(taskData -> taskData.getValue().currentOffsets.entrySet().stream())
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue, Long::max));
  }

  private Map<Integer, Long> getLagPerPartition(Map<Integer, Long> currentOffsets)
  {
    return currentOffsets
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Entry::getKey,
                e -> latestOffsetsFromKafka != null
                     && latestOffsetsFromKafka.get(e.getKey()) != null
                     && e.getValue() != null
                     ? latestOffsetsFromKafka.get(e.getKey()) - e.getValue()
                     : Integer.MIN_VALUE
            )
        );
  }

  private Runnable emitLag()
  {
    return () -> {
      try {
        Map<Integer, Long> highestCurrentOffsets = getHighestCurrentOffsets();

        if (latestOffsetsFromKafka == null) {
          throw new ISE("Latest offsets from Kafka have not been fetched");
        }

        if (!latestOffsetsFromKafka.keySet().equals(highestCurrentOffsets.keySet())) {
          log.warn(
              "Lag metric: Kafka partitions %s do not match task partitions %s",
              latestOffsetsFromKafka.keySet(),
              highestCurrentOffsets.keySet()
          );
        }

        long lag = getLagPerPartition(highestCurrentOffsets)
            .values()
            .stream()
            .mapToLong(x -> Math.max(x, 0))
            .sum();

        emitter.emit(
            ServiceMetricEvent.builder().setDimension("dataSource", dataSource).build("ingest/kafka/lag", lag)
        );
      }
      catch (Exception e) {
        log.warn(e, "Unable to compute Kafka lag");
      }
    };
  }

  private void updateCurrentOffsets() throws InterruptedException, ExecutionException, TimeoutException
  {
    final List<ListenableFuture<Void>> futures = Stream.concat(
        taskGroups.values().stream().flatMap(taskGroup -> taskGroup.tasks.entrySet().stream()),
        pendingCompletionTaskGroups.values()
                                   .stream()
                                   .flatMap(List::stream)
                                   .flatMap(taskGroup -> taskGroup.tasks.entrySet().stream())
    ).map(
        task -> Futures.transform(
            taskClient.getCurrentOffsetsAsync(task.getKey(), false),
            (Function<Map<Integer, Long>, Void>) (currentOffsets) -> {

              if (currentOffsets != null && !currentOffsets.isEmpty()) {
                task.getValue().currentOffsets = currentOffsets;
              }

              return null;
            }
        )
    ).collect(Collectors.toList());

    Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  Runnable updateCurrentAndLatestOffsets()
  {
    return () -> {
      try {
        updateCurrentOffsets();
        updateLatestOffsetsFromKafka();
        offsetsLastUpdated = DateTimes.nowUtc();
      }
      catch (Exception e) {
        log.warn(e, "Exception while getting current/latest offsets");
      }
    };
  }

  /**
   * Collect row ingestion stats from all tasks managed by this supervisor.
   *
   * @return A map of groupId->taskId->task row stats
   *
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws TimeoutException
   */
  private Map<String, Map<String, Object>> getCurrentTotalStats()
      throws InterruptedException, ExecutionException, TimeoutException
  {
    Map<String, Map<String, Object>> allStats = Maps.newHashMap();
    final List<ListenableFuture<StatsFromTaskResult>> futures = new ArrayList<>();
    final List<Pair<Integer, String>> groupAndTaskIds = new ArrayList<>();

    for (int groupId : taskGroups.keySet()) {
      TaskGroup group = taskGroups.get(groupId);
      for (String taskId : group.taskIds()) {
        futures.add(
            Futures.transform(
                taskClient.getMovingAveragesAsync(taskId),
                (Function<Map<String, Object>, StatsFromTaskResult>) (currentStats) -> {
                  return new StatsFromTaskResult(
                      groupId,
                      taskId,
                      currentStats
                  );
                }
            )
        );
        groupAndTaskIds.add(new Pair<>(groupId, taskId));
      }
    }

    for (int groupId : pendingCompletionTaskGroups.keySet()) {
      TaskGroup group = taskGroups.get(groupId);
      for (String taskId : group.taskIds()) {
        futures.add(
            Futures.transform(
                taskClient.getMovingAveragesAsync(taskId),
                (Function<Map<String, Object>, StatsFromTaskResult>) (currentStats) -> {
                  return new StatsFromTaskResult(
                      groupId,
                      taskId,
                      currentStats
                  );
                }
            )
        );
        groupAndTaskIds.add(new Pair<>(groupId, taskId));
      }
    }

    List<StatsFromTaskResult> results = Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
    for (int i = 0; i < results.size(); i++) {
      StatsFromTaskResult result = results.get(i);
      if (result != null) {
        Map<String, Object> groupMap = allStats.computeIfAbsent(result.getGroupId(), k -> Maps.newHashMap());
        groupMap.put(result.getTaskId(), result.getStats());
      } else {
        Pair<Integer, String> groupAndTaskId = groupAndTaskIds.get(i);
        log.error("Failed to get stats for group[%d]-task[%s]", groupAndTaskId.lhs, groupAndTaskId.rhs);
      }
    }

    return allStats;
  }

  @VisibleForTesting
  @Nullable
  TaskGroup removeTaskGroup(int taskGroupId)
  {
    return taskGroups.remove(taskGroupId);
  }

  @VisibleForTesting
  void moveTaskGroupToPendingCompletion(int taskGroupId)
  {
    final TaskGroup taskGroup = taskGroups.remove(taskGroupId);
    if (taskGroup != null) {
      pendingCompletionTaskGroups.computeIfAbsent(taskGroupId, k -> new CopyOnWriteArrayList<>()).add(taskGroup);
    }
  }

  @VisibleForTesting
  int getNoticesQueueSize()
  {
    return notices.size();
  }

  private static class StatsFromTaskResult
  {
    private final String groupId;
    private final String taskId;
    private final Map<String, Object> stats;

    public StatsFromTaskResult(
        int groupId,
        String taskId,
        Map<String, Object> stats
    )
    {
      this.groupId = String.valueOf(groupId);
      this.taskId = taskId;
      this.stats = stats;
    }

    public String getGroupId()
    {
      return groupId;
    }

    public String getTaskId()
    {
      return taskId;
    }

    public Map<String, Object> getStats()
    {
      return stats;
    }
  }

}
