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

package org.apache.druid.indexing.kinesis.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.kinesis.KinesisDataSourceMetadata;
import org.apache.druid.indexing.kinesis.KinesisIOConfig;
import org.apache.druid.indexing.kinesis.KinesisIndexTask;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskClient;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskClientFactory;
import org.apache.druid.indexing.kinesis.KinesisPartitions;
import org.apache.druid.indexing.kinesis.KinesisRecordSupplier;
import org.apache.druid.indexing.kinesis.KinesisTuningConfig;
import org.apache.druid.indexing.kinesis.common.Record;
import org.apache.druid.indexing.kinesis.common.RecordSupplier;
import org.apache.druid.indexing.kinesis.common.SequenceNumberPlus;
import org.apache.druid.indexing.kinesis.common.StreamPartition;
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
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Supervisor responsible for managing the KafkaIndexTasks for a single dataSource. At a high level, the class accepts a
 * {@link KinesisSupervisorSpec} which includes the Kafka topic and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the Kafka topic's partitions
 * and the list of running indexing tasks and ensures that all partitions are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * Kafka offsets.
 */
public class KinesisSupervisor implements Supervisor
{
  private static final EmittingLogger log = new EmittingLogger(KinesisSupervisor.class);
  private static final Random RANDOM = new Random();
  private static final long MAX_RUN_FREQUENCY_MILLIS = 1000; // prevent us from running too often in response to events
  private static final String NOT_SET = "";
  private static final long MINIMUM_FUTURE_TIMEOUT_IN_SECONDS = 120;

  // Internal data structures
  // --------------------------------------------------------

  /**
   * A TaskGroup is the main data structure used by KinesisSupervisor to organize and monitor Kafka partitions and
   * indexing tasks. All the tasks in a TaskGroup should always be doing the same thing (reading the same partitions and
   * starting from the same offset) and if [replicas] is configured to be 1, a TaskGroup will contain a single task (the
   * exception being if the supervisor started up and discovered and adopted some already running tasks). At any given
   * time, there should only be up to a maximum of [taskCount] actively-reading task groups (tracked in the [taskGroups]
   * map) + zero or more pending-completion task groups (tracked in [pendingCompletionTaskGroups]).
   */
  private static class TaskGroup
  {
    // This specifies the partitions and starting offsets for this task group. It is set on group creation from the data
    // in [partitionGroups] and never changes during the lifetime of this task group, which will live until a task in
    // this task group has completed successfully, at which point this will be destroyed and a new task group will be
    // created with new starting offsets. This allows us to create replacement tasks for failed tasks that process the
    // same offsets, even if the values in [partitionGroups] has been changed.
    final Map<String, String> partitionOffsets;

    final ConcurrentHashMap<String, TaskData> tasks = new ConcurrentHashMap<>();
    final Optional<DateTime> minimumMessageTime;
    final Optional<DateTime> maximumMessageTime;
    final Set<String> exclusiveStartSequenceNumberPartitions;

    DateTime completionTimeout; // is set after signalTasksToFinish(); if not done by timeout, take corrective action

    public TaskGroup(
        Map<String, String> partitionOffsets,
        Optional<DateTime> minimumMessageTime,
        Optional<DateTime> maximumMessageTime,
        Set<String> exclusiveStartSequenceNumberPartitions
    )
    {
      this.partitionOffsets = partitionOffsets;
      this.minimumMessageTime = minimumMessageTime;
      this.maximumMessageTime = maximumMessageTime;
      this.exclusiveStartSequenceNumberPartitions = exclusiveStartSequenceNumberPartitions != null
                                                    ? exclusiveStartSequenceNumberPartitions
                                                    : new HashSet<>();
    }

    Set<String> taskIds()
    {
      return tasks.keySet();
    }
  }

  private static class TaskData
  {
    TaskStatus status;
    DateTime startTime;
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
  private final ConcurrentHashMap<Integer, ConcurrentHashMap<String, String>> partitionGroups = new ConcurrentHashMap<>();
  // --------------------------------------------------------

  private final TaskStorage taskStorage;
  private final TaskMaster taskMaster;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final KinesisIndexTaskClient taskClient;
  private final ObjectMapper sortingMapper;
  private final KinesisSupervisorSpec spec;
  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private final String dataSource;
  private final KinesisSupervisorIOConfig ioConfig;
  private final KinesisSupervisorTuningConfig tuningConfig;
  private final KinesisTuningConfig taskTuningConfig;
  private final String supervisorId;
  private final TaskInfoProvider taskInfoProvider;
  private final long futureTimeoutInSeconds; // how long to wait for async operations to complete
  private final RowIngestionMetersFactory rowIngestionMetersFactory;

  private final ExecutorService exec;
  private final ScheduledExecutorService scheduledExec;
  private final ListeningExecutorService workerExec;
  private final BlockingQueue<Notice> notices = new LinkedBlockingDeque<>();
  private final Object stopLock = new Object();
  private final Object stateChangeLock = new Object();

  private boolean listenerRegistered = false;
  private long lastRunTime;

  private volatile DateTime firstRunTime;
  private volatile DateTime earlyPublishTime = null;
  private volatile RecordSupplier recordSupplier;

  private volatile boolean started = false;
  private volatile boolean stopped = false;

  private final ScheduledExecutorService metricEmittingExec;
  // used while reporting lag
  private final Map<String, String> lastCurrentOffsets = new HashMap<>();

  private final List<String> partitionIds = new CopyOnWriteArrayList<>();
  private final Set<String> subsequentlyDiscoveredPartitions = new HashSet<>();

  public KinesisSupervisor(
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final KinesisIndexTaskClientFactory taskClientFactory,
      final ObjectMapper mapper,
      final KinesisSupervisorSpec spec,
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
    this.taskTuningConfig = KinesisTuningConfig.copyOf(this.tuningConfig);
    this.supervisorId = String.format("KinesisSupervisor-%s", dataSource);
    this.exec = Execs.singleThreaded(supervisorId);
    this.scheduledExec = Execs.scheduledSingleThreaded(supervisorId + "-Scheduler-%d");
    this.metricEmittingExec = Execs.scheduledSingleThreaded(supervisorId + "-Emitter-%d");

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
              taskRunner.get().getRunningTasks(), new Predicate<TaskRunnerWorkItem>()
              {
                @Override
                public boolean apply(TaskRunnerWorkItem taskRunnerWorkItem)
                {
                  return id.equals(taskRunnerWorkItem.getTaskId());
                }
              }
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
                                         + KinesisIndexTaskClient.MAX_RETRY_WAIT_SECONDS)
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
        setupRecordSupplier();

        exec.submit(
            new Runnable()
            {
              @Override
              public void run()
              {
                try {
                  while (!Thread.currentThread().isInterrupted()) {
                    final Notice notice = notices.take();

                    try {
                      notice.handle();
                    }
                    catch (Throwable e) {
                      log.makeAlert(e, "KinesisSupervisor[%s] failed to handle notice", dataSource)
                         .addData("noticeClass", notice.getClass().getSimpleName())
                         .emit();
                    }
                  }
                }
                catch (InterruptedException e) {
                  log.info("KinesisSupervisor[%s] interrupted, exiting", dataSource);
                }
              }
            }
        );
        firstRunTime = DateTime.now().plus(ioConfig.getStartDelay());
        scheduledExec.scheduleAtFixedRate(
            buildRunTask(),
            ioConfig.getStartDelay().getMillis(),
            Math.max(ioConfig.getPeriod().getMillis(), MAX_RUN_FREQUENCY_MILLIS),
            TimeUnit.MILLISECONDS
        );

// TODO: Implement this for Kinesis which uses approximate time from latest instead of offset lag
/*
        metricEmittingExec.scheduleAtFixedRate(
            computeAndEmitLag(taskClient),
            ioConfig.getStartDelay().getMillis() + 10000, // wait for tasks to start up
            Math.max(monitorSchedulerConfig.getEmitterPeriod().getMillis(), 60 * 1000),
            TimeUnit.MILLISECONDS
        );
*/
        started = true;
        log.info(
            "Started KinesisSupervisor[%s], first run in [%s], with spec: [%s]",
            dataSource,
            ioConfig.getStartDelay(),
            spec.toString()
        );
      }
      catch (Exception e) {
        if (recordSupplier != null) {
          recordSupplier.close();
        }
        log.makeAlert(e, "Exception starting KinesisSupervisor[%s]", dataSource)
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

      log.info("Beginning shutdown of KinesisSupervisor[%s]", dataSource);

      try {
        scheduledExec.shutdownNow(); // stop recurring executions
        metricEmittingExec.shutdownNow();
        recordSupplier.close(); // aborts any in-flight sequenceNumber fetches

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

        log.info("KinesisSupervisor[%s] has stopped", dataSource);
      }
      catch (Exception e) {
        log.makeAlert(e, "Exception stopping KinesisSupervisor[%s]", dataSource)
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
    // not supported right now
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
    void handle() throws ExecutionException, InterruptedException, TimeoutException;
  }

  private class RunNotice implements Notice
  {
    @Override
    public void handle() throws ExecutionException, InterruptedException, TimeoutException
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
      recordSupplier.close();

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
      log.makeAlert("Resetting dataSource [%s]", dataSource).emit();
      resetInternal(dataSourceMetadata);
    }
  }

  @VisibleForTesting
  void resetInternal(DataSourceMetadata dataSourceMetadata)
  {
    if (dataSourceMetadata == null) {
      // Reset everything
      boolean result = indexerMetadataStorageCoordinator.deleteDataSourceMetadata(dataSource);
      log.info("Reset dataSource[%s] - dataSource metadata entry deleted? [%s]", dataSource, result);
      killTaskGroupForPartitions(partitionIds);
    } else if (!(dataSourceMetadata instanceof KinesisDataSourceMetadata)) {
      throw new IAE("Expected KinesisDataSourceMetadata but found instance of [%s]", dataSourceMetadata.getClass());
    } else {
      // Reset only the partitions in dataSourceMetadata if it has not been reset yet
      final KinesisDataSourceMetadata resetKafkaMetadata = (KinesisDataSourceMetadata) dataSourceMetadata;

      if (resetKafkaMetadata.getKinesisPartitions().getStream().equals(ioConfig.getStream())) {
        // metadata can be null
        final DataSourceMetadata metadata = indexerMetadataStorageCoordinator.getDataSourceMetadata(dataSource);
        if (metadata != null && !(metadata instanceof KinesisDataSourceMetadata)) {
          throw new IAE(
              "Expected KinesisDataSourceMetadata from metadata store but found instance of [%s]",
              metadata.getClass()
          );
        }
        final KinesisDataSourceMetadata currentMetadata = (KinesisDataSourceMetadata) metadata;

        // defend against consecutive reset requests from replicas
        // as well as the case where the metadata store do not have an entry for the reset partitions
        boolean doReset = false;
        for (Map.Entry<String, String> resetPartitionOffset : resetKafkaMetadata.getKinesisPartitions()
                                                                                .getPartitionSequenceNumberMap()
                                                                                .entrySet()) {
          final String partitionOffsetInMetadataStore = currentMetadata == null
                                                        ? null
                                                        : currentMetadata.getKinesisPartitions()
                                                                         .getPartitionSequenceNumberMap()
                                                                         .get(resetPartitionOffset.getKey());
          final TaskGroup partitionTaskGroup = taskGroups.get(getTaskGroupIdForPartition(resetPartitionOffset.getKey()));
          if (partitionOffsetInMetadataStore != null ||
              (partitionTaskGroup != null && partitionTaskGroup.partitionOffsets.get(resetPartitionOffset.getKey())
                                                                                .equals(resetPartitionOffset.getValue()))) {
            doReset = true;
            break;
          }
        }

        if (!doReset) {
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
          killTaskGroupForPartitions(
              resetKafkaMetadata.getKinesisPartitions()
                                .getPartitionSequenceNumberMap()
                                .keySet()
          );
        } else {
          throw new ISE("Unable to reset metadata");
        }
      } else {
        log.warn(
            "Reset metadata topic [%s] and supervisor's topic [%s] do not match",
            resetKafkaMetadata.getKinesisPartitions().getStream(),
            ioConfig.getStream()
        );
      }
    }
  }

  private void killTaskGroupForPartitions(Collection<String> partitions)
  {
    for (String partition : partitions) {
      TaskGroup taskGroup = taskGroups.get(getTaskGroupIdForPartition(partition));
      if (taskGroup != null) {
        // kill all tasks in this task group
        for (String taskId : taskGroup.tasks.keySet()) {
          log.info("Reset dataSource[%s] - killing task [%s]", dataSource, taskId);
          killTask(taskId);
        }
      }
      partitionGroups.remove(getTaskGroupIdForPartition(partition));
      taskGroups.remove(getTaskGroupIdForPartition(partition));
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
      for (Map.Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
        if (taskInfoProvider.getTaskLocation(entry.getKey()).equals(TaskLocation.unknown())) {
          killTask(entry.getKey());
        } else {
          entry.getValue().startTime = new DateTime(0);
        }
      }
    }

    checkTaskDuration();
  }

  @VisibleForTesting
  void runInternal() throws ExecutionException, InterruptedException, TimeoutException
  {
    possiblyRegisterListener();
    updatePartitionDataFromKinesis();
    discoverTasks();
    updateTaskStatus();
    checkTaskDuration();
    checkPendingCompletionTasks();
    checkCurrentTaskState();

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

  @VisibleForTesting
  String generateSequenceName(int groupId)
  {
    StringBuilder sb = new StringBuilder();
    Map<String, String> startPartitions = taskGroups.get(groupId).partitionOffsets;

    for (Map.Entry<String, String> entry : startPartitions.entrySet()) {
      sb.append(String.format("+%s(%s)", entry.getKey(), entry.getValue()));
    }
    String partitionOffsetStr = sb.toString().substring(1);

    Optional<DateTime> minimumMessageTime = taskGroups.get(groupId).minimumMessageTime;
    Optional<DateTime> maximumMessageTime = taskGroups.get(groupId).maximumMessageTime;
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

    String hashCode = DigestUtils.sha1Hex(dataSchema + tuningConfig + partitionOffsetStr + minMsgTimeStr + maxMsgTimeStr)
                                 .substring(0, 15);

    return Joiner.on("_").join("index_kinesis", dataSource, hashCode);
  }

  private static String getRandomId()
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Ints.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((RANDOM.nextInt() >>> (i * 4)) & 0x0F)));
    }
    return suffix.toString();
  }

  private RecordSupplier setupRecordSupplier()
  {
    if (recordSupplier == null) {
      recordSupplier = new KinesisRecordSupplier(
          ioConfig.getEndpoint(),
          ioConfig.getAwsAccessKeyId(),
          ioConfig.getAwsSecretAccessKey(),
          ioConfig.getRecordsPerFetch(),
          ioConfig.getFetchDelayMillis(),
          1,
          ioConfig.getAwsAssumedRoleArn(),
          ioConfig.getAwsExternalId(),
          ioConfig.isDeaggregate(),
          taskTuningConfig.getRecordBufferSize(),
          taskTuningConfig.getRecordBufferOfferTimeout(),
          taskTuningConfig.getRecordBufferFullWait(),
          taskTuningConfig.getFetchSequenceNumberTimeout()
      );
    }

    return recordSupplier;
  }

  private void updatePartitionDataFromKinesis()
  {
    Set<String> partitionIds = recordSupplier.getPartitionIds(ioConfig.getStream());

    if (partitionIds == null) {
      log.warn("Could not fetch partition IDs for stream[%s]", ioConfig.getStream());
      return;
    }

    log.debug("Found [%d] Kinesis partitions for stream [%s]", partitionIds.size(), ioConfig.getStream());

    Set<String> closedPartitions = getOffsetsFromMetadataStorage()
        .entrySet()
        .stream()
        .filter(x -> Record.END_OF_SHARD_MARKER.equals(x.getValue()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());

    boolean initialPartitionDiscovery = this.partitionIds.isEmpty();
    for (String partition : partitionIds) {
      if (closedPartitions.contains(partition)) {
        continue;
      }

      if (!initialPartitionDiscovery && !this.partitionIds.contains(partition)) {
        subsequentlyDiscoveredPartitions.add(partition);

        if (earlyPublishTime == null) {
          for (TaskGroup taskGroup : taskGroups.values()) {
            if (!taskGroup.taskIds().isEmpty()) {
              // a new partition was added and we are managing active tasks - set an early publish time 2 minutes in the
              // future to give things time to settle

              earlyPublishTime = DateTime.now().plusMinutes(2);
              log.info("New partition discovered - requesting early publish in 2 minutes [%s]", earlyPublishTime);
              break;
            }
          }
        }
      }

      int taskGroupId = getTaskGroupIdForPartition(partition);
      partitionGroups.putIfAbsent(taskGroupId, new ConcurrentHashMap<>());

      ConcurrentHashMap<String, String> partitionMap = partitionGroups.get(taskGroupId);

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
            "New partition [%s] discovered for topic [%s], added to task group [%d]",
            partition,
            ioConfig.getStream(),
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

    for (Task task : tasks) {
      if (!(task instanceof KinesisIndexTask) || !dataSource.equals(task.getDataSource())) {
        continue;
      }

      taskCount++;
      final KinesisIndexTask kinesisTask = (KinesisIndexTask) task;
      final String taskId = task.getId();

      // Determine which task group this task belongs to based on one of the partitions handled by this task. If we
      // later determine that this task is actively reading, we will make sure that it matches our current partition
      // allocation (getTaskGroupIdForPartition(partition) should return the same value for every partition being read
      // by this task) and kill it if it is not compatible. If the task is instead found to be in the publishing
      // state, we will permit it to complete even if it doesn't match our current partition allocation to support
      // seamless schema migration.

      Iterator<String> it = kinesisTask.getIOConfig()
                                       .getStartPartitions()
                                       .getPartitionSequenceNumberMap()
                                       .keySet()
                                       .iterator();
      final Integer taskGroupId = (it.hasNext() ? getTaskGroupIdForPartition(it.next()) : null);

      if (taskGroupId != null) {
        // check to see if we already know about this task, either in [taskGroups] or in [pendingCompletionTaskGroups]
        // and if not add it to taskGroups or pendingCompletionTaskGroups (if status = PUBLISHING)
        TaskGroup taskGroup = taskGroups.get(taskGroupId);
        if (!isTaskInPendingCompletionGroups(taskId) && (taskGroup == null || !taskGroup.tasks.containsKey(taskId))) {

          futureTaskIds.add(taskId);
          futures.add(
              Futures.transform(
                  taskClient.getStatusAsync(taskId), new Function<KinesisIndexTask.Status, Boolean>()
                  {
                    @Override
                    public Boolean apply(KinesisIndexTask.Status status)
                    {
                      if (status == KinesisIndexTask.Status.PUBLISHING) {
                        addDiscoveredTaskToPendingCompletionTaskGroups(
                            taskGroupId,
                            taskId,
                            kinesisTask.getIOConfig()
                                       .getStartPartitions()
                                       .getPartitionSequenceNumberMap()
                        );

                        // update partitionGroups with the publishing task's offsets (if they are greater than what is
                        // existing) so that the next tasks will start reading from where this task left off
                        Map<String, String> publishingTaskCurrentOffsets = taskClient.getCurrentOffsets(taskId, true);

                        for (Map.Entry<String, String> entry : publishingTaskCurrentOffsets.entrySet()) {
                          String partition = entry.getKey();
                          String offset = entry.getValue();
                          ConcurrentHashMap<String, String> partitionOffsets = partitionGroups.get(
                              getTaskGroupIdForPartition(partition)
                          );

                          boolean succeeded;
                          do {
                            succeeded = true;
                            String previousOffset = partitionOffsets.putIfAbsent(partition, offset);
                            if (previousOffset != null && previousOffset.compareTo(offset) < 0) {
                              succeeded = partitionOffsets.replace(partition, previousOffset, offset);
                            }
                          } while (!succeeded);
                        }

                      } else {
                        for (String partition : kinesisTask.getIOConfig()
                                                           .getStartPartitions()
                                                           .getPartitionSequenceNumberMap()
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

                        if (taskGroups.putIfAbsent(
                            taskGroupId,
                            new TaskGroup(
                                ImmutableMap.copyOf(
                                    kinesisTask.getIOConfig().getStartPartitions().getPartitionSequenceNumberMap()
                                ),
                                kinesisTask.getIOConfig().getMinimumMessageTime(),
                                kinesisTask.getIOConfig().getMaximumMessageTime(),
                                kinesisTask.getIOConfig().getExclusiveStartSequenceNumberPartitions()
                            )
                        ) == null) {
                          log.debug("Created new task group [%d]", taskGroupId);
                        }

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
                          taskGroups.get(taskGroupId).tasks.putIfAbsent(taskId, new TaskData());
                        }
                      }
                      return true;
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
  }

  private void addDiscoveredTaskToPendingCompletionTaskGroups(
      int groupId,
      String taskId,
      Map<String, String> startingPartitions
  )
  {
    pendingCompletionTaskGroups.putIfAbsent(groupId, Lists.<TaskGroup>newCopyOnWriteArrayList());

    CopyOnWriteArrayList<TaskGroup> taskGroupList = pendingCompletionTaskGroups.get(groupId);
    for (TaskGroup taskGroup : taskGroupList) {
      if (taskGroup.partitionOffsets.equals(startingPartitions)) {
        if (taskGroup.tasks.putIfAbsent(taskId, new TaskData()) == null) {
          log.info("Added discovered task [%s] to existing pending task group", taskId);
        }
        return;
      }
    }

    log.info("Creating new pending completion task group for discovered task [%s]", taskId);

    // reading the minimumMessageTime & maximumMessageTime from the publishing task and setting it here is not necessary as this task cannot
    // change to a state where it will read any more events
    TaskGroup newTaskGroup = new TaskGroup(ImmutableMap.copyOf(startingPartitions), Optional.<DateTime>absent(), Optional.<DateTime>absent(), null);

    newTaskGroup.tasks.put(taskId, new TaskData());
    newTaskGroup.completionTimeout = DateTime.now().plus(ioConfig.getCompletionTimeout());

    taskGroupList.add(newTaskGroup);
  }

  private void updateTaskStatus() throws ExecutionException, InterruptedException, TimeoutException
  {
    final List<ListenableFuture<Boolean>> futures = Lists.newArrayList();
    final List<String> futureTaskIds = Lists.newArrayList();

    // update status (and startTime if unknown) of current tasks in taskGroups
    for (TaskGroup group : taskGroups.values()) {
      for (Map.Entry<String, TaskData> entry : group.tasks.entrySet()) {
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
                      long millisRemaining = ioConfig.getTaskDuration().getMillis() - (System.currentTimeMillis()
                                                                                       - taskData.startTime.getMillis());
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
        for (Map.Entry<String, TaskData> entry : group.tasks.entrySet()) {
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

  private void checkTaskDuration()
      throws InterruptedException, ExecutionException, TimeoutException
  {
    final List<ListenableFuture<Map<String, String>>> futures = Lists.newArrayList();
    final List<Integer> futureGroupIds = Lists.newArrayList();

    for (Map.Entry<Integer, TaskGroup> entry : taskGroups.entrySet()) {
      Integer groupId = entry.getKey();
      TaskGroup group = entry.getValue();

      // find the longest running task from this group
      DateTime earliestTaskStart = DateTime.now();
      for (TaskData taskData : group.tasks.values()) {
        if (earliestTaskStart.isAfter(taskData.startTime)) {
          earliestTaskStart = taskData.startTime;
        }
      }

      boolean doEarlyPublish = false;
      if (earlyPublishTime != null && (earlyPublishTime.isBeforeNow() || earlyPublishTime.isEqualNow())) {
        log.info("Early publish requested - signalling tasks to publish");

        earlyPublishTime = null;
        doEarlyPublish = true;
      }

      // if this task has run longer than the configured duration, signal all tasks in the group to persist
      if (earliestTaskStart.plus(ioConfig.getTaskDuration()).isBeforeNow() || doEarlyPublish) {
        log.info("Task group [%d] has run for [%s]", groupId, ioConfig.getTaskDuration());
        futureGroupIds.add(groupId);
        futures.add(signalTasksToFinish(groupId));
      }
    }

    List<Map<String, String>> results = Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
    for (int j = 0; j < results.size(); j++) {
      Integer groupId = futureGroupIds.get(j);
      TaskGroup group = taskGroups.get(groupId);
      Map<String, String> endOffsets = results.get(j);

      if (endOffsets != null) {
        // set a timeout and put this group in pendingCompletionTaskGroups so that it can be monitored for completion
        group.completionTimeout = DateTime.now().plus(ioConfig.getCompletionTimeout());
        pendingCompletionTaskGroups.putIfAbsent(groupId, Lists.<TaskGroup>newCopyOnWriteArrayList());
        pendingCompletionTaskGroups.get(groupId).add(group);

        // set endOffsets as the next startOffsets
        for (Map.Entry<String, String> entry : endOffsets.entrySet()) {
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
      }

      // remove this task group from the list of current task groups now that it has been handled
      taskGroups.remove(groupId);
    }
  }

  private ListenableFuture<Map<String, String>> signalTasksToFinish(final int groupId)
  {
    final TaskGroup taskGroup = taskGroups.get(groupId);

    // 1) Check if any task completed (in which case we're done) and kill unassigned tasks
    Iterator<Map.Entry<String, TaskData>> i = taskGroup.tasks.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry<String, TaskData> taskEntry = i.next();
      String taskId = taskEntry.getKey();
      TaskData task = taskEntry.getValue();

      if (task.status.isSuccess()) {
        // If any task in this group has already completed, stop the rest of the tasks in the group and return.
        // This will cause us to create a new set of tasks next cycle that will start from the offsets in
        // metadata store (which will have advanced if we succeeded in publishing and will remain the same if publishing
        // failed and we need to re-ingest)
        return Futures.transform(
            stopTasksInGroup(taskGroup), new Function<Object, Map<String, String>>()
            {
              @Nullable
              @Override
              public Map<String, String> apply(@Nullable Object input)
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

    // 2) Pause running tasks
    final List<ListenableFuture<Map<String, String>>> pauseFutures = Lists.newArrayList();
    final List<String> pauseTaskIds = ImmutableList.copyOf(taskGroup.taskIds());
    for (final String taskId : pauseTaskIds) {
      pauseFutures.add(taskClient.pauseAsync(taskId));
    }

    return Futures.transform(
        Futures.successfulAsList(pauseFutures), new Function<List<Map<String, String>>, Map<String, String>>()
        {
          @Nullable
          @Override
          public Map<String, String> apply(List<Map<String, String>> input)
          {
            // 3) Build a map of the highest offset read by any task in the group for each partition
            final Map<String, String> endOffsets = new HashMap<>();
            for (int i = 0; i < input.size(); i++) {
              Map<String, String> result = input.get(i);

              if (result == null || result.isEmpty()) { // kill tasks that didn't return a value
                String taskId = pauseTaskIds.get(i);
                log.warn("Task [%s] failed to respond to [pause] in a timely manner, killing task", taskId);
                killTask(taskId);
                taskGroup.tasks.remove(taskId);

              } else { // otherwise build a map of the highest offsets seen
                for (Map.Entry<String, String> offset : result.entrySet()) {
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
              log.info("All tasks in taskGroup [%d] have failed, tasks will be re-created", groupId);
              return null;
            }

            log.info("Setting endOffsets for tasks in taskGroup [%d] to %s and resuming", groupId, endOffsets);
            for (final String taskId : setEndOffsetTaskIds) {
              setEndOffsetFutures.add(taskClient.setEndOffsetsAsync(taskId, endOffsets, true));
            }

            try {
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
              Throwables.propagate(e);
            }

            if (taskGroup.tasks.isEmpty()) {
              log.info("All tasks in taskGroup [%d] have failed, tasks will be re-created", groupId);
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

    for (Map.Entry<Integer, CopyOnWriteArrayList<TaskGroup>> pendingGroupList : pendingCompletionTaskGroups.entrySet()) {

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

        Iterator<Map.Entry<String, TaskData>> iTask = group.tasks.entrySet().iterator();
        while (iTask.hasNext()) {
          Map.Entry<String, TaskData> task = iTask.next();

          if (task.getValue().status.isFailure()) {
            iTask.remove(); // remove failed task
            if (group.tasks.isEmpty()) {
              // if all tasks in the group have failed, just nuke all task groups with this partition set and restart
              entireTaskGroupFailed = true;
              break;
            }
          }

          if (task.getValue().status.isSuccess()) {
            // If one of the pending completion tasks was successful, stop the rest of the tasks in the group as
            // we no longer need them to publish their segment.
            log.info("Task [%s] completed successfully, stopping tasks %s", task.getKey(), group.taskIds());
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
                "No task in [%s] succeeded before the completion timeout elapsed [%s]!",
                group.taskIds(),
                ioConfig.getCompletionTimeout()
            ).emit();
          }

          // reset partitions offsets for this task group so that they will be re-read from metadata storage
          partitionGroups.remove(groupId);

          // stop all the tasks in this pending completion group
          futures.add(stopTasksInGroup(group));

          // set a flag so the other pending completion groups for this set of partitions will also stop
          stopTasksInTaskGroup = true;

          // stop all the tasks in the currently reading task group and remove the bad task group
          futures.add(stopTasksInGroup(taskGroups.remove(groupId)));

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
    Iterator<Map.Entry<Integer, TaskGroup>> iTaskGroups = taskGroups.entrySet().iterator();
    while (iTaskGroups.hasNext()) {
      Map.Entry<Integer, TaskGroup> taskGroupEntry = iTaskGroups.next();
      Integer groupId = taskGroupEntry.getKey();
      TaskGroup taskGroup = taskGroupEntry.getValue();

      // Iterate the list of known tasks in this group and:
      //   1) Kill any tasks which are not "current" (have the partitions, starting offsets, and minimumMessageTime & maximumMessageTime
      //      (if applicable) in [taskGroups])
      //   2) Remove any tasks that have failed from the list
      //   3) If any task completed successfully, stop all the tasks in this group and move to the next group

      log.debug("Task group [%d] pre-pruning: %s", groupId, taskGroup.taskIds());

      Iterator<Map.Entry<String, TaskData>> iTasks = taskGroup.tasks.entrySet().iterator();
      while (iTasks.hasNext()) {
        Map.Entry<String, TaskData> task = iTasks.next();
        String taskId = task.getKey();
        TaskData taskData = task.getValue();

        // stop and remove bad tasks from the task group
        if (!isTaskCurrent(groupId, taskId)) {
          log.info("Stopping task [%s] which does not match the expected offset range and ingestion spec", taskId);
          futures.add(stopTask(taskId, false));
          iTasks.remove();
          continue;
        }

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

  void createNewTasks()
  {
    // check that there is a current task group for each group of partitions in [partitionGroups]
    for (Integer groupId : partitionGroups.keySet()) {
      if (!taskGroups.containsKey(groupId)) {
        log.info(
            "Creating new task group [%d] for partitions %s",
            groupId,
            partitionGroups.get(groupId).keySet()
        );

        Optional<DateTime> minimumMessageTime = (ioConfig.getLateMessageRejectionPeriod().isPresent() ? Optional.of(
            DateTime.now().minus(ioConfig.getLateMessageRejectionPeriod().get())
        ) : Optional.<DateTime>absent());

        Optional<DateTime> maximumMessageTime = (ioConfig.getEarlyMessageRejectionPeriod().isPresent() ? Optional.of(
            DateTime.now().plus(ioConfig.getEarlyMessageRejectionPeriod().get())
        ) : Optional.<DateTime>absent());

        try {
          Map<String, SequenceNumberPlus> startingOffsets = generateStartingOffsetsForPartitionGroup(groupId);

          Map<String, String> simpleStartingOffsets = startingOffsets
              .entrySet().stream()
              .filter(x -> x.getValue().get() != null)
              .collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().get()));

          Set<String> exclusiveStartSequenceNumberPartitions = startingOffsets
              .entrySet().stream()
              .filter(x -> x.getValue().get() != null && x.getValue().isExclusive())
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet());

          taskGroups.put(
              groupId,
              new TaskGroup(
                  simpleStartingOffsets,
                  minimumMessageTime,
                  maximumMessageTime,
                  exclusiveStartSequenceNumberPartitions
              )
          );
        }
        catch (TimeoutException e) {
          log.warn(
              e,
              "Timeout while fetching sequence numbers - if you are reading from the latest sequence number, you need to write events to the stream before the sequence number can be determined"
          );
        }
      }
    }

    // iterate through all the current task groups and make sure each one has the desired number of replica tasks
    boolean createdTask = false;
    for (Map.Entry<Integer, TaskGroup> entry : taskGroups.entrySet()) {
      TaskGroup taskGroup = entry.getValue();
      Integer groupId = entry.getKey();

      if (taskGroup.partitionOffsets == null || taskGroup.partitionOffsets
          .values().stream().allMatch(x -> x == null || Record.END_OF_SHARD_MARKER.equals(x))) {
        log.debug("Nothing to read in any partition for taskGroup [%d], skipping task creation", groupId);
        continue;
      }

      if (ioConfig.getReplicas() > taskGroup.tasks.size()) {
        log.info(
            "Number of tasks [%d] does not match configured numReplicas [%d] in task group [%d], creating more tasks",
            taskGroup.tasks.size(), ioConfig.getReplicas(), groupId
        );
        createKinesisTasksForGroup(groupId, ioConfig.getReplicas() - taskGroup.tasks.size());
        createdTask = true;
      }
    }

    if (createdTask && firstRunTime.isBeforeNow()) {
      // Schedule a run event after a short delay to update our internal data structures with the new tasks that were
      // just created. This is mainly for the benefit of the status API in situations where the run period is lengthy.
      scheduledExec.schedule(buildRunTask(), 5000, TimeUnit.MILLISECONDS);
    }
  }

  private void createKinesisTasksForGroup(int groupId, int replicas)
  {
    Map<String, String> startPartitions = taskGroups.get(groupId).partitionOffsets;
    Map<String, String> endPartitions = new HashMap<>();
    Set<String> exclusiveStartSequenceNumberPartitions = taskGroups.get(groupId).exclusiveStartSequenceNumberPartitions;
    for (String partition : startPartitions.keySet()) {
      endPartitions.put(partition, KinesisPartitions.NO_END_SEQUENCE_NUMBER);
    }

    String sequenceName = generateSequenceName(groupId);

    DateTime minimumMessageTime = taskGroups.get(groupId).minimumMessageTime.orNull();
    DateTime maximumMessageTime = taskGroups.get(groupId).maximumMessageTime.orNull();

    KinesisIOConfig kinesisIOConfig = new KinesisIOConfig(
        sequenceName,
        new KinesisPartitions(ioConfig.getStream(), startPartitions),
        new KinesisPartitions(ioConfig.getStream(), endPartitions),
        true,
        true, // should pause after reading otherwise the task may complete early which will confuse the supervisor
        minimumMessageTime,
        maximumMessageTime,
        ioConfig.getEndpoint(),
        ioConfig.getRecordsPerFetch(),
        ioConfig.getFetchDelayMillis(),
        ioConfig.getAwsAccessKeyId(),
        ioConfig.getAwsSecretAccessKey(),
        exclusiveStartSequenceNumberPartitions,
        ioConfig.getAwsAssumedRoleArn(),
        ioConfig.getAwsExternalId(),
        ioConfig.isDeaggregate()
    );

    for (int i = 0; i < replicas; i++) {
      String taskId = Joiner.on("_").join(sequenceName, getRandomId());
      KinesisIndexTask indexTask = new KinesisIndexTask(
          taskId,
          new TaskResource(sequenceName, 1),
          spec.getDataSchema(),
          taskTuningConfig,
          kinesisIOConfig,
          spec.getContext(),
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

  private ImmutableMap<String, SequenceNumberPlus> generateStartingOffsetsForPartitionGroup(int groupId)
      throws TimeoutException
  {
    ImmutableMap.Builder<String, SequenceNumberPlus> builder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : partitionGroups.get(groupId).entrySet()) {
      String partition = entry.getKey();
      String offset = entry.getValue();

      if (offset != null && !NOT_SET.equals(offset)) {
        // if we are given a startingOffset (set by a previous task group which is pending completion) then use it
        if (!Record.END_OF_SHARD_MARKER.equals(offset)) {
          builder.put(partition, SequenceNumberPlus.of(offset, true));
        }
      } else {
        // if we don't have a startingOffset (first run or we had some previous failures and reset the offsets) then
        // get the offset from metadata storage (if available) or Kafka (otherwise)

        SequenceNumberPlus offsetFromStorage = getOffsetFromStorageForPartition(partition);

        if (offsetFromStorage != null && !Record.END_OF_SHARD_MARKER.equals(offsetFromStorage.get())) {
          builder.put(partition, offsetFromStorage);
        }
      }
    }
    return builder.build();
  }

  /**
   * Queries the dataSource metadata table to see if there is a previous ending offset for this partition. If it doesn't
   * find any data, it will retrieve the latest or earliest Kinesis sequence number depending on the
   * useEarliestSequenceNumber config.
   */
  private SequenceNumberPlus getOffsetFromStorageForPartition(String partition) throws TimeoutException
  {
    Map<String, String> metadataOffsets = getOffsetsFromMetadataStorage();
    String offset = metadataOffsets.get(partition);

    if (offset != null) {
      log.debug("Getting sequence number [%s] from metadata storage for partition [%s]", offset, partition);

      if (!tuningConfig.isSkipSequenceNumberAvailabilityCheck()) {
        final StreamPartition streamPartition = StreamPartition.of(ioConfig.getStream(), partition);
        try {
          String earliestSequenceNumber = recordSupplier.getEarliestSequenceNumber(streamPartition);
          if (earliestSequenceNumber == null || earliestSequenceNumber.compareTo(offset) > 0) {
            if (tuningConfig.isResetOffsetAutomatically()) {
              resetInternal(
                  new KinesisDataSourceMetadata(
                      new KinesisPartitions(ioConfig.getStream(), ImmutableMap.of(partition, offset))
                  )
              );
              throw new ISE(
                  "Previous sequenceNumber [%s] is no longer available for partition [%s] (earliest: [%s]) - automatically resetting offset",
                  offset,
                  partition,
                  earliestSequenceNumber
              );

            } else {
              throw new ISE(
                  "Previous sequenceNumber [%s] is no longer available for partition [%s] (earliest: [%s]). You can clear the previous sequenceNumber and start reading from a valid message by using the supervisor's reset API.",
                  offset,
                  partition,
                  earliestSequenceNumber
              );
            }
          }
        }
        catch (TimeoutException e) {
          throw new ISE(e, "Timeout while fetching earliest sequence number for partition [%s]", partition);
        }
      }

      // Removed check comparing these offsets to the latest in Kinesis because it's potentially quite expensive - if no
      // data has been written recently the call will block until timeout.

      return SequenceNumberPlus.of(offset, true);

    } else {
      boolean useEarliestSequenceNumber = ioConfig.isUseEarliestSequenceNumber();
      if (subsequentlyDiscoveredPartitions.contains(partition)) {
        log.info(
            "Overriding useEarliestSequenceNumber and starting from beginning of newly discovered partition [%s] (which is probably from a split or merge)",
            partition
        );
        useEarliestSequenceNumber = true;
      }

      offset = getOffsetFromKinesisForPartition(partition, useEarliestSequenceNumber);
      log.info("Getting sequence number [%s] from Kinesis for partition [%s]", offset, partition);
      return SequenceNumberPlus.of(offset, false);
    }
  }

  private Map<String, String> getOffsetsFromMetadataStorage()
  {
    DataSourceMetadata dataSourceMetadata = indexerMetadataStorageCoordinator.getDataSourceMetadata(dataSource);
    if (dataSourceMetadata != null && dataSourceMetadata instanceof KinesisDataSourceMetadata) {
      KinesisPartitions partitions = ((KinesisDataSourceMetadata) dataSourceMetadata).getKinesisPartitions();
      if (partitions != null) {
        if (!ioConfig.getStream().equals(partitions.getStream())) {
          log.warn(
              "Stream name in metadata storage [%s] doesn't match spec stream name [%s], ignoring stored sequence numbers",
              partitions.getStream(),
              ioConfig.getStream()
          );
          return ImmutableMap.of();
        } else if (partitions.getPartitionSequenceNumberMap() != null) {
          return partitions.getPartitionSequenceNumberMap();
        }
      }
    }

    return ImmutableMap.of();
  }

  private String getOffsetFromKinesisForPartition(String partition, boolean useEarliestSequenceNumber)
      throws TimeoutException
  {
    log.info(
        "Fetching starting sequence number from Kinesis with useEarliestSequenceNumber=%s",
        useEarliestSequenceNumber
        ? "true. If there is no data in the stream, Kinesis will not return a sequence number and this call will fail with a timeout."
        : "false. If there is no new data coming into the stream, Kinesis will not return a sequence number and this call will fail with a timeout."
    );

    return useEarliestSequenceNumber
           ? recordSupplier.getEarliestSequenceNumber(StreamPartition.of(ioConfig.getStream(), partition))
           : recordSupplier.getLatestSequenceNumber(StreamPartition.of(ioConfig.getStream(), partition));
  }

  /**
   * Compares the sequence name from the task with one generated for the task's group ID and returns false if they do
   * not match. The sequence name is generated from a hash of the dataSchema, tuningConfig, starting offsets, and the
   * minimumMessageTime or maximumMessageTime if set.
   */
  private boolean isTaskCurrent(int taskGroupId, String taskId)
  {
    Optional<Task> taskOptional = taskStorage.getTask(taskId);
    if (!taskOptional.isPresent() || !(taskOptional.get() instanceof KinesisIndexTask)) {
      return false;
    }

    String taskSequenceName = ((KinesisIndexTask) taskOptional.get()).getIOConfig().getBaseSequenceName();

    return generateSequenceName(taskGroupId).equals(taskSequenceName);
  }

  private ListenableFuture<?> stopTasksInGroup(TaskGroup taskGroup)
  {
    if (taskGroup == null) {
      return Futures.immediateFuture(null);
    }

    final List<ListenableFuture<Void>> futures = Lists.newArrayList();
    for (Map.Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
      if (!entry.getValue().status.isComplete()) {
        futures.add(stopTask(entry.getKey(), false));
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

  private int getTaskGroupIdForPartition(String partitionId)
  {
    if (!partitionIds.contains(partitionId)) {
      partitionIds.add(partitionId);
    }

    return partitionIds.indexOf(partitionId) % ioConfig.getTaskCount();
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

  private SupervisorReport<KinesisSupervisorReportPayload> generateReport(boolean includeOffsets)
  {
    int numPartitions = 0;
    for (Map<String, String> partitionGroup : partitionGroups.values()) {
      numPartitions += partitionGroup.size();
    }

    KinesisSupervisorReportPayload payload = new KinesisSupervisorReportPayload(
        dataSource,
        ioConfig.getStream(),
        numPartitions,
        ioConfig.getReplicas(),
        ioConfig.getTaskDuration().getMillis() / 1000,
        spec.isSuspended()
    );

    SupervisorReport<KinesisSupervisorReportPayload> report = new SupervisorReport<>(
        dataSource,
        DateTimes.nowUtc(),
        payload
    );

    List<TaskReportData> taskReports = Lists.newArrayList();
    List<ListenableFuture<Map<String, String>>> futures = Lists.newArrayList();

    try {
      for (TaskGroup taskGroup : taskGroups.values()) {
        for (Map.Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
          String taskId = entry.getKey();
          DateTime startTime = entry.getValue().startTime;
          Long remainingSeconds = null;
          if (startTime != null) {
            remainingSeconds = Math.max(
                0, ioConfig.getTaskDuration().getMillis() - (DateTime.now().getMillis() - startTime.getMillis())
            ) / 1000;
          }

          taskReports.add(
              new TaskReportData(
                  taskId,
                  (includeOffsets ? taskGroup.partitionOffsets : null),
                  null,
                  startTime,
                  remainingSeconds,
                  TaskReportData.TaskType.ACTIVE
              )
          );

          if (includeOffsets) {
            futures.add(taskClient.getCurrentOffsetsAsync(taskId, false));
          }
        }
      }

      for (List<TaskGroup> taskGroups : pendingCompletionTaskGroups.values()) {
        for (TaskGroup taskGroup : taskGroups) {
          for (Map.Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
            String taskId = entry.getKey();
            DateTime startTime = entry.getValue().startTime;
            Long remainingSeconds = null;
            if (taskGroup.completionTimeout != null) {
              remainingSeconds = Math.max(0, taskGroup.completionTimeout.getMillis() - DateTime.now().getMillis())
                                 / 1000;
            }

            taskReports.add(
                new TaskReportData(
                    taskId,
                    (includeOffsets ? taskGroup.partitionOffsets : null),
                    null,
                    startTime,
                    remainingSeconds,
                    TaskReportData.TaskType.PUBLISHING
                )
            );

            if (includeOffsets) {
              futures.add(taskClient.getCurrentOffsetsAsync(taskId, false));
            }
          }
        }
      }

      List<Map<String, String>> results = Futures.successfulAsList(futures)
                                                 .get(futureTimeoutInSeconds, TimeUnit.SECONDS);
      for (int i = 0; i < taskReports.size(); i++) {
        TaskReportData reportData = taskReports.get(i);
        if (includeOffsets) {
          reportData.setCurrentSequenceNumbers(results.get(i));
        }
        payload.addTask(reportData);
      }
    }
    catch (Exception e) {
      log.warn(e, "Failed to generate status report");
    }

    return report;
  }

  private Runnable buildRunTask()
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        notices.add(new RunNotice());
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
  private Map<String, Map<String, Object>> getCurrentTotalStats() throws InterruptedException, ExecutionException, TimeoutException
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


// TODO: Implement this for Kinesis which uses approximate time from latest instead of offset lag
/*
  private Runnable computeAndEmitLag(final KinesisIndexTaskClient taskClient)
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        try {
          final Map<String, List<PartitionInfo>> topics = lagComputingConsumer.listTopics();
          final List<PartitionInfo> partitionInfoList = topics.get(ioConfig.getStream());
          lagComputingConsumer.assign(
              Lists.transform(partitionInfoList, new Function<PartitionInfo, TopicPartition>()
              {
                @Override
                public TopicPartition apply(PartitionInfo input)
                {
                  return new TopicPartition(ioConfig.getStream(), input.partition());
                }
              })
          );
          final Map<Integer, Long> offsetsResponse = new ConcurrentHashMap<>();
          final List<ListenableFuture<Void>> futures = Lists.newArrayList();
          for (TaskGroup taskGroup : taskGroups.values()) {
            for (String taskId : taskGroup.taskIds()) {
              futures.add(Futures.transform(
                  taskClient.getCurrentOffsetsAsync(taskId, false),
                  new Function<Map<Integer, Long>, Void>()
                  {
                    @Override
                    public Void apply(Map<Integer, Long> taskResponse)
                    {
                      if (taskResponse != null) {
                        for (final Map.Entry<Integer, Long> partitionOffsets : taskResponse.entrySet()) {
                          offsetsResponse.compute(partitionOffsets.getKey(), new BiFunction<Integer, Long, Long>()
                          {
                            @Override
                            public Long apply(Integer key, Long existingOffsetInMap)
                            {
                              // If existing value is null use the offset returned by task
                              // otherwise use the max (makes sure max offset is taken from replicas)
                              return existingOffsetInMap == null
                                     ? partitionOffsets.getValue()
                                     : Math.max(partitionOffsets.getValue(), existingOffsetInMap);
                            }
                          });
                        }
                      }
                      return null;
                    }
                  }
                          )
              );
            }
          }
          // not using futureTimeoutInSeconds as its min value is 120 seconds
          // and minimum emission period for this metric is 60 seconds
          Futures.successfulAsList(futures).get(30, TimeUnit.SECONDS);

          // for each partition, seek to end to get the highest offset
          // check the offsetsResponse map for the latest consumed offset
          // if partition info not present in offsetsResponse then use lastCurrentOffsets map
          // if not present there as well, fail the compute

          long lag = 0;
          for (PartitionInfo partitionInfo : partitionInfoList) {
            long diff;
            final TopicPartition topicPartition = new TopicPartition(ioConfig.getStream(), partitionInfo.partition());
            lagComputingConsumer.seekToEnd(ImmutableList.of(topicPartition));
            if (offsetsResponse.get(topicPartition.partition()) != null) {
              diff = lagComputingConsumer.position(topicPartition) - offsetsResponse.get(topicPartition.partition());
              lastCurrentOffsets.put(topicPartition.partition(), offsetsResponse.get(topicPartition.partition()));
            } else if (lastCurrentOffsets.get(topicPartition.partition()) != null) {
              diff = lagComputingConsumer.position(topicPartition) - lastCurrentOffsets.get(topicPartition.partition());
            } else {
              throw new ISE("Could not find latest consumed offset for partition [%d]", topicPartition.partition());
            }
            lag += diff;
            log.debug(
                "Topic - [%s] Partition - [%d] : Partition lag [%,d], Total lag so far [%,d]",
                topicPartition.topic(),
                topicPartition.partition(),
                diff,
                lag
            );
          }
          emitter.emit(
              ServiceMetricEvent.builder().setDimension("dataSource", dataSource).build("ingest/kinesis/lag", lag)
          );
        }
        catch (InterruptedException e) {
          // do nothing, probably we are shutting down
        }
        catch (Exception e) {
          log.warn(e, "Unable to compute Kinesis lag");
        }
      }
    };
  }
  */
}
