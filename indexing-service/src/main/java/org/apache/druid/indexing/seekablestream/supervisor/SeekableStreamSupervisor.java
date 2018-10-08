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

package org.apache.druid.indexing.seekablestream.supervisor;

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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IndexTaskClient;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.Task;
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
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClient;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClientFactory;
import org.apache.druid.indexing.seekablestream.SeekableStreamPartitions;
import org.apache.druid.indexing.seekablestream.SeekableStreamTuningConfig;
import org.apache.druid.indexing.seekablestream.common.Record;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.SequenceNumber;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.EntryExistsException;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
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
import java.util.Random;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * this class is the parent class of both the Kafka and Kinesis supervisor. All the main run loop
 * logic are similar enough so they're grouped together into this class.
 * <p>
 * incremental handoff & checkpointing are not yet supported by Kinesis, but the logic is left in here
 * so in the future it's easier to implement
 * <p>
 * Supervisor responsible for managing the SeekableStreamIndexTasks (Kafka/Kinesis) for a single dataSource. At a high level, the class accepts a
 * {@link SeekableStreamSupervisorSpec} which includes the stream name (topic / stream) and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the stream's partitions
 * and the list of running indexing tasks and ensures that all partitions are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * stream sequences.
 * <p>
 *
 * @param <T1> partition id type
 * @param <T2> sequence number type
 */
public abstract class SeekableStreamSupervisor<T1, T2>
    implements Supervisor
{
  public static final String IS_INCREMENTAL_HANDOFF_SUPPORTED = "IS_INCREMENTAL_HANDOFF_SUPPORTED";
  private static final EmittingLogger log = new EmittingLogger(SeekableStreamSupervisor.class);
  private static final Random RANDOM = ThreadLocalRandom.current();
  private static final long MAX_RUN_FREQUENCY_MILLIS = 1000;
  private static final long MINIMUM_FUTURE_TIMEOUT_IN_SECONDS = 120;
  private static final int MAX_INITIALIZATION_RETRIES = 20;
  private static final CopyOnWriteArrayList EMPTY_LIST = Lists.newCopyOnWriteArrayList();

  // Internal data structures
  // --------------------------------------------------------

  /**
   * A TaskGroup is the main data structure used by SeekableStreamSupervisor to organize and monitor stream partitions and
   * indexing tasks. All the tasks in a TaskGroup should always be doing the same thing (reading the same partitions and
   * starting from the same sequences) and if [replicas] is configured to be 1, a TaskGroup will contain a single task (the
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
    final ImmutableMap<T1, T2> startingSequences;

    final ConcurrentHashMap<String, TaskData> tasks = new ConcurrentHashMap<>();
    final Optional<DateTime> minimumMessageTime;
    final Optional<DateTime> maximumMessageTime;
    final Set<T1> exclusiveStartSequenceNumberPartitions;
    final TreeMap<Integer, Map<T1, T2>> checkpointSequences = new TreeMap<>();
    final String baseSequenceName;
    DateTime completionTimeout; // is set after signalTasksToFinish(); if not done by timeout, take corrective action

    TaskGroup(
        int groupId,
        ImmutableMap<T1, T2> startingSequences,
        Optional<DateTime> minimumMessageTime,
        Optional<DateTime> maximumMessageTime,
        Set<T1> exclusiveStartSequenceNumberPartitions
    )
    {
      this.groupId = groupId;
      this.startingSequences = startingSequences;
      this.minimumMessageTime = minimumMessageTime;
      this.maximumMessageTime = maximumMessageTime;
      this.checkpointSequences.put(0, startingSequences);
      this.exclusiveStartSequenceNumberPartitions = exclusiveStartSequenceNumberPartitions != null
                                                    ? exclusiveStartSequenceNumberPartitions
                                                    : new HashSet<>();
      this.baseSequenceName = generateSequenceName(startingSequences, minimumMessageTime, maximumMessageTime);
    }

    int addNewCheckpoint(Map<T1, T2> checkpoint)
    {
      checkpointSequences.put(checkpointSequences.lastKey() + 1, checkpoint);
      return checkpointSequences.lastKey();
    }

    Set<String> taskIds()
    {
      return tasks.keySet();
    }

  }

  private class TaskData
  {
    volatile TaskStatus status;
    volatile DateTime startTime;
    volatile Map<T1, T2> currentSequences = new HashMap<>();

    @Override
    public String toString()
    {
      return "TaskData{" +
             "status=" + status +
             ", startTime=" + startTime +
             ", checkpointSequences=" + currentSequences +
             '}';
    }
  }

  /**
   * Notice is used to queue tasks that are internal to the supervisor
   */
  private interface Notice
  {
    void handle() throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException;
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
      resetInternal(dataSourceMetadata);
    }
  }

  protected class CheckpointNotice implements Notice
  {
    @Nullable
    private final Integer nullableTaskGroupId;
    @Deprecated
    private final String baseSequenceName;
    private final SeekableStreamDataSourceMetadata<T1, T2> previousCheckpoint;
    private final SeekableStreamDataSourceMetadata<T1, T2> currentCheckpoint;

    public CheckpointNotice(
        @Nullable Integer nullableTaskGroupId,
        @Deprecated String baseSequenceName,
        SeekableStreamDataSourceMetadata<T1, T2> previousCheckpoint,
        SeekableStreamDataSourceMetadata<T1, T2> currentCheckpoint
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
        final TreeMap<Integer, Map<T1, T2>> checkpoints = taskGroup.checkpointSequences;

        // check validity of previousCheckpoint
        int index = checkpoints.size();
        for (int sequenceId : checkpoints.descendingKeySet()) {
          Map<T1, T2> checkpoint = checkpoints.get(sequenceId);
          // We have already verified the topic of the current checkpoint is same with that in ioConfig.
          // See checkpoint().
          if (checkpoint.equals(previousCheckpoint.getSeekableStreamPartitions()
                                                  .getMap()
          )) {
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
        final Map<T1, T2> newCheckpoint = checkpointTaskGroup(taskGroup, false).get();
        taskGroup.addNewCheckpoint(newCheckpoint);
        log.info("Handled checkpoint notice, new checkpoint is [%s] for taskGroup [%s]", newCheckpoint, taskGroupId);
      }
    }

    boolean isValidTaskGroup(int taskGroupId, @Nullable TaskGroup taskGroup)
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


  // Map<{group ID}, {actively reading task group}>; see documentation for TaskGroup class
  private final ConcurrentHashMap<Integer, TaskGroup> taskGroups = new ConcurrentHashMap<>();

  // After telling a taskGroup to stop reading and begin publishing a segment, it is moved from [taskGroups] to here so
  // we can monitor its status while we queue new tasks to read the next range of offsets. This is a list since we could
  // have multiple sets of tasks publishing at once if time-to-publish > taskDuration.
  // Map<{group ID}, List<{pending completion task groups}>>
  private final ConcurrentHashMap<Integer, CopyOnWriteArrayList<TaskGroup>> pendingCompletionTaskGroups = new ConcurrentHashMap<>();

  // The starting offset for a new partition in [partitionGroups] is initially set to NOT_SET. When a new task group
  // is created and is assigned partitions, if the offset in [partitionGroups] is NOT_SET it will take the starting
  // offset value from the metadata store, and if it can't find it there, from stream. Once a task begins
  // publishing, the offset in partitionGroups will be updated to the ending offset of the publishing-but-not-yet-
  // completed task, which will cause the next set of tasks to begin reading from where the previous task left
  // off. If that previous task now fails, we will set the offset in [partitionGroups] back to NOT_SET which will
  // cause successive tasks to again grab their starting offset from metadata store. This mechanism allows us to
  // start up successive tasks without waiting for the previous tasks to succeed and still be able to handle task
  // failures during publishing.
  // Map<{group ID}, Map<{partition ID}, {startingOffset}>>
  private final ConcurrentHashMap<Integer, ConcurrentHashMap<T1, T2>> partitionGroups = new ConcurrentHashMap<>();

  protected final List<T1> partitionIds = new CopyOnWriteArrayList<>();
  protected volatile Map<T1, T2> latestSequenceFromStream;
  protected volatile DateTime sequenceLastUpdated;

  private final Set<T1> subsequentlyDiscoveredPartitions = new HashSet<>();
  private final TaskStorage taskStorage;
  private final TaskMaster taskMaster;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final SeekableStreamIndexTaskClient<T1, T2> taskClient;
  private final ObjectMapper sortingMapper;
  private final SeekableStreamSupervisorSpec spec;
  private final String dataSource;
  private final SeekableStreamSupervisorIOConfig ioConfig;
  private final SeekableStreamSupervisorTuningConfig tuningConfig;
  private final SeekableStreamTuningConfig taskTuningConfig;
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
  private final Object recordSupplierLock = new Object();
  private final T2 END_OF_PARTITION;
  private final T2 NOT_SET;
  private final boolean useExclusiveStartingSequence;
  private boolean listenerRegistered = false;
  private long lastRunTime;
  private int initRetryCounter = 0;
  private volatile DateTime firstRunTime;
  private volatile RecordSupplier<T1, T2> recordSupplier;
  private volatile boolean started = false;
  private volatile boolean stopped = false;
  private volatile boolean lifecycleStarted = false;


  public SeekableStreamSupervisor(
      final String supervisorId,
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final SeekableStreamIndexTaskClientFactory<? extends SeekableStreamIndexTaskClient<T1, T2>> taskClientFactory,
      final ObjectMapper mapper,
      final SeekableStreamSupervisorSpec spec,
      final RowIngestionMetersFactory rowIngestionMetersFactory,
      final T2 NOT_SET,
      final T2 END_OF_PARTITION,
      final boolean useExclusiveStartingSequence
  )
  {
    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.sortingMapper = mapper.copy().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    this.spec = spec;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.NOT_SET = NOT_SET;
    this.END_OF_PARTITION = END_OF_PARTITION;
    this.useExclusiveStartingSequence = useExclusiveStartingSequence;

    this.dataSource = spec.getDataSchema().getDataSource();
    this.ioConfig = spec.getIoConfig();
    this.tuningConfig = spec.getTuningConfig();
    this.taskTuningConfig = this.tuningConfig.copyOf();
    this.supervisorId = supervisorId;
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
                                         + IndexTaskClient.MAX_RETRY_WAIT_SECONDS)
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

  protected static String getRandomId()
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Integer.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((RANDOM.nextInt() >>> (i * 4)) & 0x0F)));
    }
    return suffix.toString();
  }


  @Override
  public void start()
  {
    synchronized (stateChangeLock) {
      Preconditions.checkState(!lifecycleStarted, "already started");
      Preconditions.checkState(!exec.isShutdown(), "already stopped");

      // Try normal initialization first, if that fails then schedule periodic initialization retries
      try {
        tryInit();
      }
      catch (Exception e) {
        if (!started) {
          log.warn(
              "First initialization attempt failed for SeekableStreamSupervisor[%s], starting retries...",
              dataSource
          );

          exec.submit(
              () -> {
                try {
                  RetryUtils.retry(
                      () -> {
                        tryInit();
                        return 0;
                      },
                      (throwable) -> !started,
                      0,
                      MAX_INITIALIZATION_RETRIES,
                      null,
                      null
                  );
                }
                catch (Exception e2) {
                  log.makeAlert(
                      "Failed to initialize after %s retries, aborting. Please resubmit the supervisor spec to restart this supervisor [%s]",
                      MAX_INITIALIZATION_RETRIES,
                      supervisorId
                  ).emit();
                  throw new RuntimeException(e2);
                }
              }
          );
        }
      }
      lifecycleStarted = true;
    }
  }

  @Override
  public void stop(boolean stopGracefully)
  {
    synchronized (stateChangeLock) {
      Preconditions.checkState(lifecycleStarted, "lifecycle not started");

      log.info("Beginning shutdown of [%s]", supervisorId);

      try {
        scheduledExec.shutdownNow(); // stop recurring executions
        reportingExec.shutdownNow();
        recordSupplier.close();

        if (started) {
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
        }

        taskClient.close();
        workerExec.shutdownNow();
        exec.shutdownNow();
        started = false;

        log.info("[%s] has stopped", supervisorId);
      }
      catch (Exception e) {
        log.makeAlert(e, "Exception stopping [%s]", supervisorId)
           .emit();
      }
    }
  }

  @Override
  public void reset(DataSourceMetadata dataSourceMetadata)
  {
    log.info("Posting ResetNotice");
    notices.add(new ResetNotice(dataSourceMetadata));
  }


  @VisibleForTesting
  protected void tryInit()
  {
    synchronized (stateChangeLock) {
      if (started) {
        log.warn("SUpervisor was already started, skipping init");
        return;
      }

      if (stopped) {
        log.warn("Supervisor was already stopped, skipping init.");
        return;
      }

      try {
        recordSupplier = setupRecordSupplier();

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
                    log.makeAlert(e, "SeekableStreamSupervisor[%s] failed to handle notice", dataSource)
                       .addData("noticeClass", notice.getClass().getSimpleName())
                       .emit();
                  }
                }
              }
              catch (InterruptedException e) {
                log.info("SeekableStreamSupervisor[%s] interrupted, exiting", dataSource);
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

        // different in kafka and kinesis as emitLag and checkpointing
        // are not yet implemented in Kinesis
        scheduleReporting(reportingExec);

        started = true;
        log.info(
            "Started SeekableStreamSupervisor[%s], first run in [%s], with spec: [%s]",
            dataSource,
            ioConfig.getStartDelay(),
            spec.toString()
        );
      }
      catch (Exception e) {
        if (recordSupplier != null) {
          recordSupplier.close();
        }
        initRetryCounter++;
        log.makeAlert(e, "Exception starting SeekableStreamSupervisor[%s]", dataSource)
           .emit();

        throw new RuntimeException(e);
      }
    }
  }

  private Runnable buildRunTask()
  {
    return () -> notices.add(new RunNotice());
  }

  @Override
  public SupervisorReport getStatus()
  {
    return generateReport(true);
  }

  private SupervisorReport<? extends SeekableStreamSupervisorReportPayload<T1, T2>> generateReport(
      boolean includeOffsets
  )
  {
    int numPartitions = partitionGroups.values().stream().mapToInt(Map::size).sum();

    final SeekableStreamSupervisorReportPayload<T1, T2> payload = createReportPayload(numPartitions, includeOffsets);

    SupervisorReport<SeekableStreamSupervisorReportPayload<T1, T2>> report = new SupervisorReport<>(
        dataSource,
        DateTimes.nowUtc(),
        payload
    );

    List<TaskReportData<T1, T2>> taskReports = Lists.newArrayList();

    try {
      for (TaskGroup taskGroup : taskGroups.values()) {
        for (Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
          String taskId = entry.getKey();
          @Nullable
          DateTime startTime = entry.getValue().startTime;
          Map<T1, T2> currentOffsets = entry.getValue().currentSequences;
          Long remainingSeconds = null;
          if (startTime != null) {
            remainingSeconds = Math.max(
                0, ioConfig.getTaskDuration().getMillis() - (System.currentTimeMillis() - startTime.getMillis())
            ) / 1000;
          }

          taskReports.add(
              new TaskReportData<>(
                  taskId,
                  includeOffsets ? taskGroup.startingSequences : null,
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
            Map<T1, T2> currentOffsets = entry.getValue().currentSequences;
            Long remainingSeconds = null;
            if (taskGroup.completionTimeout != null) {
              remainingSeconds = Math.max(0, taskGroup.completionTimeout.getMillis() - System.currentTimeMillis())
                                 / 1000;
            }

            taskReports.add(
                new TaskReportData<>(
                    taskId,
                    includeOffsets ? taskGroup.startingSequences : null,
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
                (Function<Map<String, Object>, StatsFromTaskResult>) (currentStats) -> new StatsFromTaskResult(
                    groupId,
                    taskId,
                    currentStats
                )
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
                (Function<Map<String, Object>, StatsFromTaskResult>) (currentStats) -> new StatsFromTaskResult(
                    groupId,
                    taskId,
                    currentStats
                )
            )
        );
        groupAndTaskIds.add(new Pair<>(groupId, taskId));
      }
    }

    List<StatsFromTaskResult> results = Futures.successfulAsList(futures)
                                               .get(futureTimeoutInSeconds, TimeUnit.SECONDS);
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
  protected void runInternal()
      throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException
  {
    possiblyRegisterListener();
    updatePartitionDataFromStream();
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

  private void possiblyRegisterListener()
  {
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

  @VisibleForTesting
  protected void gracefulShutdownInternal() throws ExecutionException, InterruptedException, TimeoutException
  {
    for (TaskGroup taskGroup : taskGroups.values()) {
      for (Entry<String, TaskData> entry :
          taskGroup.tasks.entrySet()) {
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
  protected void resetInternal(DataSourceMetadata dataSourceMetadata)
  {
    if (dataSourceMetadata == null) {
      // Reset everything
      boolean result = indexerMetadataStorageCoordinator.deleteDataSourceMetadata(dataSource);
      log.info("Reset dataSource[%s] - dataSource metadata entry deleted? [%s]", dataSource, result);
      taskGroups.values().forEach(this::killTasksInGroup);
      taskGroups.clear();
      partitionGroups.clear();
    } else {

      if (!checkSourceMetadataMatch(dataSourceMetadata)) {
        throw new IAE(
            "Datasource metadata instance does not match required, found instance of [%s]",
            dataSourceMetadata.getClass()
        );
      }
      // Reset only the partitions in dataSourceMetadata if it has not been reset yet
      @SuppressWarnings("unchecked")
      final SeekableStreamDataSourceMetadata<T1, T2> resetMetadata = (SeekableStreamDataSourceMetadata<T1, T2>) dataSourceMetadata;

      if (resetMetadata.getSeekableStreamPartitions().getStream().equals(ioConfig.getId())) {
        // metadata can be null
        final DataSourceMetadata metadata = indexerMetadataStorageCoordinator.getDataSourceMetadata(dataSource);
        if (metadata != null && !checkSourceMetadataMatch(metadata)) {
          throw new IAE(
              "Datasource metadata instance does not match required, found instance of [%s]",
              metadata.getClass()
          );
        }

        @SuppressWarnings("unchecked")
        final SeekableStreamDataSourceMetadata<T1, T2> currentMetadata = (SeekableStreamDataSourceMetadata<T1, T2>) metadata;

        // defend against consecutive reset requests from replicas
        // as well as the case where the metadata store do not have an entry for the reset partitions
        boolean doReset = false;
        for (Entry<T1, T2> resetPartitionOffset : resetMetadata.getSeekableStreamPartitions()
                                                               .getMap()
                                                               .entrySet()) {
          final T2 partitionOffsetInMetadataStore = currentMetadata == null
                                                    ? null
                                                    : currentMetadata.getSeekableStreamPartitions()
                                                                     .getMap()
                                                                     .get(resetPartitionOffset.getKey());
          final TaskGroup partitionTaskGroup = taskGroups.get(
              getTaskGroupIdForPartition(resetPartitionOffset.getKey())
          );
          final boolean isSameOffset = partitionTaskGroup != null
                                       && partitionTaskGroup.startingSequences.get(resetPartitionOffset.getKey())
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
          final DataSourceMetadata newMetadata = currentMetadata.minus(resetMetadata);
          try {
            metadataUpdateSuccess = indexerMetadataStorageCoordinator.resetDataSourceMetadata(dataSource, newMetadata);
          }
          catch (IOException e) {
            log.error("Resetting DataSourceMetadata failed [%s]", e.getMessage());
            Throwables.propagate(e);
          }
        }
        if (metadataUpdateSuccess) {
          resetMetadata.getSeekableStreamPartitions().getMap().keySet().forEach(partition -> {
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
            "Reset metadata topic [%s] and supervisor's stream name [%s] do not match",
            resetMetadata.getSeekableStreamPartitions().getStream(),
            ioConfig.getId()
        );
      }
    }


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

  private void killTasksInGroup(TaskGroup taskGroup)
  {
    if (taskGroup != null) {
      for (String taskId : taskGroup.tasks.keySet()) {
        log.info("Killing task [%s] in the task group", taskId);
        killTask(taskId);
      }
    }
  }

  private void killTaskGroupForPartitions(Set<T1> partitions)
  {
    for (T1 partition : partitions) {
      int taskGroupId = getTaskGroupIdForPartition(partition);
      killTasksInGroup(taskGroups.get(taskGroupId));
      partitionGroups.remove(taskGroupId);
      taskGroups.remove(taskGroupId);
    }
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

  private void discoverTasks() throws ExecutionException, InterruptedException, TimeoutException
  {
    int taskCount = 0;
    List<String> futureTaskIds = Lists.newArrayList();
    List<ListenableFuture<Boolean>> futures = Lists.newArrayList();
    List<Task> tasks = taskStorage.getActiveTasks();
    final Map<Integer, TaskGroup> taskGroupsToVerify = new HashMap<>();

    for (Task task : tasks) {
      if (!checkTaskInstance(task) || !dataSource.equals(task.getDataSource())) {
        continue;
      }

      taskCount++;
      @SuppressWarnings("unchecked")
      final SeekableStreamIndexTask<T1, T2> seekableStreamIndexTask = (SeekableStreamIndexTask<T1, T2>) task;
      final String taskId = task.getId();

      // Determine which task group this task belongs to based on one of the partitions handled by this task. If we
      // later determine that this task is actively reading, we will make sure that it matches our current partition
      // allocation (getTaskGroupIdForPartition(partition) should return the same value for every partition being read
      // by this task) and kill it if it is not compatible. If the task is instead found to be in the publishing
      // state, we will permit it to complete even if it doesn't match our current partition allocation to support
      // seamless schema migration.

      Iterator<T1> it = seekableStreamIndexTask.getIOConfig()
                                               .getStartPartitions()
                                               .getMap()
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
                  taskClient.getStatusAsync(taskId), new Function<SeekableStreamIndexTask.Status, Boolean>()
                  {
                    @Override
                    public Boolean apply(SeekableStreamIndexTask.Status status)
                    {
                      try {
                        log.debug("Task [%s], status [%s]", taskId, status);
                        if (status == SeekableStreamIndexTask.Status.PUBLISHING) {
                          seekableStreamIndexTask.getIOConfig()
                                                 .getStartPartitions()
                                                 .getMap()
                                                 .keySet()
                                                 .forEach(
                                                     partition -> addDiscoveredTaskToPendingCompletionTaskGroups(
                                                         getTaskGroupIdForPartition(partition),
                                                         taskId,
                                                         seekableStreamIndexTask.getIOConfig()
                                                                                .getStartPartitions()
                                                                                .getMap()
                                                     ));

                          // update partitionGroups with the publishing task's offsets (if they are greater than what is
                          // existing) so that the next tasks will start reading from where this task left off
                          Map<T1, T2> publishingTaskEndOffsets = taskClient.getEndOffsets(taskId);

                          for (Entry<T1, T2> entry : publishingTaskEndOffsets.entrySet()) {
                            T1 partition = entry.getKey();
                            T2 offset = entry.getValue();
                            ConcurrentHashMap<T1, T2> partitionOffsets = partitionGroups.get(
                                getTaskGroupIdForPartition(partition)
                            );

                            boolean succeeded;
                            do {
                              succeeded = true;
                              T2 previousOffset = partitionOffsets.putIfAbsent(partition, offset);
                              if (previousOffset != null
                                  && (makeSequenceNumber(previousOffset).compareTo(makeSequenceNumber(offset))) < 0) {
                                succeeded = partitionOffsets.replace(partition, previousOffset, offset);
                              }
                            } while (!succeeded);
                          }
                        } else {
                          for (T1 partition : seekableStreamIndexTask.getIOConfig()
                                                                     .getStartPartitions()
                                                                     .getMap()
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
                                          seekableStreamIndexTask.getIOConfig()
                                                                 .getStartPartitions()
                                                                 .getMap()
                                      ),
                                      seekableStreamIndexTask.getIOConfig().getMinimumMessageTime(),
                                      seekableStreamIndexTask.getIOConfig().getMaximumMessageTime(),
                                      seekableStreamIndexTask.getIOConfig().getExclusiveStartSequenceNumberPartitions()
                                  );
                                }
                            );
                            taskGroupsToVerify.put(taskGroupId, taskGroup);
                            final TaskData prevTaskData = taskGroup.tasks.putIfAbsent(taskId, new TaskData());
                            if (prevTaskData != null) {
                              throw new ISE(
                                  "WTH? a taskGroup[%s] already exists for new task[%s]",
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
    log.debug("Found [%d] seekable stream indexing tasks for dataSource [%s]", taskCount, dataSource);

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
    final List<Pair<String, TreeMap<Integer, Map<T1, T2>>>> taskSequences = new ArrayList<>();
    final List<ListenableFuture<TreeMap<Integer, Map<T1, T2>>>> futures = new ArrayList<>();
    final List<String> taskIds = new ArrayList<>();

    for (String taskId : taskGroup.taskIds()) {
      final ListenableFuture<TreeMap<Integer, Map<T1, T2>>> checkpointsFuture = taskClient.getCheckpointsAsync(
          taskId,
          true
      );
      futures.add(checkpointsFuture);
      taskIds.add(taskId);
    }

    try {
      List<TreeMap<Integer, Map<T1, T2>>> futuresResult =
          Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
      for (int i = 0; i < futuresResult.size(); i++) {
        final TreeMap<Integer, Map<T1, T2>> checkpoints = futuresResult.get(i);
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

    final DataSourceMetadata rawDataSourceMetadata = indexerMetadataStorageCoordinator.getDataSourceMetadata(dataSource);

    if (rawDataSourceMetadata != null && !checkSourceMetadataMatch(rawDataSourceMetadata)) {
      throw new IAE(
          "Datasource metadata instance does not match required, found instance of [%s]",
          rawDataSourceMetadata.getClass()
      );
    }

    @SuppressWarnings("unchecked")
    final SeekableStreamDataSourceMetadata<T1, T2> latestDataSourceMetadata = (SeekableStreamDataSourceMetadata<T1, T2>) rawDataSourceMetadata;

    final boolean hasValidOffsetsFromDb = latestDataSourceMetadata != null &&
                                          latestDataSourceMetadata.getSeekableStreamPartitions() != null &&
                                          ioConfig.getId().equals(
                                              latestDataSourceMetadata.getSeekableStreamPartitions().getStream()
                                          );
    final Map<T1, T2> latestOffsetsFromDb;
    if (hasValidOffsetsFromDb) {
      latestOffsetsFromDb = latestDataSourceMetadata.getSeekableStreamPartitions().getMap();
    } else {
      latestOffsetsFromDb = null;
    }

    // order tasks of this taskGroup by the latest sequenceId
    taskSequences.sort((o1, o2) -> o2.rhs.firstKey().compareTo(o1.rhs.firstKey()));

    final Set<String> tasksToKill = new HashSet<>();
    final AtomicInteger earliestConsistentSequenceId = new AtomicInteger(-1);
    int taskIndex = 0;

    while (taskIndex < taskSequences.size()) {
      TreeMap<Integer, Map<T1, T2>> taskCheckpoints = taskSequences.get(taskIndex).rhs;
      String taskId = taskSequences.get(taskIndex).lhs;
      if (earliestConsistentSequenceId.get() == -1) {
        // find the first replica task with earliest sequenceId consistent with datasource metadata in the metadata
        // store
        if (taskCheckpoints.entrySet().stream().anyMatch(
            sequenceCheckpoint -> sequenceCheckpoint.getValue().entrySet().stream().allMatch(
                partitionOffset -> {
                  SequenceNumber<T2> offset = makeSequenceNumber(partitionOffset.getValue());
                  SequenceNumber<T2> latestOffset = makeSequenceNumber(
                      latestOffsetsFromDb == null ? partitionOffset.getValue() :
                      latestOffsetsFromDb.getOrDefault(
                          partitionOffset
                              .getKey(),
                          partitionOffset
                              .getValue()
                      )
                  );

                  return offset.equals(latestOffset);
                }
            ) && earliestConsistentSequenceId.compareAndSet(-1, sequenceCheckpoint.getKey())) || (
                pendingCompletionTaskGroups.getOrDefault(groupId, EMPTY_LIST).size() > 0
                && earliestConsistentSequenceId.compareAndSet(-1, taskCheckpoints.firstKey()))) {
          final SortedMap<Integer, Map<T1, T2>> latestCheckpoints = new TreeMap<>(
              taskCheckpoints.tailMap(earliestConsistentSequenceId.get())
          );
          log.info("Setting taskGroup sequences to [%s] for group [%d]", latestCheckpoints, groupId);
          taskGroup.checkpointSequences.clear();
          taskGroup.checkpointSequences.putAll(latestCheckpoints);
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
        if (taskCheckpoints.get(taskGroup.checkpointSequences.firstKey()) == null
            || !(taskCheckpoints.get(taskGroup.checkpointSequences.firstKey())
                                .equals(taskGroup.checkpointSequences.firstEntry().getValue()))
            || taskCheckpoints.tailMap(taskGroup.checkpointSequences.firstKey()).size()
               != taskGroup.checkpointSequences.size()) {
          log.debug(
              "Adding task [%s] to kill list, checkpoints[%s], taskgroup checkpoints [%s]",
              taskId,
              taskCheckpoints,
              taskGroup.checkpointSequences
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
              taskGroup.checkpointSequences,
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
      Map<T1, T2> startingPartitions
  )
  {
    final CopyOnWriteArrayList<TaskGroup> taskGroupList = pendingCompletionTaskGroups.computeIfAbsent(
        groupId,
        k -> new CopyOnWriteArrayList<>()
    );
    for (TaskGroup taskGroup : taskGroupList) {
      if (taskGroup.startingSequences.equals(startingPartitions)) {
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
        Optional.absent(),
        null
    );

    newTaskGroup.tasks.put(taskId, new TaskData());
    newTaskGroup.completionTimeout = DateTimes.nowUtc().plus(ioConfig.getCompletionTimeout());

    taskGroupList.add(newTaskGroup);
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

  private boolean isTaskCurrent(int taskGroupId, String taskId)
  {
    Optional<Task> taskOptional = taskStorage.getTask(taskId);
    if (!taskOptional.isPresent() || !checkTaskInstance(taskOptional.get())) {
      return false;
    }

    @SuppressWarnings("unchecked")
    SeekableStreamIndexTask<T1, T2> task = (SeekableStreamIndexTask<T1, T2>) taskOptional.get();

    String taskSequenceName = task.getIOConfig().getBaseSequenceName();
    if (taskGroups.get(taskGroupId) != null) {
      return Preconditions
          .checkNotNull(taskGroups.get(taskGroupId), "null taskGroup for taskId[%s]", taskGroupId)
          .baseSequenceName
          .equals(taskSequenceName);
    } else {
      return generateSequenceName(
          task.getIOConfig()
              .getStartPartitions()
              .getMap(),
          task.getIOConfig().getMinimumMessageTime(),
          task.getIOConfig().getMaximumMessageTime()
      ).equals(taskSequenceName);
    }
  }

  protected String generateSequenceName(
      Map<T1, T2> startPartitions,
      Optional<DateTime> minimumMessageTime,
      Optional<DateTime> maximumMessageTime
  )
  {
    StringBuilder sb = new StringBuilder();

    for (Entry<T1, T2> entry : startPartitions.entrySet()) {
      sb.append(StringUtils.format("+%s(%s)", entry.getKey().toString(), entry.getValue().toString()));
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

    return Joiner.on("_").join("index_seekable_streaming", dataSource, hashCode);
  }

  private void updatePartitionDataFromStream()
  {
    Set<T1> partitionIds;
    try {
      synchronized (recordSupplierLock) {
        partitionIds = recordSupplier.getPartitionIds(ioConfig.getId());
      }
    }
    catch (Exception e) {
      log.warn("Could not fetch partitions for topic/stream [%s]", ioConfig.getId());
      return;
    }

    if (partitionIds == null) {
      log.warn("No partitions found for stream[%s]", ioConfig.getId());
      return;
    }

    log.debug("Found [%d] partitions for stream [%s]", partitionIds.size(), ioConfig.getId());

    Set<T1> closedPartitions = getOffsetsFromMetadataStorage()
        .entrySet()
        .stream()
        .filter(x -> Record.END_OF_SHARD_MARKER.equals(x.getValue()))
        .map(Entry::getKey)
        .collect(Collectors.toSet());

    boolean initialPartitionDiscovery = this.partitionIds.isEmpty();
    for (T1 partitionId : partitionIds) {
      if (closedPartitions.contains(partitionId)) {
        continue;
      }

      if (!initialPartitionDiscovery && !this.partitionIds.contains(partitionId)) {
        subsequentlyDiscoveredPartitions.add(partitionId);
        // should check for earlyPublishTime (Kinesis) here, not supported yet
      }

      int taskGroupId = getTaskGroupIdForPartition(partitionId);

      ConcurrentHashMap<T1, T2> partitionMap = partitionGroups.computeIfAbsent(
          taskGroupId,
          k -> new ConcurrentHashMap<>()
      );

      if (partitionMap.putIfAbsent(partitionId, NOT_SET) == null) {
        log.info(
            "New partition [%s] discovered for topic [%s], added to task group [%d]",
            partitionId,
            ioConfig.getId(),
            taskGroupId
        );
      }
    }
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

  private void checkTaskDuration() throws ExecutionException, InterruptedException, TimeoutException
  {
    final List<ListenableFuture<Map<T1, T2>>> futures = Lists.newArrayList();
    final List<Integer> futureGroupIds = Lists.newArrayList();

    for (Entry<Integer, TaskGroup> entry : taskGroups.entrySet()) {
      Integer groupId = entry.getKey();
      TaskGroup group = entry.getValue();

      // find the longest running task from this group
      DateTime earliestTaskStart = DateTimes.nowUtc();
      for (TaskData taskData : group.tasks.values()) {
        if (taskData.startTime != null && earliestTaskStart.isAfter(taskData.startTime)) {
          earliestTaskStart = taskData.startTime;
        }
      }

      // earlyPublishTime is only supported in Kinesis
      // for now it is not implemented
      //      boolean doEarlyPublish = false;
      //      if (earlyPublishTime != null && (earlyPublishTime.isBeforeNow() || earlyPublishTime.isEqualNow())) {
      //        log.info("Early publish requested - signalling tasks to publish");
      //        earlyPublishTime = null;
      //        doEarlyPublish = true;
      //      }

      // if this task has run longer than the configured duration, signal all tasks in the group to persist
      if (earliestTaskStart.plus(ioConfig.getTaskDuration()).isBeforeNow()) {
        log.info("Task group [%d] has run for [%s]", groupId, ioConfig.getTaskDuration());
        futureGroupIds.add(groupId);
        futures.add(checkpointTaskGroup(group, true));
      }
    }

    List<Map<T1, T2>> results = Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
    for (int j = 0; j < results.size(); j++) {
      Integer groupId = futureGroupIds.get(j);
      TaskGroup group = taskGroups.get(groupId);
      Map<T1, T2> endOffsets = results.get(j);

      if (endOffsets != null) {
        // set a timeout and put this group in pendingCompletionTaskGroups so that it can be monitored for completion
        group.completionTimeout = DateTimes.nowUtc().plus(ioConfig.getCompletionTimeout());
        pendingCompletionTaskGroups.computeIfAbsent(groupId, k -> new CopyOnWriteArrayList<>()).add(group);

        // set endOffsets as the next startOffsets
        for (Entry<T1, T2> entry : endOffsets.entrySet()) {
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

  private ListenableFuture<Map<T1, T2>> checkpointTaskGroup(final TaskGroup taskGroup, final boolean finalize)
  {
    if (finalize) {
      // 1) Check if any task completed (in which case we're done) and kill unassigned tasks
      Iterator<Entry<String, TaskData>> i = taskGroup.tasks.entrySet().iterator();
      while (i.hasNext()) {
        Entry<String, TaskData> taskEntry = i.next();
        String taskId = taskEntry.getKey();
        TaskData task = taskEntry.getValue();

        if (task.status != null) {
          if (task.status.isSuccess()) {
            // If any task in this group has already completed, stop the rest of the tasks in the group and return.
            // This will cause us to create a new set of tasks next cycle that will start from the offsets in
            // metadata store (which will have advanced if we succeeded in publishing and will remain the same if
            // publishing failed and we need to re-ingest)
            return Futures.transform(
                stopTasksInGroup(taskGroup),
                new Function<Object, Map<T1, T2>>()
                {
                  @Nullable
                  @Override
                  public Map<T1, T2> apply(@Nullable Object input)
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
    final List<ListenableFuture<Map<T1, T2>>> pauseFutures = Lists.newArrayList();
    final List<String> pauseTaskIds = ImmutableList.copyOf(taskGroup.taskIds());
    for (final String taskId : pauseTaskIds) {
      pauseFutures.add(taskClient.pauseAsync(taskId));
    }

    return Futures.transform(
        Futures.successfulAsList(pauseFutures), new Function<List<Map<T1, T2>>, Map<T1, T2>>()
        {
          @Nullable
          @Override
          public Map<T1, T2> apply(List<Map<T1, T2>> input)
          {
            // 3) Build a map of the highest offset read by any task in the group for each partition
            final Map<T1, T2> endOffsets = new HashMap<>();
            for (int i = 0; i < input.size(); i++) {
              Map<T1, T2> result = input.get(i);

              if (result == null || result.isEmpty()) { // kill tasks that didn't return a value
                String taskId = pauseTaskIds.get(i);
                log.warn("Task [%s] failed to respond to [pause] in a timely manner, killing task", taskId);
                killTask(taskId);
                taskGroup.tasks.remove(taskId);

              } else { // otherwise build a map of the highest offsets seen
                for (Entry<T1, T2> offset : result.entrySet()) {
                  if (!endOffsets.containsKey(offset.getKey())
                      || makeSequenceNumber(endOffsets.get(offset.getKey())).compareTo(
                      makeSequenceNumber(offset.getValue())) < 0) {
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

              if (endOffsets.equals(taskGroup.checkpointSequences.lastEntry().getValue())) {
                log.warn(
                    "Checkpoint [%s] is same as the start offsets [%s] of latest sequence for the task group [%d]",
                    endOffsets,
                    taskGroup.checkpointSequences.lastEntry().getValue(),
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
                  log.warn(
                      "Task [%s] failed to respond to [set end offsets] in a timely manner, killing task",
                      taskId
                  );
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

  private void checkPendingCompletionTasks()
      throws ExecutionException, InterruptedException, TimeoutException
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

  private void createNewTasks() throws JsonProcessingException
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

        try {

          Map<T1, SequenceNumber<T2>> startingOffsets = generateStartingSequencesForPartitionGroup(groupId);

          ImmutableMap<T1, T2> simpleStartingOffsets = startingOffsets
              .entrySet()
              .stream()
              .filter(x -> x.getValue().get() != null)
              .collect(Collectors.collectingAndThen(
                  Collectors.toMap(Entry::getKey, x -> x.getValue().get()),
                  ImmutableMap::copyOf
              ));

          Set<T1> exclusiveStartSequenceNumberPartitions = startingOffsets
              .entrySet().stream()
              .filter(x -> x.getValue().get() != null && x.getValue().isExclusive())
              .map(Entry::getKey)
              .collect(Collectors.toSet());

          taskGroups.put(
              groupId,
              new TaskGroup(
                  groupId,
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
    for (Entry<Integer, TaskGroup> entry : taskGroups.entrySet()) {
      TaskGroup taskGroup = entry.getValue();
      Integer groupId = entry.getKey();

      if (taskGroup.startingSequences == null || taskGroup.startingSequences
          .values().stream().allMatch(x -> x == null || Record.END_OF_SHARD_MARKER.equals(x))) {
        log.debug("Nothing to read in any partition for taskGroup [%d], skipping task creation", groupId);
        continue;
      }

      if (ioConfig.getReplicas() > taskGroup.tasks.size()) {
        log.info(
            "Number of tasks [%d] does not match configured numReplicas [%d] in task group [%d], creating more tasks",
            taskGroup.tasks.size(), ioConfig.getReplicas(), groupId
        );
        createTasksForGroup(groupId, ioConfig.getReplicas() - taskGroup.tasks.size());
        createdTask = true;
      }
    }

    if (createdTask && firstRunTime.isBeforeNow()) {
      // Schedule a run event after a short delay to update our internal data structures with the new tasks that were
      // just created. This is mainly for the benefit of the status API in situations where the run period is lengthy.
      scheduledExec.schedule(buildRunTask(), 5000, TimeUnit.MILLISECONDS);
    }

  }

  protected void addNotice(Notice notice)
  {
    notices.add(notice);
  }

  @VisibleForTesting
  @Nullable
  protected TaskGroup removeTaskGroup(int taskGroupId)
  {
    return taskGroups.remove(taskGroupId);
  }

  @VisibleForTesting
  protected void moveTaskGroupToPendingCompletion(int taskGroupId)
  {
    final TaskGroup taskGroup = taskGroups.remove(taskGroupId);
    if (taskGroup != null) {
      pendingCompletionTaskGroups.computeIfAbsent(taskGroupId, k -> new CopyOnWriteArrayList<>()).add(taskGroup);
    }
  }

  @VisibleForTesting
  protected int getNoticesQueueSize()
  {
    return notices.size();
  }

  private ImmutableMap<T1, SequenceNumber<T2>> generateStartingSequencesForPartitionGroup(int groupId)
      throws TimeoutException
  {
    ImmutableMap.Builder<T1, SequenceNumber<T2>> builder = ImmutableMap.builder();
    for (Entry<T1, T2> entry : partitionGroups.get(groupId).entrySet()) {
      T1 partition = entry.getKey();
      T2 offset = entry.getValue();

      if (!NOT_SET.equals(offset)) {
        // if we are given a startingOffset (set by a previous task group which is pending completion) then use it
        if (!Record.END_OF_SHARD_MARKER.equals(offset)) {
          builder.put(partition, makeSequenceNumber(offset, useExclusiveStartingSequence, true));
        }
      } else {
        // if we don't have a startingOffset (first run or we had some previous failures and reset the offsets) then
        // get the offset from metadata storage (if available) or Kafka/Kinesis (otherwise)
        SequenceNumber<T2> offsetFromStorage = getOffsetFromStorageForPartition(partition);

        if (!Record.END_OF_SHARD_MARKER.equals(offset)) {
          builder.put(partition, offsetFromStorage);
        }
      }
    }
    return builder.build();
  }

  /**
   * Queries the dataSource metadata table to see if there is a previous ending offset for this partition. If it doesn't
   * find any data, it will retrieve the latest or earliest Kafka/Kinesis offset depending on the useEarliestOffset config.
   */
  private SequenceNumber<T2> getOffsetFromStorageForPartition(T1 partition) throws TimeoutException
  {
    final Map<T1, T2> metadataOffsets = getOffsetsFromMetadataStorage();
    T2 offset = metadataOffsets.get(partition);
    if (offset != null) {
      log.debug("Getting offset [%s] from metadata storage for partition [%s]", offset, partition);
      if (!taskTuningConfig.isSkipSequenceNumberAvailabilityCheck()) {
        try {
          if (!checkSequenceAvailability(partition, offset)) {
            if (taskTuningConfig.isResetOffsetAutomatically()) {
              resetInternal(
                  createDataSourceMetaData(ioConfig.getId(), ImmutableMap.of(partition, offset))
              );
              throw new ISE(
                  "Previous sequenceNumber [%s] is no longer available for partition [%s] - automatically resetting offset",
                  offset,
                  partition
              );

            } else {
              throw new ISE(
                  "Previous sequenceNumber [%s] is no longer available for partition [%s]. You can clear the previous sequenceNumber and start reading from a valid message by using the supervisor's reset API.",
                  offset,
                  partition
              );
            }
          }
        }
        catch (TimeoutException e) {
          throw new ISE(e, "Timeout while fetching earliest sequence number for partition [%s]", partition);
        }
      }
      return makeSequenceNumber(offset, useExclusiveStartingSequence, true);
    } else {
      boolean useEarliestSequenceNumber = ioConfig.isUseEarliestSequenceNumber();
      if (subsequentlyDiscoveredPartitions.contains(partition)) {
        log.info(
            "Overriding useEarliestSequenceNumber and starting from beginning of newly discovered partition [%s] (which is probably from a split or merge)",
            partition
        );
        useEarliestSequenceNumber = true;
      }

      offset = getOffsetFromStreamForPartition(partition, useEarliestSequenceNumber);
      log.info("Getting sequence number [%s] for partition [%s]", offset, partition);
      return makeSequenceNumber(offset, useExclusiveStartingSequence, false);
    }
  }

  private Map<T1, T2> getOffsetsFromMetadataStorage()
  {
    final DataSourceMetadata dataSourceMetadata = indexerMetadataStorageCoordinator.getDataSourceMetadata(dataSource);
    if (dataSourceMetadata instanceof SeekableStreamDataSourceMetadata
        && checkSourceMetadataMatch(dataSourceMetadata)) {
      @SuppressWarnings("unchecked")
      SeekableStreamPartitions<T1, T2> partitions = ((SeekableStreamDataSourceMetadata) dataSourceMetadata).getSeekableStreamPartitions();
      if (partitions != null) {
        if (!ioConfig.getId().equals(partitions.getStream())) {
          log.warn(
              "Topic/stream in metadata storage [%s] doesn't match spec topic/stream [%s], ignoring stored offsets",
              partitions.getStream(),
              ioConfig.getId()
          );
          return Collections.emptyMap();
        } else if (partitions.getMap() != null) {
          return partitions.getMap();
        }
      }
    }

    return Collections.emptyMap();
  }

  protected T2 getOffsetFromStreamForPartition(T1 partition, boolean useEarliestOffset) throws TimeoutException
  {
    synchronized (recordSupplierLock) {
      StreamPartition<T1> topicPartition = new StreamPartition<>(ioConfig.getId(), partition);
      if (!recordSupplier.getAssignment().contains(topicPartition)) {
        recordSupplier.assign(Collections.singleton(topicPartition));
      }

      return useEarliestOffset
             ? recordSupplier.getEarliestSequenceNumber(topicPartition)
             : recordSupplier.getLatestSequenceNumber(topicPartition);
    }
  }

  private void createTasksForGroup(int groupId, int replicas) throws JsonProcessingException
  {
    TaskGroup group = taskGroups.get(groupId);
    Map<T1, T2> startPartitions = group.startingSequences;
    Map<T1, T2> endPartitions = new HashMap<>();
    for (T1 partition : startPartitions.keySet()) {
      endPartitions.put(partition, END_OF_PARTITION);
    }
    Set<T1> exclusiveStartSequenceNumberPartitions = taskGroups.get(groupId).exclusiveStartSequenceNumberPartitions;

    DateTime minimumMessageTime = taskGroups.get(groupId).minimumMessageTime.orNull();
    DateTime maximumMessageTime = taskGroups.get(groupId).maximumMessageTime.orNull();

    SeekableStreamIOConfig newIoConfig = createIoConfig(
        groupId,
        startPartitions,
        endPartitions,
        group.baseSequenceName,
        minimumMessageTime,
        maximumMessageTime,
        exclusiveStartSequenceNumberPartitions,
        ioConfig
    );


    List<SeekableStreamIndexTask<T1, T2>> taskList = createIndexTasks(
        replicas,
        group.baseSequenceName,
        sortingMapper,
        group.checkpointSequences,
        newIoConfig,
        taskTuningConfig,
        rowIngestionMetersFactory
    );

    for (int i = 0; i < replicas; i++) {
      Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
      SeekableStreamIndexTask indexTask = taskList.get(i);
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

  @VisibleForTesting
  protected Runnable updateCurrentAndLatestOffsets()
  {
    return () -> {
      try {
        updateCurrentOffsets();
        updateLatestOffsetsFromStream();
        sequenceLastUpdated = DateTimes.nowUtc();
      }
      catch (Exception e) {
        log.warn(e, "Exception while getting current/latest offsets");
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
            (Function<Map<T1, T2>, Void>) (currentSequences) -> {

              if (currentSequences != null && !currentSequences.isEmpty()) {
                task.getValue().currentSequences = currentSequences;
              }

              return null;
            }
        )
    ).collect(Collectors.toList());

    Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
  }

  private void updateLatestOffsetsFromStream()
  {
    synchronized (recordSupplierLock) {
      Set<T1> partitionIds = null;
      try {
        partitionIds = recordSupplier.getPartitionIds(ioConfig.getId());
      }
      catch (Exception e) {
        log.warn("Could not fetch partitions for topic/stream [%s]", ioConfig.getId());
        Throwables.propagate(e);
      }

      Set<StreamPartition<T1>> partitions = partitionIds
          .stream()
          .map(e -> new StreamPartition<>(ioConfig.getId(), e))
          .collect(Collectors.toSet());

      recordSupplier.assign(partitions);
      recordSupplier.seekToLatest(partitions);

      latestSequenceFromStream = partitions.stream()
                                           .collect(Collectors.toMap(
                                               StreamPartition::getPartitionId,
                                               x -> recordSupplier.position(x)
                                           ));
    }

  }

  protected Map<T1, T2> getHighestCurrentOffsets()
  {
    return taskGroups
        .values()
        .stream()
        .flatMap(taskGroup -> taskGroup.tasks.entrySet().stream())
        .flatMap(taskData -> taskData.getValue().currentSequences.entrySet().stream())
        .collect(Collectors.toMap(
            Entry::getKey,
            Entry::getValue,
            (v1, v2) -> makeSequenceNumber(v1).compareTo(makeSequenceNumber(v2)) > 0 ? v1 : v2
        ));
  }

  private SequenceNumber<T2> makeSequenceNumber(T2 seq)
  {
    return makeSequenceNumber(seq, false, false);
  }


  // exposed for testing for visibility into initialization state
  @VisibleForTesting
  public boolean isStarted()
  {
    return started;
  }

  // exposed for testing for visibility into initialization state
  @VisibleForTesting
  public boolean isLifecycleStarted()
  {
    return lifecycleStarted;
  }

  // exposed for testing for visibility into initialization state
  @VisibleForTesting
  public int getInitRetryCounter()
  {
    return initRetryCounter;
  }

  // exposed for testing to allow "bootstrap.servers" to be changed after supervisor is created
  @VisibleForTesting
  public SeekableStreamSupervisorIOConfig getIoConfig()
  {
    return ioConfig;
  }

  @Override
  public abstract void checkpoint(
      @Nullable Integer taskGroupId,
      String baseSequenceName,
      DataSourceMetadata previousCheckPoint,
      DataSourceMetadata currentCheckPoint
  );

  /**
   * creates a specific task IOConfig instance for Kafka/Kinesis*
   *
   * @return specific instance of Kafka/Kinesis IOConfig
   */
  protected abstract SeekableStreamIOConfig createIoConfig(
      int groupId,
      Map<T1, T2> startPartitions,
      Map<T1, T2> endPartitions,
      String baseSequenceName,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      Set<T1> exclusiveStartSequenceNumberPartitions,
      SeekableStreamSupervisorIOConfig ioConfig
  );

  /**
   * creates a list of specific kafka/kinesis index tasks using
   * the given replicas count
   *
   * @return list of specific kafka/kinesis index taksks
   *
   * @throws JsonProcessingException
   */
  protected abstract List<SeekableStreamIndexTask<T1, T2>> createIndexTasks(
      int replicas,
      String baseSequenceName,
      ObjectMapper sortingMapper,
      TreeMap<Integer, Map<T1, T2>> sequenceOffsets,
      SeekableStreamIOConfig taskIoConfig,
      SeekableStreamTuningConfig taskTuningConfig,
      RowIngestionMetersFactory rowIngestionMetersFactory
  ) throws JsonProcessingException;

  /**
   * calculates the taskgroup id that the given partition belongs to.
   * different between Kafka/Kinesis since Kinesis uses String as partition id
   *
   * @param partition paritition id
   *
   * @return taskgroup id
   */
  protected abstract int getTaskGroupIdForPartition(T1 partition);

  /**
   * checks if the passed in DataSourceMetadata is a specific instance
   * of [kafka/kinesis]DataSourceMetadata
   *
   * @param metadata datasource metadata
   *
   * @return true if isInstance else false
   */
  protected abstract boolean checkSourceMetadataMatch(DataSourceMetadata metadata);

  /**
   * checks if the passed in Task is a specific instance of
   * [Kafka/Kinesis]IndexTask
   *
   * @param task task
   *
   * @return true if isInstance else false
   */
  protected abstract boolean checkTaskInstance(Task task);

  /**
   * creates a specific instance of kafka/kinesis datasource metadata
   *
   * @param stream stream name
   * @param map    partitionId -> sequence
   *
   * @return specific instance of datasource metadata
   */
  protected abstract SeekableStreamDataSourceMetadata<T1, T2> createDataSourceMetaData(
      String
          stream, Map<T1, T2> map
  );

  /**
   * wraps the passed in T2 sequence number into a {@link SequenceNumber} object
   * to facilitate comparison and accomodate exclusive starting sequennce in kinesis
   *
   * @return specific instance of [Kafka/Kinesis]SequenceNumber
   */
  protected abstract SequenceNumber<T2> makeSequenceNumber(T2 seq, boolean useExclusive, boolean isExclusive);

  /**
   * schedules periodic emitLag() reporting for Kafka, not yet implemented in Kinesis,
   * but will be in the future
   */
  protected abstract void scheduleReporting(ScheduledExecutorService reportingExec);

  /**
   * calculate lag per partition for kafka, kinesis implementation returns an empty
   * map
   *
   * @return map of partition id -> lag
   */
  protected abstract Map<T1, T2> getLagPerPartition(Map<T1, T2> currentOffsets);

  /**
   * returns an instance of a specific Kinesis/Kafka recordSupplier
   *
   * @return specific instance of Kafka/Kinesis RecordSupplier
   */
  protected abstract RecordSupplier<T1, T2> setupRecordSupplier();

  /**
   * creates a specific instance of Kafka/Kinesis Supervisor Report Payload
   *
   * @return specific instance of Kafka/Kinesis Supervisor Report Payload
   */
  protected abstract SeekableStreamSupervisorReportPayload<T1, T2> createReportPayload(
      int numPartitions,
      boolean includeOffsets
  );

  /**
   * checks if sequence from metadata storage is still valid
   *
   * @return true if still valid else false
   *
   * @throws TimeoutException
   */
  protected abstract boolean checkSequenceAvailability(@NotNull T1 partition, @NotNull T2 sequenceFromMetadata)
      throws TimeoutException;
}
