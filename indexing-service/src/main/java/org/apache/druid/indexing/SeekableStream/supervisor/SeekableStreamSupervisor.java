package org.apache.druid.indexing.SeekableStream.supervisor;

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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.SeekableStream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.SeekableStream.SeekableStreamIndexTask;
import org.apache.druid.indexing.SeekableStream.SeekableStreamIndexTaskClient;
import org.apache.druid.indexing.SeekableStream.SeekableStreamIndexTaskClientFactory;
import org.apache.druid.indexing.SeekableStream.SeekableStreamTuningConfig;
import org.apache.druid.indexing.SeekableStream.common.RecordSupplier;
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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
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

abstract public class SeekableStreamSupervisor<T1 extends Comparable, T2 extends Comparable> implements Supervisor
{
  //---------------------------------------GOOD---------------------------------------------------------
  private static final EmittingLogger log = new EmittingLogger(SeekableStreamSupervisor.class);
  private static final Random RANDOM = new Random();
  private static final long MAX_RUN_FREQUENCY_MILLIS = 1000;
  private static final long MINIMUM_FUTURE_TIMEOUT_IN_SECONDS = 120;
  //---------------------------------------GOOD---------------------------------------------------------


  public static final String IS_INCREMENTAL_HANDOFF_SUPPORTED = "IS_INCREMENTAL_HANDOFF_SUPPORTED";

  private class TaskGroup
  {
    final int groupId;

    final ImmutableMap<T1, T2> partitionOffsets;
    final ConcurrentHashMap<String, TaskData> tasks = new ConcurrentHashMap<>();
    final Optional<DateTime> minimumMessageTime;
    final Optional<DateTime> maximumMessageTime;
    final Set<String> exclusiveStartSequenceNumberPartitions;
    final TreeMap<Integer, Map<Integer, Long>> sequenceOffsets = new TreeMap<>();
    final String baseSequenceName;
    DateTime completionTimeout;

    public TaskGroup(
        int groupId,
        ImmutableMap<T1, T2> partitionOffsets,
        Optional<DateTime> minimumMessageTime,
        Optional<DateTime> maximumMessageTime,
        Set<String> exclusiveStartSequenceNumberPartitions
    )
    {
      this.groupId = groupId;
      this.partitionOffsets = partitionOffsets;
      this.minimumMessageTime = minimumMessageTime;
      this.maximumMessageTime = maximumMessageTime;
      this.exclusiveStartSequenceNumberPartitions = exclusiveStartSequenceNumberPartitions != null
                                                    ? exclusiveStartSequenceNumberPartitions
                                                    : new HashSet<>();
      this.baseSequenceName = generateSequenceName(partitionOffsets, minimumMessageTime, maximumMessageTime);
    }

    int addNewCheckpoint(Map<Integer, Long> checkpoint)
    {
      sequenceOffsets.put(sequenceOffsets.lastKey() + 1, checkpoint);
      return sequenceOffsets.lastKey();
    }

    public Set<String> taskIds()
    {
      return tasks.keySet();
    }
  }

  private class TaskData
  {
    volatile TaskStatus status;
    volatile DateTime startTime;
    volatile Map<T1, T2> currentOffsets = new HashMap<>();

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

  //TODO: doc
  private final ConcurrentHashMap<Integer, TaskGroup> taskGroups = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, CopyOnWriteArrayList<TaskGroup>> pendingCompletionTaskGroups = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, ConcurrentHashMap<T1, T2>> partitionGroups = new ConcurrentHashMap<>();

  private final TaskStorage taskStorage;
  private final TaskMaster taskMaster;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final SeekableStreamIndexTaskClient<T1, T2> taskClient;
  private final ObjectMapper sortingMapper;
  private final SeekableStreamSupervisorSpec spec;
  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;
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
  private final Object consumerLock = new Object();

  private boolean listenerRegistered = false;
  private long lastRunTime;

  private volatile DateTime firstRunTime;
  private volatile DateTime earlyPublishTime = null;
  private volatile RecordSupplier<T1, T2> recordSupplier;
  private volatile boolean started = false;
  private volatile boolean stopped = false;

  private final T2 NOT_SET;

  //-------------------------------GOOD----------------------------------------

  /**
   * Notice is used to queue tasks that are internal to the supervisor
   */
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

  //---------------------------GOOD-----------------------------------------
  public SeekableStreamSupervisor(
      final String supervisorId,
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final SeekableStreamIndexTaskClientFactory taskClientFactory,
      final ObjectMapper mapper,
      final SeekableStreamSupervisorSpec spec,
      final RowIngestionMetersFactory rowIngestionMetersFactory,
      final T2 NOT_SET
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
    this.NOT_SET = NOT_SET;

    this.dataSource = spec.getDataSchema().getDataSource();
    this.ioConfig = spec.getIoConfig();
    this.tuningConfig = spec.getTuningConfig();
    this.taskTuningConfig = SeekableStreamTuningConfig.copyOf(this.tuningConfig);
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

  //-------------------------------GOOD----------------------------------------
  @Override
  public void start()
  {
    synchronized (stateChangeLock) {
      Preconditions.checkState(!started, "already started");
      Preconditions.checkState(!exec.isShutdown(), "already stopped");

      try {
        recordSupplier = setupRecordSupplier();

        exec.submit(
            () -> {
              try {
                while (!Thread.currentThread().isInterrupted()) {
                  final Notice notice = notices.take();

                  try {
                    notice.handle();
                  }
                  catch (Throwable e) {
                    log.makeAlert(e, "[%s] failed to handle notice", supervisorId)
                       .addData("noticeClass", notice.getClass().getSimpleName())
                       .emit();
                  }
                }
              }
              catch (InterruptedException e) {
                log.info("[%s] interrupted, exiting", supervisorId);
              }
            }
        );
        firstRunTime = DateTimes.nowUtc().plus(ioConfig.getStartDelay());
        scheduledExec.scheduleAtFixedRate(
            () -> notices.add(new RunNotice()),
            ioConfig.getStartDelay().getMillis(),
            Math.max(ioConfig.getPeriod().getMillis(), MAX_RUN_FREQUENCY_MILLIS),
            TimeUnit.MILLISECONDS
        );


        //TODO: this logic is different from kafka and kinesis, need to look into
        scheduleReporting();

        started = true;
        log.info(
            "Started [%s], first run in [%s], with spec: [%s]",
            supervisorId,
            ioConfig.getStartDelay(),
            spec.toString()
        );

      }
      catch (Exception e) {
        if (recordSupplier != null) {
          recordSupplier.close();
        }
        log.makeAlert(e, "Exception starting [%s]", supervisorId)
           .emit();
        throw Throwables.propagate(e);
      }
    }
  }


  //---------------------------GOOOD------------------------------
  @Override
  public void stop(boolean stopGracefully)
  {
    synchronized (stateChangeLock) {
      Preconditions.checkState(started, "not started");

      log.info("Beginning shutdown of [%s]", supervisorId);

      try {
        scheduledExec.shutdownNow(); // stop recurring executions
        reportingExec.shutdownNow();
        recordSupplier.close();

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

        log.info("[%s] has stopped", supervisorId);
      }
      catch (Exception e) {
        log.makeAlert(e, "Exception stopping [%s]", supervisorId)
           .emit();
      }
    }
  }


  //-------------------------------GOOD----------------------------------------
  @Override
  public SupervisorReport getStatus()
  {
    return generateReport(true);
  }

  //-------------------------------GOOD----------------------------------------
  @Override
  public void reset(DataSourceMetadata dataSourceMetadata)
  {
    log.info("Posting ResetNotice");
    notices.add(new ResetNotice(dataSourceMetadata));
  }

  //TODO: checkpoints
  @Override
  abstract public void checkpoint(
      @Nullable Integer taskGroupId,
      String baseSequenceName,
      DataSourceMetadata previousCheckPoint,
      DataSourceMetadata currentCheckPoint
  );

  //-------------------------------GOOD----------------------------------------
  @VisibleForTesting
  void runInternal()
      throws ExecutionException, InterruptedException, TimeoutException, JsonProcessingException
  {
    possiblyRegisterListener();
    updatePartitionDataFromStream();
    discoverTasks();
    updateTaskStatus();
    checkTaskDuration();
    checkPendingCompletionTasks();
    checkCurrentTaskState();
    createNewTasks();

    if (log.isDebugEnabled()) {
      log.debug(generateReport(true).toString());
    } else {
      log.info(generateReport(false).toString());
    }
  }

  //-------------------------------GOOD----------------------------------------
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

  //-------------------------------GOOD----------------------------------------
  @VisibleForTesting
  void gracefulShutdownInternal() throws ExecutionException, InterruptedException, TimeoutException
  {
    for (TaskGroup taskGroup : taskGroups.values()) {
      for (Map.Entry<String, TaskData> entry :
          taskGroup.tasks.entrySet()) {
        if (taskInfoProvider.getTaskLocation(entry.getKey()).equals(TaskLocation.unknown())) {
          killTask(entry.getKey());
        } else {
          entry.getValue().startTime = new DateTime(0);
        }
      }
    }

    checkTaskDuration();
  }

  //-------------------------------GOOD----------------------------------------
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
    } else {

      checkSourceMetadataInstanceMatch(dataSourceMetadata);

      // Reset only the partitions in dataSourceMetadata if it has not been reset yet
      final SeekableStreamDataSourceMetadata<T1, T2> resetMetadata = (SeekableStreamDataSourceMetadata<T1, T2>) dataSourceMetadata;

      if (resetMetadata.getSeekableStreamPartitions().getId().equals(ioConfig.getId())) {
        // metadata can be null
        final DataSourceMetadata metadata = indexerMetadataStorageCoordinator.getDataSourceMetadata(dataSource);
        checkSourceMetadataInstanceMatch(metadata);

        final SeekableStreamDataSourceMetadata<T1, T2> currentMetadata = (SeekableStreamDataSourceMetadata<T1, T2>) metadata;

        // defend against consecutive reset requests from replicas
        // as well as the case where the metadata store do not have an entry for the reset partitions
        boolean doReset = false;
        for (Map.Entry<T1, T2> resetPartitionOffset : resetMetadata.getSeekableStreamPartitions()
                                                                   .getPartitionSequenceMap()
                                                                   .entrySet()) {
          final T2 partitionOffsetInMetadataStore = currentMetadata == null
                                                    ? null
                                                    : currentMetadata.getSeekableStreamPartitions()
                                                                     .getPartitionSequenceMap()
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
          resetMetadata.getSeekableStreamPartitions().getPartitionSequenceMap().keySet().forEach(partition -> {
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
            resetMetadata.getSeekableStreamPartitions().getId(),
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
      killTasksInGroup(taskGroups.get(getTaskGroupIdForPartition(partition)));
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

  abstract protected int getTaskGroupIdForPartition(T1 partition);

  abstract protected void checkSourceMetadataInstanceMatch(DataSourceMetadata metadata) throws IAE;

  // TODO: may want to put more logic in the base class
  abstract protected SupervisorReport<SeekableStreamSupervisorReportPayload> generateReport(boolean includeOffsets);

  abstract protected void updatePartitionDataFromStream();

  private void discoverTasks() throws ExecutionException, InterruptedException, TimeoutException
  {
    int taskCount = 0;
    List<String> futureTaskIds = Lists.newArrayList();
    List<ListenableFuture<Boolean>> futures = Lists.newArrayList();
    List<Task> tasks = taskStorage.getActiveTasks();
    final Map<Integer, TaskGroup> taskGroupsToVerify = new HashMap<>();

    for (Task task : tasks) {
      if (!isTaskInstanceOfThis(task) || !dataSource.equals(task.getDataSource())) {
        continue;
      }

      taskCount++;
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
                                               .getPartitionSequenceMap()
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
                                                 .getPartitionSequenceMap()
                                                 .keySet()
                                                 .forEach(
                                                     partition -> addDiscoveredTaskToPendingCompletionTaskGroups(
                                                         getTaskGroupIdForPartition(partition),
                                                         taskId,
                                                         seekableStreamIndexTask.getIOConfig()
                                                                                .getStartPartitions()
                                                                                .getPartitionSequenceMap()
                                                     )
                                                 );

                          // update partitionGroups with the publishing task's offsets (if they are greater than what is
                          // existing) so that the next tasks will start reading from where this task left off
                          Map<T1, T2> publishingTaskEndOffsets = taskClient.getEndOffsets(taskId);

                          for (Map.Entry<T1, T2> entry : publishingTaskEndOffsets.entrySet()) {
                            T1 partition = entry.getKey();
                            T2 offset = entry.getValue();
                            ConcurrentHashMap<T1, T2> partitionOffsets = partitionGroups.get(
                                getTaskGroupIdForPartition(partition)
                            );

                            boolean succeeded;
                            do {
                              succeeded = true;
                              T2 previousOffset = partitionOffsets.putIfAbsent(partition, offset);
                              if (previousOffset != null && (previousOffset.compareTo(offset)) < 0) {
                                succeeded = partitionOffsets.replace(partition, previousOffset, offset);
                              }
                            } while (!succeeded);
                          }
                        } else {
                          for (T1 partition : seekableStreamIndexTask.getIOConfig()
                                                                     .getStartPartitions()
                                                                     .getPartitionSequenceMap()
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
                                                                 .getPartitionSequenceMap()
                                      ),
                                      seekableStreamIndexTask.getIOConfig().getMinimumMessageTime(),
                                      seekableStreamIndexTask.getIOConfig().getMaximumMessageTime(),
                                      null //TODO: fix
                                  );
                                }
                            );
                            taskGroupsToVerify.put(taskGroupId, taskGroup);
                            final TaskData prevTaskGroup = taskGroup.tasks.putIfAbsent(taskId, new TaskData());
                            if (prevTaskGroup != null) {
                              throw new ISE(
                                  "WTH? a taskGroup[%s] already exists for new task[%s]",
                                  prevTaskGroup,
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
    if (!taskOptional.isPresent() || !(isTaskInstanceOfThis(taskOptional.get()))) {
      return false;
    }

    String taskSequenceName = ((SeekableStreamIndexTask) taskOptional.get()).getIOConfig().getBaseSequenceName();
    if (taskGroups.get(taskGroupId) != null) {
      return Preconditions
          .checkNotNull(taskGroups.get(taskGroupId), "null taskGroup for taskId[%s]", taskGroupId)
          .baseSequenceName
          .equals(taskSequenceName);
    } else {
      return generateSequenceName(
          ((SeekableStreamIndexTask<T1, T2>) taskOptional.get()).getIOConfig()
                                                                .getStartPartitions()
                                                                .getPartitionSequenceMap(),
          ((SeekableStreamIndexTask<T1, T2>) taskOptional.get()).getIOConfig().getMinimumMessageTime(),
          ((SeekableStreamIndexTask<T1, T2>) taskOptional.get()).getIOConfig().getMaximumMessageTime()
      ).equals(taskSequenceName);
    }
  }

  String generateSequenceName(
      Map<T1, T2> startPartitions,
      Optional<DateTime> minimumMessageTime,
      Optional<DateTime> maximumMessageTime
  )
  {
    StringBuilder sb = new StringBuilder();

    for (Map.Entry<T1, T2> entry : startPartitions.entrySet()) {
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

  abstract protected void updateTaskStatus() throws ExecutionException, InterruptedException, TimeoutException;

  abstract protected void checkTaskDuration() throws ExecutionException, InterruptedException, TimeoutException;

  abstract protected void checkPendingCompletionTasks()
      throws ExecutionException, InterruptedException, TimeoutException;

  abstract protected boolean isTaskInstanceOfThis(Task task);

  abstract protected void checkCurrentTaskState() throws ExecutionException, InterruptedException, TimeoutException;

  abstract protected void createNewTasks() throws JsonProcessingException;

  abstract protected RecordSupplier<T1, T2> setupRecordSupplier();

  abstract protected SeekableStreamSupervisorReportPayload createSeekableStreamSupervisorReportPayload();

  abstract protected void scheduleReporting();

}
