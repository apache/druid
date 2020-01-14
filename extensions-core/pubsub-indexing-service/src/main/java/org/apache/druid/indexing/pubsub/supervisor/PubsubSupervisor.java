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

package org.apache.druid.indexing.pubsub.supervisor;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IndexTaskClient;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.pubsub.PubsubIndexTask;
import org.apache.druid.indexing.pubsub.PubsubIndexTaskClient;
import org.apache.druid.indexing.pubsub.PubsubIndexTaskClientFactory;
import org.apache.druid.indexing.pubsub.PubsubIndexTaskIOConfig;
import org.apache.druid.indexing.pubsub.PubsubIndexTaskTuningConfig;
import org.apache.druid.indexing.pubsub.PubsubRecordSupplier;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * Supervisor responsible for managing the PubsubIndexTasks for a single dataSource. At a high level, the class accepts a
 * {@link PubsubSupervisorSpec} which includes the pubsub topic and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes it's list of running indexing tasks and
 * ensures that there are enough tasks to satisfy the desired number of replicas.
 * As tasks complete, new tasks are queued to process next packets.
 */
public class PubsubSupervisor implements Supervisor
{

  private static final EmittingLogger log = new EmittingLogger(PubsubSupervisor.class);
  private static final long MINIMUM_GET_OFFSET_PERIOD_MILLIS = 5000;
  private static final long INITIAL_GET_OFFSET_DELAY_MILLIS = 15000;
  private static final long INITIAL_EMIT_LAG_METRIC_DELAY_MILLIS = 25000;
  private static final Long NOT_SET = -1L;
  private static final Long END_OF_PARTITION = Long.MAX_VALUE;
  private static final long MINIMUM_FUTURE_TIMEOUT_IN_SECONDS = 120;
  protected final PubsubSupervisorStateManager stateManager;
  protected final ObjectMapper sortingMapper;
  protected final String dataSource;
  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private final PubsubSupervisorSpec spec;
  private final TaskStorage taskStorage;
  private final TaskMaster taskMaster;
  private final PubsubIndexTaskClient taskClient;
  private final String supervisorId;
  private final TaskInfoProvider taskInfoProvider;
  private final long futureTimeoutInSeconds; // how long to wait for async operations to complete
  private final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final ExecutorService exec;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final ScheduledExecutorService scheduledExec;
  private final ScheduledExecutorService reportingExec;
  private final ListeningExecutorService workerExec;
  private final BlockingQueue<Notice> notices = new LinkedBlockingDeque<>();
  private final Object stopLock = new Object();
  private final Object stateChangeLock = new Object();
  private final Object recordSupplierLock = new Object();
  private final PubsubSupervisorIOConfig ioConfig;
  private final PubsubSupervisorTuningConfig tuningConfig;
  private final PubsubIndexTaskTuningConfig taskTuningConfig;
  private volatile Map<Integer, Long> latestSequenceFromStream;
  private boolean listenerRegistered = false;
  private long lastRunTime;
  private int initRetryCounter = 0;
  private volatile DateTime firstRunTime;
  private volatile DateTime earlyStopTime = null;
  private volatile PubsubRecordSupplier recordSupplier;
  private volatile boolean started = false;
  private volatile boolean stopped = false;
  private volatile boolean lifecycleStarted = false;

  public PubsubSupervisor(
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final PubsubIndexTaskClientFactory taskClientFactory,
      final ObjectMapper mapper,
      final PubsubSupervisorSpec spec,
      final RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.sortingMapper = mapper.copy().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    this.spec = spec;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.dataSource = spec.getDataSchema().getDataSource();
    this.ioConfig = spec.getIoConfig();
    this.tuningConfig = spec.getTuningConfig();
    this.taskTuningConfig = this.tuningConfig.convertToTaskTuningConfig();
    this.supervisorId = StringUtils.format("PubsubSupervisor-%s", spec.getDataSchema().getDataSource());
    this.exec = Execs.singleThreaded(supervisorId);
    this.scheduledExec = Execs.scheduledSingleThreaded(supervisorId + "-Scheduler-%d");
    this.reportingExec = Execs.scheduledSingleThreaded(supervisorId + "-Reporting-%d");
    this.stateManager = new PubsubSupervisorStateManager(
        spec.getSupervisorStateManagerConfig(),
        spec.isSuspended()
    );

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

    this.emitter = spec.getEmitter();
    this.monitorSchedulerConfig = spec.getMonitorSchedulerConfig();
    log.error("NOT IMPLEMENTED YET!");
  }

  PubsubIndexTaskIOConfig createTaskIoConfig(PubsubSupervisorIOConfig ioConfig)
  {
    return new PubsubIndexTaskIOConfig(
        ioConfig.getProjectId(),
        ioConfig.getSubscription(),
        null,
        ioConfig.getPollTimeout(),
        DateTimes.nowUtc(),
        DateTimes.nowUtc(),
        ioConfig.getInputFormat(
            spec.getDataSchema().getParser() == null ? null : spec.getDataSchema().getParser().getParseSpec()
        )
    );
  }

  @Override
  public void start()
  {
    log.error("UNDER CONSTRUCTION!");
    PubsubIndexTaskIOConfig newIoConfig = createTaskIoConfig(ioConfig);

    Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
      try {
        taskQueue.get().add(new PubsubIndexTask(
            "pubsub-task",
            new TaskResource("pubsub-task", 1),
            this.spec.getDataSchema(),
            this.tuningConfig,
            newIoConfig,
            null,
            null,
            null,
            rowIngestionMetersFactory,
            sortingMapper,
            null
        ));
      }
      catch (Exception e) {
        log.error(e, "failed pub sup");
      }
      // TODO
    }
  }

  /**
   * @param stopGracefully If true, supervisor will cleanly shutdown managed tasks if possible (for example signalling
   *                       them to publish their segments and exit). The implementation may block until the tasks have
   *                       either acknowledged or completed. If false, supervisor will stop immediately and leave any
   *                       running tasks as they are.
   */
  @Override
  public void stop(boolean stopGracefully)
  {
    // TODO
  }

  @Override
  public SupervisorReport getStatus()
  {
    return null; // TODO
  }

  @Override
  public SupervisorStateManager.State getState()
  {
    return stateManager.getSupervisorState();
  }

  @Override
  public Map<String, Map<String, Object>> getStats()
  {
    return ImmutableMap.of(); // TODO
  }

  @Override
  @Nullable
  public Boolean isHealthy()
  {
    return stateManager.isHealthy();
  }

  @Override
  public void reset(DataSourceMetadata dataSourceMetadata)
  {
    // TODO
  }

  /**
   * The definition of checkpoint is not very strict as currently it does not affect data or control path.
   * On this call Supervisor can potentially checkpoint data processed so far to some durable storage
   * for example - Kafka/Kinesis Supervisor uses this to merge and handoff segments containing at least the data
   * represented by {@param currentCheckpoint} DataSourceMetadata
   *
   * @param taskGroupId        unique Identifier to figure out for which sequence to do checkpointing
   * @param checkpointMetadata metadata for the sequence to currently checkpoint
   */
  @Override
  public void checkpoint(int taskGroupId, DataSourceMetadata checkpointMetadata)
  {
    // TODO
  }

  /**
   * Notice is used to queue tasks that are internal to the supervisor
   */
  private interface Notice
  {
    void handle() throws ExecutionException, InterruptedException, TimeoutException;
  }

}
