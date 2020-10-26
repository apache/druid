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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 *TODO
 */
public class PubsubSupervisor implements Supervisor
{

  private static final EmittingLogger log = new EmittingLogger(PubsubSupervisor.class);
  protected final PubsubSupervisorStateManager stateManager;
  protected final String dataSource;
  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private final PubsubSupervisorSpec spec;
  private final TaskStorage taskStorage;
  private final TaskMaster taskMaster;
  private final PubsubIndexTaskClient taskClient;
  private final String supervisorId;
  private final TaskInfoProvider taskInfoProvider;
  private final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final ExecutorService exec;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final ScheduledExecutorService scheduledExec;
  private final ScheduledExecutorService reportingExec;
  private final ListeningExecutorService workerExec;
  private final PubsubSupervisorIOConfig ioConfig;
  private final PubsubSupervisorTuningConfig tuningConfig;
  private final PubsubIndexTaskTuningConfig taskTuningConfig;
  private volatile Integer taskCount = 1;
  private volatile List<String> activeTaskIds = new ArrayList<>();
  private volatile Map<String, DateTime> startTimes = new HashMap<>();

  public PubsubSupervisor(
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final PubsubIndexTaskClientFactory taskClientFactory,
      final PubsubSupervisorSpec spec,
      final RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
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
    this.taskCount = ioConfig.getTaskCount();
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

    int chatThreads = (this.tuningConfig.getChatThreads() != null
                       ? this.tuningConfig.getChatThreads()
                       : Math.min(10, this.ioConfig.getTaskCount()));
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
    PubsubIndexTaskIOConfig newIoConfig = createTaskIoConfig(ioConfig);
    String subscriptionName = newIoConfig.getSubscription();
    Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
      exec.submit(() -> {
        try {
          while (true) {
            while (activeTaskIds.size() < taskCount) {
              String taskId = IdUtils.getRandomIdWithPrefix(subscriptionName);
              PubsubIndexTask pubsubIndexTask = new PubsubIndexTask(
                  taskId,
                  new TaskResource(taskId, 1),
                  this.spec.getDataSchema(),
                  this.tuningConfig,
                  newIoConfig,
                  null,
                  null,
                  null,
                  rowIngestionMetersFactory,
                  null
              );
              try {
                taskQueue.get().add(pubsubIndexTask);
              }
              catch (EntryExistsException e) {
                log.error("EntryExistsException");
              }
              activeTaskIds.add(taskId);
              startTimes.put(taskId, DateTimes.MAX);
            }
            activeTaskIds = activeTaskIds
                .stream().filter(t -> taskStorage.getStatus(t).isPresent() && !taskStorage.getStatus(t).get().isComplete())
                .collect(Collectors.toList());
            HashMap<String, DateTime> updatedStartTimes = new HashMap<>(startTimes);
            for (String taskId: startTimes.keySet()) {
              if (!activeTaskIds.contains(taskId)) {
                updatedStartTimes.remove(taskId);
              } else if (startTimes.get(taskId).isEqual(DateTimes.MAX)) {
                DateTime startTime = taskClient.getStartTime(taskId);
                if (startTime != null) {
                  updatedStartTimes.put(taskId, startTime);
                }
              } else if (startTimes.get(taskId).withDurationAdded(ioConfig.getTaskDuration(), 1).isBeforeNow()) {
                if (taskClient.finalize(taskId)) {
                  updatedStartTimes.remove(taskId);
                }
              }
            }
            startTimes = updatedStartTimes;
            stateManager.markRunFinished();
            stateManager.maybeSetState(SupervisorStateManager.BasicState.RUNNING);
            Thread.sleep(500);
          }
        }
        catch (Exception e) {
          log.error(e, "failed pub sup");
        }
      });
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
    //TODO
  }

  @Override
  public SupervisorReport getStatus()
  {
    return null; //TODO
  }

  @Override
  public SupervisorStateManager.State getState()
  {
    return stateManager.getSupervisorState();
  }

  @Override
  public Map<String, Map<String, Object>> getStats()
  {
    return ImmutableMap.of(); //TODO
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
    //TODO
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
    //TODO
  }
}
