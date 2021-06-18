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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.IngestionTestBase;
import org.apache.druid.indexing.common.task.NoopTestTaskReportFileWriter;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.indexing.worker.shuffle.IntermediaryDataManager;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.LocalDataSegmentPuller;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.NoopDataSegmentKiller;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AbstractParallelIndexSupervisorTaskTest extends IngestionTestBase
{
  static final String DISABLE_TASK_INJECT_CONTEXT_KEY = "disableInject";
  static final TimestampSpec DEFAULT_TIMESTAMP_SPEC = new TimestampSpec("ts", "auto", null);
  static final DimensionsSpec DEFAULT_DIMENSIONS_SPEC = new DimensionsSpec(
      DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim"))
  );
  static final ParseSpec DEFAULT_PARSE_SPEC = new CSVParseSpec(
      DEFAULT_TIMESTAMP_SPEC,
      DEFAULT_DIMENSIONS_SPEC,
      null,
      Arrays.asList("ts", "dim", "val"),
      false,
      0
  );
  static final InputFormat DEFAULT_INPUT_FORMAT = new CsvInputFormat(
      Arrays.asList("ts", "dim", "val"),
      null,
      false,
      false,
      0
  );
  public static final ParallelIndexTuningConfig DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING =
      new ParallelIndexTuningConfig(
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          2,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null
      );

  protected static final double DEFAULT_TRANSIENT_TASK_FAILURE_RATE = 0.3;
  protected static final double DEFAULT_TRANSIENT_API_FAILURE_RATE = 0.3;

  private static final Logger LOG = new Logger(AbstractParallelIndexSupervisorTaskTest.class);

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Transient task failure rate emulated by the taskKiller in {@link SimpleThreadingTaskRunner}.
   * Per {@link SubTaskSpec}, there could be at most one task failure.
   *
   * @see #DEFAULT_TRANSIENT_TASK_FAILURE_RATE
   */
  private final double transientTaskFailureRate;

  /**
   * Transient API call failure rate emulated by {@link LocalParallelIndexSupervisorTaskClient}.
   * This will be applied to every API calls in the future.
   *
   * @see #DEFAULT_TRANSIENT_API_FAILURE_RATE
   */
  private final double transientApiCallFailureRate;

  private File localDeepStorage;
  private SimpleThreadingTaskRunner taskRunner;
  private ObjectMapper objectMapper;
  private LocalIndexingServiceClient indexingServiceClient;
  private IntermediaryDataManager intermediaryDataManager;
  private CoordinatorClient coordinatorClient;
  // An executor that executes API calls using a different thread from the caller thread as if they were remote calls.
  private ExecutorService remoteApiExecutor;

  protected AbstractParallelIndexSupervisorTaskTest(
      double transientTaskFailureRate,
      double transientApiCallFailureRate
  )
  {
    this.transientTaskFailureRate = transientTaskFailureRate;
    this.transientApiCallFailureRate = transientApiCallFailureRate;
  }

  @Before
  public void setUpAbstractParallelIndexSupervisorTaskTest() throws IOException
  {
    localDeepStorage = temporaryFolder.newFolder("localStorage");
    taskRunner = new SimpleThreadingTaskRunner();
    objectMapper = getObjectMapper();
    indexingServiceClient = new LocalIndexingServiceClient(objectMapper, taskRunner);
    intermediaryDataManager = new IntermediaryDataManager(
        new WorkerConfig(),
        new TaskConfig(
            null,
            null,
            null,
            null,
            null,
            false,
            null,
            null,
            ImmutableList.of(new StorageLocationConfig(temporaryFolder.newFolder(), null, null)),
            false,
            false
        ),
        null
    );
    remoteApiExecutor = Execs.singleThreaded("coordinator-api-executor");
    coordinatorClient = new LocalCoordinatorClient(remoteApiExecutor);
    prepareObjectMapper(objectMapper, getIndexIO());
  }

  @After
  public void tearDownAbstractParallelIndexSupervisorTaskTest()
  {
    remoteApiExecutor.shutdownNow();
    taskRunner.shutdown();
    temporaryFolder.delete();
  }

  protected ParallelIndexTuningConfig newTuningConfig(
      PartitionsSpec partitionsSpec,
      int maxNumConcurrentSubTasks,
      boolean forceGuaranteedRollup
  )
  {
    return new ParallelIndexTuningConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new MaxSizeSplitHintSpec(null, 1),
        partitionsSpec,
        null,
        null,
        null,
        forceGuaranteedRollup,
        null,
        null,
        null,
        null,
        maxNumConcurrentSubTasks,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  protected LocalIndexingServiceClient getIndexingServiceClient()
  {
    return indexingServiceClient;
  }

  protected CoordinatorClient getCoordinatorClient()
  {
    return coordinatorClient;
  }

  private static class TaskContainer
  {
    private final Task task;
    @MonotonicNonNull
    private volatile Future<TaskStatus> statusFuture;
    @MonotonicNonNull
    private volatile TestLocalTaskActionClient actionClient;

    private TaskContainer(Task task)
    {
      this.task = task;
    }

    private void setStatusFuture(Future<TaskStatus> statusFuture)
    {
      this.statusFuture = statusFuture;
    }

    private void setActionClient(TestLocalTaskActionClient actionClient)
    {
      this.actionClient = actionClient;
    }
  }

  public class SimpleThreadingTaskRunner
  {
    private final ConcurrentMap<String, TaskContainer> tasks = new ConcurrentHashMap<>();
    private final ListeningExecutorService service = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(5, "simple-threading-task-runner-%d")
    );

    private final ScheduledExecutorService taskKiller = Execs.scheduledSingleThreaded("simple-threading-task-killer");
    private final Set<String> killedSubtaskSpecs = new HashSet<>();

    SimpleThreadingTaskRunner()
    {
      taskKiller.scheduleAtFixedRate(
          () -> {
            for (TaskContainer taskContainer : tasks.values()) {
              boolean kill = ThreadLocalRandom.current().nextDouble() < transientTaskFailureRate;
              if (kill && !taskContainer.statusFuture.isDone()) {
                String subtaskSpecId = taskContainer.task instanceof AbstractBatchSubtask
                                       ? ((AbstractBatchSubtask) taskContainer.task).getSubtaskSpecId()
                                       : null;
                if (subtaskSpecId != null && !killedSubtaskSpecs.contains(subtaskSpecId)) {
                  killedSubtaskSpecs.add(subtaskSpecId);
                  taskContainer.statusFuture.cancel(true);
                  LOG.info(
                      "Transient task failure test. Killed task[%s] for spec[%s]",
                      taskContainer.task.getId(),
                      subtaskSpecId
                  );
                }
              }
            }
          },
          100,
          100,
          TimeUnit.MILLISECONDS
      );
    }

    public void shutdown()
    {
      service.shutdownNow();
      taskKiller.shutdownNow();
    }

    public String run(Task task)
    {
      runTask(task);
      return task.getId();
    }

    private TaskStatus runAndWait(Task task)
    {
      try {
        return runTask(task).get();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    private TaskStatus waitToFinish(Task task, long waitTime, TimeUnit timeUnit)
    {
      final TaskContainer taskContainer = tasks.get(task.getId());
      if (taskContainer == null) {
        throw new IAE("Unknown task[%s]", task.getId());
      }
      try {
        while (taskContainer.statusFuture == null && !Thread.currentThread().isInterrupted()) {
          Thread.sleep(10);
        }
        return taskContainer.statusFuture.get(waitTime, timeUnit);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      catch (ExecutionException | TimeoutException e) {
        throw new RuntimeException(e);
      }
    }

    private Future<TaskStatus> runTask(Task task)
    {
      final TaskContainer taskContainer = new TaskContainer(task);
      if (tasks.put(task.getId(), taskContainer) != null) {
        throw new ISE("Duplicate task ID[%s]", task.getId());
      }
      try {
        prepareTaskForLocking(task);
      }
      catch (EntryExistsException e) {
        throw new RuntimeException(e);
      }
      task.addToContextIfAbsent(
          SinglePhaseParallelIndexTaskRunner.CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY,
          SinglePhaseParallelIndexTaskRunner.DEFAULT_USE_LINEAGE_BASED_SEGMENT_ALLOCATION
      );
      final ListenableFuture<TaskStatus> statusFuture = service.submit(
          () -> {
            try {
              final TestLocalTaskActionClient actionClient = createActionClient(task);
              final TaskToolbox toolbox = createTaskToolbox(task, actionClient);
              taskContainer.setActionClient(actionClient);
              if (task.isReady(toolbox.getTaskActionClient())) {
                return task.run(toolbox);
              } else {
                getTaskStorage().setStatus(TaskStatus.failure(task.getId()));
                throw new ISE("task[%s] is not ready", task.getId());
              }
            }
            catch (Exception e) {
              getTaskStorage().setStatus(TaskStatus.failure(task.getId(), e.getMessage()));
              throw new RuntimeException(e);
            }
          }
      );
      taskContainer.setStatusFuture(statusFuture);
      final ListenableFuture<TaskStatus> cleanupFuture = Futures.transform(
          statusFuture,
          (Function<TaskStatus, TaskStatus>) status -> {
            shutdownTask(task);
            return status;
          }
      );
      return cleanupFuture;
    }

    @Nullable
    public String cancel(String taskId)
    {
      final TaskContainer taskContainer = tasks.remove(taskId);
      if (taskContainer != null && taskContainer.statusFuture != null) {
        taskContainer.statusFuture.cancel(true);
        return taskId;
      } else {
        return null;
      }
    }

    @Nullable
    public TaskStatus getStatus(String taskId)
    {
      final TaskContainer taskContainer = tasks.get(taskId);
      if (taskContainer != null && taskContainer.statusFuture != null) {
        try {
          if (taskContainer.statusFuture.isDone()) {
            return taskContainer.statusFuture.get();
          } else {
            return TaskStatus.running(taskId);
          }
        }
        catch (InterruptedException | ExecutionException | CancellationException e) {
          // We don't have a way to propagate this exception to the supervisorTask yet..
          // So, let's print it here.
          LOG.error(e, "Task[%s] failed", taskId);
          return TaskStatus.failure(taskId, e.getMessage());
        }
      } else {
        return null;
      }
    }

    public Set<DataSegment> getPublishedSegments(String taskId)
    {
      final TaskContainer taskContainer = tasks.get(taskId);
      if (taskContainer == null || taskContainer.actionClient == null) {
        return Collections.emptySet();
      } else {
        return taskContainer.actionClient.getPublishedSegments();
      }
    }
  }

  public class LocalIndexingServiceClient extends NoopIndexingServiceClient
  {
    private final ObjectMapper objectMapper;
    private final SimpleThreadingTaskRunner taskRunner;

    public LocalIndexingServiceClient(ObjectMapper objectMapper, SimpleThreadingTaskRunner taskRunner)
    {
      this.objectMapper = objectMapper;
      this.taskRunner = taskRunner;
    }

    @Override
    public String runTask(String taskId, Object taskObject)
    {
      final Task task = (Task) taskObject;
      return taskRunner.run(injectIfNeeded(task));
    }

    public TaskStatus runAndWait(Task task)
    {
      return taskRunner.runAndWait(injectIfNeeded(task));
    }

    public TaskStatus waitToFinish(Task task, long timeout, TimeUnit timeUnit)
    {
      return taskRunner.waitToFinish(task, timeout, timeUnit);
    }

    private Task injectIfNeeded(Task task)
    {
      if (!task.getContextValue(DISABLE_TASK_INJECT_CONTEXT_KEY, false)) {
        try {
          final byte[] json = objectMapper.writeValueAsBytes(task);
          return objectMapper.readValue(json, Task.class);
        }
        catch (IOException e) {
          LOG.error(e, "Error while serializing and deserializing task spec");
          throw new RuntimeException(e);
        }
      } else {
        return task;
      }
    }

    @Override
    public String cancelTask(String taskId)
    {
      return taskRunner.cancel(taskId);
    }

    @Override
    public TaskStatusResponse getTaskStatus(String taskId)
    {
      final Optional<Task> task = getTaskStorage().getTask(taskId);
      final String groupId = task.isPresent() ? task.get().getGroupId() : null;
      final String taskType = task.isPresent() ? task.get().getType() : null;
      final TaskStatus taskStatus = taskRunner.getStatus(taskId);

      if (taskStatus != null) {
        return new TaskStatusResponse(
            taskId,
            new TaskStatusPlus(
                taskId,
                groupId,
                taskType,
                DateTimes.EPOCH,
                DateTimes.EPOCH,
                taskStatus.getStatusCode(),
                taskStatus.isComplete() ? RunnerTaskState.NONE : RunnerTaskState.RUNNING,
                -1L,
                TaskLocation.unknown(),
                null,
                null
            )
        );
      } else {
        return new TaskStatusResponse(taskId, null);
      }
    }

    public Set<DataSegment> getPublishedSegments(Task task)
    {
      return taskRunner.getPublishedSegments(task.getId());
    }
  }

  public void prepareObjectMapper(ObjectMapper objectMapper, IndexIO indexIO)
  {
    final TaskConfig taskConfig = new TaskConfig(null, null, null, null, null, false, null, null, null, false, false);

    objectMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class, LookupEnabledTestExprMacroTable.INSTANCE)
            .addValue(IndexIO.class, indexIO)
            .addValue(ObjectMapper.class, objectMapper)
            .addValue(ChatHandlerProvider.class, new NoopChatHandlerProvider())
            .addValue(AuthConfig.class, new AuthConfig())
            .addValue(AuthorizerMapper.class, null)
            .addValue(RowIngestionMetersFactory.class, new DropwizardRowIngestionMetersFactory())
            .addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT)
            .addValue(AuthorizerMapper.class, new AuthorizerMapper(ImmutableMap.of()))
            .addValue(AppenderatorsManager.class, TestUtils.APPENDERATORS_MANAGER)
            .addValue(LocalDataSegmentPuller.class, new LocalDataSegmentPuller())
            .addValue(CoordinatorClient.class, coordinatorClient)
            .addValue(SegmentLoaderFactory.class, new SegmentLoaderFactory(indexIO, objectMapper))
            .addValue(RetryPolicyFactory.class, new RetryPolicyFactory(new RetryPolicyConfig()))
            .addValue(TaskConfig.class, taskConfig)
    );
    objectMapper.registerSubtypes(
        new NamedType(ParallelIndexSupervisorTask.class, ParallelIndexSupervisorTask.TYPE),
        new NamedType(CompactionTask.CompactionTuningConfig.class, CompactionTask.CompactionTuningConfig.TYPE),
        new NamedType(SinglePhaseSubTask.class, SinglePhaseSubTask.TYPE),
        new NamedType(PartialHashSegmentGenerateTask.class, PartialHashSegmentGenerateTask.TYPE),
        new NamedType(PartialRangeSegmentGenerateTask.class, PartialRangeSegmentGenerateTask.TYPE),
        new NamedType(PartialGenericSegmentMergeTask.class, PartialGenericSegmentMergeTask.TYPE),
        new NamedType(PartialDimensionDistributionTask.class, PartialDimensionDistributionTask.TYPE),
        new NamedType(PartialDimensionCardinalityTask.class, PartialDimensionCardinalityTask.TYPE)
    );
  }

  protected TaskToolbox createTaskToolbox(Task task, TaskActionClient actionClient) throws IOException
  {
    return new TaskToolbox(
        new TaskConfig(null, null, null, null, null, false, null, null, null, false, false),
        new DruidNode("druid/middlemanager", "localhost", false, 8091, null, true, false),
        actionClient,
        null,
        new LocalDataSegmentPusher(
            new LocalDataSegmentPusherConfig()
            {
              @Override
              public File getStorageDirectory()
              {
                return localDeepStorage;
              }
            }
        ),
        new NoopDataSegmentKiller(),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        NoopJoinableFactory.INSTANCE,
        null,
        newSegmentLoader(temporaryFolder.newFolder()),
        objectMapper,
        temporaryFolder.newFolder(task.getId()),
        getIndexIO(),
        null,
        null,
        null,
        getIndexMerger(),
        null,
        null,
        null,
        null,
        new NoopTestTaskReportFileWriter(),
        intermediaryDataManager,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new NoopChatHandlerProvider(),
        new TestUtils().getRowIngestionMetersFactory(),
        new TestAppenderatorsManager(),
        indexingServiceClient,
        coordinatorClient,
        new LocalParallelIndexTaskClientFactory(taskRunner, transientApiCallFailureRate),
        new LocalShuffleClient(intermediaryDataManager)
    );
  }

  static class TestParallelIndexSupervisorTask extends ParallelIndexSupervisorTask
  {
    TestParallelIndexSupervisorTask(
        String id,
        TaskResource taskResource,
        ParallelIndexIngestionSpec ingestionSchema,
        Map<String, Object> context
    )
    {
      super(
          id,
          null,
          taskResource,
          ingestionSchema,
          context
      );
    }
  }

  static class LocalShuffleClient implements ShuffleClient
  {
    private final IntermediaryDataManager intermediaryDataManager;

    LocalShuffleClient(IntermediaryDataManager intermediaryDataManager)
    {
      this.intermediaryDataManager = intermediaryDataManager;
    }

    @Override
    public <T, P extends PartitionLocation<T>> File fetchSegmentFile(
        File partitionDir,
        String supervisorTaskId,
        P location
    ) throws IOException
    {
      final File zippedFile = intermediaryDataManager.findPartitionFile(
          supervisorTaskId,
          location.getSubTaskId(),
          location.getInterval(),
          location.getBucketId()
      );
      if (zippedFile == null) {
        throw new ISE("Can't find segment file for location[%s] at path[%s]", location);
      }
      final File fetchedFile = new File(partitionDir, StringUtils.format("temp_%s", location.getSubTaskId()));
      FileUtils.writeAtomically(
          fetchedFile,
          out -> Files.asByteSource(zippedFile).copyTo(out)
      );
      return fetchedFile;
    }
  }

  static class LocalParallelIndexTaskClientFactory implements IndexTaskClientFactory<ParallelIndexSupervisorTaskClient>
  {
    private final ConcurrentMap<String, TaskContainer> tasks;
    private final double transientApiCallFailureRate;

    LocalParallelIndexTaskClientFactory(SimpleThreadingTaskRunner taskRunner, double transientApiCallFailureRate)
    {
      this.tasks = taskRunner.tasks;
      this.transientApiCallFailureRate = transientApiCallFailureRate;
    }

    @Override
    public ParallelIndexSupervisorTaskClient build(
        TaskInfoProvider taskInfoProvider,
        String callerId,
        int numThreads,
        Duration httpTimeout,
        long numRetries
    )
    {
      return new LocalParallelIndexSupervisorTaskClient(callerId, tasks, transientApiCallFailureRate);
    }
  }

  static class LocalParallelIndexSupervisorTaskClient extends ParallelIndexSupervisorTaskClient
  {
    private static final int MAX_TRANSIENT_API_FAILURES = 3;

    private final double transientFailureRate;
    private final ConcurrentMap<String, TaskContainer> tasks;

    LocalParallelIndexSupervisorTaskClient(
        String callerId,
        ConcurrentMap<String, TaskContainer> tasks,
        double transientFailureRate
    )
    {
      super(null, null, null, null, callerId, 0);
      this.tasks = tasks;
      this.transientFailureRate = transientFailureRate;
    }

    @Override
    public SegmentIdWithShardSpec allocateSegment(String supervisorTaskId, DateTime timestamp) throws IOException
    {
      final TaskContainer taskContainer = tasks.get(supervisorTaskId);
      final ParallelIndexSupervisorTask supervisorTask = findSupervisorTask(taskContainer);
      if (supervisorTask == null) {
        throw new ISE("Cannot find supervisor task for [%s]", supervisorTaskId);
      }
      if (!(supervisorTask.getCurrentRunner() instanceof SinglePhaseParallelIndexTaskRunner)) {
        throw new ISE("Only SinglePhaseParallelIndexTaskRunner can call this API");
      }
      SinglePhaseParallelIndexTaskRunner runner =
          (SinglePhaseParallelIndexTaskRunner) supervisorTask.getCurrentRunner();
      return runner.allocateNewSegment(supervisorTask.getDataSource(), timestamp);
    }

    @Override
    public SegmentIdWithShardSpec allocateSegment(
        String supervisorTaskId,
        DateTime timestamp,
        String sequenceName,
        @Nullable String prevSegmentId
    ) throws IOException
    {
      final TaskContainer taskContainer = tasks.get(supervisorTaskId);
      final ParallelIndexSupervisorTask supervisorTask = findSupervisorTask(taskContainer);
      if (supervisorTask == null) {
        throw new ISE("Cannot find supervisor task for [%s]", supervisorTaskId);
      }
      if (!(supervisorTask.getCurrentRunner() instanceof SinglePhaseParallelIndexTaskRunner)) {
        throw new ISE("Only SinglePhaseParallelIndexTaskRunner can call this API");
      }
      SinglePhaseParallelIndexTaskRunner runner =
          (SinglePhaseParallelIndexTaskRunner) supervisorTask.getCurrentRunner();
      SegmentIdWithShardSpec newSegmentId = null;

      int i = 0;
      do {
        SegmentIdWithShardSpec allocated = runner.allocateNewSegment(
            supervisorTask.getDataSource(),
            timestamp,
            sequenceName,
            prevSegmentId
        );
        if (newSegmentId == null) {
          newSegmentId = allocated;
        }
        if (!newSegmentId.equals(allocated)) {
          throw new ISE(
              "Segment allocation is not idempotent. Prev id was [%s] but new id is [%s]",
              newSegmentId,
              allocated
          );
        }
      } while (i++ < MAX_TRANSIENT_API_FAILURES && ThreadLocalRandom.current().nextDouble() < transientFailureRate);

      return newSegmentId;
    }

    @Override
    public void report(String supervisorTaskId, SubTaskReport report)
    {
      final TaskContainer taskContainer = tasks.get(supervisorTaskId);
      final ParallelIndexSupervisorTask supervisorTask = findSupervisorTask(taskContainer);
      if (supervisorTask == null) {
        throw new ISE("Cannot find supervisor task for [%s]", supervisorTaskId);
      }
      int i = 0;
      do {
        supervisorTask.getCurrentRunner().collectReport(report);
      } while (i++ < MAX_TRANSIENT_API_FAILURES && ThreadLocalRandom.current().nextDouble() < transientFailureRate);
    }

    @Nullable
    private ParallelIndexSupervisorTask findSupervisorTask(TaskContainer taskContainer)
    {
      if (taskContainer == null) {
        return null;
      }
      if (taskContainer.task instanceof CompactionTask) {
        final Task task = ((CompactionTask) taskContainer.task).getCurrentSubTaskHolder().getTask();
        if (!(task instanceof ParallelIndexSupervisorTask)) {
          return null;
        } else {
          return (ParallelIndexSupervisorTask) task;
        }
      } else if (!(taskContainer.task instanceof ParallelIndexSupervisorTask)) {
        return null;
      } else {
        return (ParallelIndexSupervisorTask) taskContainer.task;
      }
    }
  }

  class LocalCoordinatorClient extends CoordinatorClient
  {
    private final ExecutorService exec;

    LocalCoordinatorClient(ExecutorService exec)
    {
      super(null, null);
      this.exec = exec;
    }

    @Override
    public Collection<DataSegment> fetchUsedSegmentsInDataSourceForIntervals(
        String dataSource,
        List<Interval> intervals
    )
    {
      try {
        return exec.submit(
            () -> getStorageCoordinator().retrieveUsedSegmentsForIntervals(dataSource, intervals, Segments.ONLY_VISIBLE)
        ).get();
      }
      catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public DataSegment fetchUsedSegment(String dataSource, String segmentId)
    {
      ImmutableDruidDataSource druidDataSource;
      try {
        druidDataSource = exec.submit(
            () -> getSegmentsMetadataManager().getImmutableDataSourceWithUsedSegments(dataSource)
        ).get();
      }
      catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      if (druidDataSource == null) {
        throw new ISE("Unknown datasource[%s]", dataSource);
      }

      for (SegmentId possibleSegmentId : SegmentId.iteratePossibleParsingsWithDataSource(dataSource, segmentId)) {
        DataSegment segment = druidDataSource.getSegment(possibleSegmentId);
        if (segment != null) {
          return segment;
        }
      }
      throw new ISE("Can't find segment for id[%s]", segmentId);
    }
  }
}
