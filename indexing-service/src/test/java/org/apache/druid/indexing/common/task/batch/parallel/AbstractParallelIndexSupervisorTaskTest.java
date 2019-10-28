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

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.IngestionTestBase;
import org.apache.druid.indexing.common.task.NoopTestTaskReportFileWriter;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.worker.IntermediaryDataManager;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.NoopDataSegmentKiller;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.AllowAllAuthorizer;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class AbstractParallelIndexSupervisorTaskTest extends IngestionTestBase
{
  static final ParseSpec DEFAULT_PARSE_SPEC = new CSVParseSpec(
      new TimestampSpec(
          "ts",
          "auto",
          null
      ),
      new DimensionsSpec(
          DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim")),
          new ArrayList<>(),
          new ArrayList<>()
      ),
      null,
      Arrays.asList("ts", "dim", "val"),
      false,
      0
  );

  protected TestLocalTaskActionClient actionClient;
  protected LocalIndexingServiceClient indexingServiceClient;
  protected TaskToolbox toolbox;
  protected File localDeepStorage;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private IntermediaryDataManager intermediaryDataManager;

  protected void initializeIntermeidaryDataManager() throws IOException
  {
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
            ImmutableList.of(new StorageLocationConfig(temporaryFolder.newFolder(), null, null))
        ),
        null
    );
  }

  public class LocalIndexingServiceClient extends NoopIndexingServiceClient
  {
    private final ConcurrentMap<String, Future<TaskStatus>> tasks = new ConcurrentHashMap<>();
    private final ListeningExecutorService service = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(5, "parallel-index-supervisor-task-test-%d")
    );

    @Override
    public String runTask(Object taskObject)
    {
      final Task subTask = (Task) taskObject;
      try {
        getTaskStorage().insert(subTask, TaskStatus.running(subTask.getId()));
      }
      catch (EntryExistsException e) {
        throw new RuntimeException(e);
      }
      tasks.put(subTask.getId(), service.submit(() -> {
        try {
          final TaskToolbox toolbox = createTaskToolbox(subTask);
          if (subTask.isReady(toolbox.getTaskActionClient())) {
            return subTask.run(toolbox);
          } else {
            getTaskStorage().setStatus(TaskStatus.failure(subTask.getId()));
            throw new ISE("task[%s] is not ready", subTask.getId());
          }
        }
        catch (Exception e) {
          getTaskStorage().setStatus(TaskStatus.failure(subTask.getId(), e.getMessage()));
          throw new RuntimeException(e);
        }
      }));
      return subTask.getId();
    }

    @Override
    public TaskStatusResponse getTaskStatus(String taskId)
    {
      final Future<TaskStatus> taskStatusFuture = tasks.get(taskId);
      final Optional<Task> task = getTaskStorage().getTask(taskId);
      final String groupId = task.isPresent() ? task.get().getGroupId() : null;
      if (taskStatusFuture != null) {
        try {
          if (taskStatusFuture.isDone()) {
            final TaskStatus taskStatus = taskStatusFuture.get();
            return new TaskStatusResponse(
                taskId,
                new TaskStatusPlus(
                    taskId,
                    groupId,
                    SinglePhaseSubTask.TYPE,
                    DateTimes.EPOCH,
                    DateTimes.EPOCH,
                    taskStatus.getStatusCode(),
                    RunnerTaskState.NONE,
                    -1L,
                    TaskLocation.unknown(),
                    null,
                    null
                )
            );
          } else {
            return new TaskStatusResponse(
                taskId,
                new TaskStatusPlus(
                    taskId,
                    groupId,
                    SinglePhaseSubTask.TYPE,
                    DateTimes.EPOCH,
                    DateTimes.EPOCH,
                    TaskState.RUNNING,
                    RunnerTaskState.RUNNING,
                    -1L,
                    TaskLocation.unknown(),
                    null,
                    null
                )
            );
          }
        }
        catch (InterruptedException | ExecutionException e) {
          // We don't have a way to propagate this exception to the supervisorTask yet..
          // So, let's print it here.
          System.err.println(Throwables.getStackTraceAsString(e));
          return new TaskStatusResponse(
              taskId,
              new TaskStatusPlus(
                  taskId,
                  groupId,
                  SinglePhaseSubTask.TYPE,
                  DateTimes.EPOCH,
                  DateTimes.EPOCH,
                  TaskState.FAILED,
                  RunnerTaskState.NONE,
                  -1L,
                  TaskLocation.unknown(),
                  null,
                  null
              )
          );
        }
      } else {
        return new TaskStatusResponse(taskId, null);
      }
    }

    @Override
    public String killTask(String taskId)
    {
      final Future<TaskStatus> taskStatusFuture = tasks.remove(taskId);
      if (taskStatusFuture != null) {
        taskStatusFuture.cancel(true);
        return taskId;
      } else {
        return null;
      }
    }

    public void shutdown()
    {
      service.shutdownNow();
    }
  }

  protected TaskToolbox createTaskToolbox(Task task) throws IOException
  {
    return new TaskToolbox(
        null,
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
        null,
        newSegmentLoader(temporaryFolder.newFolder()),
        getObjectMapper(),
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
        intermediaryDataManager
    );
  }

  static class TestParallelIndexSupervisorTask extends ParallelIndexSupervisorTask
  {
    TestParallelIndexSupervisorTask(
        String id,
        TaskResource taskResource,
        ParallelIndexIngestionSpec ingestionSchema,
        Map<String, Object> context,
        IndexingServiceClient indexingServiceClient
    )
    {
      super(
          id,
          null,
          taskResource,
          ingestionSchema,
          context,
          indexingServiceClient,
          new NoopChatHandlerProvider(),
          new AuthorizerMapper(ImmutableMap.of())
          {
            @Override
            public Authorizer getAuthorizer(String name)
            {
              return new AllowAllAuthorizer();
            }
          },
          new DropwizardRowIngestionMetersFactory(),
          new TestAppenderatorsManager()
      );
    }
  }

  static class TestSinglePhaseParallelIndexTaskRunner extends SinglePhaseParallelIndexTaskRunner
  {
    TestSinglePhaseParallelIndexTaskRunner(
        TaskToolbox toolbox,
        String taskId,
        String groupId,
        ParallelIndexIngestionSpec ingestionSchema,
        Map<String, Object> context,
        @Nullable IndexingServiceClient indexingServiceClient
    )
    {
      super(
          toolbox,
          taskId,
          groupId,
          ingestionSchema,
          context,
          indexingServiceClient
      );
    }

    @Override
    Iterator<SubTaskSpec<SinglePhaseSubTask>> subTaskSpecIterator() throws IOException
    {
      final Iterator<SubTaskSpec<SinglePhaseSubTask>> iterator = super.subTaskSpecIterator();
      return new Iterator<SubTaskSpec<SinglePhaseSubTask>>()
      {
        @Override
        public boolean hasNext()
        {
          return iterator.hasNext();
        }

        @Override
        public SubTaskSpec<SinglePhaseSubTask> next()
        {
          try {
            Thread.sleep(10);
            return iterator.next();
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }
  }

  static class LocalParallelIndexTaskClientFactory implements IndexTaskClientFactory<ParallelIndexSupervisorTaskClient>
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    LocalParallelIndexTaskClientFactory(ParallelIndexSupervisorTask supervisorTask)
    {
      this.supervisorTask = supervisorTask;
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
      return new LocalParallelIndexSupervisorTaskClient(callerId, supervisorTask);
    }
  }

  static class LocalParallelIndexSupervisorTaskClient extends ParallelIndexSupervisorTaskClient
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    LocalParallelIndexSupervisorTaskClient(String callerId, ParallelIndexSupervisorTask supervisorTask)
    {
      super(null, null, null, null, callerId, 0);
      this.supervisorTask = supervisorTask;
    }

    @Override
    public SegmentIdWithShardSpec allocateSegment(String supervisorTaskId, DateTime timestamp) throws IOException
    {
      return supervisorTask.allocateNewSegment(timestamp);
    }

    @Override
    public void report(String supervisorTaskId, SubTaskReport report)
    {
      supervisorTask.getCurrentRunner().collectReport(report);
    }
  }
}
