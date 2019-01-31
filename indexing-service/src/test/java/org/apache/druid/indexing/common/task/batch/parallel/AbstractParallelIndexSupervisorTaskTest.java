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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
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
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.IngestionTestBase;
import org.apache.druid.indexing.common.task.NoopTestTaskFileWriter;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.security.AllowAllAuthorizer;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

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

  TaskActionClient actionClient;
  LocalIndexingServiceClient indexingServiceClient;
  TaskToolbox toolbox;
  File localDeepStorage;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  class LocalIndexingServiceClient extends NoopIndexingServiceClient
  {
    private final ConcurrentMap<String, Future<TaskStatus>> tasks = new ConcurrentHashMap<>();
    private final ListeningExecutorService service = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(5, "parallel-index-supervisor-task-test-%d")
    );

    @Override
    public String runTask(Object taskObject)
    {
      final ParallelIndexSubTask subTask = (ParallelIndexSubTask) taskObject;
      tasks.put(subTask.getId(), service.submit(() -> {
        try {
          final TaskToolbox toolbox = createTaskToolbox(subTask);
          if (subTask.isReady(toolbox.getTaskActionClient())) {
            return subTask.run(toolbox);
          } else {
            throw new ISE("task[%s] is not ready", subTask.getId());
          }
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));
      return subTask.getId();
    }

    @Override
    public TaskStatusResponse getTaskStatus(String taskId)
    {
      final Future<TaskStatus> taskStatusFuture = tasks.get(taskId);
      if (taskStatusFuture != null) {
        try {
          if (taskStatusFuture.isDone()) {
            final TaskStatus taskStatus = taskStatusFuture.get();
            return new TaskStatusResponse(
                taskId,
                new TaskStatusPlus(
                    taskId,
                    "index_sub",
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
                    "index_sub",
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
          // We don't have a way to pass this exception to the supervisorTask yet..
          // So, let's print it here.
          System.err.println(Throwables.getStackTraceAsString(e));
          return new TaskStatusResponse(
              taskId,
              new TaskStatusPlus(
                  taskId,
                  "index_sub",
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

    void shutdown()
    {
      service.shutdownNow();
    }
  }

  TaskToolbox createTaskToolbox(Task task) throws IOException
  {
    return new TaskToolbox(
        null,
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
            },
            getObjectMapper()
        ),
        new DataSegmentKiller()
        {
          @Override
          public void kill(DataSegment segment)
          {
          }

          @Override
          public void killAll()
          {
          }
        },
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
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
        new NoopTestTaskFileWriter()
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
          new DropwizardRowIngestionMetersFactory()
      );
    }

    @Override
    public TaskStatus run(TaskToolbox toolbox) throws Exception
    {
      return TaskStatus.fromCode(
          getId(),
          new TestParallelIndexTaskRunner(
              toolbox,
              getId(),
              getGroupId(),
              getIngestionSchema(),
              getContext(),
              new NoopIndexingServiceClient()
          ).run()
      );
    }
  }

  static class TestParallelIndexTaskRunner extends SinglePhaseParallelIndexTaskRunner
  {
    TestParallelIndexTaskRunner(
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
    Stream<ParallelIndexSubTaskSpec> subTaskSpecIterator() throws IOException
    {
      final FiniteFirehoseFactory baseFirehoseFactory = (FiniteFirehoseFactory) getIngestionSchema()
          .getIOConfig()
          .getFirehoseFactory();
      return baseFirehoseFactory.getSplits().map(split -> {
        try {
          // taskId is suffixed by the current time and this sleep is to make sure that every sub task has different id
          Thread.sleep(10);
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return newTaskSpec((InputSplit<?>) split);
      });
    }
  }

  static class LocalParallelIndexTaskClientFactory implements IndexTaskClientFactory<ParallelIndexTaskClient>
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    LocalParallelIndexTaskClientFactory(ParallelIndexSupervisorTask supervisorTask)
    {
      this.supervisorTask = supervisorTask;
    }

    @Override
    public ParallelIndexTaskClient build(
        TaskInfoProvider taskInfoProvider,
        String callerId,
        int numThreads,
        Duration httpTimeout,
        long numRetries
    )
    {
      return new LocalParallelIndexTaskClient(callerId, supervisorTask);
    }
  }

  static class LocalParallelIndexTaskClient extends ParallelIndexTaskClient
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    LocalParallelIndexTaskClient(String callerId, ParallelIndexSupervisorTask supervisorTask)
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
    public void report(String supervisorTaskId, List<DataSegment> pushedSegments)
    {
      supervisorTask.getRunner().collectReport(new PushedSegmentsReport(getSubtaskId(), pushedSegments));
    }
  }
}
