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
package io.druid.indexing.common.task;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.client.indexing.NoopIndexingServiceClient;
import io.druid.client.indexing.TaskStatusResponse;
import io.druid.data.input.FiniteFirehoseFactory;
import io.druid.data.input.InputSplit;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexer.TaskLocation;
import io.druid.indexer.TaskState;
import io.druid.indexer.TaskStatusPlus;
import io.druid.indexing.common.TaskInfoProvider;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.segment.loading.LocalDataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentPusherConfig;
import io.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import io.druid.server.security.AllowAllAuthorizer;
import io.druid.server.security.Authorizer;
import io.druid.server.security.AuthorizerMapper;
import io.druid.timeline.DataSegment;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class AbstractSinglePhaseParallelIndexSupervisorTaskTest extends IngestionTestBase
{
  static final ParseSpec DEFAULT_PARSE_SPEC = new CSVParseSpec(
      new TimestampSpec(
          "ts",
          "auto",
          null
      ),
      new DimensionsSpec(
          DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim")),
          Lists.newArrayList(),
          Lists.newArrayList()
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
        Execs.multiThreaded(5, "parallel-index-single-phase-supervisor-task-test-%d")
    );

    @Override
    public String runTask(Object taskObject)
    {
      final SinglePhaseParallelIndexSubTask subTask = (SinglePhaseParallelIndexSubTask) taskObject;
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
                    "index_single_phase_sub",
                    DateTimes.EPOCH,
                    DateTimes.EPOCH,
                    taskStatus.getStatusCode(),
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
                    "index_single_phase_sub",
                    DateTimes.EPOCH,
                    DateTimes.EPOCH,
                    TaskState.RUNNING,
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
                  "index_single_phase_sub",
                  DateTimes.EPOCH,
                  DateTimes.EPOCH,
                  TaskState.FAILED,
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
        getObjectMapper(),
        temporaryFolder.newFolder(task.getId()),
        getIndexIO(),
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

  static class TestSinglePhaseParallelIndexSupervisorTask extends SinglePhaseParallelIndexSupervisorTask
  {
    TestSinglePhaseParallelIndexSupervisorTask(
        String id,
        TaskResource taskResource,
        SinglePhaseParallelIndexIngestionSpec ingestionSchema,
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
          }
      );
    }

    @Override
    Stream<SinglePhaseParallelIndexSubTaskSpec> subTaskSpecIterator() throws IOException
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

  static class LocalSinglePhaseParallelIndexTaskClientFactory
      implements IndexTaskClientFactory<SinglePhaseParallelIndexTaskClient>
  {
    private final SinglePhaseParallelIndexSupervisorTask supervisorTask;

    LocalSinglePhaseParallelIndexTaskClientFactory(SinglePhaseParallelIndexSupervisorTask supervisorTask)
    {
      this.supervisorTask = supervisorTask;
    }

    @Override
    public SinglePhaseParallelIndexTaskClient build(
        TaskInfoProvider taskInfoProvider,
        String callerId,
        int numThreads,
        Duration httpTimeout,
        long numRetries
    )
    {
      return new LocalSinglePhaseParallelIndexTaskClient(callerId, supervisorTask);
    }
  }

  static class LocalSinglePhaseParallelIndexTaskClient extends SinglePhaseParallelIndexTaskClient
  {
    private final SinglePhaseParallelIndexSupervisorTask supervisorTask;

    public LocalSinglePhaseParallelIndexTaskClient(String callerId, SinglePhaseParallelIndexSupervisorTask supervisorTask)
    {
      super(null, null, null, null, callerId, 0);
      this.supervisorTask = supervisorTask;
    }

    @Override
    public void report(String supervisorTaskId, List<DataSegment> pushedSegments)
    {
      supervisorTask.collectReport(new PushedSegmentsReport(getSubtaskId(), pushedSegments));
    }
  }
}
