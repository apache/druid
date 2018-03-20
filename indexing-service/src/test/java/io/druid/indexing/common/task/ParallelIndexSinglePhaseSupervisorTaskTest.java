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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.client.indexing.TaskStatusResponse;
import io.druid.data.input.FiniteFirehoseFactory;
import io.druid.data.input.InputSplit;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexer.TaskState;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.LocalDataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentPusherConfig;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ParallelIndexSinglePhaseSupervisorTaskTest extends IngestionTestBase
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final ParseSpec DEFAULT_PARSE_SPEC = new CSVParseSpec(
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

  private TaskActionClient actionClient;
  private LocalIndexingServiceClient indexingServiceClient;
  private TaskToolbox toolbox;
  private File localDeepStorage;
  private File inputDir;

  @Before
  public void setup() throws IOException
  {
    inputDir = temporaryFolder.newFolder("data");
    // set up data
    for (int i = 0; i < 5; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "test_" + i).toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("2017-12-%d,%d th test file\n", 25 + i, i));
      }
    }

    for (int i = 0; i < 5; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "filtered_" + i).toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("2017-12-%d,%d th test file\n", 25 + i, i));
      }
    }

    indexingServiceClient = new LocalIndexingServiceClient();
    localDeepStorage = temporaryFolder.newFolder("localStorage");
  }

  @After
  public void teardown()
  {
    indexingServiceClient.shutdown();
    temporaryFolder.delete();
  }

  @Test
  public void testIsReady() throws Exception
  {
    final ParallelIndexSinglePhaseSupervisorTask task = newTask(
        Intervals.of("2017/2018"),
        new ParallelIndexSinglePhaseIOConfig(
            new LocalFirehoseFactory(inputDir, "test_*", null),
            false
        )
    );
    actionClient = createActionClient(task);
    toolbox = createTaskToolbox(task);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));

    final Iterator<ParallelIndexSinglePhaseSubTaskSpec> subTaskSpecIterator = task.subTaskSpecIterator();

    while (subTaskSpecIterator.hasNext()) {
      final ParallelIndexSinglePhaseSubTaskSpec spec = subTaskSpecIterator.next();
      final ParallelIndexSinglePhaseSubTask subTask = new ParallelIndexSinglePhaseSubTask(
          spec.getGroupId(),
          null,
          spec.getSupervisorTaskId(),
          0,
          spec.getIngestionSpec(),
          spec.getContext()
      );
      final TaskActionClient subTaskActionClient = createActionClient(subTask);
      prepareTaskForLocking(subTask);
      Assert.assertTrue(subTask.isReady(subTaskActionClient));
    }
  }

  @Test
  public void testWithoutInterval() throws Exception
  {
    final ParallelIndexSinglePhaseSupervisorTask task = newTask(
        null,
        new ParallelIndexSinglePhaseIOConfig(
            new LocalFirehoseFactory(inputDir, "test_*", null),
            false
        )
    );
    actionClient = createActionClient(task);
    toolbox = createTaskToolbox(task);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));
    Assert.assertEquals(TaskState.SUCCESS, task.run(toolbox).getStatusCode());
  }

  @Test()
  public void testRunInParallel() throws Exception
  {
    final ParallelIndexSinglePhaseSupervisorTask task = newTask(
        Intervals.of("2017/2018"),
        new ParallelIndexSinglePhaseIOConfig(
            new LocalFirehoseFactory(inputDir, "test_*", null),
            false
        )
    );
    actionClient = createActionClient(task);
    toolbox = createTaskToolbox(task);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));
    Assert.assertEquals(TaskState.SUCCESS, task.run(toolbox).getStatusCode());
  }

  @Test
  public void testRunInSequential() throws Exception
  {
    final ParallelIndexSinglePhaseSupervisorTask task = newTask(
        Intervals.of("2017/2018"),
        new ParallelIndexSinglePhaseIOConfig(
            new LocalFirehoseFactory(inputDir, "test_*", null)
            {
              @Override
              public boolean isSplittable()
              {
                return false;
              }
            },
            false
        )
    );
    actionClient = createActionClient(task);
    toolbox = createTaskToolbox(task);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));
    Assert.assertEquals(TaskState.SUCCESS, task.run(toolbox).getStatusCode());
  }

  private ParallelIndexSinglePhaseSupervisorTask newTask(
      Interval interval,
      ParallelIndexSinglePhaseIOConfig ioConfig
  )
  {
    // set up ingestion spec
    final ParallelIndexSinglePhaseIngestionSpec singlePhaseIngestionSpec = new ParallelIndexSinglePhaseIngestionSpec(
        new DataSchema(
            "dataSource",
            getObjectMapper().convertValue(
                new StringInputRowParser(
                    DEFAULT_PARSE_SPEC,
                    null
                ),
                Map.class
            ),
            new AggregatorFactory[]{
                new LongSumAggregatorFactory("val", "val")
            },
            new UniformGranularitySpec(
                Granularities.DAY,
                Granularities.MINUTE,
                interval == null ? null : Collections.singletonList(interval)
            ),
            null,
            getObjectMapper()
        ),
        ioConfig,
        null
    );

    // set up test tools
    return new TestParallelIndexSinglePhaseSupervisorTask(
        null,
        null,
        singlePhaseIngestionSpec,
        new HashMap<>(),
        indexingServiceClient
    );
  }

  private static class TestParallelIndexSinglePhaseSupervisorTask extends ParallelIndexSinglePhaseSupervisorTask
  {

    TestParallelIndexSinglePhaseSupervisorTask(
        String id,
        TaskResource taskResource,
        ParallelIndexSinglePhaseIngestionSpec ingestionSchema,
        Map<String, Object> context,
        IndexingServiceClient indexingServiceClient
    )
    {
      super(id, taskResource, ingestionSchema, context, indexingServiceClient);
    }

    @Override
    Iterator<ParallelIndexSinglePhaseSubTaskSpec> subTaskSpecIterator() throws IOException
    {
      final FiniteFirehoseFactory baseFirehoseFactory = (FiniteFirehoseFactory) getIngestionSchema()
          .getIOConfig()
          .getFirehoseFactory();
      return Iterators.transform(baseFirehoseFactory.getSplits(), split -> {
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

  private class LocalIndexingServiceClient extends IndexingServiceClient
  {
    private final ConcurrentMap<String, Future<TaskStatus>> tasks = new ConcurrentHashMap<>();
    private final ListeningExecutorService service = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(5, "parallel-index-single-phase-supervisor-task-test-%d")
    );

    LocalIndexingServiceClient()
    {
      super(null, null);
    }

    @Override
    public String runTask(Object taskObject)
    {
      final ParallelIndexSinglePhaseSubTask subTask = (ParallelIndexSinglePhaseSubTask) taskObject;
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
    @Nullable
    public TaskStatusResponse getTaskStatus(String taskId)
    {
      final Future<TaskStatus> taskStatusFuture = tasks.get(taskId);
      if (taskStatusFuture != null) {
        try {
          if (taskStatusFuture.isDone()) {
            final TaskStatus taskStatus = taskStatusFuture.get();
            return new TaskStatusResponse(
                taskId,
                new io.druid.client.indexing.TaskStatus(taskId, taskStatus.getStatusCode(), taskStatus.getReport(), -1)
            );
          } else {
            return new TaskStatusResponse(
                taskId,
                new io.druid.client.indexing.TaskStatus(taskId, TaskState.RUNNING, null, -1)
            );
          }
        }
        catch (InterruptedException | ExecutionException e) {
          // We don't have a way to pass this exception to the supervisorTask yet..
          // So, let's print it here.
          System.err.println(Throwables.getStackTraceAsString(e));
          return new TaskStatusResponse(
              taskId,
              new io.druid.client.indexing.TaskStatus(taskId, TaskState.FAILED, null, -1)
          );
        }
      } else {
        return null;
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

  private TaskToolbox createTaskToolbox(Task task) throws IOException
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
        null
    );
  }
}
