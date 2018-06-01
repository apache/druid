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

import io.druid.client.indexing.IndexingServiceClient;
import io.druid.data.input.FiniteFirehoseFactory;
import io.druid.data.input.InputSplit;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.indexer.TaskState;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import io.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import io.druid.server.security.AuthorizerMapper;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ParallelIndexSupervisorTaskTest extends AbstractParallelIndexSupervisorTaskTest
{
  private File inputDir;

  @Before
  public void setup() throws IOException
  {
    inputDir = temporaryFolder.newFolder("data");
    // set up data
    for (int i = 0; i < 5; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "test_" + i).toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("2017-12-%d,%d th test file\n", 24 + i, i));
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
    final ParallelIndexSupervisorTask task = newTask(
        Intervals.of("2017/2018"),
        new ParallelIndexIOConfig(
            new LocalFirehoseFactory(inputDir, "test_*", null),
            false
        )
    );
    actionClient = createActionClient(task);
    toolbox = createTaskToolbox(task);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));

    final SinglePhaseParallelIndexTaskRunner runner = (SinglePhaseParallelIndexTaskRunner) task.createRunner();
    final Iterator<ParallelIndexSubTaskSpec> subTaskSpecIterator = runner.subTaskSpecIterator().iterator();

    while (subTaskSpecIterator.hasNext()) {
      final ParallelIndexSubTaskSpec spec = subTaskSpecIterator.next();
      final ParallelIndexSubTask subTask = new ParallelIndexSubTask(
          null,
          spec.getGroupId(),
          null,
          spec.getSupervisorTaskId(),
          0,
          spec.getIngestionSpec(),
          spec.getContext(),
          indexingServiceClient,
          null
      );
      final TaskActionClient subTaskActionClient = createActionClient(subTask);
      prepareTaskForLocking(subTask);
      Assert.assertTrue(subTask.isReady(subTaskActionClient));
    }
  }

  @Test
  public void testWithoutInterval() throws Exception
  {
    final ParallelIndexSupervisorTask task = newTask(
        null,
        new ParallelIndexIOConfig(
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
    final ParallelIndexSupervisorTask task = newTask(
        Intervals.of("2017/2018"),
        new ParallelIndexIOConfig(
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
    final ParallelIndexSupervisorTask task = newTask(
        Intervals.of("2017/2018"),
        new ParallelIndexIOConfig(
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

  private ParallelIndexSupervisorTask newTask(
      Interval interval,
      ParallelIndexIOConfig ioConfig
  )
  {
    // set up ingestion spec
    final ParallelIndexIngestionSpec ingestionSpec = new ParallelIndexIngestionSpec(
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
            2,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        )
    );

    // set up test tools
    return new TestSupervisorTask(
        null,
        null,
        ingestionSpec,
        new HashMap<>(),
        indexingServiceClient
    );
  }

  private static class TestSupervisorTask extends TestParallelIndexSupervisorTask
  {
    private final IndexingServiceClient indexingServiceClient;

    TestSupervisorTask(
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
          indexingServiceClient
      );
      this.indexingServiceClient = indexingServiceClient;
    }

    @Override
    public TaskStatus run(TaskToolbox toolbox) throws Exception
    {
      return TaskStatus.fromCode(
          getId(),
          new TestRunner(
              this,
              indexingServiceClient,
              new NoopChatHandlerProvider(),
              new AuthorizerMapper(Collections.emptyMap())
          ).run(toolbox)
      );
    }
  }

  private static class TestRunner extends TestParallelIndexTaskRunner
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    TestRunner(
        ParallelIndexSupervisorTask supervisorTask,
        @Nullable IndexingServiceClient indexingServiceClient,
        @Nullable ChatHandlerProvider chatHandlerProvider,
        AuthorizerMapper authorizerMapper
    )
    {
      super(
          supervisorTask.getId(),
          supervisorTask.getGroupId(),
          supervisorTask.getIngestionSchema(),
          supervisorTask.getContext(),
          indexingServiceClient,
          chatHandlerProvider,
          authorizerMapper
      );
      this.supervisorTask = supervisorTask;
    }

    @Override
    ParallelIndexSubTaskSpec newTaskSpec(InputSplit split)
    {
      final FiniteFirehoseFactory baseFirehoseFactory = (FiniteFirehoseFactory) getIngestionSchema()
          .getIOConfig()
          .getFirehoseFactory();
      return new TestParallelIndexSubTaskSpec(
          supervisorTask.getId() + "_" + getAndIncrementNextSpecId(),
          supervisorTask.getGroupId(),
          supervisorTask,
          this,
          new ParallelIndexIngestionSpec(
              getIngestionSchema().getDataSchema(),
              new ParallelIndexIOConfig(
                  baseFirehoseFactory.withSplit(split),
                  getIngestionSchema().getIOConfig().isAppendToExisting()
              ),
              getIngestionSchema().getTuningConfig()
          ),
          supervisorTask.getContext(),
          split
      );
    }
  }

  private static class TestParallelIndexSubTaskSpec extends ParallelIndexSubTaskSpec
  {
    private final SinglePhaseParallelIndexTaskRunner runner;

    TestParallelIndexSubTaskSpec(
        String id,
        String groupId,
        ParallelIndexSupervisorTask supervisorTask,
        SinglePhaseParallelIndexTaskRunner runner,
        ParallelIndexIngestionSpec ingestionSpec,
        Map<String, Object> context,
        InputSplit inputSplit
    )
    {
      super(id, groupId, supervisorTask.getId(), ingestionSpec, context, inputSplit);
      this.runner = runner;
    }

    @Override
    public ParallelIndexSubTask newSubTask(int numAttempts)
    {
      return new ParallelIndexSubTask(
          null,
          getGroupId(),
          null,
          getSupervisorTaskId(),
          numAttempts,
          getIngestionSpec(),
          getContext(),
          null,
          new LocalParallelIndexTaskClientFactory(runner)
      );
    }
  }
}
