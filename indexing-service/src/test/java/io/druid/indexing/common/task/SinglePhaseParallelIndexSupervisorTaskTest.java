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
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SinglePhaseParallelIndexSupervisorTaskTest extends AbstractSinglePhaseParallelIndexSupervisorTaskTest
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
    final SinglePhaseParallelIndexSupervisorTask task = newTask(
        Intervals.of("2017/2018"),
        new SinglePhaseParallelIndexIOConfig(
            new LocalFirehoseFactory(inputDir, "test_*", null),
            false
        )
    );
    actionClient = createActionClient(task);
    toolbox = createTaskToolbox(task);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));

    final Iterator<SinglePhaseParallelIndexSubTaskSpec> subTaskSpecIterator = task.subTaskSpecIterator().iterator();

    while (subTaskSpecIterator.hasNext()) {
      final SinglePhaseParallelIndexSubTaskSpec spec = subTaskSpecIterator.next();
      final SinglePhaseParallelIndexSubTask subTask = new SinglePhaseParallelIndexSubTask(
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
    final SinglePhaseParallelIndexSupervisorTask task = newTask(
        null,
        new SinglePhaseParallelIndexIOConfig(
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
    final SinglePhaseParallelIndexSupervisorTask task = newTask(
        Intervals.of("2017/2018"),
        new SinglePhaseParallelIndexIOConfig(
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
    final SinglePhaseParallelIndexSupervisorTask task = newTask(
        Intervals.of("2017/2018"),
        new SinglePhaseParallelIndexIOConfig(
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

  private SinglePhaseParallelIndexSupervisorTask newTask(
      Interval interval,
      SinglePhaseParallelIndexIOConfig ioConfig
  )
  {
    // set up ingestion spec
    final SinglePhaseParallelIndexIngestionSpec singlePhaseIngestionSpec = new SinglePhaseParallelIndexIngestionSpec(
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
        new SinglePhaseParallelIndexTuningConfig(
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
        singlePhaseIngestionSpec,
        new HashMap<>(),
        indexingServiceClient
    );
  }

  private static class TestSupervisorTask extends TestSinglePhaseParallelIndexSupervisorTask
  {
    TestSupervisorTask(
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
          indexingServiceClient
      );
    }

    @Override
    SinglePhaseParallelIndexSubTaskSpec newTaskSpec(InputSplit split)
    {
      final FiniteFirehoseFactory baseFirehoseFactory = (FiniteFirehoseFactory) getIngestionSchema()
          .getIOConfig()
          .getFirehoseFactory();
      return new TestSinglePhaseParallelIndexSubTaskSpec(
          getId() + "_" + getAndIncrementNextSpecId(),
          getGroupId(),
          this,
          new SinglePhaseParallelIndexIngestionSpec(
              getIngestionSchema().getDataSchema(),
              new SinglePhaseParallelIndexIOConfig(
                  baseFirehoseFactory.withSplit(split),
                  getIngestionSchema().getIOConfig().isAppendToExisting()
              ),
              getIngestionSchema().getTuningConfig()
          ),
          getContext(),
          split
      );
    }
  }

  private static class TestSinglePhaseParallelIndexSubTaskSpec extends SinglePhaseParallelIndexSubTaskSpec
  {
    private final SinglePhaseParallelIndexSupervisorTask supervisorTask;

    TestSinglePhaseParallelIndexSubTaskSpec(
        String id,
        String groupId,
        SinglePhaseParallelIndexSupervisorTask supervisorTask,
        SinglePhaseParallelIndexIngestionSpec ingestionSpec,
        Map<String, Object> context,
        InputSplit inputSplit
    )
    {
      super(id, groupId, supervisorTask.getId(), ingestionSpec, context, inputSplit);
      this.supervisorTask = supervisorTask;
    }

    @Override
    public SinglePhaseParallelIndexSubTask newSubTask(int numAttempts)
    {
      return new SinglePhaseParallelIndexSubTask(
          null,
          getGroupId(),
          null,
          getSupervisorTaskId(),
          numAttempts,
          getIngestionSpec(),
          getContext(),
          null,
          new LocalSinglePhaseParallelIndexTaskClientFactory(supervisorTask)
      );
    }
  }
}
