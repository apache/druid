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

import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.firehose.LocalFirehoseFactory;
import org.apache.druid.timeline.DataSegment;
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
import java.util.List;
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

    final SinglePhaseParallelIndexTaskRunner runner = (SinglePhaseParallelIndexTaskRunner) task.createRunner(toolbox);
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

  private void runTestWithoutIntervalTask() throws Exception
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
    shutdownTask(task);
  }

  @Test
  public void testWithoutInterval() throws Exception
  {
    // Ingest all data.
    runTestWithoutIntervalTask();

    // Read the segments for one day.
    final Interval interval = Intervals.of("2017-12-24/P1D");
    final List<DataSegment> oldSegments =
        getStorageCoordinator().retrieveUsedSegmentsForInterval("dataSource", interval);
    Assert.assertEquals(1, oldSegments.size());

    // Reingest the same data. Each segment should get replaced by a segment with a newer version.
    runTestWithoutIntervalTask();

    // Verify that the segment has been replaced.
    final List<DataSegment> newSegments =
        getStorageCoordinator().retrieveUsedSegmentsForInterval("dataSource", interval);
    Assert.assertEquals(1, newSegments.size());
    Assert.assertTrue(oldSegments.get(0).getVersion().compareTo(newSegments.get(0).getVersion()) < 0);
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

  @Test
  public void testPublishEmptySegments() throws Exception
  {
    final ParallelIndexSupervisorTask task = newTask(
        Intervals.of("2020/2021"),
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
  public void testWith1MaxNumSubTasks() throws Exception
  {
    final ParallelIndexSupervisorTask task = newTask(
        Intervals.of("2017/2018"),
        new ParallelIndexIOConfig(
            new LocalFirehoseFactory(inputDir, "test_*", null),
            false
        ),
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
            1,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        )
    );
    actionClient = createActionClient(task);
    toolbox = createTaskToolbox(task);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));
    Assert.assertEquals(TaskState.SUCCESS, task.run(toolbox).getStatusCode());
    Assert.assertNull("Runner must be null if the task was in the sequential mode", task.getRunner());
  }

  private ParallelIndexSupervisorTask newTask(
      Interval interval,
      ParallelIndexIOConfig ioConfig
  )
  {
    return newTask(
        interval,
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
  }

  private ParallelIndexSupervisorTask newTask(
      Interval interval,
      ParallelIndexIOConfig ioConfig,
      ParallelIndexTuningConfig tuningConfig
  )
  {
    // set up ingestion spec
    //noinspection unchecked
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
        tuningConfig
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
    ParallelIndexTaskRunner createRunner(TaskToolbox toolbox)
    {
      setRunner(
          new TestRunner(
              toolbox,
              this,
              indexingServiceClient
          )
      );
      return getRunner();
    }
  }

  private static class TestRunner extends TestParallelIndexTaskRunner
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    TestRunner(
        TaskToolbox toolbox,
        ParallelIndexSupervisorTask supervisorTask,
        @Nullable IndexingServiceClient indexingServiceClient
    )
    {
      super(
          toolbox,
          supervisorTask.getId(),
          supervisorTask.getGroupId(),
          supervisorTask.getIngestionSchema(),
          supervisorTask.getContext(),
          indexingServiceClient
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
    private final ParallelIndexSupervisorTask supervisorTask;

    TestParallelIndexSubTaskSpec(
        String id,
        String groupId,
        ParallelIndexSupervisorTask supervisorTask,
        ParallelIndexIngestionSpec ingestionSpec,
        Map<String, Object> context,
        InputSplit inputSplit
    )
    {
      super(id, groupId, supervisorTask.getId(), ingestionSpec, context, inputSplit);
      this.supervisorTask = supervisorTask;
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
          new LocalParallelIndexTaskClientFactory(supervisorTask)
      );
    }
  }
}
