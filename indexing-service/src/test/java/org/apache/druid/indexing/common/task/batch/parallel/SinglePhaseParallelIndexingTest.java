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

import com.google.common.collect.ImmutableList;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.firehose.LocalFirehoseFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class SinglePhaseParallelIndexingTest extends AbstractParallelIndexSupervisorTaskTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK},
        new Object[]{LockGranularity.SEGMENT}
    );
  }

  private final LockGranularity lockGranularity;
  private File inputDir;

  public SinglePhaseParallelIndexingTest(LockGranularity lockGranularity)
  {
    this.lockGranularity = lockGranularity;
  }

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

    final SinglePhaseParallelIndexTaskRunner runner = task.createSinglePhaseTaskRunner(toolbox);
    final Iterator<SubTaskSpec<SinglePhaseSubTask>> subTaskSpecIterator = runner.subTaskSpecIterator();

    while (subTaskSpecIterator.hasNext()) {
      final SinglePhaseSubTaskSpec spec = (SinglePhaseSubTaskSpec) subTaskSpecIterator.next();
      final SinglePhaseSubTask subTask = new SinglePhaseSubTask(
          null,
          spec.getGroupId(),
          null,
          spec.getSupervisorTaskId(),
          0,
          spec.getIngestionSpec(),
          spec.getContext(),
          indexingServiceClient,
          null,
          new TestAppenderatorsManager()
      );
      final TaskActionClient subTaskActionClient = createActionClient(subTask);
      prepareTaskForLocking(subTask);
      Assert.assertTrue(subTask.isReady(subTaskActionClient));
    }
  }

  private void runTestTask(@Nullable Interval interval, Granularity segmentGranularity, boolean appendToExisting)
      throws Exception
  {
    final ParallelIndexSupervisorTask task = newTask(
        interval,
        segmentGranularity,
        new ParallelIndexIOConfig(
            new LocalFirehoseFactory(inputDir, "test_*", null),
            appendToExisting
        )
    );
    actionClient = createActionClient(task);
    toolbox = createTaskToolbox(task);

    prepareTaskForLocking(task);
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertTrue(task.isReady(actionClient));
    Assert.assertEquals(TaskState.SUCCESS, task.run(toolbox).getStatusCode());
    shutdownTask(task);
  }

  private void runTestTask(@Nullable Interval interval, Granularity segmentGranularity) throws Exception
  {
    runTestTask(interval, segmentGranularity, false);
  }

  private void testRunAndOverwrite(@Nullable Interval inputInterval, Granularity secondSegmentGranularity)
      throws Exception
  {
    // Ingest all data.
    runTestTask(inputInterval, Granularities.DAY);

    final Interval interval = inputInterval == null ? Intervals.ETERNITY : inputInterval;
    final List<DataSegment> allSegments = getStorageCoordinator().getUsedSegmentsForInterval("dataSource", interval);

    // Reingest the same data. Each segment should get replaced by a segment with a newer version.
    runTestTask(inputInterval, secondSegmentGranularity);

    // Verify that the segment has been replaced.
    final List<DataSegment> newSegments = getStorageCoordinator().getUsedSegmentsForInterval("dataSource", interval);
    allSegments.addAll(newSegments);
    final VersionedIntervalTimeline<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(allSegments);
    final List<DataSegment> visibles = timeline.lookup(interval)
                                               .stream()
                                               .flatMap(holder -> holder.getObject().stream())
                                               .map(PartitionChunk::getObject)
                                               .collect(Collectors.toList());
    Assert.assertEquals(newSegments, visibles);
  }

  @Test
  public void testWithoutInterval() throws Exception
  {
    testRunAndOverwrite(null, Granularities.DAY);
  }

  @Test()
  public void testRunInParallel() throws Exception
  {
    // Ingest all data.
    testRunAndOverwrite(Intervals.of("2017/2018"), Granularities.DAY);
  }

  @Test
  public void testWithoutIntervalWithDifferentSegmentGranularity() throws Exception
  {
    testRunAndOverwrite(null, Granularities.MONTH);
  }

  @Test()
  public void testRunInParallelWithDifferentSegmentGranularity() throws Exception
  {
    // Ingest all data.
    testRunAndOverwrite(Intervals.of("2017/2018"), Granularities.MONTH);
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
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
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
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertTrue(task.isReady(actionClient));
    Assert.assertEquals(TaskState.SUCCESS, task.run(toolbox).getStatusCode());
  }

  @Test
  public void testWith1MaxNumConcurrentSubTasks() throws Exception
  {
    final ParallelIndexSupervisorTask task = newTask(
        Intervals.of("2017/2018"),
        Granularities.DAY,
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
            null,
            null,
            null
        )
    );
    actionClient = createActionClient(task);
    toolbox = createTaskToolbox(task);

    prepareTaskForLocking(task);
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertTrue(task.isReady(actionClient));
    Assert.assertEquals(TaskState.SUCCESS, task.run(toolbox).getStatusCode());
    Assert.assertNull("Runner must be null if the task was in the sequential mode", task.getCurrentRunner());
  }

  @Test
  public void testAppendToExisting() throws Exception
  {
    final Interval interval = Intervals.of("2017/2018");
    runTestTask(interval, Granularities.DAY, true);
    final List<DataSegment> oldSegments = getStorageCoordinator().getUsedSegmentsForInterval("dataSource", interval);

    runTestTask(interval, Granularities.DAY, true);
    final List<DataSegment> newSegments = getStorageCoordinator().getUsedSegmentsForInterval("dataSource", interval);
    Assert.assertTrue(newSegments.containsAll(oldSegments));
    final VersionedIntervalTimeline<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(newSegments);
    final List<DataSegment> visibles = timeline.lookup(interval)
                                               .stream()
                                               .flatMap(holder -> holder.getObject().stream())
                                               .map(PartitionChunk::getObject)
                                               .collect(Collectors.toList());
    Assert.assertEquals(newSegments, visibles);
  }

  private ParallelIndexSupervisorTask newTask(@Nullable Interval interval, ParallelIndexIOConfig ioConfig)
  {
    return newTask(interval, Granularities.DAY, ioConfig);
  }

  private ParallelIndexSupervisorTask newTask(
      @Nullable Interval interval,
      Granularity segmentGranularity,
      ParallelIndexIOConfig ioConfig
  )
  {
    return newTask(
        interval,
        segmentGranularity,
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
            null
        )
    );
  }

  private ParallelIndexSupervisorTask newTask(
      @Nullable Interval interval,
      Granularity segmentGranularity,
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
                segmentGranularity,
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

  public static class TestSupervisorTask extends TestParallelIndexSupervisorTask
  {
    public TestSupervisorTask(
        String id,
        TaskResource taskResource,
        ParallelIndexIngestionSpec ingestionSchema,
        Map<String, Object> context,
        IndexingServiceClient indexingServiceClient
    )
    {
      super(id, taskResource, ingestionSchema, context, indexingServiceClient);
    }

    @Override
    SinglePhaseParallelIndexTaskRunner createSinglePhaseTaskRunner(TaskToolbox toolbox)
    {
      return new TestSinglePhaseRunner(toolbox, this, getIndexingServiceClient());
    }
  }

  public static class TestSinglePhaseRunner extends TestSinglePhaseParallelIndexTaskRunner
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    TestSinglePhaseRunner(
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
    SinglePhaseSubTaskSpec newTaskSpec(InputSplit split)
    {
      final FiniteFirehoseFactory baseFirehoseFactory = (FiniteFirehoseFactory) getIngestionSchema()
          .getIOConfig()
          .getFirehoseFactory();
      return new TestSinglePhaseSubTaskSpec(
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

  public static class TestSinglePhaseSubTaskSpec extends SinglePhaseSubTaskSpec
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    TestSinglePhaseSubTaskSpec(
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
    public SinglePhaseSubTask newSubTask(int numAttempts)
    {
      return new SinglePhaseSubTask(
          null,
          getGroupId(),
          null,
          getSupervisorTaskId(),
          numAttempts,
          getIngestionSpec(),
          getContext(),
          null,
          new LocalParallelIndexTaskClientFactory(supervisorTask),
          new TestAppenderatorsManager()
      );
    }
  }
}
