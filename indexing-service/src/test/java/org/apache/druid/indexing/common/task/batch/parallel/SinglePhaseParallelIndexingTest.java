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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.incremental.ParseExceptionReport;
import org.apache.druid.segment.incremental.RowIngestionMetersTotals;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.partition.NumberedOverwriteShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class SinglePhaseParallelIndexingTest extends AbstractParallelIndexSupervisorTaskTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Parameterized.Parameters(name = "{0}, useInputFormatApi={1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK, false},
        new Object[]{LockGranularity.TIME_CHUNK, true},
        new Object[]{LockGranularity.SEGMENT, true}
    );
  }

  private static final Interval INTERVAL_TO_INDEX = Intervals.of("2017-12/P1M");
  private static final String VALID_INPUT_SOURCE_FILTER = "test_*";

  private final LockGranularity lockGranularity;
  private final boolean useInputFormatApi;

  private File inputDir;

  public SinglePhaseParallelIndexingTest(LockGranularity lockGranularity, boolean useInputFormatApi)
  {
    super(DEFAULT_TRANSIENT_TASK_FAILURE_RATE, DEFAULT_TRANSIENT_API_FAILURE_RATE);
    this.lockGranularity = lockGranularity;
    this.useInputFormatApi = useInputFormatApi;
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
        if (i == 0) {
          // thrown away due to timestamp outside interval
          writer.write(StringUtils.format("2012-12-%d,%d th test file\n", 25 + i, i));
          // unparseable metric value
          writer.write(StringUtils.format("2017-12-%d,%d th test file,badval\n", 25 + i, i));
          // unparseable row
          writer.write(StringUtils.format("2017unparseable\n"));
        }
      }
    }

    for (int i = 0; i < 5; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "filtered_" + i).toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("2017-12-%d,%d th test file\n", 25 + i, i));
      }
    }

    getObjectMapper().registerSubtypes(SettableSplittableLocalInputSource.class);
  }

  @After
  public void teardown()
  {
    temporaryFolder.delete();
  }

  @Test
  public void testIsReady() throws Exception
  {
    final ParallelIndexSupervisorTask task = newTask(INTERVAL_TO_INDEX, false, true);
    final TaskActionClient actionClient = createActionClient(task);
    final TaskToolbox toolbox = createTaskToolbox(task, actionClient);
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
          spec.getId(),
          0,
          spec.getIngestionSpec(),
          spec.getContext()
      );
      final TaskActionClient subTaskActionClient = createActionClient(subTask);
      prepareTaskForLocking(subTask);
      Assert.assertTrue(subTask.isReady(subTaskActionClient));
      Assert.assertEquals(
          Collections.singleton(
              new ResourceAction(new Resource(
                  LocalInputSource.TYPE_KEY,
                  ResourceType.EXTERNAL
              ), Action.READ)),
          subTask.getInputSourceResources()
      );
    }
  }

  private ParallelIndexSupervisorTask runTestTask(
      @Nullable Interval interval,
      Granularity segmentGranularity,
      boolean appendToExisting,
      Collection<DataSegment> originalSegmentsIfAppend
  )
  {
    // The task could run differently between when appendToExisting is false and true even when this is an initial write
    final ParallelIndexSupervisorTask task = newTask(interval, segmentGranularity, appendToExisting, true);
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());
    assertShardSpec(
        task,
        interval == null ? LockGranularity.TIME_CHUNK : lockGranularity,
        appendToExisting,
        originalSegmentsIfAppend
    );
    TaskContainer taskContainer = getIndexingServiceClient().getTaskContainer(task.getId());
    return (ParallelIndexSupervisorTask) taskContainer.getTask();
  }

  private ParallelIndexSupervisorTask runOverwriteTask(
      @Nullable Interval interval,
      Granularity segmentGranularity,
      LockGranularity actualLockGranularity
  )
  {
    final ParallelIndexSupervisorTask task = newTask(interval, segmentGranularity, false, true);
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());
    assertShardSpecAfterOverwrite(task, actualLockGranularity);
    TaskContainer taskContainer = getIndexingServiceClient().getTaskContainer(task.getId());
    return (ParallelIndexSupervisorTask) taskContainer.getTask();
  }

  private void testRunAndOverwrite(@Nullable Interval inputInterval, Granularity secondSegmentGranularity)
  {
    // Ingest all data.
    runTestTask(inputInterval, Granularities.DAY, false, Collections.emptyList());

    final Collection<DataSegment> allSegments = new HashSet<>(
        inputInterval == null
        ? getStorageCoordinator().retrieveAllUsedSegments("dataSource", Segments.ONLY_VISIBLE)
        : getStorageCoordinator().retrieveUsedSegmentsForInterval("dataSource", inputInterval, Segments.ONLY_VISIBLE)
    );

    // Reingest the same data. Each segment should get replaced by a segment with a newer version.
    final LockGranularity actualLockGranularity;
    if (inputInterval == null) {
      actualLockGranularity = LockGranularity.TIME_CHUNK;
    } else {
      actualLockGranularity = secondSegmentGranularity.equals(Granularities.DAY)
                              ? lockGranularity
                              : LockGranularity.TIME_CHUNK;
    }
    runOverwriteTask(inputInterval, secondSegmentGranularity, actualLockGranularity);

    // Verify that the segment has been replaced.
    final Collection<DataSegment> newSegments =
        inputInterval == null
        ? getStorageCoordinator().retrieveAllUsedSegments("dataSource", Segments.ONLY_VISIBLE)
        : getStorageCoordinator().retrieveUsedSegmentsForInterval("dataSource", inputInterval, Segments.ONLY_VISIBLE);
    Assert.assertFalse(newSegments.isEmpty());
    allSegments.addAll(newSegments);
    final SegmentTimeline timeline = SegmentTimeline.forSegments(allSegments);

    final Interval timelineInterval = inputInterval == null ? Intervals.ETERNITY : inputInterval;
    final Set<DataSegment> visibles = timeline.findNonOvershadowedObjectsInInterval(
        timelineInterval,
        Partitions.ONLY_COMPLETE
    );
    Assert.assertEquals(new HashSet<>(newSegments), visibles);
  }

  private void assertShardSpec(
      ParallelIndexSupervisorTask task,
      LockGranularity actualLockGranularity,
      boolean appendToExisting,
      Collection<DataSegment> originalSegmentsIfAppend
  )
  {
    final Collection<DataSegment> segments = getIndexingServiceClient().getPublishedSegments(task);
    if (!appendToExisting && actualLockGranularity == LockGranularity.TIME_CHUNK) {
      // Initial write
      final Map<Interval, List<DataSegment>> intervalToSegments = SegmentUtils.groupSegmentsByInterval(segments);
      for (List<DataSegment> segmentsPerInterval : intervalToSegments.values()) {
        for (DataSegment segment : segmentsPerInterval) {
          Assert.assertSame(NumberedShardSpec.class, segment.getShardSpec().getClass());
          final NumberedShardSpec shardSpec = (NumberedShardSpec) segment.getShardSpec();
          Assert.assertEquals(segmentsPerInterval.size(), shardSpec.getNumCorePartitions());
        }
      }
    } else {
      // Append or initial write with segment lock
      final Map<Interval, List<DataSegment>> intervalToOriginalSegments = SegmentUtils.groupSegmentsByInterval(
          originalSegmentsIfAppend
      );
      for (DataSegment segment : segments) {
        Assert.assertSame(NumberedShardSpec.class, segment.getShardSpec().getClass());
        final NumberedShardSpec shardSpec = (NumberedShardSpec) segment.getShardSpec();
        final List<DataSegment> originalSegmentsInInterval = intervalToOriginalSegments.get(segment.getInterval());
        final int expectedNumCorePartitions =
            originalSegmentsInInterval == null || originalSegmentsInInterval.isEmpty()
            ? 0
            : originalSegmentsInInterval.get(0).getShardSpec().getNumCorePartitions();
        Assert.assertEquals(expectedNumCorePartitions, shardSpec.getNumCorePartitions());
      }
    }
  }

  private void assertShardSpecAfterOverwrite(ParallelIndexSupervisorTask task, LockGranularity actualLockGranularity)
  {
    final Collection<DataSegment> segments = getIndexingServiceClient().getPublishedSegments(task);
    final Map<Interval, List<DataSegment>> intervalToSegments = SegmentUtils.groupSegmentsByInterval(segments);
    if (actualLockGranularity != LockGranularity.SEGMENT) {
      // Check the core partition set in the shardSpec
      for (List<DataSegment> segmentsPerInterval : intervalToSegments.values()) {
        for (DataSegment segment : segmentsPerInterval) {
          Assert.assertSame(NumberedShardSpec.class, segment.getShardSpec().getClass());
          final NumberedShardSpec shardSpec = (NumberedShardSpec) segment.getShardSpec();
          Assert.assertEquals(segmentsPerInterval.size(), shardSpec.getNumCorePartitions());
        }
      }
    } else {
      for (List<DataSegment> segmentsPerInterval : intervalToSegments.values()) {
        for (DataSegment segment : segmentsPerInterval) {
          Assert.assertSame(NumberedOverwriteShardSpec.class, segment.getShardSpec().getClass());
          final NumberedOverwriteShardSpec shardSpec = (NumberedOverwriteShardSpec) segment.getShardSpec();
          Assert.assertEquals(segmentsPerInterval.size(), shardSpec.getAtomicUpdateGroupSize());
        }
      }
    }
  }

  @Test
  public void testWithoutInterval()
  {
    testRunAndOverwrite(null, Granularities.DAY);
  }

  @Test
  public void testRunInParallel()
  {
    // Ingest all data.
    testRunAndOverwrite(Intervals.of("2017-12/P1M"), Granularities.DAY);
  }

  @Test
  public void testRunInParallelIngestNullColumn()
  {
    if (!useInputFormatApi) {
      return;
    }
    // Ingest all data.
    final List<DimensionSchema> dimensionSchemas = DimensionsSpec.getDefaultSchemas(
        Arrays.asList("ts", "unknownDim", "dim")
    );
    ParallelIndexSupervisorTask task = new ParallelIndexSupervisorTask(
        null,
        null,
        null,
        new ParallelIndexIngestionSpec(
            new DataSchema(
                "dataSource",
                DEFAULT_TIMESTAMP_SPEC,
                DEFAULT_DIMENSIONS_SPEC.withDimensions(dimensionSchemas),
                new AggregatorFactory[]{
                    new LongSumAggregatorFactory("val", "val")
                },
                new UniformGranularitySpec(
                    Granularities.DAY,
                    Granularities.MINUTE,
                    Collections.singletonList(Intervals.of("2017-12/P1M"))
                ),
                null
            ),
            new ParallelIndexIOConfig(
                null,
                new SettableSplittableLocalInputSource(inputDir, VALID_INPUT_SOURCE_FILTER, true),
                DEFAULT_INPUT_FORMAT,
                false,
                null
            ),
            DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING
        ),
        null
    );

    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());

    Set<DataSegment> segments = getIndexingServiceClient().getPublishedSegments(task);
    for (DataSegment segment : segments) {
      for (int i = 0; i < dimensionSchemas.size(); i++) {
        Assert.assertEquals(dimensionSchemas.get(i).getName(), segment.getDimensions().get(i));
      }
    }
  }

  @Test
  public void testRunInParallelIngestNullColumn_storeEmptyColumnsOff_shouldNotStoreEmptyColumns()
  {
    if (!useInputFormatApi) {
      return;
    }
    // Ingest all data.
    final List<DimensionSchema> dimensionSchemas = DimensionsSpec.getDefaultSchemas(
        Arrays.asList("ts", "unknownDim", "dim")
    );
    ParallelIndexSupervisorTask task = new ParallelIndexSupervisorTask(
        null,
        null,
        null,
        new ParallelIndexIngestionSpec(
            new DataSchema(
                "dataSource",
                DEFAULT_TIMESTAMP_SPEC,
                DEFAULT_DIMENSIONS_SPEC.withDimensions(dimensionSchemas),
                new AggregatorFactory[]{
                    new LongSumAggregatorFactory("val", "val")
                },
                new UniformGranularitySpec(
                    Granularities.DAY,
                    Granularities.MINUTE,
                    Collections.singletonList(Intervals.of("2017-12/P1M"))
                ),
                null
            ),
            new ParallelIndexIOConfig(
                null,
                new SettableSplittableLocalInputSource(inputDir, VALID_INPUT_SOURCE_FILTER, true),
                DEFAULT_INPUT_FORMAT,
                false,
                null
            ),
            DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING
        ),
        null
    );

    task.addToContext(Tasks.STORE_EMPTY_COLUMNS_KEY, false);
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());

    Set<DataSegment> segments = getIndexingServiceClient().getPublishedSegments(task);
    for (DataSegment segment : segments) {
      Assert.assertFalse(segment.getDimensions().contains("unknownDim"));
    }
  }

  @Test
  public void testRunInParallelTaskReports()
  {
    ParallelIndexSupervisorTask task = runTestTask(
        Intervals.of("2017-12/P1M"),
        Granularities.DAY,
        false,
        Collections.emptyList()
    );
    Map<String, Object> actualReports = task.doGetLiveReports("full");
    final long processedBytes = useInputFormatApi ? 335 : 0;
    Map<String, Object> expectedReports = buildExpectedTaskReportParallel(
        task.getId(),
        ImmutableList.of(
            new ParseExceptionReport(
                "{ts=2017unparseable}",
                "unparseable",
                ImmutableList.of(getErrorMessageForUnparseableTimestamp()),
                1L
            ),
            new ParseExceptionReport(
                "{ts=2017-12-25, dim=0 th test file, val=badval}",
                "processedWithError",
                ImmutableList.of("Unable to parse value[badval] for field[val]"),
                1L
            )
        ),
        new RowIngestionMetersTotals(10, processedBytes, 1, 1, 1)
    );
    compareTaskReports(expectedReports, actualReports);
  }

  //
  // Ingest all data.

  @Test
  public void testWithoutIntervalWithDifferentSegmentGranularity()
  {
    testRunAndOverwrite(null, Granularities.MONTH);
  }

  @Test()
  public void testRunInParallelWithDifferentSegmentGranularity()
  {
    // Ingest all data.
    testRunAndOverwrite(Intervals.of("2017-12/P1M"), Granularities.MONTH);
  }

  @Test
  public void testRunInSequential()
  {
    final Interval interval = Intervals.of("2017-12/P1M");
    final boolean appendToExisting = false;
    final ParallelIndexSupervisorTask task = newTask(interval, appendToExisting, false);
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());
    assertShardSpec(task, lockGranularity, appendToExisting, Collections.emptyList());

    TaskContainer taskContainer = getIndexingServiceClient().getTaskContainer(task.getId());
    final ParallelIndexSupervisorTask executedTask = (ParallelIndexSupervisorTask) taskContainer.getTask();
    Map<String, Object> actualReports = executedTask.doGetLiveReports("full");

    final long processedBytes = useInputFormatApi ? 335 : 0;
    RowIngestionMetersTotals expectedTotals = new RowIngestionMetersTotals(10, processedBytes, 1, 1, 1);
    List<ParseExceptionReport> expectedUnparseableEvents = ImmutableList.of(
        new ParseExceptionReport(
            "{ts=2017unparseable}",
            "unparseable",
            ImmutableList.of(getErrorMessageForUnparseableTimestamp()),
            1L
        ),
        new ParseExceptionReport(
            "{ts=2017-12-25, dim=0 th test file, val=badval}",
            "processedWithError",
            ImmutableList.of("Unable to parse value[badval] for field[val]"),
            1L
        )
    );

    Map<String, Object> expectedReports;
    if (useInputFormatApi) {
      expectedReports = buildExpectedTaskReportSequential(
          task.getId(),
          expectedUnparseableEvents,
          new RowIngestionMetersTotals(0, 0, 0, 0, 0),
          expectedTotals
      );
    } else {
      // when useInputFormatApi is false, maxConcurrentSubTasks=2 and it uses the single phase runner
      // instead of sequential runner
      expectedReports = buildExpectedTaskReportParallel(
          task.getId(),
          expectedUnparseableEvents,
          expectedTotals
      );
    }

    compareTaskReports(expectedReports, actualReports);
  }

  @Test
  public void testPublishEmptySegments()
  {
    final ParallelIndexSupervisorTask task = newTask(Intervals.of("2020-12/P1M"), false, true);
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());
  }

  @Test
  public void testWith1MaxNumConcurrentSubTasks()
  {
    final Interval interval = Intervals.of("2017-12/P1M");
    final boolean appendToExisting = false;
    final ParallelIndexSupervisorTask task = newTask(
        interval,
        Granularities.DAY,
        appendToExisting,
        true,
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
            1,
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
            null
        ),
        VALID_INPUT_SOURCE_FILTER
    );
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());
    Assert.assertNull("Runner must be null if the task was in the sequential mode", task.getCurrentRunner());
    assertShardSpec(task, lockGranularity, appendToExisting, Collections.emptyList());
  }

  @Test
  public void testAppendToExisting()
  {
    final Interval interval = Intervals.of("2017-12/P1M");
    runTestTask(interval, Granularities.DAY, true, Collections.emptyList());
    final Collection<DataSegment> oldSegments =
        getStorageCoordinator().retrieveUsedSegmentsForInterval("dataSource", interval, Segments.ONLY_VISIBLE);

    runTestTask(interval, Granularities.DAY, true, oldSegments);
    final Collection<DataSegment> newSegments =
        getStorageCoordinator().retrieveUsedSegmentsForInterval("dataSource", interval, Segments.ONLY_VISIBLE);
    Assert.assertTrue(newSegments.containsAll(oldSegments));
    final SegmentTimeline timeline = SegmentTimeline.forSegments(newSegments);
    final Set<DataSegment> visibles = timeline.findNonOvershadowedObjectsInInterval(interval, Partitions.ONLY_COMPLETE);
    Assert.assertEquals(new HashSet<>(newSegments), visibles);
  }

  @Test
  public void testMultipleAppends()
  {
    final Interval interval = null;
    final ParallelIndexSupervisorTask task = newTask(interval, Granularities.DAY, true, true);
    final ParallelIndexSupervisorTask task2 = newTask(interval, Granularities.DAY, true, true);
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, true);
    task.addToContext(Tasks.USE_SHARED_LOCK, true);
    task2.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, true);
    task2.addToContext(Tasks.USE_SHARED_LOCK, true);
    getIndexingServiceClient().runTask(task.getId(), task);
    getIndexingServiceClient().runTask(task2.getId(), task2);

    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().waitToFinish(task, 1, TimeUnit.DAYS).getStatusCode());
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().waitToFinish(task2, 1, TimeUnit.DAYS).getStatusCode());
  }

  @Test
  public void testRunParallelWithNoInputSplitToProcess()
  {
    // The input source filter on this task does not match any input
    // Hence, the this task will has no input split to process
    final ParallelIndexSupervisorTask task = newTask(
        Intervals.of("2017-12/P1M"),
        Granularities.DAY,
        true,
        true,
        AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING,
        "non_existing_file_filter"
    );
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    // Task state should still be SUCCESS even if no input split to process
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());
  }

  @Test
  public void testOverwriteAndAppend()
  {
    final Interval interval = Intervals.of("2017-12/P1M");
    testRunAndOverwrite(interval, Granularities.DAY);
    final Collection<DataSegment> beforeAppendSegments =
        getStorageCoordinator().retrieveUsedSegmentsForInterval("dataSource", interval, Segments.ONLY_VISIBLE);

    runTestTask(
        interval,
        Granularities.DAY,
        true,
        beforeAppendSegments
    );
    final Collection<DataSegment> afterAppendSegments =
        getStorageCoordinator().retrieveUsedSegmentsForInterval("dataSource", interval, Segments.ONLY_VISIBLE);
    Assert.assertTrue(afterAppendSegments.containsAll(beforeAppendSegments));
    final SegmentTimeline timeline = SegmentTimeline.forSegments(afterAppendSegments);
    final Set<DataSegment> visibles = timeline.findNonOvershadowedObjectsInInterval(interval, Partitions.ONLY_COMPLETE);
    Assert.assertEquals(new HashSet<>(afterAppendSegments), visibles);
  }

  @Test
  public void testMaxLocksWith1MaxNumConcurrentSubTasks()
  {
    final Interval interval = Intervals.of("2017-12/P1M");
    final boolean appendToExisting = false;
    final ParallelIndexSupervisorTask task = newTask(
        interval,
        Granularities.DAY,
        appendToExisting,
        true,
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
            1,
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
            0,
            null
        ),
        VALID_INPUT_SOURCE_FILTER
    );
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);

    if (lockGranularity.equals(LockGranularity.TIME_CHUNK)) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage(
          "Number of locks exceeded maxAllowedLockCount [0]"
      );
      getIndexingServiceClient().runAndWait(task);
    } else {
      Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());
      Assert.assertNull("Runner must be null if the task was in the sequential mode", task.getCurrentRunner());
      assertShardSpec(task, lockGranularity, appendToExisting, Collections.emptyList());
    }
  }


  @Test
  public void testMaxLocksWith2MaxNumConcurrentSubTasks()
  {
    final Interval interval = Intervals.of("2017-12/P1M");
    final boolean appendToExisting = false;
    final ParallelIndexSupervisorTask task = newTask(
        interval,
        Granularities.DAY,
        appendToExisting,
        true,
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
            null,
            0,
            null
        ),
        VALID_INPUT_SOURCE_FILTER
    );
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);

    if (lockGranularity.equals(LockGranularity.TIME_CHUNK)) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage(
          "Number of locks exceeded maxAllowedLockCount [0]"
      );
      getIndexingServiceClient().runAndWait(task);
    } else {
      Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());
      Assert.assertNull("Runner must be null if the task was in the sequential mode", task.getCurrentRunner());
      assertShardSpec(task, lockGranularity, appendToExisting, Collections.emptyList());
    }
  }

  @Test
  public void testIngestBothExplicitAndImplicitDims() throws IOException
  {
    final Interval interval = Intervals.of("2017-12/P1M");
    for (int i = 0; i < 5; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "test_" + i + ".json").toPath(), StandardCharsets.UTF_8)) {

        writer.write(
            getObjectMapper().writeValueAsString(
                ImmutableMap.of(
                    "ts",
                    StringUtils.format("2017-12-%d", 24 + i),
                    "implicitDim",
                    "implicit_" + i,
                    "explicitDim",
                    "explicit_" + i
                )
            )
        );
        writer.write(
            getObjectMapper().writeValueAsString(
                ImmutableMap.of(
                    "ts",
                    StringUtils.format("2017-12-%d", 25 + i),
                    "implicitDim",
                    "implicit_" + i,
                    "explicitDim",
                    "explicit_" + i
                )
            )
        );
      }
    }

    final ParallelIndexSupervisorTask task = new ParallelIndexSupervisorTask(
        null,
        null,
        null,
        new ParallelIndexIngestionSpec(
            new DataSchema(
                "dataSource",
                DEFAULT_TIMESTAMP_SPEC,
                DimensionsSpec.builder()
                              .setDefaultSchemaDimensions(ImmutableList.of("ts", "explicitDim"))
                              .setIncludeAllDimensions(true)
                              .build(),
                new AggregatorFactory[]{new CountAggregatorFactory("cnt")},
                new UniformGranularitySpec(
                    Granularities.DAY,
                    Granularities.MINUTE,
                    Collections.singletonList(interval)
                ),
                null
            ),
            new ParallelIndexIOConfig(
                null,
                new SettableSplittableLocalInputSource(inputDir, "*.json", true),
                new JsonInputFormat(
                    new JSONPathSpec(true, null),
                    null,
                    null,
                    null,
                    null
                ),
                false,
                null
            ),
            AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING
        ),
        null
    );
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());

    Set<DataSegment> segments = getIndexingServiceClient().getPublishedSegments(task);
    for (DataSegment segment : segments) {
      Assert.assertEquals(ImmutableList.of("ts", "explicitDim", "implicitDim"), segment.getDimensions());
    }
  }

  @Test
  public void testIngestBothExplicitAndImplicitDimsSchemaDiscovery() throws IOException
  {
    final Interval interval = Intervals.of("2017-12/P1M");
    for (int i = 0; i < 5; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "test_" + i + ".json").toPath(), StandardCharsets.UTF_8)) {

        writer.write(
            getObjectMapper().writeValueAsString(
                ImmutableMap.of(
                    "ts",
                    StringUtils.format("2017-12-%d", 24 + i),
                    "implicitDim",
                    "implicit_" + i,
                    "explicitDim",
                    "explicit_" + i
                )
            )
        );
        writer.write(
            getObjectMapper().writeValueAsString(
                ImmutableMap.of(
                    "ts",
                    StringUtils.format("2017-12-%d", 25 + i),
                    "implicitDim",
                    "implicit_" + i,
                    "explicitDim",
                    "explicit_" + i
                )
            )
        );
      }
    }

    final ParallelIndexSupervisorTask task = new ParallelIndexSupervisorTask(
        null,
        null,
        null,
        new ParallelIndexIngestionSpec(
            new DataSchema(
                "dataSource",
                DEFAULT_TIMESTAMP_SPEC,
                DimensionsSpec.builder()
                              .setDefaultSchemaDimensions(ImmutableList.of("ts", "explicitDim"))
                              .useSchemaDiscovery(true)
                              .build(),
                new AggregatorFactory[]{new CountAggregatorFactory("cnt")},
                new UniformGranularitySpec(
                    Granularities.DAY,
                    Granularities.MINUTE,
                    Collections.singletonList(interval)
                ),
                null
            ),
            new ParallelIndexIOConfig(
                null,
                new SettableSplittableLocalInputSource(inputDir, "*.json", true),
                new JsonInputFormat(
                    new JSONPathSpec(true, null),
                    null,
                    null,
                    null,
                    null
                ),
                false,
                null
            ),
            AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING
        ),
        null
    );
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());

    Set<DataSegment> segments = getIndexingServiceClient().getPublishedSegments(task);
    for (DataSegment segment : segments) {
      Assert.assertEquals(ImmutableList.of("ts", "explicitDim", "implicitDim"), segment.getDimensions());
    }
  }

  private ParallelIndexSupervisorTask newTask(
      @Nullable Interval interval,
      boolean appendToExisting,
      boolean splittableInputSource
  )
  {
    return newTask(interval, Granularities.DAY, appendToExisting, splittableInputSource);
  }

  private ParallelIndexSupervisorTask newTask(
      @Nullable Interval interval,
      Granularity segmentGranularity,
      boolean appendToExisting,
      boolean splittableInputSource
  )
  {
    return newTask(
        interval,
        segmentGranularity,
        appendToExisting,
        splittableInputSource,
        AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING,
        VALID_INPUT_SOURCE_FILTER
    );
  }

  private ParallelIndexSupervisorTask newTask(
      @Nullable Interval interval,
      Granularity segmentGranularity,
      boolean appendToExisting,
      boolean splittableInputSource,
      ParallelIndexTuningConfig tuningConfig,
      String inputSourceFilter
  )
  {
    // set up ingestion spec
    final ParallelIndexIngestionSpec ingestionSpec;
    if (useInputFormatApi) {
      ingestionSpec = new ParallelIndexIngestionSpec(
          new DataSchema(
              "dataSource",
              DEFAULT_TIMESTAMP_SPEC,
              DEFAULT_DIMENSIONS_SPEC,
              new AggregatorFactory[]{
                  new LongSumAggregatorFactory("val", "val")
              },
              new UniformGranularitySpec(
                  segmentGranularity,
                  Granularities.MINUTE,
                  interval == null ? null : Collections.singletonList(interval)
              ),
              null
          ),
          new ParallelIndexIOConfig(
              null,
              new SettableSplittableLocalInputSource(inputDir, inputSourceFilter, splittableInputSource),
              DEFAULT_INPUT_FORMAT,
              appendToExisting,
              null
          ),
          tuningConfig
      );
    } else {
      ingestionSpec = new ParallelIndexIngestionSpec(
          new DataSchema(
              "dataSource",
              DEFAULT_TIMESTAMP_SPEC,
              DEFAULT_DIMENSIONS_SPEC,
              DEFAULT_METRICS_SPEC,
              new UniformGranularitySpec(
                  segmentGranularity,
                  Granularities.MINUTE,
                  interval == null ? null : Collections.singletonList(interval)
              ),
              null
          ),
          new ParallelIndexIOConfig(
              null,
              new LocalInputSource(inputDir, inputSourceFilter),
              createInputFormatFromParseSpec(DEFAULT_PARSE_SPEC),
              appendToExisting,
              null
          ),
          tuningConfig
      );
    }

    // set up test tools
    return new ParallelIndexSupervisorTask(
        null,
        null,
        null,
        ingestionSpec,
        Collections.emptyMap()
    );
  }

  private String getErrorMessageForUnparseableTimestamp()
  {
    return StringUtils.format(
        "Timestamp[2017unparseable] is unparseable! Event: {ts=2017unparseable} (Path: %s, Record: 5, Line: 5)",
        new File(inputDir, "test_0").toURI()
    );
  }

  private static class SettableSplittableLocalInputSource extends LocalInputSource
  {
    private final boolean splittableInputSource;

    @JsonCreator
    private SettableSplittableLocalInputSource(
        @JsonProperty("baseDir") File baseDir,
        @JsonProperty("filter") String filter,
        @JsonProperty("splittableInputSource") boolean splittableInputSource
    )
    {
      super(baseDir, filter);
      this.splittableInputSource = splittableInputSource;
    }

    @JsonProperty
    public boolean isSplittableInputSource()
    {
      return splittableInputSource;
    }

    @Override
    public boolean isSplittable()
    {
      return splittableInputSource;
    }
  }
}
