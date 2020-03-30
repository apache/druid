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
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.overlord.Segments;
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
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.VersionedIntervalTimeline;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

@RunWith(Parameterized.class)
public class SinglePhaseParallelIndexingTest extends AbstractParallelIndexSupervisorTaskTest
{
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

  private final LockGranularity lockGranularity;
  private final boolean useInputFormatApi;

  private File inputDir;

  public SinglePhaseParallelIndexingTest(LockGranularity lockGranularity, boolean useInputFormatApi)
  {
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
          0,
          spec.getIngestionSpec(),
          spec.getContext(),
          getIndexingServiceClient(),
          null,
          new TestAppenderatorsManager()
      );
      final TaskActionClient subTaskActionClient = createActionClient(subTask);
      prepareTaskForLocking(subTask);
      Assert.assertTrue(subTask.isReady(subTaskActionClient));
    }
  }

  private void runTestTask(@Nullable Interval interval, Granularity segmentGranularity, boolean appendToExisting)
  {
    final ParallelIndexSupervisorTask task = newTask(interval, segmentGranularity, appendToExisting, true);
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());
  }

  private void runTestTask(@Nullable Interval interval, Granularity segmentGranularity)
  {
    runTestTask(interval, segmentGranularity, false);
  }

  private void testRunAndOverwrite(@Nullable Interval inputInterval, Granularity secondSegmentGranularity)
  {
    // Ingest all data.
    runTestTask(inputInterval, Granularities.DAY);

    final Interval interval = inputInterval == null ? Intervals.ETERNITY : inputInterval;
    final Collection<DataSegment> allSegments = new HashSet<>(
        getStorageCoordinator().retrieveUsedSegmentsForInterval("dataSource", interval, Segments.ONLY_VISIBLE)
    );

    // Reingest the same data. Each segment should get replaced by a segment with a newer version.
    runTestTask(inputInterval, secondSegmentGranularity);

    // Verify that the segment has been replaced.
    final Collection<DataSegment> newSegments =
        getStorageCoordinator().retrieveUsedSegmentsForInterval("dataSource", interval, Segments.ONLY_VISIBLE);
    allSegments.addAll(newSegments);
    final VersionedIntervalTimeline<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(allSegments);
    final Set<DataSegment> visibles = timeline.findNonOvershadowedObjectsInInterval(interval, Partitions.ONLY_COMPLETE);
    Assert.assertEquals(new HashSet<>(newSegments), visibles);
  }

  @Test
  public void testWithoutInterval()
  {
    testRunAndOverwrite(null, Granularities.DAY);
  }

  @Test()
  public void testRunInParallel()
  {
    // Ingest all data.
    testRunAndOverwrite(Intervals.of("2017-12/P1M"), Granularities.DAY);
  }

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
    final ParallelIndexSupervisorTask task = newTask(Intervals.of("2017-12/P1M"), false, false);
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());
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
    final ParallelIndexSupervisorTask task = newTask(
        Intervals.of("2017-12/P1M"),
        Granularities.DAY,
        false,
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
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());
    Assert.assertNull("Runner must be null if the task was in the sequential mode", task.getCurrentRunner());
  }

  @Test
  public void testAppendToExisting()
  {
    final Interval interval = Intervals.of("2017-12/P1M");
    runTestTask(interval, Granularities.DAY, true);
    final Collection<DataSegment> oldSegments =
        getStorageCoordinator().retrieveUsedSegmentsForInterval("dataSource", interval, Segments.ONLY_VISIBLE);

    runTestTask(interval, Granularities.DAY, true);
    final Collection<DataSegment> newSegments =
        getStorageCoordinator().retrieveUsedSegmentsForInterval("dataSource", interval, Segments.ONLY_VISIBLE);
    Assert.assertTrue(newSegments.containsAll(oldSegments));
    final VersionedIntervalTimeline<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(newSegments);
    final Set<DataSegment> visibles = timeline.findNonOvershadowedObjectsInInterval(interval, Partitions.ONLY_COMPLETE);
    Assert.assertEquals(new HashSet<>(newSegments), visibles);
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
        AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING
    );
  }

  private ParallelIndexSupervisorTask newTask(
      @Nullable Interval interval,
      Granularity segmentGranularity,
      boolean appendToExisting,
      boolean splittableInputSource,
      ParallelIndexTuningConfig tuningConfig
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
              new SettableSplittableLocalInputSource(inputDir, "test_*", splittableInputSource),
              DEFAULT_INPUT_FORMAT,
              appendToExisting
          ),
          tuningConfig
      );
    } else {
      ingestionSpec = new ParallelIndexIngestionSpec(
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
          new ParallelIndexIOConfig(
              new LocalFirehoseFactory(inputDir, "test_*", null),
              appendToExisting
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
        Collections.emptyMap(),
        getIndexingServiceClient(),
        null,
        null,
        null,
        null
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
