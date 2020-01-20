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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.PartitionBoundaries;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.hamcrest.Matchers;
import org.joda.time.Interval;
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class RangePartitionMultiPhaseParallelIndexingTest extends AbstractMultiPhaseParallelIndexingTest
{
  private static final int NUM_FILE = 10;
  private static final int NUM_ROW = 20;
  private static final int NUM_DAY = 2;
  private static final int NUM_PARTITION = 2;
  private static final int YEAR = 2017;
  private static final String DIM1 = "dim1";
  private static final String DIM2 = "dim2";
  private static final List<String> DIMS = ImmutableList.of(DIM1, DIM2);
  private static final String TEST_FILE_NAME_PREFIX = "test_";
  private static final ParseSpec PARSE_SPEC = new CSVParseSpec(
      new TimestampSpec(
          "ts",
          "auto",
          null
      ),
      new DimensionsSpec(
          DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", DIM1, DIM2)),
          new ArrayList<>(),
          new ArrayList<>()
      ),
      null,
      Arrays.asList("ts", DIM1, DIM2, "val"),
      false,
      0
  );

  @Parameterized.Parameters(name = "{0}, useInputFormatApi={1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK, false},
        new Object[]{LockGranularity.TIME_CHUNK, true},
        new Object[]{LockGranularity.SEGMENT, true}
    );
  }

  private File inputDir;
  private SetMultimap<Interval, String> intervalToDim1;

  public RangePartitionMultiPhaseParallelIndexingTest(LockGranularity lockGranularity, boolean useInputFormatApi)
  {
    super(lockGranularity, useInputFormatApi);
  }

  @Override
  @Before
  public void setup() throws IOException
  {
    super.setup();
    inputDir = temporaryFolder.newFolder("data");
    intervalToDim1 = createInputFiles(inputDir);
  }

  private static SetMultimap<Interval, String> createInputFiles(File inputDir) throws IOException
  {
    SetMultimap<Interval, String> intervalToDim1 = HashMultimap.create();

    for (int fileIndex = 0; fileIndex < NUM_FILE; fileIndex++) {
      Path path = new File(inputDir, TEST_FILE_NAME_PREFIX + fileIndex).toPath();
      try (final Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
        for (int i = 0; i < (NUM_ROW / NUM_DAY); i++) {
          for (int d = 0; d < NUM_DAY; d++) {
            writeRow(writer, i + d, fileIndex + d, intervalToDim1);
          }
        }
      }
    }

    return intervalToDim1;
  }

  private static void writeRow(Writer writer, int day, int fileIndex, Multimap<Interval, String> intervalToDim1)
      throws IOException
  {
    Interval interval = Intervals.of("%s-12-%d/%s-12-%d", YEAR, day + 1, YEAR, day + 2);
    String startDate = interval.getStart().toString("y-M-d");
    String dim1Value = String.valueOf(fileIndex + 10);
    writer.write(StringUtils.format("%s,%s,%d th test file\n", startDate, dim1Value, fileIndex));
    intervalToDim1.put(interval, dim1Value);
  }

  @Test
  public void createsCorrectRangePartitions() throws Exception
  {
    int targetRowsPerSegment = NUM_ROW / NUM_DAY / NUM_PARTITION;
    final Set<DataSegment> publishedSegments = runTestTask(
        PARSE_SPEC,
        Intervals.of("%s/%s", YEAR, YEAR + 1),
        inputDir,
        TEST_FILE_NAME_PREFIX + "*",
        new SingleDimensionPartitionsSpec(
            targetRowsPerSegment,
            null,
            DIM1,
            false
        )
    );
    assertRangePartitions(publishedSegments);
  }

  private void assertRangePartitions(Set<DataSegment> publishedSegments) throws IOException
  {
    Multimap<Interval, DataSegment> intervalToSegments = ArrayListMultimap.create();
    publishedSegments.forEach(s -> intervalToSegments.put(s.getInterval(), s));

    SortedSet<Interval> publishedIntervals = new TreeSet<>(Comparators.intervalsByStartThenEnd());
    publishedIntervals.addAll(intervalToSegments.keySet());
    assertHasExpectedIntervals(publishedIntervals);

    Interval firstInterval = publishedIntervals.first();
    Interval lastInterval = publishedIntervals.last();
    File tempSegmentDir = temporaryFolder.newFolder();

    intervalToSegments.asMap().forEach((interval, segments) -> {
      assertNumPartition(interval, segments, firstInterval, lastInterval);

      List<String> allValues = new ArrayList<>(NUM_ROW);
      for (DataSegment segment : segments) {
        List<String> values = getColumnValues(segment, tempSegmentDir);
        assertValuesInRange(values, segment);
        allValues.addAll(values);
      }

      assertIntervalHasAllExpectedValues(interval, allValues);
    });
  }

  private void assertHasExpectedIntervals(Set<Interval> publishedSegmentIntervals)
  {
    Assert.assertEquals(intervalToDim1.keySet(), publishedSegmentIntervals);
  }

  private static void assertNumPartition(
      Interval interval,
      Collection<DataSegment> segments,
      Interval firstInterval,
      Interval lastInterval
  )
  {
    int expectedNumPartition = NUM_PARTITION;
    if (interval.equals(firstInterval) || interval.equals(lastInterval)) {
      expectedNumPartition -= 1;
    }
    expectedNumPartition *= NUM_DAY;
    Assert.assertEquals(expectedNumPartition, segments.size());
  }

  private List<String> getColumnValues(DataSegment segment, File tempDir)
  {
    List<ScanResultValue> results = querySegment(segment, DIMS, tempDir);
    Assert.assertEquals(1, results.size());
    List<LinkedHashMap<String, String>> rows = (List<LinkedHashMap<String, String>>) results.get(0).getEvents();
    return rows.stream()
               .map(row -> row.get(DIM1))
               .collect(Collectors.toList());
  }

  private static void assertValuesInRange(List<String> values, DataSegment segment)
  {
    SingleDimensionShardSpec shardSpec = (SingleDimensionShardSpec) segment.getShardSpec();
    String start = shardSpec.getStart();
    String end = shardSpec.getEnd();
    Assert.assertTrue(shardSpec.toString(), start != null || end != null);

    for (String value : values) {
      if (start != null) {
        Assert.assertThat(value.compareTo(start), Matchers.greaterThanOrEqualTo(0));
      }

      if (end != null) {
        Assert.assertThat(value.compareTo(end), Matchers.lessThan(0));
      }
    }
  }

  private void assertIntervalHasAllExpectedValues(Interval interval, List<String> actualValues)
  {
    List<String> expectedValues = new ArrayList<>(intervalToDim1.get(interval));
    Assert.assertEquals(expectedValues.size(), actualValues.size());
    Collections.sort(expectedValues);
    Collections.sort(actualValues);
    Assert.assertEquals(expectedValues, actualValues);
  }

  @Override
  ParallelIndexSupervisorTask createParallelIndexSupervisorTask(
      String id,
      TaskResource taskResource,
      ParallelIndexIngestionSpec ingestionSchema,
      Map<String, Object> context,
      IndexingServiceClient indexingServiceClient
  )
  {
    return new TestSupervisorTask(id, taskResource, ingestionSchema, context, indexingServiceClient);
  }

  private static class TestSupervisorTask extends TestParallelIndexSupervisorTask
  {
    TestSupervisorTask(
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
    PartialDimensionDistributionParallelIndexTaskRunner createPartialDimensionDistributionRunner(TaskToolbox toolbox)
    {
      return new TestPartialDimensionDistributionRunner(toolbox, this, getIndexingServiceClient());
    }

    @Override
    PartialRangeSegmentGenerateParallelIndexTaskRunner createPartialRangeSegmentGenerateRunner(
        TaskToolbox toolbox,
        Map<Interval, PartitionBoundaries> intervalToPartitions
    )
    {
      return new TestPartialRangeSegmentGenerateRunner(
          toolbox,
          this,
          getIndexingServiceClient(),
          intervalToPartitions
      );
    }

    @Override
    public PartialGenericSegmentMergeParallelIndexTaskRunner createPartialGenericSegmentMergeRunner(
        TaskToolbox toolbox,
        List<PartialGenericSegmentMergeIOConfig> ioConfigs
    )
    {
      return new TestPartialGenericSegmentMergeParallelIndexTaskRunner(
          toolbox,
          this,
          ioConfigs,
          getIndexingServiceClient()
      );
    }
  }

  private static class TestPartialDimensionDistributionRunner
      extends PartialDimensionDistributionParallelIndexTaskRunner
  {
    private TestPartialDimensionDistributionRunner(
        TaskToolbox toolbox,
        ParallelIndexSupervisorTask supervisorTask,
        IndexingServiceClient indexingServiceClient
    )
    {
      super(
          toolbox,
          supervisorTask.getId(),
          supervisorTask.getGroupId(),
          supervisorTask.getIngestionSchema(),
          supervisorTask.getContext(),
          indexingServiceClient,
          new LocalParallelIndexTaskClientFactory(supervisorTask)
      );
    }
  }

  private static class TestPartialRangeSegmentGenerateRunner extends PartialRangeSegmentGenerateParallelIndexTaskRunner
  {
    private TestPartialRangeSegmentGenerateRunner(
        TaskToolbox toolbox,
        ParallelIndexSupervisorTask supervisorTask,
        IndexingServiceClient indexingServiceClient,
        Map<Interval, PartitionBoundaries> intervalToPartitions
    )
    {
      super(
          toolbox,
          supervisorTask.getId(),
          supervisorTask.getGroupId(),
          supervisorTask.getIngestionSchema(),
          supervisorTask.getContext(),
          indexingServiceClient,
          intervalToPartitions,
          new LocalParallelIndexTaskClientFactory(supervisorTask),
          new TestAppenderatorsManager()
      );
    }
  }


  private static class TestPartialGenericSegmentMergeParallelIndexTaskRunner
      extends PartialGenericSegmentMergeParallelIndexTaskRunner
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    private TestPartialGenericSegmentMergeParallelIndexTaskRunner(
        TaskToolbox toolbox,
        ParallelIndexSupervisorTask supervisorTask,
        List<PartialGenericSegmentMergeIOConfig> mergeIOConfigs,
        IndexingServiceClient indexingServiceClient
    )
    {
      super(
          toolbox,
          supervisorTask.getId(),
          supervisorTask.getGroupId(),
          supervisorTask.getIngestionSchema().getDataSchema(),
          mergeIOConfigs,
          supervisorTask.getIngestionSchema().getTuningConfig(),
          supervisorTask.getContext(),
          indexingServiceClient
      );
      this.supervisorTask = supervisorTask;
    }

    @Override
    SubTaskSpec<PartialGenericSegmentMergeTask> newTaskSpec(PartialGenericSegmentMergeIOConfig ioConfig)
    {
      final PartialGenericSegmentMergeIngestionSpec ingestionSpec =
          new PartialGenericSegmentMergeIngestionSpec(
              supervisorTask.getIngestionSchema().getDataSchema(),
              ioConfig,
              getTuningConfig()
          );
      return new SubTaskSpec<PartialGenericSegmentMergeTask>(
          getTaskId() + "_" + getAndIncrementNextSpecId(),
          getGroupId(),
          getTaskId(),
          getContext(),
          new InputSplit<>(ioConfig.getPartitionLocations())
      )
      {
        @Override
        public PartialGenericSegmentMergeTask newSubTask(int numAttempts)
        {
          return new TestPartialGenericSegmentMergeTask(
              null,
              getGroupId(),
              null,
              getSupervisorTaskId(),
              numAttempts,
              ingestionSpec,
              getContext(),
              getIndexingServiceClient(),
              new LocalParallelIndexTaskClientFactory(supervisorTask),
              getToolbox()
          );
        }
      };
    }
  }

  private static class TestPartialGenericSegmentMergeTask extends PartialGenericSegmentMergeTask
  {
    private final TaskToolbox toolbox;

    private TestPartialGenericSegmentMergeTask(
        @Nullable String id,
        String groupId,
        TaskResource taskResource,
        String supervisorTaskId,
        int numAttempts,
        PartialGenericSegmentMergeIngestionSpec ingestionSchema,
        Map<String, Object> context,
        IndexingServiceClient indexingServiceClient,
        IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory,
        TaskToolbox toolbox
    )
    {
      super(
          id,
          groupId,
          taskResource,
          supervisorTaskId,
          numAttempts,
          ingestionSchema,
          context,
          indexingServiceClient,
          taskClientFactory,
          null
      );
      this.toolbox = toolbox;
    }

    @Override
    File fetchSegmentFile(File partitionDir, GenericPartitionLocation location)
    {
      final File zippedFile = toolbox.getIntermediaryDataManager().findPartitionFile(
          getSupervisorTaskId(),
          location.getSubTaskId(),
          location.getInterval(),
          location.getPartitionId()
      );
      if (zippedFile == null) {
        throw new ISE("Can't find segment file for location[%s] at path[%s]", location);
      }
      return zippedFile;
    }
  }
}
