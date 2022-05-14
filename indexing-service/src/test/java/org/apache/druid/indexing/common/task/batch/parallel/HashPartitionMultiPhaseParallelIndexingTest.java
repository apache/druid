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
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.HashPartitionFunction;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class HashPartitionMultiPhaseParallelIndexingTest extends AbstractMultiPhaseParallelIndexingTest
{
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec("ts", "auto", null);
  private static final DimensionsSpec DIMENSIONS_SPEC = new DimensionsSpec(
      DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim1", "dim2"))
  );
  private static final ParseSpec PARSE_SPEC = new CSVParseSpec(
      TIMESTAMP_SPEC,
      DIMENSIONS_SPEC,
      null,
      Arrays.asList("ts", "dim1", "dim2", "val"),
      false,
      0
  );
  private static final InputFormat INPUT_FORMAT = new CsvInputFormat(
      Arrays.asList("ts", "dim1", "dim2", "val"),
      null,
      false,
      false,
      0
  );
  private static final Interval INTERVAL_TO_INDEX = Intervals.of("2017-12/P1M");
  private static final String INPUT_FILTER = "test_*";

  @Parameterized.Parameters(
      name = "lockGranularity={0}, useInputFormatApi={1}, maxNumConcurrentSubTasks={2}, intervalToIndex={3}, numShards={4}"
  )
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK, false, 2, INTERVAL_TO_INDEX, 2},
        new Object[]{LockGranularity.TIME_CHUNK, true, 2, INTERVAL_TO_INDEX, 2},
        new Object[]{LockGranularity.TIME_CHUNK, true, 2, null, 2},
        new Object[]{LockGranularity.TIME_CHUNK, true, 1, INTERVAL_TO_INDEX, 2},
        new Object[]{LockGranularity.SEGMENT, true, 2, INTERVAL_TO_INDEX, 2},
        new Object[]{LockGranularity.TIME_CHUNK, true, 2, INTERVAL_TO_INDEX, null},
        new Object[]{LockGranularity.TIME_CHUNK, true, 2, null, null},
        new Object[]{LockGranularity.TIME_CHUNK, true, 1, INTERVAL_TO_INDEX, null},
        new Object[]{LockGranularity.SEGMENT, true, 2, INTERVAL_TO_INDEX, null}
    );
  }

  private final int maxNumConcurrentSubTasks;
  @Nullable
  private final Interval intervalToIndex;
  @Nullable
  private final Integer numShards;

  private File inputDir;
  // sorted input intervals
  private List<Interval> inputIntervals;

  public HashPartitionMultiPhaseParallelIndexingTest(
      LockGranularity lockGranularity,
      boolean useInputFormatApi,
      int maxNumConcurrentSubTasks,
      @Nullable Interval intervalToIndex,
      @Nullable Integer numShards
  )
  {
    super(lockGranularity, useInputFormatApi, DEFAULT_TRANSIENT_TASK_FAILURE_RATE, DEFAULT_TRANSIENT_API_FAILURE_RATE);
    this.maxNumConcurrentSubTasks = maxNumConcurrentSubTasks;
    this.intervalToIndex = intervalToIndex;
    this.numShards = numShards;
  }

  @Before
  public void setup() throws IOException
  {
    inputDir = temporaryFolder.newFolder("data");
    final Set<Interval> intervals = new HashSet<>();
    // set up data
    for (int i = 0; i < 10; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "test_" + i).toPath(), StandardCharsets.UTF_8)) {
        for (int j = 0; j < 10; j++) {
          writer.write(StringUtils.format("2017-12-%d,%d,%d th test file\n", j + 1, i + 10, i));
          writer.write(StringUtils.format("2017-12-%d,%d,%d th test file\n", j + 2, i + 11, i));
          intervals.add(SEGMENT_GRANULARITY.bucket(DateTimes.of(StringUtils.format("2017-12-%d", j + 1))));
          intervals.add(SEGMENT_GRANULARITY.bucket(DateTimes.of(StringUtils.format("2017-12-%d", j + 2))));
        }
      }
    }

    for (int i = 0; i < 5; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "filtered_" + i).toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("2017-12-%d,%d,%d th test file\n", i + 1, i + 10, i));
      }
    }
    inputIntervals = new ArrayList<>(intervals);
    inputIntervals.sort(Comparators.intervalsByStartThenEnd());
  }


  // The next test also verifies replace functionality. Now, they are together to save on test execution time
  // due to Travis CI 10 minute default running time (with no output) -- having it separate made it
  // last longer. At some point we should really simplify this file, so it runs faster (splitting, etc.)
  @Test
  public void testRun() throws Exception
  {

    // verify dropExisting false:
    final Integer maxRowsPerSegment = numShards == null ? 10 : null;
    final Set<DataSegment> publishedSegments = runTask(createTask(
        new HashedPartitionsSpec(
            maxRowsPerSegment,
            numShards,
            ImmutableList.of("dim1", "dim2")
        ),
        inputDir,
        false,
        false
    ), TaskState.SUCCESS);

    final Map<Interval, Integer> expectedIntervalToNumSegments = computeExpectedIntervalToNumSegments(
        maxRowsPerSegment,
        numShards
    );
    assertHashedPartition(publishedSegments, expectedIntervalToNumSegments);

    // verify dropExisting true:
    if (intervalToIndex == null) {
      // replace only works when intervals are provided
      return;
    }
    final Set<DataSegment> publishedSegmentsAfterReplace = runTask(createTask(
        new HashedPartitionsSpec(
            maxRowsPerSegment,
            numShards,
            ImmutableList.of("dim1", "dim2")
        ),
        newInputDirForReplace(),
        false,
        true
    ), TaskState.SUCCESS);

    final Map<Interval, Integer> expectedIntervalToNumSegmentsAfterReplace = computeExpectedIntervalToNumSegments(
        maxRowsPerSegment,
        numShards
    );

    // Regardless of whether numShards is set or not the replace will put data in six intervals.
    // When numShards are set (2) it will generate 12 segments. When not, the hash ingestion code will estimate
    // one shard perinterval thus siz segments:

    // adjust expected wrt to tombstones:
    int tombstones = 0;
    for (DataSegment ds : publishedSegmentsAfterReplace) {
      if (ds.isTombstone()) {
        expectedIntervalToNumSegmentsAfterReplace.put(ds.getInterval(), 1);
        tombstones++;
      } else if (numShards == null) {
        expectedIntervalToNumSegmentsAfterReplace.put(ds.getInterval(), 1);
      }
    }
    Assert.assertEquals(5, tombstones); // five tombstones
    int expectedSegments = 12;
    if (numShards == null) {
      expectedSegments = 6;
    }
    Assert.assertEquals(expectedSegments, publishedSegmentsAfterReplace.size() - tombstones); //  six segments
    assertHashedPartition(publishedSegmentsAfterReplace, expectedIntervalToNumSegmentsAfterReplace);
  }

  @Test
  public void testRunWithHashPartitionFunction() throws Exception
  {
    final Integer maxRowsPerSegment = numShards == null ? 10 : null;
    final Set<DataSegment> publishedSegments = runTask(createTask(
        new HashedPartitionsSpec(
            maxRowsPerSegment,
            numShards,
            ImmutableList.of("dim1", "dim2"),
            HashPartitionFunction.MURMUR3_32_ABS
        ),
        inputDir, false, false
    ), TaskState.SUCCESS);
    final Map<Interval, Integer> expectedIntervalToNumSegments = computeExpectedIntervalToNumSegments(
        maxRowsPerSegment,
        numShards
    );
    assertHashedPartition(publishedSegments, expectedIntervalToNumSegments);
  }

  private Map<Interval, Integer> computeExpectedIntervalToNumSegments(
      @Nullable Integer maxRowsPerSegment,
      @Nullable Integer numShards
  )
  {
    final Map<Interval, Integer> expectedIntervalToNumSegments = new HashMap<>();
    for (int i = 0; i < inputIntervals.size(); i++) {
      if (numShards == null) {
        if (i == 0 || i == inputIntervals.size() - 1) {
          expectedIntervalToNumSegments.put(inputIntervals.get(i), Math.round((float) 10 / maxRowsPerSegment));
        } else {
          expectedIntervalToNumSegments.put(inputIntervals.get(i), Math.round((float) 20 / maxRowsPerSegment));
        }
      } else {
        expectedIntervalToNumSegments.put(inputIntervals.get(i), numShards);
      }
    }
    return expectedIntervalToNumSegments;
  }

  @Test
  public void testAppendLinearlyPartitionedSegmensToHashPartitionedDatasourceSuccessfullyAppend()
  {
    final Set<DataSegment> publishedSegments = new HashSet<>();
    publishedSegments.addAll(
        runTask(
            createTask(
                new HashedPartitionsSpec(null, numShards, ImmutableList.of("dim1", "dim2")),
                inputDir, false, false
            ),
            TaskState.SUCCESS)
    );
    // Append
    publishedSegments.addAll(
        runTask(
            createTask(
                new DynamicPartitionsSpec(5, null),
                inputDir, true, false
            ),
            TaskState.SUCCESS));
    // And append again
    publishedSegments.addAll(
        runTask(
            createTask(
                new DynamicPartitionsSpec(10, null),
                inputDir, true, false
            ),
            TaskState.SUCCESS)
    );

    final Map<Interval, List<DataSegment>> intervalToSegments = new HashMap<>();
    publishedSegments.forEach(
        segment -> intervalToSegments.computeIfAbsent(segment.getInterval(), k -> new ArrayList<>()).add(segment)
    );
    for (Entry<Interval, List<DataSegment>> entry : intervalToSegments.entrySet()) {
      final List<DataSegment> segments = entry.getValue();
      final List<DataSegment> hashedSegments = segments
          .stream()
          .filter(segment -> segment.getShardSpec().getClass() == HashBasedNumberedShardSpec.class)
          .collect(Collectors.toList());
      final List<DataSegment> linearSegments = segments
          .stream()
          .filter(segment -> segment.getShardSpec().getClass() == NumberedShardSpec.class)
          .collect(Collectors.toList());

      for (DataSegment hashedSegment : hashedSegments) {
        final HashBasedNumberedShardSpec hashShardSpec = (HashBasedNumberedShardSpec) hashedSegment.getShardSpec();
        for (DataSegment linearSegment : linearSegments) {
          Assert.assertEquals(hashedSegment.getInterval(), linearSegment.getInterval());
          Assert.assertEquals(hashedSegment.getVersion(), linearSegment.getVersion());
          final NumberedShardSpec numberedShardSpec = (NumberedShardSpec) linearSegment.getShardSpec();
          Assert.assertEquals(hashShardSpec.getNumCorePartitions(), numberedShardSpec.getNumCorePartitions());
          Assert.assertTrue(hashShardSpec.getPartitionNum() < numberedShardSpec.getPartitionNum());
        }
      }
    }
  }

  private ParallelIndexSupervisorTask createTask(
      PartitionsSpec partitionsSpec,
      File inputDirectory,
      boolean appendToExisting,
      boolean dropExisting
  )
  {
    if (isUseInputFormatApi()) {
      return createTask(
          TIMESTAMP_SPEC,
          DIMENSIONS_SPEC,
          INPUT_FORMAT,
          null,
          intervalToIndex,
          inputDirectory,
          INPUT_FILTER,
          partitionsSpec,
          maxNumConcurrentSubTasks,
          appendToExisting,
          dropExisting
      );
    } else {
      return createTask(
          null,
          null,
          null,
          PARSE_SPEC,
          intervalToIndex,
          inputDirectory,
          INPUT_FILTER,
          partitionsSpec,
          maxNumConcurrentSubTasks,
          appendToExisting,
          dropExisting
      );
    }
  }

  private void assertHashedPartition(
      Set<DataSegment> publishedSegments,
      Map<Interval, Integer> expectedIntervalToNumSegments
  ) throws IOException
  {
    final Map<Interval, List<DataSegment>> intervalToSegments = new HashMap<>();
    publishedSegments.forEach(
        segment -> intervalToSegments.computeIfAbsent(segment.getInterval(), k -> new ArrayList<>()).add(segment)
    );
    Assert.assertEquals(new HashSet<>(inputIntervals), intervalToSegments.keySet());
    final File tempSegmentDir = temporaryFolder.newFolder();
    for (Entry<Interval, List<DataSegment>> entry : intervalToSegments.entrySet()) {
      Interval interval = entry.getKey();
      List<DataSegment> segmentsInInterval = entry.getValue();
      Assert.assertEquals(expectedIntervalToNumSegments.get(interval).intValue(), segmentsInInterval.size());
      for (DataSegment segment : segmentsInInterval) {
        HashBasedNumberedShardSpec shardSpec = null;
        if (segment.isTombstone()) {
          Assert.assertSame(TombstoneShardSpec.class, segment.getShardSpec().getClass());
        } else {
          Assert.assertSame(HashBasedNumberedShardSpec.class, segment.getShardSpec().getClass());
          shardSpec = (HashBasedNumberedShardSpec) segment.getShardSpec();
          Assert.assertEquals(HashPartitionFunction.MURMUR3_32_ABS, shardSpec.getPartitionFunction());
        }
        List<ScanResultValue> results = querySegment(segment, ImmutableList.of("dim1", "dim2"), tempSegmentDir);
        if (segment.isTombstone()) {
          Assert.assertTrue(results.isEmpty());
        } else {
          final int hash = shardSpec.getPartitionFunction().hash(
              HashBasedNumberedShardSpec.serializeGroupKey(
                  getObjectMapper(),
                  (List<Object>) results.get(0).getEvents()
              ),
              shardSpec.getNumBuckets()
          );
          for (ScanResultValue value : results) {
            Assert.assertEquals(
                hash,
                shardSpec.getPartitionFunction().hash(
                    HashBasedNumberedShardSpec.serializeGroupKey(
                        getObjectMapper(),
                        (List<Object>) value.getEvents()
                    ),
                    shardSpec.getNumBuckets()
                )
            );
          }
        }
      }
    }
  }

  private File newInputDirForReplace() throws IOException
  {
    File inputDirectory = temporaryFolder.newFolder("dataReplace");
    // set up data
    Set<Integer> fileIds = new HashSet<>();
    fileIds.add(3);
    fileIds.add(7);
    fileIds.add(9);
    for (Integer i : fileIds) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDirectory, "test_" + i).toPath(), StandardCharsets.UTF_8)) {
        for (int j = 0; j < 10; j++) {
          writer.write(StringUtils.format("2017-12-%d,%d,%d th test file\n", i + 1, j + 10, i));
          writer.write(StringUtils.format("2017-12-%d,%d,%d th test file\n", i + 2, j + 11, i));
        }
      }
    }

    return inputDirectory;
  }

}
