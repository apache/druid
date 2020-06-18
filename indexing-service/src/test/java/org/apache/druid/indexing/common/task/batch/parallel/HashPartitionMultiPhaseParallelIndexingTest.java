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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

  @Parameterized.Parameters(name = "{0}, useInputFormatApi={1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK, false, 2},
        new Object[]{LockGranularity.TIME_CHUNK, true, 2},
        new Object[]{LockGranularity.TIME_CHUNK, true, 1},
        new Object[]{LockGranularity.SEGMENT, true, 2}
    );
  }

  private final int maxNumConcurrentSubTasks;

  private File inputDir;

  public HashPartitionMultiPhaseParallelIndexingTest(
      LockGranularity lockGranularity,
      boolean useInputFormatApi,
      int maxNumConcurrentSubTasks
  )
  {
    super(lockGranularity, useInputFormatApi);
    this.maxNumConcurrentSubTasks = maxNumConcurrentSubTasks;
  }

  @Before
  public void setup() throws IOException
  {
    inputDir = temporaryFolder.newFolder("data");
    // set up data
    for (int i = 0; i < 10; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "test_" + i).toPath(), StandardCharsets.UTF_8)) {
        for (int j = 0; j < 10; j++) {
          writer.write(StringUtils.format("2017-12-%d,%d,%d th test file\n", j + 1, i + 10, i));
          writer.write(StringUtils.format("2017-12-%d,%d,%d th test file\n", j + 2, i + 11, i));
        }
      }
    }

    for (int i = 0; i < 5; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "filtered_" + i).toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("2017-12-%d,%d,%d th test file\n", i + 1, i + 10, i));
      }
    }
  }

  @Test
  public void testRun() throws Exception
  {
    final Set<DataSegment> publishedSegments = runTestTask(
        new HashedPartitionsSpec(null, 2, ImmutableList.of("dim1", "dim2")),
        TaskState.SUCCESS,
        false
    );
    assertHashedPartition(publishedSegments);
  }

  @Test
  public void testAppendLinearlyPartitionedSegmensToHashPartitionedDatasourceSuccessfullyAppend()
  {
    final Set<DataSegment> publishedSegments = new HashSet<>();
    publishedSegments.addAll(
        runTestTask(
            new HashedPartitionsSpec(null, 2, ImmutableList.of("dim1", "dim2")),
            TaskState.SUCCESS,
            false
        )
    );
    // Append
    publishedSegments.addAll(
        runTestTask(
            new DynamicPartitionsSpec(5, null),
            TaskState.SUCCESS,
            true
        )
    );
    // And append again
    publishedSegments.addAll(
        runTestTask(
            new DynamicPartitionsSpec(10, null),
            TaskState.SUCCESS,
            true
        )
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

  private Set<DataSegment> runTestTask(
      PartitionsSpec partitionsSpec,
      TaskState expectedTaskState,
      boolean appendToExisting
  )
  {
    if (isUseInputFormatApi()) {
      return runTestTask(
          TIMESTAMP_SPEC,
          DIMENSIONS_SPEC,
          INPUT_FORMAT,
          null,
          INTERVAL_TO_INDEX,
          inputDir,
          "test_*",
          partitionsSpec,
          maxNumConcurrentSubTasks,
          expectedTaskState,
          appendToExisting
      );
    } else {
      return runTestTask(
          null,
          null,
          null,
          PARSE_SPEC,
          INTERVAL_TO_INDEX,
          inputDir,
          "test_*",
          partitionsSpec,
          maxNumConcurrentSubTasks,
          expectedTaskState,
          appendToExisting
      );
    }
  }

  private void assertHashedPartition(Set<DataSegment> publishedSegments) throws IOException
  {
    final Map<Interval, List<DataSegment>> intervalToSegments = new HashMap<>();
    publishedSegments.forEach(
        segment -> intervalToSegments.computeIfAbsent(segment.getInterval(), k -> new ArrayList<>()).add(segment)
    );
    final File tempSegmentDir = temporaryFolder.newFolder();
    for (List<DataSegment> segmentsInInterval : intervalToSegments.values()) {
      Assert.assertEquals(2, segmentsInInterval.size());
      for (DataSegment segment : segmentsInInterval) {
        List<ScanResultValue> results = querySegment(segment, ImmutableList.of("dim1", "dim2"), tempSegmentDir);
        final int hash = HashBasedNumberedShardSpec.hash(getObjectMapper(), (List<Object>) results.get(0).getEvents());
        for (ScanResultValue value : results) {
          Assert.assertEquals(
              hash,
              HashBasedNumberedShardSpec.hash(getObjectMapper(), (List<Object>) value.getEvents())
          );
        }
      }
    }
  }
}
