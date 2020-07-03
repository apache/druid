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

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.CompactionTask.Builder;
import org.apache.druid.indexing.common.task.SpecificSegmentsSpec;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PartialCompactionTest extends AbstractMultiPhaseParallelIndexingTest
{
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec("ts", "auto", null);
  private static final DimensionsSpec DIMENSIONS_SPEC = new DimensionsSpec(
      DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim1", "dim2"))
  );
  private static final InputFormat INPUT_FORMAT = new CsvInputFormat(
      Arrays.asList("ts", "dim1", "dim2", "val"),
      null,
      false,
      false,
      0
  );
  private static final Interval INTERVAL_TO_INDEX = Intervals.of("2017-12/P1M");
  private static final RetryPolicyFactory RETRY_POLICY_FACTORY = new RetryPolicyFactory(new RetryPolicyConfig());

  private File inputDir;

  public PartialCompactionTest()
  {
    super(LockGranularity.SEGMENT, true);
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
  }

  @Test
  public void testPartialCompactHashAndDynamicPartitionedSegments()
  {
    final Map<Interval, List<DataSegment>> hashPartitionedSegments = SegmentUtils.groupSegmentsByInterval(
        runTestTask(
            new HashedPartitionsSpec(null, 3, null),
            TaskState.SUCCESS,
            false
        )
    );
    final Map<Interval, List<DataSegment>> linearlyPartitionedSegments = SegmentUtils.groupSegmentsByInterval(
        runTestTask(
            new DynamicPartitionsSpec(10, null),
            TaskState.SUCCESS,
            true
        )
    );
    // Pick half of each partition lists to compact together
    hashPartitionedSegments.values().forEach(
        segmentsInInterval -> segmentsInInterval.sort(
            Comparator.comparing(segment -> segment.getShardSpec().getPartitionNum())
        )
    );
    linearlyPartitionedSegments.values().forEach(
        segmentsInInterval -> segmentsInInterval.sort(
            Comparator.comparing(segment -> segment.getShardSpec().getPartitionNum())
        )
    );
    final List<DataSegment> segmentsToCompact = new ArrayList<>();
    for (List<DataSegment> segmentsInInterval : hashPartitionedSegments.values()) {
      segmentsToCompact.addAll(
          segmentsInInterval.subList(segmentsInInterval.size() / 2, segmentsInInterval.size())
      );
    }
    for (List<DataSegment> segmentsInInterval : linearlyPartitionedSegments.values()) {
      segmentsToCompact.addAll(
          segmentsInInterval.subList(0, segmentsInInterval.size() / 2)
      );
    }
    final CompactionTask compactionTask = newCompactionTaskBuilder()
        .inputSpec(SpecificSegmentsSpec.fromSegments(segmentsToCompact))
        .tuningConfig(newTuningConfig(new DynamicPartitionsSpec(20, null), 2, false))
        .build();
    final Map<Interval, List<DataSegment>> compactedSegments = SegmentUtils.groupSegmentsByInterval(
        runTask(compactionTask, TaskState.SUCCESS)
    );
    for (List<DataSegment> segmentsInInterval : compactedSegments.values()) {
      final int expectedAtomicUpdateGroupSize = segmentsInInterval.size();
      for (DataSegment segment : segmentsInInterval) {
        Assert.assertEquals(expectedAtomicUpdateGroupSize, segment.getShardSpec().getAtomicUpdateGroupSize());
      }
    }
  }

  @Test
  public void testPartialCompactRangeAndDynamicPartitionedSegments()
  {
    final Map<Interval, List<DataSegment>> rangePartitionedSegments = SegmentUtils.groupSegmentsByInterval(
        runTestTask(
            new SingleDimensionPartitionsSpec(10, null, "dim1", false),
            TaskState.SUCCESS,
            false
        )
    );
    final Map<Interval, List<DataSegment>> linearlyPartitionedSegments = SegmentUtils.groupSegmentsByInterval(
        runTestTask(
            new DynamicPartitionsSpec(10, null),
            TaskState.SUCCESS,
            true
        )
    );
    // Pick half of each partition lists to compact together
    rangePartitionedSegments.values().forEach(
        segmentsInInterval -> segmentsInInterval.sort(
            Comparator.comparing(segment -> segment.getShardSpec().getPartitionNum())
        )
    );
    linearlyPartitionedSegments.values().forEach(
        segmentsInInterval -> segmentsInInterval.sort(
            Comparator.comparing(segment -> segment.getShardSpec().getPartitionNum())
        )
    );
    final List<DataSegment> segmentsToCompact = new ArrayList<>();
    for (List<DataSegment> segmentsInInterval : rangePartitionedSegments.values()) {
      segmentsToCompact.addAll(
          segmentsInInterval.subList(segmentsInInterval.size() / 2, segmentsInInterval.size())
      );
    }
    for (List<DataSegment> segmentsInInterval : linearlyPartitionedSegments.values()) {
      segmentsToCompact.addAll(
          segmentsInInterval.subList(0, segmentsInInterval.size() / 2)
      );
    }
    final CompactionTask compactionTask = newCompactionTaskBuilder()
        .inputSpec(SpecificSegmentsSpec.fromSegments(segmentsToCompact))
        .tuningConfig(newTuningConfig(new DynamicPartitionsSpec(20, null), 2, false))
        .build();
    final Map<Interval, List<DataSegment>> compactedSegments = SegmentUtils.groupSegmentsByInterval(
        runTask(compactionTask, TaskState.SUCCESS)
    );
    for (List<DataSegment> segmentsInInterval : compactedSegments.values()) {
      final int expectedAtomicUpdateGroupSize = segmentsInInterval.size();
      for (DataSegment segment : segmentsInInterval) {
        Assert.assertEquals(expectedAtomicUpdateGroupSize, segment.getShardSpec().getAtomicUpdateGroupSize());
      }
    }
  }

  private Set<DataSegment> runTestTask(
      PartitionsSpec partitionsSpec,
      TaskState expectedTaskState,
      boolean appendToExisting
  )
  {
    return runTestTask(
        TIMESTAMP_SPEC,
        DIMENSIONS_SPEC,
        INPUT_FORMAT,
        null,
        INTERVAL_TO_INDEX,
        inputDir,
        "test_*",
        partitionsSpec,
        2,
        expectedTaskState,
        appendToExisting
    );
  }

  private Builder newCompactionTaskBuilder()
  {
    return new Builder(
        DATASOURCE,
        getObjectMapper(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new NoopChatHandlerProvider(),
        new DropwizardRowIngestionMetersFactory(),
        getIndexingServiceClient(),
        getCoordinatorClient(),
        getSegmentLoaderFactory(),
        RETRY_POLICY_FACTORY,
        new TestAppenderatorsManager()
    );
  }
}
