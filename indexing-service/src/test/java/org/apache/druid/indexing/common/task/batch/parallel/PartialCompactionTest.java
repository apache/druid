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

import com.google.common.collect.ImmutableMap;
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
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.CompactionTask.Builder;
import org.apache.druid.indexing.common.task.MinorCompactionInputSpec;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.DataSegmentsWithSchemas;
import org.apache.druid.segment.SegmentUtils;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
      0,
      null
  );
  private static final Interval INTERVAL_TO_INDEX = Intervals.of("2017-12/P1M");

  private File inputDir;

  public PartialCompactionTest()
  {
    super(LockGranularity.TIME_CHUNK, true, DEFAULT_TRANSIENT_TASK_FAILURE_RATE, DEFAULT_TRANSIENT_API_FAILURE_RATE);
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
    DataSegmentsWithSchemas dataSegmentsWithSchemas =
        runTestTask(
            new HashedPartitionsSpec(null, 3, null),
            TaskState.SUCCESS,
            false
        );
    verifySchema(dataSegmentsWithSchemas);
    final Map<Interval, List<DataSegment>> hashPartitionedSegments =
        SegmentUtils.groupSegmentsByInterval(dataSegmentsWithSchemas.getSegments());

    dataSegmentsWithSchemas =
        runTestTask(
            new DynamicPartitionsSpec(10, null),
            TaskState.SUCCESS,
            true
        );
    verifySchema(dataSegmentsWithSchemas);
    final Map<Interval, List<DataSegment>> linearlyPartitionedSegments =
        SegmentUtils.groupSegmentsByInterval(dataSegmentsWithSchemas.getSegments());
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
        .inputSpec(
            new MinorCompactionInputSpec(
                INTERVAL_TO_INDEX,
                segmentsToCompact.stream().map(DataSegment::toDescriptor).collect(Collectors.toList())
            ), true
        )
        .tuningConfig(newTuningConfig(new DynamicPartitionsSpec(20, null), 2, false))
        .context(ImmutableMap.of(Tasks.USE_CONCURRENT_LOCKS, true))
        .build();
    dataSegmentsWithSchemas = runTask(compactionTask, TaskState.SUCCESS);
    verifySchema(dataSegmentsWithSchemas);
    final Map<Interval, List<DataSegment>> compactedSegments = SegmentUtils.groupSegmentsByInterval(
        dataSegmentsWithSchemas.getSegments()
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
    DataSegmentsWithSchemas dataSegmentsWithSchemas =
        runTestTask(
            new SingleDimensionPartitionsSpec(10, null, "dim1", false),
            TaskState.SUCCESS,
            false
        );
    final Map<Interval, List<DataSegment>> rangePartitionedSegments =
        SegmentUtils.groupSegmentsByInterval(dataSegmentsWithSchemas.getSegments());

    dataSegmentsWithSchemas =
        runTestTask(
            new DynamicPartitionsSpec(10, null),
            TaskState.SUCCESS,
            true
        );
    final Map<Interval, List<DataSegment>> linearlyPartitionedSegments =
        SegmentUtils.groupSegmentsByInterval(dataSegmentsWithSchemas.getSegments());

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
        .inputSpec(
            new MinorCompactionInputSpec(
                INTERVAL_TO_INDEX,
                segmentsToCompact.stream().map(DataSegment::toDescriptor).collect(Collectors.toList())
            ), true
        )
        .tuningConfig(newTuningConfig(new DynamicPartitionsSpec(20, null), 2, false))
        .context(ImmutableMap.of(Tasks.USE_CONCURRENT_LOCKS, true))
        .build();

    dataSegmentsWithSchemas = runTask(compactionTask, TaskState.SUCCESS);
    final Map<Interval, List<DataSegment>> compactedSegments = SegmentUtils.groupSegmentsByInterval(
        dataSegmentsWithSchemas.getSegments()
    );
    for (List<DataSegment> segmentsInInterval : compactedSegments.values()) {
      final int expectedAtomicUpdateGroupSize = segmentsInInterval.size();
      for (DataSegment segment : segmentsInInterval) {
        Assert.assertEquals(expectedAtomicUpdateGroupSize, segment.getShardSpec().getAtomicUpdateGroupSize());
      }
    }
  }

  @Test
  public void testMinorCompactionUpgradesNonCompactedSegments()
  {
    DataSegmentsWithSchemas dataSegmentsWithSchemas = runTestTask(
        new HashedPartitionsSpec(null, 4, null),
        TaskState.SUCCESS,
        false
    );
    verifySchema(dataSegmentsWithSchemas);
    final Map<Interval, List<DataSegment>> hashPartitionedSegments =
        SegmentUtils.groupSegmentsByInterval(dataSegmentsWithSchemas.getSegments());
    hashPartitionedSegments.values().forEach(
        segmentsInInterval -> segmentsInInterval.sort(
            Comparator.comparing(segment -> segment.getShardSpec().getPartitionNum())
        )
    );

    final List<DataSegment> segmentsToCompact = new ArrayList<>();
    for (List<DataSegment> segmentsInInterval : hashPartitionedSegments.values()) {
      segmentsToCompact.addAll(segmentsInInterval.subList(0, Math.min(2, segmentsInInterval.size())));
    }
    final Set<DataSegment> originalSegments = dataSegmentsWithSchemas.getSegments();
    final Set<String> compactedSegmentIds = segmentsToCompact.stream()
                                                             .map(segment -> segment.getId().toString())
                                                             .collect(Collectors.toSet());
    final Set<String> nonCompactedSegmentIds =
        originalSegments.stream()
                        .map(segment -> segment.getId().toString())
                        .filter(segmentId -> !compactedSegmentIds.contains(segmentId))
                        .collect(Collectors.toSet());
    Assert.assertFalse(nonCompactedSegmentIds.isEmpty());
    final Set<String> originalSegmentIds = new HashSet<>(compactedSegmentIds);
    originalSegmentIds.addAll(nonCompactedSegmentIds);

    final CompactionTask compactionTask = newCompactionTaskBuilder()
        .inputSpec(
            new MinorCompactionInputSpec(
                INTERVAL_TO_INDEX,
                segmentsToCompact.stream().map(DataSegment::toDescriptor).collect(Collectors.toList())
            ), true
        )
        .tuningConfig(newTuningConfig(new DynamicPartitionsSpec(20, null), 2, false))
        .context(ImmutableMap.of(Tasks.USE_CONCURRENT_LOCKS, true))
        .build();
    dataSegmentsWithSchemas = runTask(compactionTask, TaskState.SUCCESS);
    verifySchema(dataSegmentsWithSchemas);

    // Check published segment set after compaction
    final Set<DataSegment> publishedAfterCompaction = dataSegmentsWithSchemas.getSegments();
    Assert.assertFalse(SegmentUtils.groupSegmentsByInterval(publishedAfterCompaction).isEmpty());

    final Set<String> finalSegmentIds = publishedAfterCompaction.stream()
                                                                .map(segment -> segment.getId().toString())
                                                                .collect(Collectors.toSet());

    final Map<String, String> upgradedFromSegmentIdMap =
        getStorageCoordinator().retrieveUpgradedFromSegmentIds(DATASOURCE, finalSegmentIds);
    Assert.assertFalse(upgradedFromSegmentIdMap.isEmpty());
    Assert.assertTrue(upgradedFromSegmentIdMap.values().stream().noneMatch(compactedSegmentIds::contains));
    Assert.assertTrue(originalSegmentIds.containsAll(upgradedFromSegmentIdMap.values()));
    for (final String successorSegmentId : upgradedFromSegmentIdMap.keySet()) {
      Assert.assertTrue(finalSegmentIds.contains(successorSegmentId));
    }

    // Validate new segment ids (replacements and/or upgraded replicas)
    final Set<String> newPublishedSegmentIds = new HashSet<>(finalSegmentIds);
    newPublishedSegmentIds.removeAll(originalSegmentIds);
    Assert.assertFalse(newPublishedSegmentIds.isEmpty());
    Assert.assertTrue(
        newPublishedSegmentIds.stream().anyMatch(id -> !upgradedFromSegmentIdMap.containsKey(id))
    );

    // Index newly published ids by day for compacted-source checks below.
    final Map<Interval, Set<String>> newSegmentIdsByInterval =
        publishedAfterCompaction.stream()
                                .filter(segment -> !segment.isTombstone()
                                               && newPublishedSegmentIds.contains(segment.getId().toString())
                                )
                                .collect(Collectors.groupingBy(
                                    DataSegment::getInterval,
                                    Collectors.mapping(
                                        segment -> segment.getId().toString(),
                                        Collectors.toCollection(HashSet::new)
                                    )
                                ));

    // Verify non-compacted segments are being replaced
    for (final String parentSegmentId : nonCompactedSegmentIds) {
      final List<String> successorSegmentIds = upgradedFromSegmentIdMap.entrySet()
                                                                       .stream()
                                                                       .filter(e -> parentSegmentId.equals(e.getValue()))
                                                                       .map(Map.Entry::getKey)
                                                                       .toList();
      if (finalSegmentIds.contains(parentSegmentId)) {
        Assert.assertTrue(successorSegmentIds.isEmpty());
      } else if (!successorSegmentIds.isEmpty()) {
        Assert.assertEquals(1, successorSegmentIds.size());
        Assert.assertTrue(finalSegmentIds.contains(successorSegmentIds.get(0)));
      }
    }

    // Verify compacted segments have new published ID
    for (final DataSegment compactedSource : segmentsToCompact) {
      final String compactedSourceId = compactedSource.getId().toString();
      Assert.assertFalse(finalSegmentIds.contains(compactedSourceId));
      final Set<String> newIdsInSameInterval = newSegmentIdsByInterval.getOrDefault(compactedSource.getInterval(), Set.of());
      Assert.assertFalse(newIdsInSameInterval.isEmpty());
    }

    // non-compacted parents removed from published set match retrieveUpgradedToSegmentIds
    final Set<String> removedNonCompactedParentIds =
        nonCompactedSegmentIds.stream().filter(id -> !finalSegmentIds.contains(id)).collect(Collectors.toSet());
    if (!removedNonCompactedParentIds.isEmpty()) {
      final Map<String, Set<String>> upgradedToSegmentIdsByParent =
          getStorageCoordinator().retrieveUpgradedToSegmentIds(DATASOURCE, removedNonCompactedParentIds);
      for (final String parentSegmentId : removedNonCompactedParentIds) {
        final Set<String> expectedSuccessorIds = upgradedFromSegmentIdMap.entrySet()
                                                                         .stream()
                                                                         .filter(e -> parentSegmentId.equals(e.getValue()))
                                                                         .map(Map.Entry::getKey)
                                                                         .collect(Collectors.toSet());
        if (expectedSuccessorIds.isEmpty()) {
          continue;
        }
        final Set<String> coordinatorSuccessorIds =
            new HashSet<>(upgradedToSegmentIdsByParent.getOrDefault(parentSegmentId, Set.of()));
        coordinatorSuccessorIds.remove(parentSegmentId);
        Assert.assertTrue(coordinatorSuccessorIds.containsAll(expectedSuccessorIds));
      }
    }
  }

  private DataSegmentsWithSchemas runTestTask(
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
        appendToExisting,
        false
    );
  }

  private Builder newCompactionTaskBuilder()
  {
    return new Builder(
        DATASOURCE,
        getSegmentCacheManagerFactory()
    );
  }
}
