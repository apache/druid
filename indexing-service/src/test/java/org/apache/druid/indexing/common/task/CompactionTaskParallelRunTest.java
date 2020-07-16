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

package org.apache.druid.indexing.common.task;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SegmentsSplitHintSpec;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.CompactionTask.Builder;
import org.apache.druid.indexing.common.task.batch.parallel.AbstractParallelIndexSupervisorTaskTest;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.firehose.WindowedSegmentId;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedOverwriteShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionIds;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@RunWith(Parameterized.class)
public class CompactionTaskParallelRunTest extends AbstractParallelIndexSupervisorTaskTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK},
        new Object[]{LockGranularity.SEGMENT}
    );
  }

  private static final String DATA_SOURCE = "test";
  private static final RetryPolicyFactory RETRY_POLICY_FACTORY = new RetryPolicyFactory(new RetryPolicyConfig());
  private static final Interval INTERVAL_TO_INDEX = Intervals.of("2014-01-01/2014-01-02");

  private final AppenderatorsManager appenderatorsManager = new TestAppenderatorsManager();
  private final LockGranularity lockGranularity;
  private final RowIngestionMetersFactory rowIngestionMetersFactory;

  private File inputDir;

  public CompactionTaskParallelRunTest(LockGranularity lockGranularity)
  {
    this.lockGranularity = lockGranularity;
    this.rowIngestionMetersFactory = new TestUtils().getRowIngestionMetersFactory();
  }

  @Before
  public void setup() throws IOException
  {
    getObjectMapper().registerSubtypes(ParallelIndexTuningConfig.class, DruidInputSource.class);

    inputDir = temporaryFolder.newFolder();
    final File tmpFile = File.createTempFile("druid", "index", inputDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("2014-01-01T00:00:10Z,a,1\n");
      writer.write("2014-01-01T00:00:10Z,b,2\n");
      writer.write("2014-01-01T00:00:10Z,c,3\n");
      writer.write("2014-01-01T01:00:20Z,a,1\n");
      writer.write("2014-01-01T01:00:20Z,b,2\n");
      writer.write("2014-01-01T01:00:20Z,c,3\n");
      writer.write("2014-01-01T02:00:30Z,a,1\n");
      writer.write("2014-01-01T02:00:30Z,b,2\n");
      writer.write("2014-01-01T02:00:30Z,c,3\n");
    }
  }

  @Test
  public void testRunParallel()
  {
    runIndexTask(null, true);

    final Builder builder = new Builder(
        DATA_SOURCE,
        getObjectMapper(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new NoopChatHandlerProvider(),
        rowIngestionMetersFactory,
        null,
        null,
        getSegmentLoaderFactory(),
        RETRY_POLICY_FACTORY,
        appenderatorsManager
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING)
        .build();

    runTask(compactionTask);
  }

  @Test
  public void testCompactHashAndDynamicPartitionedSegments()
  {
    runIndexTask(new HashedPartitionsSpec(null, 2, null), false);
    runIndexTask(null, true);
    final Builder builder = new Builder(
        DATA_SOURCE,
        getObjectMapper(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new NoopChatHandlerProvider(),
        rowIngestionMetersFactory,
        null,
        null,
        getSegmentLoaderFactory(),
        RETRY_POLICY_FACTORY,
        appenderatorsManager
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING)
        .build();

    final Map<Interval, List<DataSegment>> intervalToSegments = SegmentUtils.groupSegmentsByInterval(
        runTask(compactionTask)
    );
    Assert.assertEquals(3, intervalToSegments.size());
    Assert.assertEquals(
        ImmutableSet.of(
            Intervals.of("2014-01-01T00/PT1H"),
            Intervals.of("2014-01-01T01/PT1H"),
            Intervals.of("2014-01-01T02/PT1H")
        ),
        intervalToSegments.keySet()
    );
    for (Entry<Interval, List<DataSegment>> entry : intervalToSegments.entrySet()) {
      final List<DataSegment> segmentsInInterval = entry.getValue();
      Assert.assertEquals(1, segmentsInInterval.size());
      final ShardSpec shardSpec = segmentsInInterval.get(0).getShardSpec();
      if (lockGranularity == LockGranularity.TIME_CHUNK) {
        Assert.assertSame(NumberedShardSpec.class, shardSpec.getClass());
        final NumberedShardSpec numberedShardSpec = (NumberedShardSpec) shardSpec;
        Assert.assertEquals(0, numberedShardSpec.getPartitionNum());
        Assert.assertEquals(1, numberedShardSpec.getNumCorePartitions());
      } else {
        Assert.assertSame(NumberedOverwriteShardSpec.class, shardSpec.getClass());
        final NumberedOverwriteShardSpec numberedShardSpec = (NumberedOverwriteShardSpec) shardSpec;
        Assert.assertEquals(PartitionIds.NON_ROOT_GEN_START_PARTITION_ID, numberedShardSpec.getPartitionNum());
        Assert.assertEquals(1, numberedShardSpec.getAtomicUpdateGroupSize());
      }
    }
  }

  @Test
  public void testCompactRangeAndDynamicPartitionedSegments()
  {
    runIndexTask(new SingleDimensionPartitionsSpec(2, null, "dim", false), false);
    runIndexTask(null, true);
    final Builder builder = new Builder(
        DATA_SOURCE,
        getObjectMapper(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new NoopChatHandlerProvider(),
        rowIngestionMetersFactory,
        null,
        null,
        getSegmentLoaderFactory(),
        RETRY_POLICY_FACTORY,
        appenderatorsManager
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING)
        .build();

    final Map<Interval, List<DataSegment>> intervalToSegments = SegmentUtils.groupSegmentsByInterval(
        runTask(compactionTask)
    );
    Assert.assertEquals(3, intervalToSegments.size());
    Assert.assertEquals(
        ImmutableSet.of(
            Intervals.of("2014-01-01T00/PT1H"),
            Intervals.of("2014-01-01T01/PT1H"),
            Intervals.of("2014-01-01T02/PT1H")
        ),
        intervalToSegments.keySet()
    );
    for (Entry<Interval, List<DataSegment>> entry : intervalToSegments.entrySet()) {
      final List<DataSegment> segmentsInInterval = entry.getValue();
      Assert.assertEquals(1, segmentsInInterval.size());
      final ShardSpec shardSpec = segmentsInInterval.get(0).getShardSpec();
      if (lockGranularity == LockGranularity.TIME_CHUNK) {
        Assert.assertSame(NumberedShardSpec.class, shardSpec.getClass());
        final NumberedShardSpec numberedShardSpec = (NumberedShardSpec) shardSpec;
        Assert.assertEquals(0, numberedShardSpec.getPartitionNum());
        Assert.assertEquals(1, numberedShardSpec.getNumCorePartitions());
      } else {
        Assert.assertSame(NumberedOverwriteShardSpec.class, shardSpec.getClass());
        final NumberedOverwriteShardSpec numberedShardSpec = (NumberedOverwriteShardSpec) shardSpec;
        Assert.assertEquals(PartitionIds.NON_ROOT_GEN_START_PARTITION_ID, numberedShardSpec.getPartitionNum());
        Assert.assertEquals(1, numberedShardSpec.getAtomicUpdateGroupSize());
      }
    }
  }

  @Test
  public void testDruidInputSourceCreateSplitsWithIndividualSplits()
  {
    runIndexTask(null, true);

    List<InputSplit<List<WindowedSegmentId>>> splits = Lists.newArrayList(
        DruidInputSource.createSplits(
            getCoordinatorClient(),
            RETRY_POLICY_FACTORY,
            DATA_SOURCE,
            INTERVAL_TO_INDEX,
            new SegmentsSplitHintSpec(1L) // each segment gets its own split with this config
        )
    );

    List<DataSegment> segments = new ArrayList<>(
        getCoordinatorClient().fetchUsedSegmentsInDataSourceForIntervals(
            DATA_SOURCE,
            ImmutableList.of(INTERVAL_TO_INDEX)
        )
    );

    Set<String> segmentIdsFromSplits = new HashSet<>();
    Set<String> segmentIdsFromCoordinator = new HashSet<>();
    Assert.assertEquals(segments.size(), splits.size());
    for (int i = 0; i < segments.size(); i++) {
      segmentIdsFromCoordinator.add(segments.get(i).getId().toString());
      segmentIdsFromSplits.add(splits.get(i).get().get(0).getSegmentId());
    }
    Assert.assertEquals(segmentIdsFromCoordinator, segmentIdsFromSplits);
  }

  private void runIndexTask(@Nullable PartitionsSpec partitionsSpec, boolean appendToExisting)
  {
    ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
        null,
        new LocalInputSource(inputDir, "druid*"),
        new CsvInputFormat(
            Arrays.asList("ts", "dim", "val"),
            "|",
            null,
            false,
            0
        ),
        appendToExisting
    );
    ParallelIndexTuningConfig tuningConfig = newTuningConfig(partitionsSpec, 2, !appendToExisting);
    ParallelIndexSupervisorTask indexTask = new ParallelIndexSupervisorTask(
        null,
        null,
        null,
        new ParallelIndexIngestionSpec(
            new DataSchema(
                DATA_SOURCE,
                new TimestampSpec("ts", "auto", null),
                new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim"))),
                new AggregatorFactory[]{new LongSumAggregatorFactory("val", "val")},
                new UniformGranularitySpec(
                    Granularities.HOUR,
                    Granularities.MINUTE,
                    ImmutableList.of(INTERVAL_TO_INDEX)
                ),
                null
            ),
            ioConfig,
            tuningConfig
        ),
        null,
        null,
        new NoopChatHandlerProvider(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    runTask(indexTask);
  }

  private Set<DataSegment> runTask(Task task)
  {
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertEquals(TaskState.SUCCESS, getIndexingServiceClient().runAndWait(task).getStatusCode());
    return getIndexingServiceClient().getPublishedSegments(task);
  }
}
