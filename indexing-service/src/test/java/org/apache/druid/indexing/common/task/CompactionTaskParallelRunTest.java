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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SegmentsSplitHintSpec;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexer.report.IngestionStatsAndErrors;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.CompactionTask.Builder;
import org.apache.druid.indexing.common.task.batch.parallel.AbstractParallelIndexSupervisorTaskTest;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.indexing.input.WindowedSegmentId;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.DataSegmentsWithSchemas;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.loading.NoopSegmentCacheManager;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NumberedOverwriteShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionIds;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@ParameterizedClass(name = "{0}")
@MethodSource("constructorFeeder")
public class CompactionTaskParallelRunTest extends AbstractParallelIndexSupervisorTaskTest
{
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK},
        new Object[]{LockGranularity.SEGMENT}
    );
  }

  private static final String DATA_SOURCE = "test";
  private static final Interval INTERVAL_TO_INDEX = Intervals.of("2014-01-01/2014-01-02");

  private static final AggregateProjectionSpec PROJECTION_SPEC =
      AggregateProjectionSpec.builder("projection1")
                             .virtualColumns(
                                 Granularities.toVirtualColumn(
                                     Granularities.HOUR,
                                     Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
                                 )
                             )
                             .groupingColumns(
                                 new LongDimensionSchema(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME),
                                 new StringDimensionSchema("dim", DimensionSchema.MultiValueHandling.ARRAY, null)
                             )
                             .aggregators(new LongSumAggregatorFactory("val", "val"))
                             .build();

  private final LockGranularity lockGranularity;

  // Whether the currently-running test case is allowed to fetch segments in the primary logic of the CompactionTask.
  // Verified by a special SegmentCacheManager in createTaskToolbox.
  private boolean allowSegmentFetchesByCompactionTask = false;
  private File inputDir;

  public CompactionTaskParallelRunTest(LockGranularity lockGranularity)
  {
    super(DEFAULT_TRANSIENT_TASK_FAILURE_RATE, DEFAULT_TRANSIENT_API_FAILURE_RATE);
    this.lockGranularity = lockGranularity;
  }

  @BeforeEach
  public void setup() throws IOException
  {
    getObjectMapper().registerSubtypes(ParallelIndexTuningConfig.class, DruidInputSource.class);
    getObjectMapper().registerSubtypes(CompactionTask.CompactionTuningConfig.class, DruidInputSource.class);

    inputDir = FileUtils.createTempDirInLocation(temporaryFolder.toPath(), "input");
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
  public void testRunParallelWithDynamicPartitioningMatchCompactionState()
  {
    allowSegmentFetchesByCompactionTask = true;
    runIndexTask(null, true);

    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING)
        .build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);
    final Set<DataSegment> compactedSegments = dataSegmentsWithSchemas.getSegments();

    for (DataSegment segment : compactedSegments) {
      Assertions.assertSame(
          lockGranularity == LockGranularity.TIME_CHUNK ? NumberedShardSpec.class : NumberedOverwriteShardSpec.class,
          segment.getShardSpec().getClass()
      );
      // Expect compaction state to exist as store compaction state by default
      LongSumAggregatorFactory expectedLongSumMetric = new LongSumAggregatorFactory("val", "val");
      CompactionState expectedState =
          CompactionState.builder()
                         .partitionsSpec(new DynamicPartitionsSpec(null, Long.MAX_VALUE))
                         .dimensionsSpec(new DimensionsSpec(
                             ImmutableList.of(
                                 new StringDimensionSchema("ts", DimensionSchema.MultiValueHandling.ARRAY, null),
                                 new StringDimensionSchema("dim", DimensionSchema.MultiValueHandling.ARRAY, null)
                             )
                         ))
                         .metricsSpec(ImmutableList.of(expectedLongSumMetric))
                         .indexSpec(compactionTask.getTuningConfig().getIndexSpec().getEffectiveSpec())
                         .granularitySpec(new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.MINUTE,
                             true,
                             ImmutableList.of(segment.getInterval())
                         ))
                         .build();
      Assertions.assertEquals(expectedState, segment.getLastCompactionState(), "Compaction state for " + segment.getId());
    }
  }

  @Test
  public void testRunParallelWithHashPartitioningMatchCompactionState() throws Exception
  {
    allowSegmentFetchesByCompactionTask = true;

    // Hash partitioning is not supported with segment lock yet
    Assumptions.assumeFalse(lockGranularity == LockGranularity.SEGMENT);
    runIndexTask(null, true);

    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(newTuningConfig(new HashedPartitionsSpec(null, 3, null), 2, true))
        .build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);
    final Set<DataSegment> compactedSegments = dataSegmentsWithSchemas.getSegments();
    for (DataSegment segment : compactedSegments) {
      // Expect compaction state to exist as store compaction state by default
      LongSumAggregatorFactory expectedLongSumMetric = new LongSumAggregatorFactory("val", "val");
      Assertions.assertSame(HashBasedNumberedShardSpec.class, segment.getShardSpec().getClass());
      CompactionState expectedState =
          CompactionState.builder()
                         .partitionsSpec(new HashedPartitionsSpec(null, 3, null))
                         .dimensionsSpec(new DimensionsSpec(
                             ImmutableList.of(
                                 new StringDimensionSchema("ts", DimensionSchema.MultiValueHandling.ARRAY, null),
                                 new StringDimensionSchema("dim", DimensionSchema.MultiValueHandling.ARRAY, null)
                             )
                         ))
                         .metricsSpec(ImmutableList.of(expectedLongSumMetric))
                         .indexSpec(compactionTask.getTuningConfig().getIndexSpec().getEffectiveSpec())
                         .granularitySpec(new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.MINUTE,
                             true,
                             ImmutableList.of(segment.getInterval())
                         ))
                         .build();
      Assertions.assertEquals(expectedState, segment.getLastCompactionState(), "Compaction state for " + segment.getId());
    }

    List<IngestionStatsAndErrors> reports = getIngestionReports();
    Assertions.assertEquals(reports.size(), 3); // since three index tasks are run by single compaction task

    // this test reads 3 segments and publishes 6 segments
    Assertions.assertEquals(
        3,
        reports.stream().mapToLong(IngestionStatsAndErrors::getSegmentsRead).sum()
    );
    Assertions.assertEquals(
        6,
        reports.stream()
               .mapToLong(IngestionStatsAndErrors::getSegmentsPublished)
               .sum()
    );
  }

  @Test
  public void testRunParallelWithRangePartitioning() throws Exception
  {
    allowSegmentFetchesByCompactionTask = true;

    // Range partitioning is not supported with segment lock yet
    Assumptions.assumeFalse(lockGranularity == LockGranularity.SEGMENT);
    runIndexTask(null, true);

    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(newTuningConfig(new SingleDimensionPartitionsSpec(7, null, "dim", false), 2, true))
        .build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);
    final Set<DataSegment> compactedSegments = dataSegmentsWithSchemas.getSegments();
    for (DataSegment segment : compactedSegments) {
      // Expect compaction state to exist as store compaction state by default
      LongSumAggregatorFactory expectedLongSumMetric = new LongSumAggregatorFactory("val", "val");
      Assertions.assertSame(SingleDimensionShardSpec.class, segment.getShardSpec().getClass());
      CompactionState expectedState =
          CompactionState.builder()
                         .partitionsSpec(new SingleDimensionPartitionsSpec(7, null, "dim", false))
                         .dimensionsSpec(new DimensionsSpec(
                             ImmutableList.of(
                                 new StringDimensionSchema("ts", DimensionSchema.MultiValueHandling.ARRAY, null),
                                 new StringDimensionSchema("dim", DimensionSchema.MultiValueHandling.ARRAY, null)
                             )
                         ))
                         .metricsSpec(ImmutableList.of(expectedLongSumMetric))
                         .indexSpec(compactionTask.getTuningConfig().getIndexSpec().getEffectiveSpec())
                         .granularitySpec(new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.MINUTE,
                             true,
                             ImmutableList.of(segment.getInterval())
                         ))
                         .build();
      Assertions.assertEquals(expectedState, segment.getLastCompactionState(), "Compaction state for " + segment.getId());
    }
  }

  @Test
  public void testRunParallelWithRangePartitioningAndNoUpfrontSegmentFetching() throws Exception
  {
    allowSegmentFetchesByCompactionTask = false;

    // Range partitioning is not supported with segment lock yet
    Assumptions.assumeFalse(lockGranularity == LockGranularity.SEGMENT);
    runIndexTask(null, true);

    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );

    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(newTuningConfig(new SingleDimensionPartitionsSpec(7, null, "dim", false), 2, true))
        .dimensionsSpec(new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("ts", "dim"))))
        .metricsSpec(new AggregatorFactory[]{new LongSumAggregatorFactory("val", "val")})
        .granularitySpec(
            new ClientCompactionTaskGranularitySpec(
                Granularities.HOUR,
                Granularities.MINUTE,
                true
            )
        )
        .build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);
    final Set<DataSegment> compactedSegments = dataSegmentsWithSchemas.getSegments();
    for (DataSegment segment : compactedSegments) {
      // Expect compaction state to exist as store compaction state by default
      LongSumAggregatorFactory expectedLongSumMetric = new LongSumAggregatorFactory("val", "val");
      Assertions.assertSame(SingleDimensionShardSpec.class, segment.getShardSpec().getClass());
      CompactionState expectedState =
          CompactionState.builder()
                         .partitionsSpec(new SingleDimensionPartitionsSpec(7, null, "dim", false))
                         .dimensionsSpec(
                             new DimensionsSpec(
                                 DimensionsSpec.getDefaultSchemas(
                                     ImmutableList.of(
                                         "ts",
                                         "dim"
                                     )
                                 )
                             )
                         )
                         .metricsSpec(ImmutableList.of(expectedLongSumMetric))
                         .indexSpec(compactionTask.getTuningConfig().getIndexSpec().getEffectiveSpec())
                         .granularitySpec(new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.MINUTE,
                             true,
                             ImmutableList.of(Intervals.of("2014-01-01T00:00:00Z/2014-01-01T03:00:00Z"))
                         ))
                         .build();
      Assertions.assertEquals(expectedState, segment.getLastCompactionState(), "Compaction state for " + segment.getId());
    }
  }

  @Test
  public void testRunParallelWithMultiDimensionRangePartitioning() throws Exception
  {
    allowSegmentFetchesByCompactionTask = true;

    // Range partitioning is not supported with segment lock yet
    Assumptions.assumeFalse(lockGranularity == LockGranularity.SEGMENT);
    runIndexTask(null, true);

    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(newTuningConfig(
            new DimensionRangePartitionsSpec(7, null, Arrays.asList("dim1", "dim2"), false),
            2,
            true
        )).build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);
    final Set<DataSegment> compactedSegments = dataSegmentsWithSchemas.getSegments();
    for (DataSegment segment : compactedSegments) {
      // Expect compaction state to exist as store compaction state by default
      LongSumAggregatorFactory expectedLongSumMetric = new LongSumAggregatorFactory("val", "val");
      Assertions.assertSame(DimensionRangeShardSpec.class, segment.getShardSpec().getClass());
      CompactionState expectedState =
          CompactionState.builder()
                         .partitionsSpec(new DimensionRangePartitionsSpec(
                             7,
                             null,
                             Arrays.asList("dim1", "dim2"),
                             false
                         ))
                         .dimensionsSpec(new DimensionsSpec(
                             ImmutableList.of(
                                 new StringDimensionSchema("ts", DimensionSchema.MultiValueHandling.ARRAY, null),
                                 new StringDimensionSchema("dim", DimensionSchema.MultiValueHandling.ARRAY, null)
                             )
                         ))
                         .metricsSpec(ImmutableList.of(expectedLongSumMetric))
                         .indexSpec(compactionTask.getTuningConfig().getIndexSpec().getEffectiveSpec())
                         .granularitySpec(new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.MINUTE,
                             true,
                             ImmutableList.of(segment.getInterval())
                         ))
                         .build();
      Assertions.assertEquals(expectedState, segment.getLastCompactionState(), "Compaction state for " + segment.getId());
    }
  }

  @Test
  public void testRunParallelWithRangePartitioningWithSingleTask() throws Exception
  {
    allowSegmentFetchesByCompactionTask = true;

    // Range partitioning is not supported with segment lock yet
    Assumptions.assumeFalse(lockGranularity == LockGranularity.SEGMENT);
    runIndexTask(null, true);

    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(newTuningConfig(new SingleDimensionPartitionsSpec(7, null, "dim", false), 1, true))
        .build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);
    final Set<DataSegment> compactedSegments = dataSegmentsWithSchemas.getSegments();
    for (DataSegment segment : compactedSegments) {
      // Expect compaction state to exist as store compaction state by default
      LongSumAggregatorFactory expectedLongSumMetric = new LongSumAggregatorFactory("val", "val");
      Assertions.assertSame(SingleDimensionShardSpec.class, segment.getShardSpec().getClass());
      CompactionState expectedState =
          CompactionState.builder()
                         .partitionsSpec(new SingleDimensionPartitionsSpec(7, null, "dim", false))
                         .dimensionsSpec(new DimensionsSpec(
                             ImmutableList.of(
                                 new StringDimensionSchema("ts", DimensionSchema.MultiValueHandling.ARRAY, null),
                                 new StringDimensionSchema("dim", DimensionSchema.MultiValueHandling.ARRAY, null)
                             )
                         ))
                         .metricsSpec(ImmutableList.of(expectedLongSumMetric))
                         .indexSpec(compactionTask.getTuningConfig().getIndexSpec().getEffectiveSpec())
                         .granularitySpec(new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.MINUTE,
                             true,
                             ImmutableList.of(segment.getInterval())
                         ))
                         .build();
      Assertions.assertEquals(expectedState, segment.getLastCompactionState(), "Compaction state for " + segment.getId());
    }
  }

  @Test
  public void testRunParallelWithMultiDimensionRangePartitioningWithSingleTask() throws Exception
  {
    allowSegmentFetchesByCompactionTask = true;

    // Range partitioning is not supported with segment lock yet
    Assumptions.assumeFalse(lockGranularity == LockGranularity.SEGMENT);
    runIndexTask(null, true);

    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(newTuningConfig(
            new DimensionRangePartitionsSpec(7, null, Arrays.asList("dim1", "dim2"), false),
            1,
            true
        )).build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);
    final Set<DataSegment> compactedSegments = dataSegmentsWithSchemas.getSegments();
    for (DataSegment segment : compactedSegments) {
      // Expect compaction state to exist as store compaction state by default
      LongSumAggregatorFactory expectedLongSumMetric = new LongSumAggregatorFactory("val", "val");
      Assertions.assertSame(DimensionRangeShardSpec.class, segment.getShardSpec().getClass());
      CompactionState expectedState =
          CompactionState.builder()
                         .partitionsSpec(new DimensionRangePartitionsSpec(
                             7,
                             null,
                             Arrays.asList("dim1", "dim2"),
                             false
                         ))
                         .dimensionsSpec(new DimensionsSpec(
                             ImmutableList.of(
                                 new StringDimensionSchema("ts", DimensionSchema.MultiValueHandling.ARRAY, null),
                                 new StringDimensionSchema("dim", DimensionSchema.MultiValueHandling.ARRAY, null)
                             )
                         ))
                         .metricsSpec(ImmutableList.of(expectedLongSumMetric))
                         .indexSpec(compactionTask.getTuningConfig().getIndexSpec().getEffectiveSpec())
                         .granularitySpec(new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.MINUTE,
                             true,
                             ImmutableList.of(segment.getInterval())
                         ))
                         .build();
      Assertions.assertEquals(expectedState, segment.getLastCompactionState(), "Compaction state for " + segment.getId());
    }
  }

  @Test
  public void testRunCompactionStateNotStoreIfContextSetToFalse()
  {
    allowSegmentFetchesByCompactionTask = true;
    runIndexTask(null, true);

    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING)
        .context(ImmutableMap.of(Tasks.STORE_COMPACTION_STATE_KEY, false))
        .build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);
    final Set<DataSegment> compactedSegments = dataSegmentsWithSchemas.getSegments();

    for (DataSegment segment : compactedSegments) {
      Assertions.assertSame(
          lockGranularity == LockGranularity.TIME_CHUNK ? NumberedShardSpec.class : NumberedOverwriteShardSpec.class,
          segment.getShardSpec().getClass()
      );
      // Expect compaction state to exist as store compaction state by default
      Assertions.assertEquals(null, segment.getLastCompactionState());
    }
  }

  @Test
  public void testRunCompactionWithFilterShouldStoreInState() throws Exception
  {
    allowSegmentFetchesByCompactionTask = true;
    runIndexTask(null, true);

    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING)
        .transformSpec(new CompactionTransformSpec(new SelectorDimFilter("dim", "a", null), null))
        .build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);
    final Set<DataSegment> compactedSegments = dataSegmentsWithSchemas.getSegments();

    Assertions.assertEquals(3, compactedSegments.size());

    for (DataSegment segment : compactedSegments) {
      Assertions.assertSame(
          lockGranularity == LockGranularity.TIME_CHUNK ? NumberedShardSpec.class : NumberedOverwriteShardSpec.class,
          segment.getShardSpec().getClass()
      );
      LongSumAggregatorFactory expectedLongSumMetric = new LongSumAggregatorFactory("val", "val");
      CompactionState expectedState =
          CompactionState.builder()
                         .partitionsSpec(new DynamicPartitionsSpec(null, Long.MAX_VALUE))
                         .dimensionsSpec(new DimensionsSpec(
                             ImmutableList.of(
                                 new StringDimensionSchema("ts", DimensionSchema.MultiValueHandling.ARRAY, null),
                                 new StringDimensionSchema("dim", DimensionSchema.MultiValueHandling.ARRAY, null)
                             )
                         ))
                         .metricsSpec(ImmutableList.of(expectedLongSumMetric))
                         .transformSpec(compactionTask.getTransformSpec())
                         .indexSpec(compactionTask.getTuningConfig().getIndexSpec().getEffectiveSpec())
                         .granularitySpec(new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.MINUTE,
                             true,
                             ImmutableList.of(segment.getInterval())
                         ))
                         .build();
      Assertions.assertEquals(expectedState, segment.getLastCompactionState(), "Compaction state for " + segment.getId());
    }
  }

  @Test
  public void testRunCompactionWithNewMetricsShouldStoreInState() throws Exception
  {
    allowSegmentFetchesByCompactionTask = true;
    runIndexTask(null, true);

    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING)
        .metricsSpec(new AggregatorFactory[]{
            new CountAggregatorFactory("cnt"),
            new LongSumAggregatorFactory("val", "val")
        })
        .build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);
    final Set<DataSegment> compactedSegments = dataSegmentsWithSchemas.getSegments();

    Assertions.assertEquals(3, compactedSegments.size());

    for (DataSegment segment : compactedSegments) {
      Assertions.assertSame(
          lockGranularity == LockGranularity.TIME_CHUNK ? NumberedShardSpec.class : NumberedOverwriteShardSpec.class,
          segment.getShardSpec().getClass()
      );
      CountAggregatorFactory expectedCountMetric = new CountAggregatorFactory("cnt");
      LongSumAggregatorFactory expectedLongSumMetric = new LongSumAggregatorFactory("val", "val");
      CompactionState expectedState =
          CompactionState.builder()
                         .partitionsSpec(new DynamicPartitionsSpec(null, Long.MAX_VALUE))
                         .dimensionsSpec(new DimensionsSpec(
                             ImmutableList.of(
                                 new StringDimensionSchema("ts", DimensionSchema.MultiValueHandling.ARRAY, null),
                                 new StringDimensionSchema("dim", DimensionSchema.MultiValueHandling.ARRAY, null)
                             )
                         ))
                         .metricsSpec(ImmutableList.of(expectedCountMetric, expectedLongSumMetric))
                         .transformSpec(compactionTask.getTransformSpec())
                         .indexSpec(compactionTask.getTuningConfig().getIndexSpec().getEffectiveSpec())
                         .granularitySpec(new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.MINUTE,
                             true,
                             ImmutableList.of(segment.getInterval())
                         ))
                         .build();
      Assertions.assertEquals(expectedState, segment.getLastCompactionState(), "Compaction state for " + segment.getId());
    }
  }

  @Test
  public void testCompactHashAndDynamicPartitionedSegments()
  {
    allowSegmentFetchesByCompactionTask = true;
    runIndexTask(new HashedPartitionsSpec(null, 2, null), false);
    runIndexTask(null, true);
    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING)
        .build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);
    final Set<DataSegment> compactedSegments = dataSegmentsWithSchemas.getSegments();

    final Map<Interval, List<DataSegment>> intervalToSegments = SegmentUtils.groupSegmentsByInterval(
        compactedSegments
    );
    Assertions.assertEquals(3, intervalToSegments.size());
    Assertions.assertEquals(
        ImmutableSet.of(
            Intervals.of("2014-01-01T00/PT1H"),
            Intervals.of("2014-01-01T01/PT1H"),
            Intervals.of("2014-01-01T02/PT1H")
        ),
        intervalToSegments.keySet()
    );
    for (Entry<Interval, List<DataSegment>> entry : intervalToSegments.entrySet()) {
      final List<DataSegment> segmentsInInterval = entry.getValue();
      Assertions.assertEquals(1, segmentsInInterval.size());
      final ShardSpec shardSpec = segmentsInInterval.get(0).getShardSpec();
      if (lockGranularity == LockGranularity.TIME_CHUNK) {
        Assertions.assertSame(NumberedShardSpec.class, shardSpec.getClass());
        final NumberedShardSpec numberedShardSpec = (NumberedShardSpec) shardSpec;
        Assertions.assertEquals(0, numberedShardSpec.getPartitionNum());
        Assertions.assertEquals(1, numberedShardSpec.getNumCorePartitions());
      } else {
        Assertions.assertSame(NumberedOverwriteShardSpec.class, shardSpec.getClass());
        final NumberedOverwriteShardSpec numberedShardSpec = (NumberedOverwriteShardSpec) shardSpec;
        Assertions.assertEquals(PartitionIds.NON_ROOT_GEN_START_PARTITION_ID, numberedShardSpec.getPartitionNum());
        Assertions.assertEquals(1, numberedShardSpec.getAtomicUpdateGroupSize());
      }
    }
  }

  @Test
  public void testCompactRangeAndDynamicPartitionedSegments()
  {
    allowSegmentFetchesByCompactionTask = true;
    runIndexTask(new SingleDimensionPartitionsSpec(2, null, "dim", false), false);
    runIndexTask(null, true);
    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING)
        .build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);
    final Set<DataSegment> compactedSegments = dataSegmentsWithSchemas.getSegments();

    final Map<Interval, List<DataSegment>> intervalToSegments = SegmentUtils.groupSegmentsByInterval(
        compactedSegments
    );
    Assertions.assertEquals(3, intervalToSegments.size());
    Assertions.assertEquals(
        ImmutableSet.of(
            Intervals.of("2014-01-01T00/PT1H"),
            Intervals.of("2014-01-01T01/PT1H"),
            Intervals.of("2014-01-01T02/PT1H")
        ),
        intervalToSegments.keySet()
    );
    for (Entry<Interval, List<DataSegment>> entry : intervalToSegments.entrySet()) {
      final List<DataSegment> segmentsInInterval = entry.getValue();
      Assertions.assertEquals(1, segmentsInInterval.size());
      final ShardSpec shardSpec = segmentsInInterval.get(0).getShardSpec();
      if (lockGranularity == LockGranularity.TIME_CHUNK) {
        Assertions.assertSame(NumberedShardSpec.class, shardSpec.getClass());
        final NumberedShardSpec numberedShardSpec = (NumberedShardSpec) shardSpec;
        Assertions.assertEquals(0, numberedShardSpec.getPartitionNum());
        Assertions.assertEquals(1, numberedShardSpec.getNumCorePartitions());
      } else {
        Assertions.assertSame(NumberedOverwriteShardSpec.class, shardSpec.getClass());
        final NumberedOverwriteShardSpec numberedShardSpec = (NumberedOverwriteShardSpec) shardSpec;
        Assertions.assertEquals(PartitionIds.NON_ROOT_GEN_START_PARTITION_ID, numberedShardSpec.getPartitionNum());
        Assertions.assertEquals(1, numberedShardSpec.getAtomicUpdateGroupSize());
      }
    }
  }

  @Test
  public void testDruidInputSourceCreateSplitsWithIndividualSplits() throws Exception
  {
    allowSegmentFetchesByCompactionTask = true;
    runIndexTask(null, true);

    List<InputSplit<List<WindowedSegmentId>>> splits = Lists.newArrayList(
        DruidInputSource.createSplits(
            null,
            getCoordinatorClient(),
            DATA_SOURCE,
            INTERVAL_TO_INDEX,
            new SegmentsSplitHintSpec(null, 1)
        )
    );

    List<DataSegment> segments = new ArrayList<>(
        getCoordinatorClient().fetchUsedSegments(
            DATA_SOURCE,
            ImmutableList.of(INTERVAL_TO_INDEX)
        ).get()
    );

    Set<String> segmentIdsFromSplits = new HashSet<>();
    Set<String> segmentIdsFromCoordinator = new HashSet<>();
    Assertions.assertEquals(segments.size(), splits.size());
    for (int i = 0; i < segments.size(); i++) {
      segmentIdsFromCoordinator.add(segments.get(i).getId().toString());
      segmentIdsFromSplits.add(splits.get(i).get().get(0).getSegmentId());
    }
    Assertions.assertEquals(segmentIdsFromCoordinator, segmentIdsFromSplits);
  }

  @Test
  public void testCompactionDropSegmentsOfInputIntervalIfDropFlagIsSet() throws Exception
  {
    allowSegmentFetchesByCompactionTask = true;
    runIndexTask(null, true);

    Collection<DataSegment> usedSegments = getCoordinatorClient().fetchUsedSegments(
        DATA_SOURCE,
        ImmutableList.of(INTERVAL_TO_INDEX)
    ).get();
    Assertions.assertEquals(3, usedSegments.size());
    for (DataSegment segment : usedSegments) {
      Assertions.assertTrue(Granularities.HOUR.isAligned(segment.getInterval()));
    }

    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );
    final CompactionTask compactionTask = builder
        // Set the dropExisting flag to true in the IOConfig of the compaction task
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null), true)
        .tuningConfig(AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING)
        .granularitySpec(new ClientCompactionTaskGranularitySpec(Granularities.MINUTE, null, null))
        .build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);

    usedSegments = getCoordinatorClient().fetchUsedSegments(
        DATA_SOURCE,
        ImmutableList.of(INTERVAL_TO_INDEX)
    ).get();
    // All the HOUR segments got covered by tombstones even if we do not have all MINUTES segments fully covering the 3 HOURS interval.
    // In fact, we only have 3 minutes of data out of the 3 hours interval.
    Assertions.assertEquals(180, usedSegments.size());
    int tombstonesCount = 0;
    for (DataSegment segment : usedSegments) {
      Assertions.assertTrue(Granularities.MINUTE.isAligned(segment.getInterval()));
      if (segment.isTombstone()) {
        tombstonesCount++;
      }
    }
    Assertions.assertEquals(177, tombstonesCount);
  }

  @Test
  public void testCompactionDoesNotDropSegmentsIfDropFlagNotSet() throws Exception
  {
    allowSegmentFetchesByCompactionTask = true;
    runIndexTask(null, true);

    Collection<DataSegment> usedSegments = getCoordinatorClient().fetchUsedSegments(
        DATA_SOURCE,
        ImmutableList.of(INTERVAL_TO_INDEX)
    ).get();
    Assertions.assertEquals(3, usedSegments.size());
    for (DataSegment segment : usedSegments) {
      Assertions.assertTrue(Granularities.HOUR.isAligned(segment.getInterval()));
    }

    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING)
        .granularitySpec(new ClientCompactionTaskGranularitySpec(Granularities.MINUTE, null, null))
        .build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);

    usedSegments = getCoordinatorClient().fetchUsedSegments(
        DATA_SOURCE,
        ImmutableList.of(INTERVAL_TO_INDEX)
    ).get();
    // All the HOUR segments did not get dropped since MINUTES segments did not fully covering the 3 HOURS interval.
    Assertions.assertEquals(6, usedSegments.size());
    int hourSegmentCount = 0;
    int minuteSegmentCount = 0;
    for (DataSegment segment : usedSegments) {
      if (Granularities.MINUTE.isAligned(segment.getInterval())) {
        minuteSegmentCount++;
      }
      if (Granularities.MINUTE.isAligned(segment.getInterval())) {
        hourSegmentCount++;
      }
    }
    Assertions.assertEquals(3, hourSegmentCount);
    Assertions.assertEquals(3, minuteSegmentCount);
  }



  @Test
  public void testRunParallelWithProjections()
  {
    allowSegmentFetchesByCompactionTask = true;
    runIndexTaskWithProjections(null, true);

    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING)
        .build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);
    final Set<DataSegment> compactedSegments = dataSegmentsWithSchemas.getSegments();

    for (DataSegment segment : compactedSegments) {
      Assertions.assertSame(
          lockGranularity == LockGranularity.TIME_CHUNK ? NumberedShardSpec.class : NumberedOverwriteShardSpec.class,
          segment.getShardSpec().getClass()
      );
      // Expect compaction state to exist as store compaction state by default
      CompactionState expectedState =
          CompactionState.builder()
                         .partitionsSpec(new DynamicPartitionsSpec(null, Long.MAX_VALUE))
                         .dimensionsSpec(new DimensionsSpec(
                             ImmutableList.of(
                                 new StringDimensionSchema("ts", DimensionSchema.MultiValueHandling.ARRAY, null),
                                 new StringDimensionSchema("dim", DimensionSchema.MultiValueHandling.ARRAY, null),
                                 new LongDimensionSchema("val")
                             )
                         ))
                         .metricsSpec(Collections.emptyList())
                         .indexSpec(compactionTask.getTuningConfig().getIndexSpec().getEffectiveSpec())
                         .granularitySpec(new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.MINUTE,
                             true,
                             ImmutableList.of(segment.getInterval())
                         ))
                         .projections(ImmutableList.of(PROJECTION_SPEC))
                         .build();
      Assertions.assertEquals(expectedState, segment.getLastCompactionState(), "Compaction state for " + segment.getId());
    }
  }

  @Test
  public void testRunParallelAddProjections()
  {
    allowSegmentFetchesByCompactionTask = true;
    runIndexTaskWithProjections(null, true);

    final Builder builder = new Builder(
        DATA_SOURCE,
        getSegmentCacheManagerFactory()
    );
    final AggregateProjectionSpec addProjection =
        AggregateProjectionSpec.builder("projection2")
                               .aggregators(new LongSumAggregatorFactory("val", "val"))
                               .build();
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null))
        .tuningConfig(AbstractParallelIndexSupervisorTaskTest.DEFAULT_TUNING_CONFIG_FOR_PARALLEL_INDEXING)
        .projections(
            ImmutableList.of(
                PROJECTION_SPEC,
                addProjection
            )
        )
        .build();

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(compactionTask);
    verifySchema(dataSegmentsWithSchemas);
    final Set<DataSegment> compactedSegments = dataSegmentsWithSchemas.getSegments();

    for (DataSegment segment : compactedSegments) {
      Assertions.assertSame(
          lockGranularity == LockGranularity.TIME_CHUNK ? NumberedShardSpec.class : NumberedOverwriteShardSpec.class,
          segment.getShardSpec().getClass()
      );
      // Expect compaction state to exist as store compaction state by default
      CompactionState expectedState =
          CompactionState.builder()
                         .partitionsSpec(new DynamicPartitionsSpec(null, Long.MAX_VALUE))
                         .dimensionsSpec(new DimensionsSpec(
                             ImmutableList.of(
                                 new StringDimensionSchema("ts", DimensionSchema.MultiValueHandling.ARRAY, null),
                                 new StringDimensionSchema("dim", DimensionSchema.MultiValueHandling.ARRAY, null),
                                 new LongDimensionSchema("val")
                             )
                         ))
                         .metricsSpec(Collections.emptyList())
                         .indexSpec(compactionTask.getTuningConfig().getIndexSpec().getEffectiveSpec())
                         .granularitySpec(new UniformGranularitySpec(
                             Granularities.HOUR,
                             Granularities.MINUTE,
                             true,
                             ImmutableList.of(segment.getInterval())
                         ))
                         .projections(ImmutableList.of(PROJECTION_SPEC, addProjection))
                         .build();
      Assertions.assertEquals(expectedState, segment.getLastCompactionState(), "Compaction state for " + segment.getId());
    }
  }

  @Test
  public void testRunParallelWithRangePartitioningFilteringAllRows() throws Exception
  {
    allowSegmentFetchesByCompactionTask = true;

    // Range partitioning is not supported with segment lock yet
    Assumptions.assumeFalse(lockGranularity == LockGranularity.SEGMENT);

    runIndexTask(null, true);

    Collection<DataSegment> usedSegments = getCoordinatorClient().fetchUsedSegments(
        DATA_SOURCE,
        ImmutableList.of(INTERVAL_TO_INDEX)
    ).get();
    Assertions.assertEquals(3, usedSegments.size());

    // Compact with a transform that filters out ALL rows
    final Builder builder = new Builder(DATA_SOURCE, getSegmentCacheManagerFactory());
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null), true) // dropExisting=true
        .tuningConfig(newTuningConfig(
            new SingleDimensionPartitionsSpec(7, null, "dim", false),
            2,
            true
        ))
        .transformSpec(new CompactionTransformSpec(
            new SelectorDimFilter("dim", "nonexistent_value", null), // Filters out all rows
            null
        ))
        .build();

    runTask(compactionTask);

    usedSegments = getCoordinatorClient().fetchUsedSegments(
        DATA_SOURCE,
        ImmutableList.of(INTERVAL_TO_INDEX)
    ).get();

    Assertions.assertNotNull(usedSegments);

    int tombstoneCount = 0;
    for (DataSegment segment : usedSegments) {
      if (segment.isTombstone()) {
        tombstoneCount++;
      }
    }

    Assertions.assertTrue(tombstoneCount > 0, "Expected tombstones when all rows filtered in REPLACE mode");
  }

  @Test
  public void testRunParallelRangePartitioningFilterAllRowsReplaceLegacyMode() throws Exception
  {
    allowSegmentFetchesByCompactionTask = true;

    Assumptions.assumeFalse(lockGranularity == LockGranularity.SEGMENT);

    runIndexTask(null, true);

    Collection<DataSegment> usedSegments = getCoordinatorClient().fetchUsedSegments(
        DATA_SOURCE,
        ImmutableList.of(INTERVAL_TO_INDEX)
    ).get();
    Assertions.assertEquals(3, usedSegments.size());

    final Builder builder = new Builder(DATA_SOURCE, getSegmentCacheManagerFactory());
    final CompactionTask compactionTask = builder
        .inputSpec(new CompactionIntervalSpec(INTERVAL_TO_INDEX, null), false) // dropExisting=false -> REPLACE_LEGACY mode
        .tuningConfig(newTuningConfig(
            new SingleDimensionPartitionsSpec(7, null, "dim", false),
            2,
            true
        ))
        .transformSpec(new CompactionTransformSpec(
            new SelectorDimFilter("dim", "nonexistent_value", null), // Filters all rows
            null
        ))
        .build();

    runTask(compactionTask);

    // In REPLACE_LEGACY mode, should NOT create tombstones when all rows filtered
    // Original segments should remain unchanged
    usedSegments = getCoordinatorClient().fetchUsedSegments(
        DATA_SOURCE,
        ImmutableList.of(INTERVAL_TO_INDEX)
    ).get();

    Assertions.assertNotNull(usedSegments);

    Assertions.assertEquals(
        3,
        usedSegments.size(),
        "Original segments should remain in REPLACE_LEGACY mode when all rows filtered"
    );

    for (DataSegment segment : usedSegments) {
      Assertions.assertFalse(segment.isTombstone(), "No tombstones should be created in REPLACE_LEGACY mode");
    }
  }

  @Override
  protected TaskToolbox createTaskToolbox(Task task, TaskActionClient actionClient) throws IOException
  {
    final TaskToolbox baseToolbox = super.createTaskToolbox(task, actionClient);
    if (allowSegmentFetchesByCompactionTask) {
      return baseToolbox;
    } else {
      return new TaskToolbox.Builder(baseToolbox)
          .segmentCacheManager(new NoopSegmentCacheManager())
          .build();
    }
  }

  private void runIndexTask(@Nullable PartitionsSpec partitionsSpec, boolean appendToExisting)
  {
    ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
        new LocalInputSource(inputDir, "druid*"),
        new CsvInputFormat(
            Arrays.asList("ts", "dim", "val"),
            "|",
            null,
            false,
            0,
            null
        ),
        appendToExisting,
        null
    );
    ParallelIndexTuningConfig tuningConfig = newTuningConfig(partitionsSpec, 2, !appendToExisting);
    ParallelIndexSupervisorTask indexTask = new ParallelIndexSupervisorTask(
        null,
        null,
        null,
        new ParallelIndexIngestionSpec(
            DataSchema.builder()
                      .withDataSource(DATA_SOURCE)
                      .withTimestamp(new TimestampSpec("ts", "auto", null))
                      .withDimensions(DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim")))
                      .withAggregators(new LongSumAggregatorFactory("val", "val"))
                      .withGranularity(
                          new UniformGranularitySpec(
                              Granularities.HOUR,
                              Granularities.MINUTE,
                              ImmutableList.of(INTERVAL_TO_INDEX)
                          )
                      )
                      .build(),
            ioConfig,
            tuningConfig
        ),
        null
    );

    Assertions.assertEquals(
        Collections.singleton(
            new ResourceAction(
                new Resource(
                    LocalInputSource.TYPE_KEY,
                    ResourceType.EXTERNAL
                ), Action.READ
            )),
        indexTask.getInputSourceResources()
    );

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(indexTask);
    verifySchema(dataSegmentsWithSchemas);
  }

  private void runIndexTaskWithProjections(@Nullable PartitionsSpec partitionsSpec, boolean appendToExisting)
  {
    ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
        new LocalInputSource(inputDir, "druid*"),
        new CsvInputFormat(
            Arrays.asList("ts", "dim", "val"),
            "|",
            null,
            false,
            0,
            null
        ),
        appendToExisting,
        null
    );
    ParallelIndexTuningConfig tuningConfig = newTuningConfig(partitionsSpec, 2, !appendToExisting);
    ParallelIndexSupervisorTask indexTask = new ParallelIndexSupervisorTask(
        null,
        null,
        null,
        new ParallelIndexIngestionSpec(
            DataSchema.builder()
                      .withDataSource(DATA_SOURCE)
                      .withTimestamp(new TimestampSpec("ts", "auto", null))
                      .withDimensions(
                          DimensionsSpec.builder()
                                        .setDimensions(
                                            ImmutableList.of(
                                                new StringDimensionSchema("ts"),
                                                new StringDimensionSchema("dim"),
                                                new LongDimensionSchema("val")
                                            )
                                        )
                                        .build()
                      )
                      .withProjections(
                          ImmutableList.of(PROJECTION_SPEC)
                      )
                      .withGranularity(
                          new UniformGranularitySpec(
                              Granularities.HOUR,
                              Granularities.MINUTE,
                              ImmutableList.of(INTERVAL_TO_INDEX)
                          )
                      )
                      .build(),
            ioConfig,
            tuningConfig
        ),
        null
    );

    Assertions.assertEquals(
        Collections.singleton(
            new ResourceAction(
                new Resource(
                    LocalInputSource.TYPE_KEY,
                    ResourceType.EXTERNAL
                ), Action.READ
            )),
        indexTask.getInputSourceResources()
    );

    final DataSegmentsWithSchemas dataSegmentsWithSchemas = runTask(indexTask);
    verifySchema(dataSegmentsWithSchemas);
  }

  private DataSegmentsWithSchemas runTask(Task task)
  {
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    TaskStatus status = getIndexingServiceClient().runAndWait(task);
    Assertions.assertEquals(TaskState.SUCCESS, status.getStatusCode(), status.toString());
    return getIndexingServiceClient().getSegmentAndSchemas(task);
  }
}
