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
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.partitions.DimensionBasedPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

@RunWith(Parameterized.class)
public class HashPartitionAdjustingCorePartitionSizeTest extends AbstractMultiPhaseParallelIndexingTest
{
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec("ts", "auto", null);
  private static final DimensionsSpec DIMENSIONS_SPEC = new DimensionsSpec(
      DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim1", "dim2"))
  );
  private static final AggregatorFactory[] METRICS = new AggregatorFactory[]{
      new LongSumAggregatorFactory("val", "val")
  };
  private static final InputFormat INPUT_FORMAT = new CsvInputFormat(
      Arrays.asList("ts", "dim1", "dim2", "val"),
      null,
      false,
      false,
      0
  );
  private static final Interval INTERVAL_TO_INDEX = Intervals.of("2020-01-01/P1M");
  private static final GranularitySpec GRANULARITY_SPEC = new UniformGranularitySpec(
      Granularities.DAY,
      Granularities.NONE,
      Collections.singletonList(INTERVAL_TO_INDEX)
  );
  private static final int MAX_NUM_CONCURRENT_SUB_TASKS = 2;

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK},
        new Object[]{LockGranularity.SEGMENT}
    );
  }

  public HashPartitionAdjustingCorePartitionSizeTest(LockGranularity lockGranularity)
  {
    super(lockGranularity, true);
  }

  @Test
  public void testLessPartitionsThanBuckets() throws IOException
  {
    final String data;
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
         final DataOutputStream out = new DataOutputStream(baos)) {
      for (int i = 0; i < 3; i++) {
        out.writeUTF(StringUtils.format("2020-01-01T00:00:00,%s,b1,%d\n", "a" + (i + 1), 10 * (i + 1)));
      }
      data = baos.toString(StringUtils.UTF8_STRING);
    }
    final ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
        null,
        new InlineInputSource(data),
        INPUT_FORMAT,
        false
    );
    final DimensionBasedPartitionsSpec partitionsSpec = new HashedPartitionsSpec(
        null,
        10,
        ImmutableList.of("dim1")
    );
    final ParallelIndexSupervisorTask task = new ParallelIndexSupervisorTask(
        null,
        null,
        null,
        new ParallelIndexIngestionSpec(
            new DataSchema(
                "testDatasource",
                TIMESTAMP_SPEC,
                DIMENSIONS_SPEC,
                METRICS,
                GRANULARITY_SPEC,
                null
            ),
            ioConfig,
            newTuningConfig(partitionsSpec, MAX_NUM_CONCURRENT_SUB_TASKS)
        ),
        null,
        null,
        null,
        null,
        null,
        null
    );
    final Set<DataSegment> segments = runTask(task, TaskState.SUCCESS);
    Assert.assertEquals(3, segments.size());
    segments.forEach(segment -> {
      Assert.assertSame(HashBasedNumberedShardSpec.class, segment.getShardSpec().getClass());
      final HashBasedNumberedShardSpec shardSpec = (HashBasedNumberedShardSpec) segment.getShardSpec();
      Assert.assertEquals(3, shardSpec.getPartitions());
      Assert.assertEquals(10, shardSpec.getNumBuckets());
      Assert.assertEquals(ImmutableList.of("dim1"), shardSpec.getPartitionDimensions());
    });
  }

  @Test
  public void testEqualNumberOfPartitionsToBuckets() throws IOException
  {
    final String data;
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
         final DataOutputStream out = new DataOutputStream(baos)) {
      for (int i = 0; i < 10; i++) {
        out.writeUTF(StringUtils.format("2020-01-01T00:00:00,%s,b1,%d\n", "aa" + (i + 10), 10 * (i + 1)));
      }
      data = baos.toString(StringUtils.UTF8_STRING);
    }
    final ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
        null,
        new InlineInputSource(data),
        INPUT_FORMAT,
        false
    );
    final DimensionBasedPartitionsSpec partitionsSpec = new HashedPartitionsSpec(
        null,
        5,
        ImmutableList.of("dim1")
    );
    final ParallelIndexSupervisorTask task = new ParallelIndexSupervisorTask(
        null,
        null,
        null,
        new ParallelIndexIngestionSpec(
            new DataSchema(
                "testDatasource",
                TIMESTAMP_SPEC,
                DIMENSIONS_SPEC,
                METRICS,
                GRANULARITY_SPEC,
                null
            ),
            ioConfig,
            newTuningConfig(partitionsSpec, MAX_NUM_CONCURRENT_SUB_TASKS)
        ),
        null,
        null,
        null,
        null,
        null,
        null
    );
    final Set<DataSegment> segments = runTask(task, TaskState.SUCCESS);
    Assert.assertEquals(5, segments.size());
    segments.forEach(segment -> {
      Assert.assertSame(HashBasedNumberedShardSpec.class, segment.getShardSpec().getClass());
      final HashBasedNumberedShardSpec shardSpec = (HashBasedNumberedShardSpec) segment.getShardSpec();
      Assert.assertEquals(5, shardSpec.getPartitions());
      Assert.assertEquals(5, shardSpec.getNumBuckets());
      Assert.assertEquals(ImmutableList.of("dim1"), shardSpec.getPartitionDimensions());
    });
  }
}
