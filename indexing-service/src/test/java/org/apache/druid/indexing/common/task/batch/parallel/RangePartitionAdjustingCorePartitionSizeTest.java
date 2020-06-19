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
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.partitions.DimensionBasedPartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
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
import java.util.List;
import java.util.Set;

@RunWith(Parameterized.class)
public class RangePartitionAdjustingCorePartitionSizeTest extends AbstractMultiPhaseParallelIndexingTest
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
  private static final Interval INTERVAL_TO_INDEX = Intervals.of("2020-01-01/P1M");

  @Parameterized.Parameters(name = "{0}, maxNumConcurrentSubTasks={1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK, 2},
        new Object[]{LockGranularity.TIME_CHUNK, 1},
        new Object[]{LockGranularity.SEGMENT, 2}
    );
  }

  private final int maxNumConcurrentSubTasks;

  public RangePartitionAdjustingCorePartitionSizeTest(LockGranularity lockGranularity, int maxNumConcurrentSubTasks)
  {
    super(lockGranularity, true);
    this.maxNumConcurrentSubTasks = maxNumConcurrentSubTasks;
  }

  @Test
  public void testLessPartitionsThanBuckets() throws IOException
  {
    final File inputDir = temporaryFolder.newFolder();
    for (int i = 0; i < 2; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "test_" + i).toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("2020-01-01T00:00:00,aaa,b1,10\n"));
      }
    }
    for (int i = 0; i < 3; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "test_" + (i + 2)).toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("2020-01-01T00:00:00,zzz,b1,10\n"));
      }
    }
    final DimensionBasedPartitionsSpec partitionsSpec = new SingleDimensionPartitionsSpec(
        2,
        null,
        "dim1",
        false
    );
    final List<DataSegment> segments = new ArrayList<>(
        runTestTask(
            TIMESTAMP_SPEC,
            DIMENSIONS_SPEC,
            INPUT_FORMAT,
            null,
            INTERVAL_TO_INDEX,
            inputDir,
            "test_*",
            partitionsSpec,
            maxNumConcurrentSubTasks,
            TaskState.SUCCESS
        )
    );
    Assert.assertEquals(1, segments.size());
    final DataSegment segment = segments.get(0);
    Assert.assertSame(SingleDimensionShardSpec.class, segment.getShardSpec().getClass());
    final SingleDimensionShardSpec shardSpec = (SingleDimensionShardSpec) segment.getShardSpec();
    Assert.assertEquals(1, shardSpec.getNumCorePartitions());
    Assert.assertEquals(0, shardSpec.getPartitionNum());
    Assert.assertEquals("dim1", shardSpec.getDimension());
  }

  @Test
  public void testEqualNumberOfPartitionsToBuckets() throws IOException
  {
    final File inputDir = temporaryFolder.newFolder();
    for (int i = 0; i < 10; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "test_" + i).toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("2020-01-01T00:00:00,%s,b1,%d\n", "aa" + (i + 10), 10 * (i + 1)));
      }
    }
    final DimensionBasedPartitionsSpec partitionsSpec = new SingleDimensionPartitionsSpec(
        2,
        null,
        "dim1",
        false
    );
    final Set<DataSegment> segments = runTestTask(
        TIMESTAMP_SPEC,
        DIMENSIONS_SPEC,
        INPUT_FORMAT,
        null,
        INTERVAL_TO_INDEX,
        inputDir,
        "test_*",
        partitionsSpec,
        maxNumConcurrentSubTasks,
        TaskState.SUCCESS
    );
    Assert.assertEquals(5, segments.size());
    segments.forEach(segment -> {
      Assert.assertSame(SingleDimensionShardSpec.class, segment.getShardSpec().getClass());
      final SingleDimensionShardSpec shardSpec = (SingleDimensionShardSpec) segment.getShardSpec();
      Assert.assertEquals(5, shardSpec.getNumCorePartitions());
      Assert.assertTrue(shardSpec.getPartitionNum() < shardSpec.getNumCorePartitions());
      Assert.assertEquals("dim1", shardSpec.getDimension());
    });
  }
}
