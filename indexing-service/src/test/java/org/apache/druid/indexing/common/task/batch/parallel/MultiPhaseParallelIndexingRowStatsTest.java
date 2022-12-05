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
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.incremental.RowIngestionMetersTotals;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;

public class MultiPhaseParallelIndexingRowStatsTest extends AbstractMultiPhaseParallelIndexingTest
{
  private static final String TIME = "ts";
  private static final String DIM1 = "dim1";
  private static final String DIM2 = "dim2";

  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec(TIME, "auto", null);
  private static final DimensionsSpec DIMENSIONS_SPEC = new DimensionsSpec(
      DimensionsSpec.getDefaultSchemas(Arrays.asList(TIME, DIM1, DIM2))
  );
  private static final ParseSpec PARSE_SPEC = new CSVParseSpec(
      TIMESTAMP_SPEC,
      DIMENSIONS_SPEC,
      null,
      Arrays.asList("ts", "dim1", "dim2", "val"),
      false,
      0
  );

  private static final Interval INTERVAL_TO_INDEX = Intervals.of("2017-12/P1M");

  private File inputDir;

  public MultiPhaseParallelIndexingRowStatsTest()
  {
    super(
        LockGranularity.SEGMENT,
        false,
        DEFAULT_TRANSIENT_TASK_FAILURE_RATE,
        DEFAULT_TRANSIENT_API_FAILURE_RATE
    );
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
  public void testHashPartitionRowStats()
  {
    testHashPartitionRowStats(2);
  }

  @Test
  @Ignore("assumes record rates, to be fixed PR #12852")
  public void testHashPartitionRowStats_concurrentSubTasks_1()
  {
    testHashPartitionRowStats(1);
  }

  private void testHashPartitionRowStats(int maxNumConcurrentSubTasks)
  {
    final Integer numShards = 10;

    ParallelIndexSupervisorTask task = createTask(
        null,
        null,
        null,
        PARSE_SPEC,
        INTERVAL_TO_INDEX,
        inputDir,
        "test_*",
        new HashedPartitionsSpec(null, numShards, ImmutableList.of("dim1", "dim2"), null),
        maxNumConcurrentSubTasks,
        false,
        false
    );

    final RowIngestionMetersTotals expectedTotals = new RowIngestionMetersTotals(200, 0, 0, 0);
    final Map<String, Object> expectedReports =
        maxNumConcurrentSubTasks <= 1
        ? buildExpectedTaskReportSequential(
            task.getId(),
            ImmutableList.of(),
            new RowIngestionMetersTotals(0, 0, 0, 0),
            expectedTotals
        )
        : buildExpectedTaskReportParallel(
            task.getId(),
            ImmutableList.of(),
            expectedTotals
        );

    Map<String, Object> actualReports = runTaskAndGetReports(task, TaskState.SUCCESS);
    compareTaskReports(expectedReports, actualReports);
  }

  @Test
  public void testRangePartitionRowStats()
  {
    final int targetRowsPerSegment = 20;
    ParallelIndexSupervisorTask task = createTask(
        null,
        null,
        null,
        PARSE_SPEC,
        INTERVAL_TO_INDEX,
        inputDir,
        "test_*",
        //new DimensionRangePartitionsSpec(targetRowsPerSegment, null, DIMS, false),
        new SingleDimensionPartitionsSpec(targetRowsPerSegment, null, DIM1, false),
        10,
        false,
        false
    );
    Map<String, Object> expectedReports = buildExpectedTaskReportParallel(
        task.getId(),
        ImmutableList.of(),
        new RowIngestionMetersTotals(200, 0, 0, 0)
    );
    Map<String, Object> actualReports = runTaskAndGetReports(task, TaskState.SUCCESS);
    compareTaskReports(expectedReports, actualReports);
  }
}
