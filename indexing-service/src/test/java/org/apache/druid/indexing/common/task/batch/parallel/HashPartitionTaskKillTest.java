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
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HashPartitionTaskKillTest extends AbstractMultiPhaseParallelIndexingTest
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

  private File inputDir;
  // sorted input intervals
  private List<Interval> inputIntervals;


  public HashPartitionTaskKillTest()
  {
    super(LockGranularity.TIME_CHUNK, true, 0, 0);
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

  @Test(timeout = 5000L)
  public void testSubTaskFailsAtPartialSegmentGeneration() throws Exception
  {

    final ParallelIndexSupervisorTask task =
        newTask(TIMESTAMP_SPEC, DIMENSIONS_SPEC, INPUT_FORMAT, null, INTERVAL_TO_INDEX, inputDir,
                "test_*",
                new HashedPartitionsSpec(null, 3,
                                         ImmutableList.of("dim1", "dim2")
                ),
                2, false
        );

    final TaskActionClient actionClient = createActionClient(task);
    final TaskToolbox toolbox = createTaskToolbox(task, actionClient);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));

    final TaskStatus taskStatus = task.run(toolbox);

    Assert.assertTrue(taskStatus.isFailure());
    Assert.assertEquals("Hash partition task failed while in partial segment generation phase. See task logs for details",
                        taskStatus.getErrorMsg());
  }

  @Test(timeout = 5000L)
  public void testSubTaskFailsWhenAttemptedToRunWhileStopped() throws Exception
  {

    final ParallelIndexSupervisorTask task =
        newTask(TIMESTAMP_SPEC, DIMENSIONS_SPEC, INPUT_FORMAT, null, INTERVAL_TO_INDEX, inputDir,
                "test_*",
                new HashedPartitionsSpec(null, 3,
                                         ImmutableList.of("dim1", "dim2")
                ),
                2, false
        );

    final TaskActionClient actionClient = createActionClient(task);
    final TaskToolbox toolbox = createTaskToolbox(task, actionClient);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));
    task.stopGracefully(null);
    final TaskStatus taskStatus = task.run(toolbox);

    Assert.assertTrue(taskStatus.isFailure());
    Assert.assertEquals("Attempting to run a task that has been stopped. See overlord & task logs for more details.",
                        taskStatus.getErrorMsg());

  }

}
