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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.firehose.LocalFirehoseFactory;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Force and verify the failure modes for hash partitioning task
 */
public class HashPartitionTaskKillTest extends AbstractMultiPhaseParallelIndexingTest
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

  private File inputDir;


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
    // sorted input intervals
    List<Interval> inputIntervals = new ArrayList<>(intervals);
    inputIntervals.sort(Comparators.intervalsByStartThenEnd());
  }

  @Test(timeout = 5000L)
  public void failsInFirstPhase() throws Exception
  {
    final ParallelIndexSupervisorTask task =
        createTestTask(TIMESTAMP_SPEC, DIMENSIONS_SPEC, INPUT_FORMAT, null, INTERVAL_TO_INDEX, inputDir,
                "test_*",
                new HashedPartitionsSpec(null, null, // num shards is null to force it to go to first phase
                                         ImmutableList.of("dim1", "dim2")
                ),
                2, false, true, 0
        );

    final TaskActionClient actionClient = createActionClient(task);
    final TaskToolbox toolbox = createTaskToolbox(task, actionClient);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));
    task.stopGracefully(null);

    TaskStatus taskStatus = task.runHashPartitionMultiPhaseParallel(toolbox);

    Assert.assertTrue(taskStatus.isFailure());
    Assert.assertEquals(
        "Failed in phase[PHASE-1]. See task logs for details.",
        taskStatus.getErrorMsg()
    );
  }

  @Test(timeout = 5000L)
  public void failsInSecondPhase() throws Exception
  {
    final ParallelIndexSupervisorTask task =
        createTestTask(TIMESTAMP_SPEC, DIMENSIONS_SPEC, INPUT_FORMAT, null, INTERVAL_TO_INDEX, inputDir,
                "test_*",
                new HashedPartitionsSpec(null, 3,
                                         ImmutableList.of("dim1", "dim2")
                ),
                2, false, true, 0
        );

    final TaskActionClient actionClient = createActionClient(task);
    final TaskToolbox toolbox = createTaskToolbox(task, actionClient);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));
    task.stopGracefully(null);

    TaskStatus taskStatus = task.runHashPartitionMultiPhaseParallel(toolbox);

    Assert.assertTrue(taskStatus.isFailure());
    Assert.assertEquals(
        "Failed in phase[PHASE-2]. See task logs for details.",
        taskStatus.getErrorMsg()
    );
  }

  @Test(timeout = 5000L)
  public void failsInThirdPhase() throws Exception
  {
    final ParallelIndexSupervisorTask task =
        createTestTask(TIMESTAMP_SPEC, DIMENSIONS_SPEC, INPUT_FORMAT,
                null,
                INTERVAL_TO_INDEX,
                inputDir,
                "test_*",
                new HashedPartitionsSpec(null, 3,
                                         ImmutableList.of("dim1", "dim2")
                ),
                2,
                false,
                true,
                1
        );

    final TaskActionClient actionClient = createActionClient(task);
    final TaskToolbox toolbox = createTaskToolbox(task, actionClient);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));
    task.stopGracefully(null);


    TaskStatus taskStatus = task.runHashPartitionMultiPhaseParallel(toolbox);

    Assert.assertTrue(taskStatus.isFailure());
    Assert.assertEquals(
        "Failed in phase[PHASE-3]. See task logs for details.",
        taskStatus.getErrorMsg()
    );
  }

  private ParallelIndexSupervisorTask createTestTask(
      @Nullable TimestampSpec timestampSpec,
      @Nullable DimensionsSpec dimensionsSpec,
      @Nullable InputFormat inputFormat,
      @Nullable ParseSpec parseSpec,
      Interval interval,
      File inputDir,
      String filter,
      PartitionsSpec partitionsSpec,
      int maxNumConcurrentSubTasks,
      boolean appendToExisting,
      boolean useInputFormatApi,
      int succeedsBeforeFailing
  )
  {
    GranularitySpec granularitySpec = new UniformGranularitySpec(
        SEGMENT_GRANULARITY,
        Granularities.MINUTE,
        interval == null ? null : Collections.singletonList(interval)
    );

    ParallelIndexTuningConfig tuningConfig = newTuningConfig(
        partitionsSpec,
        maxNumConcurrentSubTasks,
        !appendToExisting
    );

    final ParallelIndexIngestionSpec ingestionSpec;

    if (useInputFormatApi) {
      Preconditions.checkArgument(parseSpec == null);
      ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
          null,
          new LocalInputSource(inputDir, filter),
          inputFormat,
          appendToExisting,
          null
      );
      ingestionSpec = new ParallelIndexIngestionSpec(
          new DataSchema(
              DATASOURCE,
              timestampSpec,
              dimensionsSpec,
              new AggregatorFactory[]{new LongSumAggregatorFactory("val", "val")},
              granularitySpec,
              null
          ),
          ioConfig,
          tuningConfig
      );
    } else {
      Preconditions.checkArgument(inputFormat == null);
      ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
          new LocalFirehoseFactory(inputDir, filter, null),
          appendToExisting
      );
      //noinspection unchecked
      ingestionSpec = new ParallelIndexIngestionSpec(
          new DataSchema(
              "dataSource",
              getObjectMapper().convertValue(
                  new StringInputRowParser(parseSpec, null),
                  Map.class
              ),
              new AggregatorFactory[]{
                  new LongSumAggregatorFactory("val", "val")
              },
              granularitySpec,
              null,
              getObjectMapper()
          ),
          ioConfig,
          tuningConfig
      );
    }

    return new ParallelIndexSupervisorTaskTest(
        null,
        null,
        null,
        ingestionSpec,
        null,
        Collections.emptyMap(),
        succeedsBeforeFailing
    );
  }

  static class ParallelIndexSupervisorTaskTest extends ParallelIndexSupervisorTask
  {
    private final int succeedsBeforeFailing;
    private int numRuns = 0;

    public ParallelIndexSupervisorTaskTest(
        String id,
        @Nullable String groupId,
        TaskResource taskResource,
        ParallelIndexIngestionSpec ingestionSchema,
        @Nullable String baseSubtaskSpecName,
        Map<String, Object> context,
        int succedsBeforeFailing
    )
    {
      super(id, groupId, taskResource, ingestionSchema, baseSubtaskSpecName, context);
      this.succeedsBeforeFailing = succedsBeforeFailing;
    }

    @Override
    <T extends Task, R extends SubTaskReport> ParallelIndexTaskRunner<T, R> createRunner(
        TaskToolbox toolbox,
        Function<TaskToolbox, ParallelIndexTaskRunner<T, R>> runnerCreator
    )
    {

      // for the hash partition task it is kind of hacky to figure out what is the failure phase
      // basically we force the failure in first phase by having numShards being null (this is
      // determined by the implementation of the run method -- which may change and suddenly this test
      // will break requiring messing with the logic below).
      // For the other two subsequent failures we need to have numShards non-null, so it bypasses
      // the first failure, so the conditions for failure in the different phase are given below:
      ParallelIndexTaskRunner<T, R> retVal;
      if (succeedsBeforeFailing == 0
          && this.getIngestionSchema().getTuningConfig().getNumShards() == null) {
        retVal = (ParallelIndexTaskRunner<T, R>) new TestRunner(false, "PHASE-1");
      } else if (succeedsBeforeFailing == 0
                 && this.getIngestionSchema().getTuningConfig().getNumShards() != null) {
        retVal = (ParallelIndexTaskRunner<T, R>) new TestRunner(false, "PHASE-2");
      } else if (succeedsBeforeFailing == 1
                 && numRuns == 1
                 && this.getIngestionSchema().getTuningConfig().getNumShards() != null) {
        retVal = (ParallelIndexTaskRunner<T, R>) new TestRunner(false, "PHASE-3");
      } else {
        numRuns++;
        retVal = (ParallelIndexTaskRunner<T, R>) new TestRunner(true, "SUCCESFUL-PHASE");
      }
      return retVal;
    }
  }

  static class TestRunner
      implements ParallelIndexTaskRunner<PartialDimensionCardinalityTask, DimensionCardinalityReport>
  {

    // These variables are at the class level since they are used to controlling after how many invocations of
    // run the runner should fail
    private final boolean succeeds;
    private final String phase;

    TestRunner(boolean succeeds, String phase)
    {
      this.succeeds = succeeds;
      this.phase = phase;
    }

    @Override
    public String getName()
    {
      if (succeeds) {
        return StringUtils.format(phase);
      } else {
        return StringUtils.format(phase);
      }
    }

    @Override
    public TaskState run() 
    {
      if (succeeds) {
        return TaskState.SUCCESS;
      }
      return TaskState.FAILED;
    }

    @Override
    public void stopGracefully(String stopReason)
    {

    }

    @Override
    public String getStopReason()
    {
      return null;
    }

    @Override
    public void collectReport(DimensionCardinalityReport report)
    {

    }

    @Override
    public Map<String, DimensionCardinalityReport> getReports()
    {
      return Collections.emptyMap();
    }

    @Override
    public ParallelIndexingPhaseProgress getProgress()
    {
      return null;
    }

    @Override
    public Set<String> getRunningTaskIds()
    {
      return null;
    }

    @Override
    public List<SubTaskSpec<PartialDimensionCardinalityTask>> getSubTaskSpecs()
    {
      return null;
    }

    @Override
    public List<SubTaskSpec<PartialDimensionCardinalityTask>> getRunningSubTaskSpecs()
    {
      return null;
    }

    @Override
    public List<SubTaskSpec<PartialDimensionCardinalityTask>> getCompleteSubTaskSpecs()
    {
      return null;
    }

    @Nullable
    @Override
    public SubTaskSpec<PartialDimensionCardinalityTask> getSubTaskSpec(String subTaskSpecId)
    {
      return null;
    }

    @Nullable
    @Override
    public SubTaskSpecStatus getSubTaskState(String subTaskSpecId)
    {
      return null;
    }

    @Nullable
    @Override
    public TaskHistory<PartialDimensionCardinalityTask> getCompleteSubTaskSpecAttemptHistory(String subTaskSpecId)
    {
      return null;
    }

  }


}
