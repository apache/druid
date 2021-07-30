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
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringDistribution;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringSketch;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Force and verify the failure modes for range partitioning task
 */
public class RangePartitionTaskKillTest extends AbstractMultiPhaseParallelIndexingTest
{
  private static final int NUM_PARTITION = 2;
  private static final int NUM_ROW = 20;
  private static final int DIM_FILE_CARDINALITY = 2;
  private static final int YEAR = 2017;
  private static final Interval INTERVAL_TO_INDEX = Intervals.of("%s-12/P1M", YEAR);
  private static final String TIME = "ts";
  private static final String DIM1 = "dim1";
  private static final String DIM2 = "dim2";
  private static final String LIST_DELIMITER = "|";
  private static final String TEST_FILE_NAME_PREFIX = "test_";
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec(TIME, "auto", null);
  private static final DimensionsSpec DIMENSIONS_SPEC = new DimensionsSpec(
      DimensionsSpec.getDefaultSchemas(Arrays.asList(TIME, DIM1, DIM2))
  );

  private static final InputFormat INPUT_FORMAT = new CsvInputFormat(
      Arrays.asList(TIME, DIM1, DIM2, "val"),
      LIST_DELIMITER,
      false,
      false,
      0
  );

  private File inputDir;

  public RangePartitionTaskKillTest()
  {
    super(LockGranularity.SEGMENT, true, DEFAULT_TRANSIENT_TASK_FAILURE_RATE, DEFAULT_TRANSIENT_API_FAILURE_RATE);
  }

  @Before
  public void setup() throws IOException
  {
    inputDir = temporaryFolder.newFolder("data");
  }

  @Test(timeout = 5000L)
  public void failsFirstPhase() throws Exception
  {
    int targetRowsPerSegment = NUM_ROW * 2 / DIM_FILE_CARDINALITY / NUM_PARTITION;
    final ParallelIndexSupervisorTask task =
        newTask(TIMESTAMP_SPEC,
                DIMENSIONS_SPEC,
                INPUT_FORMAT,
                null,
                INTERVAL_TO_INDEX,
                inputDir,
                TEST_FILE_NAME_PREFIX + "*",
                new SingleDimensionPartitionsSpec(
                    targetRowsPerSegment,
                    null,
                    DIM1,
                    false
                ),
                2,
                false,
                0
        );

    final TaskActionClient actionClient = createActionClient(task);
    final TaskToolbox toolbox = createTaskToolbox(task, actionClient);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));
    task.stopGracefully(null);


    TaskStatus taskStatus = task.runRangePartitionMultiPhaseParallel(toolbox);

    Assert.assertTrue(taskStatus.isFailure());
    Assert.assertEquals(
        "Failed in phase[PHASE-1]. See task logs for details.",
        taskStatus.getErrorMsg()
    );
  }

  @Test(timeout = 5000L)
  public void failsSecondPhase() throws Exception
  {
    int targetRowsPerSegment = NUM_ROW * 2 / DIM_FILE_CARDINALITY / NUM_PARTITION;
    final ParallelIndexSupervisorTask task =
        newTask(TIMESTAMP_SPEC,
                DIMENSIONS_SPEC,
                INPUT_FORMAT,
                null,
                INTERVAL_TO_INDEX,
                inputDir,
                TEST_FILE_NAME_PREFIX + "*",
                new SingleDimensionPartitionsSpec(
                    targetRowsPerSegment,
                    null,
                    DIM1,
                    false
                ),
                2,
                false,
                1
        );

    final TaskActionClient actionClient = createActionClient(task);
    final TaskToolbox toolbox = createTaskToolbox(task, actionClient);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));
    task.stopGracefully(null);


    TaskStatus taskStatus = task.runRangePartitionMultiPhaseParallel(toolbox);

    Assert.assertTrue(taskStatus.isFailure());
    Assert.assertEquals(
        "Failed in phase[PHASE-2]. See task logs for details.",
        taskStatus.getErrorMsg()
    );
  }

  @Test(timeout = 5000L)
  public void failsThirdPhase() throws Exception
  {
    int targetRowsPerSegment = NUM_ROW * 2 / DIM_FILE_CARDINALITY / NUM_PARTITION;
    final ParallelIndexSupervisorTask task =
        newTask(TIMESTAMP_SPEC,
                DIMENSIONS_SPEC,
                INPUT_FORMAT,
                null,
                INTERVAL_TO_INDEX,
                inputDir,
                TEST_FILE_NAME_PREFIX + "*",
                new SingleDimensionPartitionsSpec(
                    targetRowsPerSegment,
                    null,
                    DIM1,
                    false
                ),
                2,
                false,
                2
        );

    final TaskActionClient actionClient = createActionClient(task);
    final TaskToolbox toolbox = createTaskToolbox(task, actionClient);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));
    task.stopGracefully(null);


    TaskStatus taskStatus = task.runRangePartitionMultiPhaseParallel(toolbox);

    Assert.assertTrue(taskStatus.isFailure());
    Assert.assertEquals(
        "Failed in phase[PHASE-3]. See task logs for details.",
        taskStatus.getErrorMsg()
    );
  }


  static class ParallelIndexSupervisorTaskTest extends ParallelIndexSupervisorTask
  {
    // These variables control how many runners get created until it fails:
    private final int succeedsBeforeFailing;
    private int numRuns;

    // These maps are a hacky way to provide some sort of mock object in the runner to make the run continue
    // until it fails (whatever they contain is nonsense other that it allows the code to make progress):
    private final Map<String, DimensionDistributionReport> firstMap;
    private final Map<String, DimensionDistributionReport> secondMap;

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

      this.firstMap = new HashMap<>();
      Map<Interval, StringDistribution> intervalToDistribution = new HashMap<>();
      intervalToDistribution.put(Intervals.of("2011-04-01/2011-04-02"), new StringSketch());
      this.firstMap.put("A", new DimensionDistributionReport("id", intervalToDistribution));

      this.secondMap = Collections.emptyMap();

    }

    @Override
    <T extends Task, R extends SubTaskReport> ParallelIndexTaskRunner<T, R> createRunner(
        TaskToolbox toolbox,
        Function<TaskToolbox, ParallelIndexTaskRunner<T, R>> runnerCreator
    )
    {

      // Below are the conditions to determine phase:
      ParallelIndexTaskRunner<T, R> retVal;
      if (succeedsBeforeFailing == 0) {
        retVal = (ParallelIndexTaskRunner<T, R>) new TestRunner(false, firstMap, "PHASE-1");
      } else if (succeedsBeforeFailing == 1
                 && numRuns == 1) {
        retVal = (ParallelIndexTaskRunner<T, R>) new TestRunner(false, secondMap, "PHASE-2");
      } else if (succeedsBeforeFailing == 2
                 && numRuns == 2) {
        retVal = (ParallelIndexTaskRunner<T, R>) new TestRunner(false, secondMap, "PHASE-3");
      } else {
        numRuns++;
        Map<String, DimensionDistributionReport> map;
        if (numRuns < 2) {
          map = firstMap;
        } else {
          map = secondMap;
        }
        retVal = (ParallelIndexTaskRunner<T, R>) new TestRunner(true, map, "SUCCESFUL-PHASE");
      }
      return retVal;
    }
  }

  static class TestRunner
      implements ParallelIndexTaskRunner<PartialDimensionDistributionTask, DimensionDistributionReport>
  {

    private final boolean succeeds;
    private final String phase;

    private final Map<String, DimensionDistributionReport> distributionMap;

    TestRunner(boolean succeeds, Map<String, DimensionDistributionReport> distributionMap, String phase)
    {
      this.succeeds = succeeds;
      this.distributionMap = distributionMap;
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
    public void stopGracefully()
    {

    }

    @Override
    public void collectReport(DimensionDistributionReport report)
    {

    }

    @Override
    public Map<String, DimensionDistributionReport> getReports()
    {
      return distributionMap;
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
    public List<SubTaskSpec<PartialDimensionDistributionTask>> getSubTaskSpecs()
    {
      return null;
    }

    @Override
    public List<SubTaskSpec<PartialDimensionDistributionTask>> getRunningSubTaskSpecs()
    {
      return null;
    }

    @Override
    public List<SubTaskSpec<PartialDimensionDistributionTask>> getCompleteSubTaskSpecs()
    {
      return null;
    }

    @Nullable
    @Override
    public SubTaskSpec<PartialDimensionDistributionTask> getSubTaskSpec(String subTaskSpecId)
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
    public TaskHistory<PartialDimensionDistributionTask> getCompleteSubTaskSpecAttemptHistory(String subTaskSpecId)
    {
      return null;
    }

  }

  protected ParallelIndexSupervisorTask newTask(
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

}
