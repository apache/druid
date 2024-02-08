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

import com.google.common.collect.Iterables;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.NoopInputFormat;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.hamcrest.CoreMatchers;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class ParallelIndexSupervisorTaskKillTest extends AbstractParallelIndexSupervisorTaskTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public ParallelIndexSupervisorTaskKillTest()
  {
    // We don't need to emulate transient failures for this test.
    super(0.0, 0.0);
  }

  @After
  public void teardown()
  {
    temporaryFolder.delete();
  }

  @Test(timeout = 5000L)
  public void testStopGracefully() throws Exception
  {
    final ParallelIndexSupervisorTask task = newTask(
        Intervals.of("2017/2018"),
        new ParallelIndexIOConfig(
            null,
            // Sub tasks would run forever
            new TestInputSource(Pair.of(new TestInput(Integer.MAX_VALUE, TaskState.SUCCESS), 4)),
            new NoopInputFormat(),
            false,
            null
        )
    );
    getIndexingServiceClient().runTask(task.getId(), task);
    while (task.getCurrentRunner() == null) {
      Thread.sleep(100);
    }
    task.stopGracefully(null);
    expectedException.expect(RuntimeException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(ExecutionException.class));
    getIndexingServiceClient().waitToFinish(task, 3000L, TimeUnit.MILLISECONDS);

    final SinglePhaseParallelIndexTaskRunner runner = (SinglePhaseParallelIndexTaskRunner) task.getCurrentRunner();
    Assert.assertTrue(runner.getRunningTaskIds().isEmpty());
    // completeSubTaskSpecs should be empty because no task has reported its status to TaskMonitor
    Assert.assertTrue(runner.getCompleteSubTaskSpecs().isEmpty());

    Assert.assertEquals(4, runner.getTaskMonitor().getNumCanceledTasks());
  }

  @Test(timeout = 5000L)
  public void testSubTaskFail() throws Exception
  {
    final ParallelIndexSupervisorTask task = newTask(
        Intervals.of("2017/2018"),
        new ParallelIndexIOConfig(
            null,
            new TestInputSource(
                Pair.of(new TestInput(10L, TaskState.FAILED), 1),
                Pair.of(new TestInput(Integer.MAX_VALUE, TaskState.FAILED), 3)
            ),
            new NoopInputFormat(),
            false,
            null
        )
    );
    final TaskActionClient actionClient = createActionClient(task);
    final TaskToolbox toolbox = createTaskToolbox(task, actionClient);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));

    final TaskStatus taskStatus = task.run(toolbox);
    Assert.assertEquals("Failed in phase[segment generation]. See task logs for details.",
                        taskStatus.getErrorMsg());
    Assert.assertEquals(TaskState.FAILED, taskStatus.getStatusCode());

    final SinglePhaseParallelIndexTaskRunner runner = (SinglePhaseParallelIndexTaskRunner) task.getCurrentRunner();
    Assert.assertTrue(runner.getRunningTaskIds().isEmpty());
    final List<SubTaskSpec<SinglePhaseSubTask>> completeSubTaskSpecs = runner.getCompleteSubTaskSpecs();
    Assert.assertEquals(1, completeSubTaskSpecs.size());
    final TaskHistory<SinglePhaseSubTask> history = runner.getCompleteSubTaskSpecAttemptHistory(
        completeSubTaskSpecs.get(0).getId()
    );
    Assert.assertNotNull(history);
    Assert.assertEquals(3, history.getAttemptHistory().size());
    for (TaskStatusPlus status : history.getAttemptHistory()) {
      Assert.assertEquals(TaskState.FAILED, status.getStatusCode());
    }

    Assert.assertEquals(3, runner.getTaskMonitor().getNumCanceledTasks());
  }

  private ParallelIndexSupervisorTask newTask(
      Interval interval,
      ParallelIndexIOConfig ioConfig
  )
  {
    final TestInputSource inputSource = (TestInputSource) ioConfig.getInputSource();
    final int numTotalSubTasks = inputSource.estimateNumSplits(new NoopInputFormat(), null);
    // set up ingestion spec
    final ParallelIndexIngestionSpec ingestionSpec = new ParallelIndexIngestionSpec(
        new DataSchema(
            "dataSource",
            DEFAULT_TIMESTAMP_SPEC,
            DEFAULT_DIMENSIONS_SPEC,
            new AggregatorFactory[]{
                new LongSumAggregatorFactory("val", "val")
            },
            new UniformGranularitySpec(
                Granularities.DAY,
                Granularities.MINUTE,
                interval == null ? null : Collections.singletonList(interval)
            ),
            null
        ),
        ioConfig,
        new ParallelIndexTuningConfig(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            numTotalSubTasks,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        )
    );

    // set up test tools
    return new TestSupervisorTask(
        ingestionSpec,
        Collections.singletonMap(AbstractParallelIndexSupervisorTaskTest.DISABLE_TASK_INJECT_CONTEXT_KEY, true)
    );
  }

  private static class TestInput
  {
    private final long runTime;
    private final TaskState finalState;

    private TestInput(long runTime, TaskState finalState)
    {
      this.runTime = runTime;
      this.finalState = finalState;
    }
  }

  private static class TestInputSource extends AbstractInputSource implements SplittableInputSource<TestInput>
  {
    private final List<InputSplit<TestInput>> splits;

    @SafeVarargs
    private TestInputSource(Pair<TestInput, Integer>... inputSpecs)
    {
      splits = new ArrayList<>();
      for (Pair<TestInput, Integer> inputSpec : inputSpecs) {
        final int numInputs = inputSpec.rhs;
        for (int i = 0; i < numInputs; i++) {
          splits.add(new InputSplit<>(new TestInput(inputSpec.lhs.runTime, inputSpec.lhs.finalState)));
        }
      }
    }

    private TestInputSource(InputSplit<TestInput> split)
    {
      this.splits = Collections.singletonList(split);
    }

    @Override
    public Stream<InputSplit<TestInput>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      return splits.stream();
    }

    @Override
    public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      return splits.size();
    }

    @Override
    public SplittableInputSource<TestInput> withSplit(InputSplit<TestInput> split)
    {
      return new TestInputSource(split);
    }

    @Override
    public boolean needsFormat()
    {
      return false;
    }
  }

  private static class TestSupervisorTask extends TestParallelIndexSupervisorTask
  {
    private TestSupervisorTask(
        ParallelIndexIngestionSpec ingestionSchema,
        Map<String, Object> context
    )
    {
      super(
          null,
          null,
          ingestionSchema,
          context
      );
    }

    @Override
    SinglePhaseParallelIndexTaskRunner createSinglePhaseTaskRunner(TaskToolbox toolbox)
    {
      return new TestRunner(
          toolbox,
          this
      );
    }
  }

  private static class TestRunner extends SinglePhaseParallelIndexTaskRunner
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    private TestRunner(
        TaskToolbox toolbox,
        ParallelIndexSupervisorTask supervisorTask
    )
    {
      super(
          toolbox,
          supervisorTask.getId(),
          supervisorTask.getGroupId(),
          supervisorTask.getIngestionSchema(),
          supervisorTask.getContext()
      );
      this.supervisorTask = supervisorTask;
    }

    @Override
    SinglePhaseSubTaskSpec newTaskSpec(InputSplit split)
    {
      final SplittableInputSource baseInputSource = (SplittableInputSource) getIngestionSchema()
          .getIOConfig()
          .getInputSource();
      return new TestSinglePhaseSubTaskSpec(
          supervisorTask.getId() + "_" + getAndIncrementNextSpecId(),
          supervisorTask.getGroupId(),
          supervisorTask,
          new ParallelIndexIngestionSpec(
              getIngestionSchema().getDataSchema(),
              new ParallelIndexIOConfig(
                  null,
                  baseInputSource.withSplit(split),
                  getIngestionSchema().getIOConfig().getInputFormat(),
                  getIngestionSchema().getIOConfig().isAppendToExisting(),
                  getIngestionSchema().getIOConfig().isDropExisting()
              ),
              getIngestionSchema().getTuningConfig()
          ),
          supervisorTask.getContext(),
          split
      );
    }
  }

  private static class TestSinglePhaseSubTaskSpec extends SinglePhaseSubTaskSpec
  {
    private TestSinglePhaseSubTaskSpec(
        String id,
        String groupId,
        ParallelIndexSupervisorTask supervisorTask,
        ParallelIndexIngestionSpec ingestionSpec,
        Map<String, Object> context,
        InputSplit inputSplit
    )
    {
      super(id, groupId, supervisorTask.getId(), ingestionSpec, context, inputSplit);
    }

    @Override
    public SinglePhaseSubTask newSubTask(int numAttempts)
    {
      return new TestSinglePhaseSubTask(
          null,
          getGroupId(),
          null,
          getSupervisorTaskId(),
          getId(),
          numAttempts,
          getIngestionSpec(),
          getContext()
      );
    }
  }

  private static class TestSinglePhaseSubTask extends SinglePhaseSubTask
  {
    private TestSinglePhaseSubTask(
        @Nullable String id,
        String groupId,
        TaskResource taskResource,
        String supervisorTaskId,
        String subtaskSpecId,
        int numAttempts,
        ParallelIndexIngestionSpec ingestionSchema,
        Map<String, Object> context
    )
    {
      super(
          id,
          groupId,
          taskResource,
          supervisorTaskId,
          subtaskSpecId,
          numAttempts,
          ingestionSchema,
          context
      );
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient)
    {
      return true;
    }

    @Override
    public TaskStatus runTask(final TaskToolbox toolbox) throws Exception
    {
      final TestInputSource inputSource = (TestInputSource) getIngestionSchema().getIOConfig().getInputSource();
      final TestInput testInput = Iterables.getOnlyElement(inputSource.splits).get();
      Thread.sleep(testInput.runTime);
      return TaskStatus.fromCode(getId(), testInput.finalState);
    }
  }
}
