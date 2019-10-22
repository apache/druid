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
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.hamcrest.CoreMatchers;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class ParallelIndexSupervisorTaskKillTest extends AbstractParallelIndexSupervisorTaskTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private ExecutorService service;

  @Before
  public void setup() throws IOException
  {
    indexingServiceClient = new LocalIndexingServiceClient();
    localDeepStorage = temporaryFolder.newFolder("localStorage");
    service = Execs.singleThreaded("ParallelIndexSupervisorTaskKillTest-%d");
  }

  @After
  public void teardown()
  {
    indexingServiceClient.shutdown();
    temporaryFolder.delete();
    service.shutdownNow();
  }

  @Test(timeout = 5000L)
  public void testStopGracefully() throws Exception
  {
    final ParallelIndexSupervisorTask task = newTask(
        Intervals.of("2017/2018"),
        new ParallelIndexIOConfig(
            // Sub tasks would run forever
            new TestFirehoseFactory(Pair.of(new TestInput(Integer.MAX_VALUE, TaskState.SUCCESS), 4)),
            false
        )
    );
    actionClient = createActionClient(task);
    toolbox = createTaskToolbox(task);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));

    final Future<TaskState> future = service.submit(() -> task.run(toolbox).getStatusCode());
    while (task.getCurrentRunner() == null) {
      Thread.sleep(100);
    }
    task.stopGracefully(null);
    expectedException.expect(ExecutionException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(InterruptedException.class));
    future.get();

    final TestSinglePhaseParallelIndexTaskRunner runner = (TestSinglePhaseParallelIndexTaskRunner) task.getCurrentRunner();
    Assert.assertTrue(runner.getRunningTaskIds().isEmpty());
    // completeSubTaskSpecs should be empty because no task has reported its status to TaskMonitor
    Assert.assertTrue(runner.getCompleteSubTaskSpecs().isEmpty());

    Assert.assertEquals(4, runner.getTaskMonitor().getNumKilledTasks());
  }

  @Test(timeout = 5000L)
  public void testSubTaskFail() throws Exception
  {
    final ParallelIndexSupervisorTask task = newTask(
        Intervals.of("2017/2018"),
        new ParallelIndexIOConfig(
            new TestFirehoseFactory(
                Pair.of(new TestInput(10L, TaskState.FAILED), 1),
                Pair.of(new TestInput(Integer.MAX_VALUE, TaskState.FAILED), 3)
            ),
            false
        )
    );
    actionClient = createActionClient(task);
    toolbox = createTaskToolbox(task);

    prepareTaskForLocking(task);
    Assert.assertTrue(task.isReady(actionClient));

    final TaskState state = task.run(toolbox).getStatusCode();
    Assert.assertEquals(TaskState.FAILED, state);

    final TestSinglePhaseParallelIndexTaskRunner runner = (TestSinglePhaseParallelIndexTaskRunner) task.getCurrentRunner();
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

    Assert.assertEquals(3, runner.getTaskMonitor().getNumKilledTasks());
  }

  private ParallelIndexSupervisorTask newTask(
      Interval interval,
      ParallelIndexIOConfig ioConfig
  )
  {
    final TestFirehoseFactory firehoseFactory = (TestFirehoseFactory) ioConfig.getFirehoseFactory();
    final int numTotalSubTasks = firehoseFactory.getNumSplits(null);
    // set up ingestion spec
    final ParallelIndexIngestionSpec ingestionSpec = new ParallelIndexIngestionSpec(
        new DataSchema(
            "dataSource",
            getObjectMapper().convertValue(
                new StringInputRowParser(
                    DEFAULT_PARSE_SPEC,
                    null
                ),
                Map.class
            ),
            new AggregatorFactory[]{
                new LongSumAggregatorFactory("val", "val")
            },
            new UniformGranularitySpec(
                Granularities.DAY,
                Granularities.MINUTE,
                interval == null ? null : Collections.singletonList(interval)
            ),
            null,
            getObjectMapper()
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
            numTotalSubTasks,
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
        Collections.emptyMap(),
        indexingServiceClient
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

  private static class TestFirehoseFactory implements FiniteFirehoseFactory<StringInputRowParser, TestInput>
  {
    private final List<InputSplit<TestInput>> splits;

    @SafeVarargs
    private TestFirehoseFactory(Pair<TestInput, Integer>... inputSpecs)
    {
      splits = new ArrayList<>();
      for (Pair<TestInput, Integer> inputSpec : inputSpecs) {
        final int numInputs = inputSpec.rhs;
        for (int i = 0; i < numInputs; i++) {
          splits.add(new InputSplit<>(new TestInput(inputSpec.lhs.runTime, inputSpec.lhs.finalState)));
        }
      }
    }

    private TestFirehoseFactory(InputSplit<TestInput> split)
    {
      this.splits = Collections.singletonList(split);
    }

    @Override
    public Stream<InputSplit<TestInput>> getSplits(@Nullable SplitHintSpec splitHintSpec)
    {
      return splits.stream();
    }

    @Override
    public int getNumSplits(@Nullable SplitHintSpec splitHintSpec)
    {
      return splits.size();
    }

    @Override
    public FiniteFirehoseFactory<StringInputRowParser, TestInput> withSplit(InputSplit<TestInput> split)
    {
      return new TestFirehoseFactory(split);
    }
  }

  private static class TestSupervisorTask extends TestParallelIndexSupervisorTask
  {
    private final IndexingServiceClient indexingServiceClient;

    private TestSupervisorTask(
        ParallelIndexIngestionSpec ingestionSchema,
        Map<String, Object> context,
        IndexingServiceClient indexingServiceClient
    )
    {
      super(
          null,
          null,
          ingestionSchema,
          context,
          indexingServiceClient
      );
      this.indexingServiceClient = indexingServiceClient;
    }

    @Override
    SinglePhaseParallelIndexTaskRunner createSinglePhaseTaskRunner(TaskToolbox toolbox)
    {
      return new TestRunner(
          toolbox,
          this,
          indexingServiceClient
      );
    }
  }

  private static class TestRunner extends TestSinglePhaseParallelIndexTaskRunner
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    private TestRunner(
        TaskToolbox toolbox,
        ParallelIndexSupervisorTask supervisorTask,
        IndexingServiceClient indexingServiceClient
    )
    {
      super(
          toolbox,
          supervisorTask.getId(),
          supervisorTask.getGroupId(),
          supervisorTask.getIngestionSchema(),
          supervisorTask.getContext(),
          indexingServiceClient
      );
      this.supervisorTask = supervisorTask;
    }

    @Override
    SinglePhaseSubTaskSpec newTaskSpec(InputSplit split)
    {
      final FiniteFirehoseFactory baseFirehoseFactory = (FiniteFirehoseFactory) getIngestionSchema()
          .getIOConfig()
          .getFirehoseFactory();
      return new TestSinglePhaseSubTaskSpec(
          supervisorTask.getId() + "_" + getAndIncrementNextSpecId(),
          supervisorTask.getGroupId(),
          supervisorTask,
          new ParallelIndexIngestionSpec(
              getIngestionSchema().getDataSchema(),
              new ParallelIndexIOConfig(
                  baseFirehoseFactory.withSplit(split),
                  getIngestionSchema().getIOConfig().isAppendToExisting()
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
    private final ParallelIndexSupervisorTask supervisorTask;

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
      this.supervisorTask = supervisorTask;
    }

    @Override
    public SinglePhaseSubTask newSubTask(int numAttempts)
    {
      return new TestSinglePhaseSubTask(
          null,
          getGroupId(),
          null,
          getSupervisorTaskId(),
          numAttempts,
          getIngestionSpec(),
          getContext(),
          null,
          new LocalParallelIndexTaskClientFactory(supervisorTask)
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
        int numAttempts,
        ParallelIndexIngestionSpec ingestionSchema,
        Map<String, Object> context,
        IndexingServiceClient indexingServiceClient,
        IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory
    )
    {
      super(
          id,
          groupId,
          taskResource,
          supervisorTaskId,
          numAttempts,
          ingestionSchema,
          context,
          indexingServiceClient,
          taskClientFactory,
          new TestAppenderatorsManager()
      );
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient)
    {
      return true;
    }

    @Override
    public TaskStatus run(final TaskToolbox toolbox) throws Exception
    {
      final TestFirehoseFactory firehoseFactory = (TestFirehoseFactory) getIngestionSchema().getIOConfig()
                                                                                            .getFirehoseFactory();
      final TestInput testInput = Iterables.getOnlyElement(firehoseFactory.splits).get();
      Thread.sleep(testInput.runTime);
      return TaskStatus.fromCode(getId(), testInput.finalState);
    }
  }
}
