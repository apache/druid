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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.NoopInputFormat;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.SegmentAllocators;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTaskRunner.SubTaskSpecStatus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ParallelIndexSupervisorTaskResourceTest extends AbstractParallelIndexSupervisorTaskTest
{
  private static final int NUM_SUB_TASKS = 10;

  /**
   * specId -> spec
   */
  private final ConcurrentMap<String, SinglePhaseSubTaskSpec> subTaskSpecs = new ConcurrentHashMap<>();

  /**
   * specId -> taskStatusPlus
   */
  private final ConcurrentMap<String, TaskStatusPlus> runningSpecs = new ConcurrentHashMap<>();

  /**
   * specId -> taskStatusPlus list
   */
  private final ConcurrentHashMap<String, List<TaskStatusPlus>> taskHistories = new ConcurrentHashMap<>();

  /**
   * taskId -> subTaskSpec
   */
  private final ConcurrentMap<String, SinglePhaseSubTaskSpec> taskIdToSpec = new ConcurrentHashMap<>();

  /**
   * taskId -> task
   */
  private final CopyOnWriteArrayList<TestSubTask> runningTasks = new CopyOnWriteArrayList<>();

  private TestSupervisorTask task;

  public ParallelIndexSupervisorTaskResourceTest()
  {
    // We don't need to emulate transient failures for this test.
    super(0.0, 0.0);
  }

  @After
  public void teardown()
  {
    temporaryFolder.delete();
  }

  @Test(timeout = 20000L)
  public void testAPIs() throws Exception
  {
    task = newTask(
        Intervals.of("2017/2018"),
        new ParallelIndexIOConfig(
            null,
            new TestInputSource(IntStream.range(0, NUM_SUB_TASKS).boxed().collect(Collectors.toList())),
            new NoopInputFormat(),
            false,
            null
        )
    );
    getIndexingServiceClient().runTask(task.getId(), task);
    Thread.sleep(1000);

    final SinglePhaseParallelIndexTaskRunner runner = (SinglePhaseParallelIndexTaskRunner) task.getCurrentRunner();
    Assert.assertNotNull("runner is null", runner);

    // test getMode
    Response response = task.getMode(newRequest());
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals("parallel", response.getEntity());

    // test expectedNumSucceededTasks
    response = task.getProgress(newRequest());
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        NUM_SUB_TASKS,
        ((ParallelIndexingPhaseProgress) response.getEntity()).getEstimatedExpectedSucceeded()
    );

    // Since taskMonitor works based on polling, it's hard to use a fancier way to check its state.
    // We use polling to check the state of taskMonitor in this test.
    while (getNumSubTasks(ParallelIndexingPhaseProgress::getRunning) < NUM_SUB_TASKS) {
      Thread.sleep(100);
    }

    int succeededTasks = 0;
    int failedTasks = 0;
    checkState(
        succeededTasks,
        failedTasks,
        buildStateMap()
    );

    // numRunningTasks and numSucceededTasks after some successful subTasks
    succeededTasks += 2;
    for (int i = 0; i < succeededTasks; i++) {
      runningTasks.get(0).setState(TaskState.SUCCESS);
    }

    while (getNumSubTasks(ParallelIndexingPhaseProgress::getSucceeded) < succeededTasks) {
      Thread.sleep(100);
    }

    checkState(
        succeededTasks,
        failedTasks,
        buildStateMap()
    );

    // numRunningTasks and numSucceededTasks after some failed subTasks
    failedTasks += 3;
    for (int i = 0; i < failedTasks; i++) {
      runningTasks.get(0).setState(TaskState.FAILED);
    }

    // Wait for new tasks to be started
    while (getNumSubTasks(ParallelIndexingPhaseProgress::getFailed) < failedTasks
           || runningTasks.size() < NUM_SUB_TASKS - succeededTasks) {
      Thread.sleep(100);
    }

    checkState(
        succeededTasks,
        failedTasks,
        buildStateMap()
    );

    // Make sure only one subTask is running
    succeededTasks += 7;
    for (int i = 0; i < 7; i++) {
      runningTasks.get(0).setState(TaskState.SUCCESS);
    }

    while (getNumSubTasks(ParallelIndexingPhaseProgress::getSucceeded) < succeededTasks) {
      Thread.sleep(100);
    }

    checkState(
        succeededTasks,
        failedTasks,
        buildStateMap()
    );

    Assert.assertEquals(1, runningSpecs.size());
    final String lastRunningSpecId = runningSpecs.keySet().iterator().next();
    final List<TaskStatusPlus> taskHistory = taskHistories.get(lastRunningSpecId);
    // This should be a failed task history because new tasks appear later in runningTasks.
    Assert.assertEquals(1, taskHistory.size());

    // Test one more failure
    runningTasks.get(0).setState(TaskState.FAILED);
    failedTasks++;
    while (getNumSubTasks(ParallelIndexingPhaseProgress::getFailed) < failedTasks) {
      Thread.sleep(100);
    }
    while (getNumSubTasks(ParallelIndexingPhaseProgress::getRunning) < 1) {
      Thread.sleep(100);
    }

    checkState(
        succeededTasks,
        failedTasks,
        buildStateMap()
    );
    Assert.assertEquals(2, taskHistory.size());

    runningTasks.get(0).setState(TaskState.SUCCESS);
    succeededTasks++;
    while (getNumSubTasks(ParallelIndexingPhaseProgress::getSucceeded) < succeededTasks) {
      Thread.sleep(100);
    }

    Assert.assertEquals(
        TaskState.SUCCESS,
        getIndexingServiceClient().waitToFinish(task, 1000, TimeUnit.MILLISECONDS).getStatusCode()
    );
  }

  @SuppressWarnings({"ConstantConditions"})
  private int getNumSubTasks(Function<ParallelIndexingPhaseProgress, Integer> func)
  {
    final Response response = task.getProgress(newRequest());
    Assert.assertEquals(200, response.getStatus());
    return func.apply((ParallelIndexingPhaseProgress) response.getEntity());
  }

  private Map<String, SubTaskSpecStatus> buildStateMap()
  {
    final Map<String, SubTaskSpecStatus> stateMap = new HashMap<>();
    subTaskSpecs.forEach((specId, spec) -> {
      final List<TaskStatusPlus> taskHistory = taskHistories.get(specId);
      final TaskStatusPlus runningTaskStatus = runningSpecs.get(specId);
      stateMap.put(
          specId,
          new SubTaskSpecStatus(spec, runningTaskStatus, taskHistory == null ? Collections.emptyList() : taskHistory)
      );
    });
    return stateMap;
  }

  /**
   * Test all endpoints of {@link ParallelIndexSupervisorTask}.
   */
  private void checkState(
      int expectedSucceededTasks,
      int expectedFailedTask,
      Map<String, SubTaskSpecStatus> expectedSubTaskStateResponses // subTaskSpecId -> response
  )
  {
    Response response = task.getProgress(newRequest());
    Assert.assertEquals(200, response.getStatus());
    final ParallelIndexingPhaseProgress monitorStatus = (ParallelIndexingPhaseProgress) response.getEntity();

    // numRunningTasks
    Assert.assertEquals(runningTasks.size(), monitorStatus.getRunning());

    // numSucceededTasks
    Assert.assertEquals(expectedSucceededTasks, monitorStatus.getSucceeded());

    // numFailedTasks
    Assert.assertEquals(expectedFailedTask, monitorStatus.getFailed());

    // numCompleteTasks
    Assert.assertEquals(expectedSucceededTasks + expectedFailedTask, monitorStatus.getComplete());

    // numTotalTasks
    Assert.assertEquals(runningTasks.size() + expectedSucceededTasks + expectedFailedTask, monitorStatus.getTotal());

    // runningSubTasks
    response = task.getRunningTasks(newRequest());
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        runningTasks.stream().map(AbstractTask::getId).collect(Collectors.toSet()),
        new HashSet<>((Collection<String>) response.getEntity())
    );

    // subTaskSpecs
    response = task.getSubTaskSpecs(newRequest());
    Assert.assertEquals(200, response.getStatus());
    List<SubTaskSpec<SinglePhaseSubTask>> actualSubTaskSpecMap =
        (List<SubTaskSpec<SinglePhaseSubTask>>) response.getEntity();
    Assert.assertEquals(
        subTaskSpecs.keySet(),
        actualSubTaskSpecMap.stream().map(SubTaskSpec::getId).collect(Collectors.toSet())
    );

    // runningSubTaskSpecs
    response = task.getRunningSubTaskSpecs(newRequest());
    Assert.assertEquals(200, response.getStatus());
    actualSubTaskSpecMap =
        (List<SubTaskSpec<SinglePhaseSubTask>>) response.getEntity();
    Assert.assertEquals(
        runningSpecs.keySet(),
        actualSubTaskSpecMap.stream().map(SubTaskSpec::getId).collect(Collectors.toSet())
    );

    // completeSubTaskSpecs
    final List<SubTaskSpec<SinglePhaseSubTask>> completeSubTaskSpecs = expectedSubTaskStateResponses
        .entrySet()
        .stream()
        .filter(entry -> !runningSpecs.containsKey(entry.getKey()))
        .map(entry -> entry.getValue().getSpec())
        .sorted(Comparator.comparing(SubTaskSpec::getId))
        .collect(Collectors.toList());

    response = task.getCompleteSubTaskSpecs(newRequest());
    Assert.assertEquals(200, response.getStatus());
    List<SubTaskSpec<SinglePhaseSubTask>> actual = (List<SubTaskSpec<SinglePhaseSubTask>>) response.getEntity();
    actual.sort(Comparator.comparing(SubTaskSpec::getId));
    Assert.assertEquals(completeSubTaskSpecs, actual);

    // subTaskSpec
    final String subTaskId = runningSpecs.keySet().iterator().next();
    response = task.getSubTaskSpec(subTaskId, newRequest());
    Assert.assertEquals(200, response.getStatus());
    final SubTaskSpec<SinglePhaseSubTask> subTaskSpec =
        (SubTaskSpec<SinglePhaseSubTask>) response.getEntity();
    Assert.assertEquals(subTaskId, subTaskSpec.getId());

    // subTaskState
    response = task.getSubTaskState(subTaskId, newRequest());
    Assert.assertEquals(200, response.getStatus());
    final SubTaskSpecStatus expectedResponse = Preconditions.checkNotNull(
        expectedSubTaskStateResponses.get(subTaskId),
        "response for task[%s]",
        subTaskId
    );
    final SubTaskSpecStatus actualResponse = (SubTaskSpecStatus) response.getEntity();
    Assert.assertEquals(expectedResponse.getSpec().getId(), actualResponse.getSpec().getId());
    Assert.assertEquals(expectedResponse.getCurrentStatus(), actualResponse.getCurrentStatus());
    Assert.assertEquals(expectedResponse.getTaskHistory(), actualResponse.getTaskHistory());

    // completeSubTaskSpecAttemptHistory
    final String completeSubTaskSpecId = expectedSubTaskStateResponses
        .entrySet()
        .stream()
        .filter(entry -> {
          final TaskStatusPlus currentStatus = entry.getValue().getCurrentStatus();
          return currentStatus != null &&
                 (currentStatus.getStatusCode() == TaskState.SUCCESS
                  || currentStatus.getStatusCode() == TaskState.FAILED);
        })
        .map(Entry::getKey)
        .findFirst()
        .orElse(null);
    if (completeSubTaskSpecId != null) {
      response = task.getCompleteSubTaskSpecAttemptHistory(completeSubTaskSpecId, newRequest());
      Assert.assertEquals(200, response.getStatus());
      Assert.assertEquals(
          expectedSubTaskStateResponses.get(completeSubTaskSpecId).getTaskHistory(),
          response.getEntity()
      );
    }
  }

  private static HttpServletRequest newRequest()
  {
    final HttpServletRequest request = EasyMock.niceMock(HttpServletRequest.class);
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null);
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(new AuthenticationResult("test", "test", "test", Collections.emptyMap()));
    EasyMock.replay(request);
    return request;
  }

  private TestSupervisorTask newTask(
      Interval interval,
      ParallelIndexIOConfig ioConfig
  )
  {
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
            NUM_SUB_TASKS,
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
        null,
        null,
        ingestionSpec,
        Collections.singletonMap(AbstractParallelIndexSupervisorTaskTest.DISABLE_TASK_INJECT_CONTEXT_KEY, true)
    );
  }

  private static class TestInputSource extends AbstractInputSource implements SplittableInputSource<Integer>
  {
    private final List<Integer> ids;

    TestInputSource(List<Integer> ids)
    {
      this.ids = ids;
    }

    @Override
    public Stream<InputSplit<Integer>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      return ids.stream().map(InputSplit::new);
    }

    @Override
    public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      return ids.size();
    }

    @Override
    public SplittableInputSource<Integer> withSplit(InputSplit<Integer> split)
    {
      return new TestInputSource(Collections.singletonList(split.get()));
    }

    @Override
    public boolean needsFormat()
    {
      return false;
    }
  }

  private class TestSupervisorTask extends TestParallelIndexSupervisorTask
  {
    TestSupervisorTask(
        String id,
        TaskResource taskResource,
        ParallelIndexIngestionSpec ingestionSchema,
        Map<String, Object> context
    )
    {
      super(
          id,
          taskResource,
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

  private class TestRunner extends SinglePhaseParallelIndexTaskRunner
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    TestRunner(
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
      final TestInputSource baseInputSource = (TestInputSource) getIngestionSchema()
          .getIOConfig()
          .getInputSource();
      final TestSubTaskSpec spec = new TestSubTaskSpec(
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
      subTaskSpecs.put(spec.getId(), spec);
      return spec;
    }
  }

  private class TestSubTaskSpec extends SinglePhaseSubTaskSpec
  {
    TestSubTaskSpec(
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
      try {
        // taskId is suffixed by the current time and this sleep is to make sure that every sub task has different id
        Thread.sleep(10);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      final TestSubTask subTask = new TestSubTask(
          getGroupId(),
          getSupervisorTaskId(),
          getId(),
          numAttempts,
          getIngestionSpec(),
          getContext()
      );
      final TestInputSource inputSource = (TestInputSource) getIngestionSpec().getIOConfig().getInputSource();
      final InputSplit<Integer> split = inputSource.createSplits(
          getIngestionSpec().getIOConfig().getInputFormat(),
          null
      )
                                                   .findFirst()
                                                   .orElse(null);
      if (split == null) {
        throw new ISE("Split is null");
      }
      runningTasks.add(subTask);
      taskIdToSpec.put(subTask.getId(), this);
      runningSpecs.put(
          getId(),
          new TaskStatusPlus(
              subTask.getId(),
              subTask.getGroupId(),
              subTask.getType(),
              DateTimes.EPOCH,
              DateTimes.EPOCH,
              TaskState.RUNNING,
              RunnerTaskState.RUNNING,
              -1L,
              TaskLocation.unknown(),
              null,
              null
          )
      );
      return subTask;
    }
  }

  private class TestSubTask extends SinglePhaseSubTask
  {
    private volatile TaskState state = TaskState.RUNNING;

    TestSubTask(
        String groupId,
        String supervisorTaskId,
        String subtaskSpecId,
        int numAttempts,
        ParallelIndexIngestionSpec ingestionSchema,
        Map<String, Object> context
    )
    {
      super(
          null,
          groupId,
          null,
          supervisorTaskId,
          subtaskSpecId,
          numAttempts,
          ingestionSchema,
          context
      );
    }

    @Override
    public TaskStatus runTask(final TaskToolbox toolbox) throws Exception
    {
      while (state == TaskState.RUNNING) {
        Thread.sleep(100);
      }

      // build LocalParallelIndexTaskClient
      final ParallelIndexSupervisorTaskClient taskClient = toolbox.getSupervisorTaskClientProvider().build(
          getSupervisorTaskId(),
          Duration.ZERO,
          0
      );
      final DynamicPartitionsSpec partitionsSpec = (DynamicPartitionsSpec) getIngestionSchema()
          .getTuningConfig()
          .getGivenOrDefaultPartitionsSpec();
      final SegmentAllocator segmentAllocator = SegmentAllocators.forLinearPartitioning(
          toolbox,
          getSubtaskSpecId(),
          new SupervisorTaskAccess(getSupervisorTaskId(), taskClient),
          getIngestionSchema().getDataSchema(),
          getTaskLockHelper(),
          AbstractTask.computeBatchIngestionMode(getIngestionSchema().getIOConfig()),
          partitionsSpec,
          true
      );

      final SegmentIdWithShardSpec segmentIdentifier = segmentAllocator.allocate(
          new MapBasedInputRow(DateTimes.of("2017-01-01"), Collections.emptyList(), Collections.emptyMap()),
          getSubtaskSpecId(),
          null,
          false
      );

      final DataSegment segment = new DataSegment(
          segmentIdentifier.getDataSource(),
          segmentIdentifier.getInterval(),
          segmentIdentifier.getVersion(),
          null,
          null,
          null,
          segmentIdentifier.getShardSpec(),
          0,
          1L
      );

      taskClient.report(
          new PushedSegmentsReport(getId(), Collections.emptySet(), Collections.singleton(segment), ImmutableMap.of())
      );
      return TaskStatus.fromCode(getId(), state);
    }

    void setState(TaskState state)
    {
      Preconditions.checkArgument(
          state == TaskState.SUCCESS || state == TaskState.FAILED,
          "state[%s] should be SUCCESS of FAILED",
          state
      );
      this.state = state;
      final int taskIndex = IntStream.range(0, runningTasks.size())
                                     .filter(i -> runningTasks.get(i).getId().equals(getId())).findAny()
                                     .orElse(-1);
      if (taskIndex == -1) {
        throw new ISE("Can't find an index for task[%s]", getId());
      }
      runningTasks.remove(taskIndex);
      final String specId = Preconditions.checkNotNull(taskIdToSpec.get(getId()), "spec for task[%s]", getId()).getId();
      runningSpecs.remove(specId);
      taskHistories.computeIfAbsent(specId, k -> new ArrayList<>()).add(
          new TaskStatusPlus(
              getId(),
              getGroupId(),
              getType(),
              DateTimes.EPOCH,
              DateTimes.EPOCH,
              state,
              RunnerTaskState.NONE,
              -1L,
              TaskLocation.unknown(),
              null,
              null
          )
      );
    }
  }
}
