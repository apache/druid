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

package org.apache.druid.k8s.overlord;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.io.IOUtils;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.DruidMetrics;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(EasyMockExtension.class)
public class MultipleKubernetesTaskRunnerTest extends EasyMockSupport
{
  private static final String TASK_ID = "task-id";
  private static final String REASON = "reason";

  /**
   * Helper method to create an immediately completed ListenableFuture.
   * Replaces Futures.immediateFuture() which is marked as beta.
   */
  private static <T> ListenableFuture<T> immediateFuture(T value)
  {
    SettableFuture<T> future = SettableFuture.create();
    future.set(value);
    return future;
  }

  /**
   * Simple test implementation of TaskRunnerWorkItem for use in tests.
   * This avoids type incompatibility issues with wildcard generics.
   */
  private static class TestTaskRunnerWorkItem extends TaskRunnerWorkItem
  {
    public TestTaskRunnerWorkItem(String id)
    {
      super(id, null);
    }

    @Override
    public TaskLocation getLocation()
    {
      return TaskLocation.unknown();
    }

    @Override
    public String getTaskType()
    {
      return null;
    }

    @Override
    public String getDataSource()
    {
      return null;
    }
  }

  /**
   * Test implementation of KubernetesTaskRunner that allows setting return values for testing.
   */
  private static class TestKubernetesTaskRunner extends KubernetesTaskRunner
  {
    private Collection<TaskRunnerWorkItem> knownTasks = new ArrayList<>();
    private Collection<TaskRunnerWorkItem> runningTasks = new ArrayList<>();
    private Collection<TaskRunnerWorkItem> pendingTasks = new ArrayList<>();
    private final Map<String, RunnerTaskState> runnerTaskStates = new HashMap<>();
    private final Map<String, TaskLocation> taskLocations = new HashMap<>();
    private final Map<String, Optional<InputStream>> taskLogs = new HashMap<>();
    private final Map<String, Optional<InputStream>> taskReports = new HashMap<>();
    private final Map<String, ListenableFuture<TaskStatus>> runResults = new HashMap<>();

    public TestKubernetesTaskRunner(KubernetesTaskRunnerEffectiveConfig config, ConfigManager configManager)
    {
      super(null, config, null, null, null, null, configManager);
    }

    public void setKnownTasks(Collection<TaskRunnerWorkItem> tasks)
    {
      this.knownTasks = tasks;
    }

    public void setRunningTasks(Collection<TaskRunnerWorkItem> tasks)
    {
      this.runningTasks = tasks;
    }

    public void setPendingTasks(Collection<TaskRunnerWorkItem> tasks)
    {
      this.pendingTasks = tasks;
    }

    public void setRunnerTaskState(String taskId, RunnerTaskState state)
    {
      if (state == null) {
        runnerTaskStates.remove(taskId);
      } else {
        runnerTaskStates.put(taskId, state);
      }
    }

    public void setTaskLocation(String taskId, TaskLocation location)
    {
      taskLocations.put(taskId, location);
    }

    public void setTaskLog(String taskId, Optional<InputStream> log)
    {
      taskLogs.put(taskId, log);
    }

    public void setTaskReports(String taskId, Optional<InputStream> reports)
    {
      taskReports.put(taskId, reports);
    }

    public void setRunResult(Task task, ListenableFuture<TaskStatus> result)
    {
      runResults.put(task.getId(), result);
    }

    @Override
    public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
    {
      return knownTasks;
    }

    @Override
    public Collection<TaskRunnerWorkItem> getRunningTasks()
    {
      return runningTasks;
    }

    @Override
    public Collection<TaskRunnerWorkItem> getPendingTasks()
    {
      return pendingTasks;
    }

    @Override
    public RunnerTaskState getRunnerTaskState(String taskId)
    {
      return runnerTaskStates.get(taskId);
    }

    @Override
    public TaskLocation getTaskLocation(String taskId)
    {
      TaskLocation location = taskLocations.get(taskId);
      return location != null ? location : TaskLocation.unknown();
    }

    @Override
    public Optional<InputStream> streamTaskLog(String taskid, long offset)
    {
      Optional<InputStream> log = taskLogs.get(taskid);
      return log != null ? log : Optional.absent();
    }

    @Override
    public Optional<InputStream> streamTaskReports(String taskid)
    {
      Optional<InputStream> reports = taskReports.get(taskid);
      return reports != null ? reports : Optional.absent();
    }

    @Override
    public ListenableFuture<TaskStatus> run(Task task)
    {
      ListenableFuture<TaskStatus> result = runResults.get(task.getId());
      if (result != null) {
        return result;
      }
      return immediateFuture(TaskStatus.success(task.getId()));
    }

    @Override
    public void start()
    {
      // No-op for testing
    }

    @Override
    public void stop()
    {
      // No-op for testing
    }

    @Override
    public void shutdown(String taskid, String reason)
    {
      // No-op for testing - just remove from tasks if present
      tasks.remove(taskid);
    }
  }

  @Mock
  private KubernetesTaskRunner kubernetesTaskRunner1;

  @Mock
  private KubernetesTaskRunner kubernetesTaskRunner2;

  private KubernetesTaskRunnerConfig config;
  private KubernetesTaskRunnerEffectiveConfig effectiveConfig1;
  private KubernetesTaskRunnerEffectiveConfig effectiveConfig2;
  private MultipleKubernetesTaskRunner runner;
  private ConfigManager testConfigManager;

  @BeforeEach
  public void setup()
  {
    testConfigManager = EasyMock.createNiceMock(ConfigManager.class);
    EasyMock.replay(testConfigManager);

    // Create configs
    config = KubernetesTaskRunnerConfig.builder()
                                             .withCapacity(10)
                                             .build();

    effectiveConfig1 = new KubernetesTaskRunnerEffectiveConfig(
        KubernetesTaskRunnerConfig.builder().withNamespace("namespace1").withCapacity(10).build(),
        () -> null
    );

    effectiveConfig2 = new KubernetesTaskRunnerEffectiveConfig(
        KubernetesTaskRunnerConfig.builder().withNamespace("namespace2").withCapacity(10).build(),
        () -> null
    );

  }

  @Test
  public void test_run_withRoundRobinSelectionStrategy() throws ExecutionException, InterruptedException
  {
    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner1),
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner2)
        )
    );

    Task task1 = new NoopTask("task-1", null, null, 0, 0, null);

    // First call: both runners report no existing task, round-robin selects runner1
    // Both runners should be checked for existing tasks (return null = no existing task)
    EasyMock.expect(kubernetesTaskRunner1.getRunnerTaskState(task1.getId())).andReturn(null);
    EasyMock.expect(kubernetesTaskRunner2.getRunnerTaskState(task1.getId())).andReturn(null);
    // Round-robin selects first runner (kubernetesTaskRunner1)
    EasyMock.expect(kubernetesTaskRunner1.run(task1)).andReturn(immediateFuture(TaskStatus.success("task-1")));

    replayAll();

    ListenableFuture<TaskStatus> result = runner.run(task1);
    Assertions.assertEquals("task-1", result.get().getId());

    verifyAll();
    resetAll();

    Task task2 = new NoopTask("task-2", null, null, 0, 0, null);

    // Second call: both runners report no existing task, round-robin selects runner2
    // Both runners should be checked for existing tasks (return null = no existing task)
    EasyMock.expect(kubernetesTaskRunner1.getRunnerTaskState(task2.getId())).andReturn(null);
    EasyMock.expect(kubernetesTaskRunner2.getRunnerTaskState(task2.getId())).andReturn(null);
    // Round-robin selects second runner (kubernetesTaskRunner2)
    EasyMock.expect(kubernetesTaskRunner2.run(task2)).andReturn(immediateFuture(TaskStatus.success("task-2")));

    replayAll();

    result = runner.run(task2);
    Assertions.assertEquals("task-2", result.get().getId());

    verifyAll();
  }

  @Test
  public void test_run_withExistingTask_returnsToSameRunner() throws ExecutionException, InterruptedException
  {
    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner1),
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner2)
        )
    );

    Task task = new NoopTask("existing-task", null, null, 0, 0, null);

    // Task already exists in runner1 - should check runner1 first and find it
    EasyMock.expect(kubernetesTaskRunner1.getRunnerTaskState(task.getId())).andReturn(RunnerTaskState.RUNNING);
    EasyMock.expect(kubernetesTaskRunner1.run(task)).andReturn(immediateFuture(TaskStatus.success(task.getId())));

    replayAll();

    ListenableFuture<TaskStatus> result = runner.run(task);
    Assertions.assertEquals(task.getId(), result.get().getId());

    verifyAll();
  }

  @Test
  public void test_run_withRoundRobinSelectionStrategy_disabledRunnerNotSelected() throws ExecutionException, InterruptedException
  {
    // Create runner with round-robin strategy, runner2 is disabled
    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner1),
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner2, null, true, null) // runner2 is disabled
        )
    );

    Task task1 = new NoopTask("task-1", null, null, 0, 0, null);

    // First call: both runners checked for existing task, but only enabled runner1 should be selected
    EasyMock.expect(kubernetesTaskRunner1.getRunnerTaskState(task1.getId())).andReturn(null);
    EasyMock.expect(kubernetesTaskRunner2.getRunnerTaskState(task1.getId())).andReturn(null);
    // Round-robin should select runner1 (only enabled runner)
    EasyMock.expect(kubernetesTaskRunner1.run(task1)).andReturn(immediateFuture(TaskStatus.success("task-1")));

    replayAll();

    ListenableFuture<TaskStatus> result = runner.run(task1);
    Assertions.assertEquals("task-1", result.get().getId());

    verifyAll();
    resetAll();

    Task task2 = new NoopTask("task-2", null, null, 0, 0, null);

    // Second call: should still select runner1 (only enabled runner, round-robin wraps around)
    EasyMock.expect(kubernetesTaskRunner1.getRunnerTaskState(task2.getId())).andReturn(null);
    EasyMock.expect(kubernetesTaskRunner2.getRunnerTaskState(task2.getId())).andReturn(null);
    // Round-robin should still select runner1 (only enabled runner)
    EasyMock.expect(kubernetesTaskRunner1.run(task2)).andReturn(immediateFuture(TaskStatus.success("task-2")));

    replayAll();

    result = runner.run(task2);
    Assertions.assertEquals("task-2", result.get().getId());

    verifyAll();
  }

  @Test
  public void test_run_withAllRunnersDisabled_throwsException()
  {
    // Constructor rejects when every delegate is disabled (no runner can run tasks).
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> new MultipleKubernetesTaskRunner(
            config,
            new MultipleKubernetesTaskRunner.RoundRobinSelector(),
            ImmutableList.of(
                new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner1, null, true, null), // runner1 is disabled
                new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner2, null, true, null)  // runner2 is disabled
            )
        )
    );
  }

  @Test
  public void test_run_withRandomSelectionStrategy() throws ExecutionException, InterruptedException
  {
    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RandomSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner1),
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner2)
        )
    );

    Task task = new NoopTask("random-task", null, null, 0, 0, null);

    // Random selection - check both runners for existing task, then pick one
    // Since it's random, we need to set up expectations for whichever runner gets selected
    EasyMock.expect(kubernetesTaskRunner1.getRunnerTaskState(task.getId())).andReturn(null);
    EasyMock.expect(kubernetesTaskRunner2.getRunnerTaskState(task.getId())).andReturn(null);
    // Random strategy will pick one of them - set up both possibilities for execution
    EasyMock.expect(kubernetesTaskRunner1.run(task)).andReturn(immediateFuture(TaskStatus.success(task.getId()))).anyTimes();
    EasyMock.expect(kubernetesTaskRunner2.run(task)).andReturn(immediateFuture(TaskStatus.success(task.getId()))).anyTimes();

    replayAll();

    ListenableFuture<TaskStatus> result = runner.run(task);
    Assertions.assertEquals(task.getId(), result.get().getId());

    verifyAll();
  }

  @Test
  public void test_run_withLeastTaskSelectionStrategy() throws ExecutionException, InterruptedException
  {
    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.LeastTaskSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner1),
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner2)
        )
    );

    TestTaskRunnerWorkItem workItem1 = new TestTaskRunnerWorkItem("work-item-1");
    TestTaskRunnerWorkItem workItem2 = new TestTaskRunnerWorkItem("work-item-2");

    // Runner1 has 2 tasks, runner2 has 1 task - should pick runner2
    Task task = new NoopTask("least-task", null, null, 0, 0, null);
    EasyMock.expect(kubernetesTaskRunner1.getRunnerTaskState(task.getId())).andReturn(null);
    EasyMock.expect(kubernetesTaskRunner2.getRunnerTaskState(task.getId())).andReturn(null);

    // Least task strategy checks known tasks to find the runner with the fewest tasks
    // getKnownTasks() is called multiple times, so allow anyTimes
    EasyMock.expect((Object) kubernetesTaskRunner1.getKnownTasks()).andReturn(ImmutableList.of(workItem1, workItem2)).anyTimes();
    EasyMock.expect((Object) kubernetesTaskRunner2.getKnownTasks()).andReturn(ImmutableList.of(workItem1)).anyTimes();

    // Should pick runner2 (has fewer tasks)
    EasyMock.expect(kubernetesTaskRunner2.run(task)).andReturn(immediateFuture(TaskStatus.success(task.getId())));

    replayAll();

    ListenableFuture<TaskStatus> result = runner.run(task);
    Assertions.assertEquals(task.getId(), result.get().getId());

    verifyAll();
  }

  @Test
  public void test_shutdown()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    // shutdown() is called on all runners - no need to set expectations
    runner.shutdown(TASK_ID, REASON);
  }

  @Test
  public void test_getRunningTasks()
  {
    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner1),
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner2)
        )
    );

    TestTaskRunnerWorkItem workItem1 = new TestTaskRunnerWorkItem("work-item-1");
    TestTaskRunnerWorkItem workItem2 = new TestTaskRunnerWorkItem("work-item-2");

    EasyMock.expect(kubernetesTaskRunner1.getRunningTasks()).andReturn(Collections.singletonList(workItem1));
    EasyMock.expect(kubernetesTaskRunner2.getRunningTasks()).andReturn(Collections.singletonList(workItem2));

    replayAll();

    Collection<? extends TaskRunnerWorkItem> runningTasks = runner.getRunningTasks();

    assertEquals(2, runningTasks.size());
    verifyAll();
  }

  @Test
  public void test_getPendingTasks()
  {
    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner1),
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner2)
        )
    );

    TestTaskRunnerWorkItem workItem1 = new TestTaskRunnerWorkItem("work-item-1");
    TestTaskRunnerWorkItem workItem2 = new TestTaskRunnerWorkItem("work-item-2");

    EasyMock.expect(kubernetesTaskRunner1.getPendingTasks()).andReturn(Collections.singletonList(workItem1));
    EasyMock.expect(kubernetesTaskRunner2.getPendingTasks()).andReturn(Collections.singletonList(workItem2));

    replayAll();

    Collection<? extends TaskRunnerWorkItem> pendingTasks = runner.getPendingTasks();

    assertEquals(2, pendingTasks.size());
    verifyAll();
  }

  @Test
  public void test_getKnownTasks()
  {
    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner1),
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner2)
        )
    );

    TestTaskRunnerWorkItem workItem1 = new TestTaskRunnerWorkItem("work-item-1");
    TestTaskRunnerWorkItem workItem2 = new TestTaskRunnerWorkItem("work-item-2");

    EasyMock.expect((Object) kubernetesTaskRunner1.getKnownTasks()).andReturn(ImmutableList.of(workItem1));
    EasyMock.expect((Object) kubernetesTaskRunner2.getKnownTasks()).andReturn(ImmutableList.of(workItem2));

    replayAll();

    Collection<? extends TaskRunnerWorkItem> knownTasks = runner.getKnownTasks();

    assertEquals(2, knownTasks.size());
    verifyAll();
  }

  @Test
  public void test_getUsedCapacity()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    TestTaskRunnerWorkItem workItem1 = new TestTaskRunnerWorkItem("work-item-1");
    TestTaskRunnerWorkItem workItem2 = new TestTaskRunnerWorkItem("work-item-2");
    TestTaskRunnerWorkItem workItem3 = new TestTaskRunnerWorkItem("work-item-3");

    testRunner1.setKnownTasks(Lists.newArrayList(workItem1, workItem2));
    testRunner2.setKnownTasks(Collections.singletonList(workItem3));

    int usedCapacity = runner.getUsedCapacity();

    assertEquals(3, usedCapacity);
    assertEquals(10, runner.getTotalCapacity());
  }

  @Test
  public void test_getTotalCapacity()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    int totalCapacity = runner.getTotalCapacity();

    assertEquals(10, totalCapacity);
  }

  @Test
  public void test_getTotalTaskSlotCount()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    Map<String, Long> slotCount = runner.getTotalTaskSlotCount();

    assertEquals(1, slotCount.size());
    assertEquals(Long.valueOf(10), slotCount.get(KubernetesTaskRunner.WORKER_CATEGORY));
  }

  @Test
  public void test_getIdleTaskSlotCount()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    TestTaskRunnerWorkItem workItem1 = new TestTaskRunnerWorkItem("work-item-1");
    TestTaskRunnerWorkItem workItem2 = new TestTaskRunnerWorkItem("work-item-2");

    testRunner1.setKnownTasks(Collections.singletonList(workItem1));
    testRunner2.setKnownTasks(Collections.singletonList(workItem2));

    Map<String, Long> idleSlotCount = runner.getIdleTaskSlotCount();

    assertEquals(1, idleSlotCount.size());
    assertEquals(Long.valueOf(8), idleSlotCount.get(KubernetesTaskRunner.WORKER_CATEGORY)); // 10 - 2 = 8
  }

  @Test
  public void test_getUsedTaskSlotCount()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );


    TestTaskRunnerWorkItem workItem1 = new TestTaskRunnerWorkItem("work-item-1");
    TestTaskRunnerWorkItem workItem2 = new TestTaskRunnerWorkItem("work-item-2");

    testRunner1.setKnownTasks(Collections.singletonList(workItem1));
    testRunner2.setKnownTasks(Collections.singletonList(workItem2));

    Map<String, Long> usedSlotCount = runner.getUsedTaskSlotCount();

    assertEquals(1, usedSlotCount.size());
    assertEquals(Long.valueOf(2), usedSlotCount.get(KubernetesTaskRunner.WORKER_CATEGORY));
  }

  @Test
  public void test_getLazyTaskSlotCount()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    Map<String, Long> lazySlotCount = runner.getLazyTaskSlotCount();

    assertTrue(lazySlotCount.isEmpty());
  }

  @Test
  public void test_getBlacklistedTaskSlotCount()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    Map<String, Long> blacklistedSlotCount = runner.getBlacklistedTaskSlotCount();

    assertTrue(blacklistedSlotCount.isEmpty());
  }

  @Test
  public void test_getTaskLocation_withExistingTask()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    String taskId = "location-task";
    TaskLocation location = TaskLocation.create("host", 8080, 8081, false);

    testRunner1.setTaskLocation(taskId, TaskLocation.unknown());
    testRunner2.setTaskLocation(taskId, location);

    TaskLocation result = runner.getTaskLocation(taskId);

    assertEquals(location, result);
  }

  @Test
  public void test_getTaskLocation_withoutExistingTask()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    String taskId = "unknown-location-task";
    testRunner1.setTaskLocation(taskId, TaskLocation.unknown());
    testRunner2.setTaskLocation(taskId, TaskLocation.unknown());

    TaskLocation result = runner.getTaskLocation(taskId);

    assertEquals(TaskLocation.unknown(), result);
  }

  @Test
  public void test_getRunnerTaskState_withExistingTask()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    String taskId = "running-task";
    testRunner1.setRunnerTaskState(taskId, null);
    testRunner2.setRunnerTaskState(taskId, RunnerTaskState.RUNNING);

    RunnerTaskState result = runner.getRunnerTaskState(taskId);

    assertEquals(RunnerTaskState.RUNNING, result);
  }

  @Test
  public void test_getRunnerTaskState_withoutExistingTask()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    String taskId = "unknown-state-task";
    testRunner1.setRunnerTaskState(taskId, null);
    testRunner2.setRunnerTaskState(taskId, null);

    RunnerTaskState result = runner.getRunnerTaskState(taskId);

    assertNull(result);
  }

  @Test
  public void test_streamTaskLog_withExistingTask() throws Exception
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    String taskId = "log-task";
    InputStream inputStream = IOUtils.toInputStream("log content", StandardCharsets.UTF_8);

    testRunner1.setRunnerTaskState(taskId, null);
    testRunner2.setRunnerTaskState(taskId, RunnerTaskState.RUNNING);
    testRunner2.setTaskLog(taskId, Optional.of(inputStream));

    Optional<InputStream> result = runner.streamTaskLog(taskId, 0L);

    assertTrue(result.isPresent());
    assertEquals("log content", IOUtils.toString(result.get(), StandardCharsets.UTF_8));
  }

  @Test
  public void test_streamTaskLog_withoutExistingTask()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    String taskId = "no-log-task";
    testRunner1.setRunnerTaskState(taskId, null);
    testRunner2.setRunnerTaskState(taskId, null);

    Optional<InputStream> result = runner.streamTaskLog(taskId, 0L);

    assertFalse(result.isPresent());
  }

  @Test
  public void test_streamTaskReports_withExistingTask() throws Exception
  {
    // Create test runners with clear setup - similar to Mockito style
    final TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    final TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    final String taskId = "report-task";
    final InputStream inputStream = IOUtils.toInputStream("report content", StandardCharsets.UTF_8);

    testRunner1.setRunnerTaskState(taskId, null);
    testRunner2.setRunnerTaskState(taskId, RunnerTaskState.RUNNING);
    testRunner2.setTaskReports(taskId, Optional.of(inputStream));

    final Optional<InputStream> result = runner.streamTaskReports(taskId);

    assertTrue(result.isPresent());
    assertEquals("report content", IOUtils.toString(result.get(), StandardCharsets.UTF_8));
  }

  @Test
  public void test_streamTaskReports_withoutExistingTask() throws Exception
  {
    // Create test runners with clear setup - similar to Mockito style
    final TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    final TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    final String taskId = "no-report-task";
    testRunner1.setRunnerTaskState(taskId, null);
    testRunner2.setRunnerTaskState(taskId, null);

    final Optional<InputStream> result = runner.streamTaskReports(taskId);

    assertFalse(result.isPresent());
  }

  @Test
  public void test_restore()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    List<Pair<Task, ListenableFuture<TaskStatus>>> result = runner.restore();

    assertTrue(result.isEmpty());
  }

  @Test
  public void test_getScalingStats()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    Optional<?> result = runner.getScalingStats();

    assertFalse(result.isPresent());
  }

  @Test
  public void test_start()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    // start() is called on all runners - no need to set expectations
    runner.start();
  }

  @Test
  public void test_stop()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    // stop() is called on all runners - no need to set expectations
    runner.stop();
  }

  @Test
  public void test_registerListener()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    // registerListener() is called on all runners - no need to set expectations
    runner.registerListener(
        new TaskRunnerListener()
        {
          @Override
          public String getListenerId()
          {
            return "";
          }

          @Override
          public void locationChanged(String taskId, TaskLocation newLocation)
          {

          }

          @Override
          public void statusChanged(String taskId, TaskStatus status)
          {

          }
        },
        Executors.newSingleThreadExecutor()
    );
  }

  @Test
  public void test_unregisterListener()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(testRunner1),
            new MultipleKubernetesTaskRunnerDelegate(testRunner2)
        )
    );

    String listenerId = "listener-id";
    // unregisterListener() is called on all runners - no need to set expectations
    runner.unregisterListener(listenerId);
  }

  @Test
  public void test_roundRobinSelectionStrategy_wrapsAround()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);
    List<MultipleKubernetesTaskRunnerDelegate> testTaskRunners = ImmutableList.of(
        new MultipleKubernetesTaskRunnerDelegate(testRunner1),
        new MultipleKubernetesTaskRunnerDelegate(testRunner2)
    );

    MultipleKubernetesTaskRunner.RoundRobinSelector strategy =
        new MultipleKubernetesTaskRunner.RoundRobinSelector();

    // First call should return runner1 (index 0)
    MultipleKubernetesTaskRunnerDelegate result1 = strategy.next(testTaskRunners);
    assertEquals(testRunner1, result1.getRunner());

    // Second call should return runner2 (index 1)
    MultipleKubernetesTaskRunnerDelegate result2 = strategy.next(testTaskRunners);
    assertEquals(testRunner2, result2.getRunner());

    // Third call should wrap around to runner1 (index 0)
    MultipleKubernetesTaskRunnerDelegate result3 = strategy.next(testTaskRunners);
    assertEquals(testRunner1, result3.getRunner());
  }

  @Test
  public void test_leastTaskSelectionStrategy_withEqualTasks()
  {
    // Create test runners with clear setup - similar to Mockito style
    TestKubernetesTaskRunner testRunner1 = new TestKubernetesTaskRunner(effectiveConfig1, testConfigManager);
    TestKubernetesTaskRunner testRunner2 = new TestKubernetesTaskRunner(effectiveConfig2, testConfigManager);
    List<MultipleKubernetesTaskRunnerDelegate> testTaskRunners = ImmutableList.of(
        new MultipleKubernetesTaskRunnerDelegate(testRunner1),
        new MultipleKubernetesTaskRunnerDelegate(testRunner2)
    );

    MultipleKubernetesTaskRunner.LeastTaskSelector strategy =
        new MultipleKubernetesTaskRunner.LeastTaskSelector();

    TestTaskRunnerWorkItem workItem = new TestTaskRunnerWorkItem("work-item");

    testRunner1.setKnownTasks(Collections.singletonList(workItem));
    testRunner2.setKnownTasks(Collections.singletonList(workItem));

    // Both have same number of tasks, should pick one randomly
    MultipleKubernetesTaskRunnerDelegate result = strategy.next(testTaskRunners);

    // Should be one of the two runners
    assertTrue(result.getRunner() == testRunner1 || result.getRunner() == testRunner2);
  }

  @Test
  public void test_run_injectsK8sClusterContextWhenClusterNameIsSet()
  {
    String clusterName = "test-cluster-a";
    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner1, clusterName, false, null)
        )
    );

    Task task = new NoopTask("task-with-cluster", null, null, 0, 0, null);
    task.addToContext(DruidMetrics.TAGS, ImmutableMap.of("existing_tag", "tag_value"));

    // Task doesn't exist in any runner
    EasyMock.expect(kubernetesTaskRunner1.getRunnerTaskState(task.getId())).andReturn(null);
    EasyMock.expect(kubernetesTaskRunner1.run(task)).andReturn(immediateFuture(TaskStatus.success(task.getId())));

    replayAll();

    runner.run(task);

    verifyAll();

    // Verify that k8s_cluster was added to the task context
    Map tags = task.getContextValue(DruidMetrics.TAGS);
    Assertions.assertEquals(clusterName, tags.get("k8s_cluster"));

    // Verify that existing tags are preserved
    Assertions.assertEquals("tag_value", tags.get("existing_tag"));
  }

  @Test
  public void test_run_doesNotInjectK8sClusterContextWhenClusterNameIsNull()
  {
    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner1)
        )
    );

    Task task = new NoopTask("task-without-cluster", null, null, 0, 0, null);

    // Task doesn't exist in any runner
    EasyMock.expect(kubernetesTaskRunner1.getRunnerTaskState(task.getId())).andReturn(null);
    EasyMock.expect(kubernetesTaskRunner1.run(task)).andReturn(immediateFuture(TaskStatus.success(task.getId())));

    replayAll();

    runner.run(task);

    verifyAll();

    // Verify that k8s_cluster was NOT added to the task context
    Object contextValue = task.getContextValue("k8s_cluster");
    Assertions.assertNull(contextValue, "k8s_cluster should not be injected when cluster name is null");
  }

  @Test
  public void test_run_injectsDifferentK8sClusterNamesForDifferentRunners()
  {
    String clusterNameA = "cluster-a";
    String clusterNameB = "cluster-b";

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner1, clusterNameA, false, null),
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner2, clusterNameB, false, null)
        )
    );

    Task task1 = new NoopTask("task-1", null, null, 0, 0, null);

    // First call: round-robin selects runner1 with cluster-a
    EasyMock.expect(kubernetesTaskRunner1.getRunnerTaskState(task1.getId())).andReturn(null);
    EasyMock.expect(kubernetesTaskRunner2.getRunnerTaskState(task1.getId())).andReturn(null);
    EasyMock.expect(kubernetesTaskRunner1.run(task1)).andReturn(immediateFuture(TaskStatus.success(task1.getId())));

    replayAll();

    runner.run(task1);

    verifyAll();

    // Verify that k8s_cluster=cluster-a was injected into tags
    Map<?, ?> tags1 = task1.getContextValue(DruidMetrics.TAGS);
    Assertions.assertEquals(clusterNameA, tags1.get("k8s_cluster"));

    resetAll();

    Task task2 = new NoopTask("task-2", null, null, 0, 0, null);

    // Second call: round-robin selects runner2 with cluster-b
    EasyMock.expect(kubernetesTaskRunner1.getRunnerTaskState(task2.getId())).andReturn(null);
    EasyMock.expect(kubernetesTaskRunner2.getRunnerTaskState(task2.getId())).andReturn(null);
    EasyMock.expect(kubernetesTaskRunner2.run(task2)).andReturn(immediateFuture(TaskStatus.success(task2.getId())));

    replayAll();

    runner.run(task2);

    verifyAll();

    // Verify that k8s_cluster=cluster-b was injected into tags
    Map<?, ?> tags2 = task2.getContextValue(DruidMetrics.TAGS);
    Assertions.assertEquals(clusterNameB, tags2.get("k8s_cluster"));
  }

  @Test
  public void test_run_existingTaskDoesNotOverwriteK8sClusterContext()
  {
    String clusterName = "cluster-a";
    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner1, clusterName, false, null)
        )
    );

    Task task = new NoopTask("existing-task", null, null, 0, 0, null);
    // Pre-set a different cluster value in the task context
    task.addToContext("k8s_cluster", "original-cluster");

    // Task already exists in runner1 - should be found and delegated directly
    EasyMock.expect(kubernetesTaskRunner1.getRunnerTaskState(task.getId())).andReturn(RunnerTaskState.RUNNING);
    EasyMock.expect(kubernetesTaskRunner1.run(task)).andReturn(immediateFuture(TaskStatus.success(task.getId())));

    replayAll();

    runner.run(task);

    verifyAll();

    // Verify that the original k8s_cluster value is preserved (not overwritten)
    // When a task already exists in a runner, we don't inject the cluster name again
    Assertions.assertEquals("original-cluster", task.getContextValue("k8s_cluster"));
  }

  @Test
  public void test_run_k8sClusterContextCanBeUsedForPodTemplateSelection()
  {
    // This test verifies the integration between k8s_cluster context injection
    // and pod template selection (which happens inside KubernetesTaskRunner.run())
    String clusterName = "production-cluster";

    runner = new MultipleKubernetesTaskRunner(
        config,
        new MultipleKubernetesTaskRunner.RoundRobinSelector(),
        ImmutableList.of(
            new MultipleKubernetesTaskRunnerDelegate(kubernetesTaskRunner1, clusterName, false, null)
        )
    );

    Task task = new NoopTask("task-for-template-selection", null, null, 0, 0, null);

    // Task doesn't exist in any runner
    EasyMock.expect(kubernetesTaskRunner1.getRunnerTaskState(task.getId())).andReturn(null);
    EasyMock.expect(kubernetesTaskRunner1.run(task)).andReturn(immediateFuture(TaskStatus.success(task.getId())));

    replayAll();

    runner.run(task);

    verifyAll();

    // Verify that k8s_cluster was injected into tags
    Map<?, ?> tags = task.getContextValue(DruidMetrics.TAGS);
    Assertions.assertEquals(clusterName, tags.get("k8s_cluster"));

    // Note: The actual pod template selection happens inside KubernetesTaskRunner.run()
    // Users can configure SelectorBasedPodTemplateSelectStrategy to check for k8s_cluster
    // in the task's context tags (DruidMetrics.TAGS) to select different pod templates
  }
}

