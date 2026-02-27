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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor.TaskGroup;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Regression tests for autoscaling + pending-completion timeout cleanup.
 */
public class SeekableStreamSupervisorAutoscaleMetadataTest extends SeekableStreamSupervisorTestBase
{
  private SupervisorStateManagerConfig supervisorConfig;
  private TaskQueue taskQueue;

  @Before
  public void setup()
  {
    supervisorConfig = new SupervisorStateManagerConfig();
    taskQueue = EasyMock.mock(TaskQueue.class);
    EmittingLogger.registerEmitter(emitter);
  }

  @Test
  public void test_checkPendingCompletionTimeout_afterAutoscaling_killsOnlyActiveGroupsWithOverlappingPartitions()
      throws Exception
  {
    // Setup supervisor with post-scaling taskCount=2 (no start() to avoid background threads).
    final SeekableStreamSupervisorIOConfig ioConfig = createIOConfig(2, null);
    setupSpecExpectations(ioConfig);
    EasyMock.expect(spec.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.replay(taskMaster);

    // killTask calls taskQueue.shutdown for each killed task
    taskQueue.shutdown(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(taskQueue);

    // No start() call: the constructor initializes all fields needed by
    // checkPendingCompletionTasks (ioConfig, stateManager, workerExec, etc.).
    // Avoiding start() prevents tryInit() from spawning background notice-polling threads.
    final AutoscalingTestSupervisor supervisor = new AutoscalingTestSupervisor(6);

    // Populate partitionOffsets with advanced values.
    final ConcurrentHashMap<String, String> offsets = supervisor.getPartitionOffsets();
    offsets.put("0", "100");
    offsets.put("1", "100");
    offsets.put("2", "100");
    offsets.put("3", "100");
    offsets.put("4", "100");
    offsets.put("5", "100");

    for (int i = 0; i < 6; i++) {
      Assert.assertEquals("100", offsets.get(String.valueOf(i)));
    }

    // Add one old pending completion group with stale groupId 0 and timed-out tasks.
    final TaskGroup oldPending0 = supervisor.addTaskGroupToPendingCompletionTaskGroup(
        0,
        ImmutableMap.of("0", "50"),
        null, null,
        Set.of("pending-task-g0"),
        ImmutableSet.of()
    );
    oldPending0.completionTimeout = DateTimes.EPOCH; // expired

    // Set task status via reflection (TaskData is private; status must be non-null).
    setTaskStatus(oldPending0, "pending-task-g0", TaskStatus.running("pending-task-g0"));

    // Add new active groups after scaling:
    // - groupId 0 has no partition overlap with the timed-out pending group
    // - groupId 1 overlaps on partition 0
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        0,
        ImmutableMap.of("2", "100", "4", "100"),
        null, null,
        Set.of("active-task-g0"),
        ImmutableSet.of()
    );
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        1,
        ImmutableMap.of("0", "100", "1", "100", "3", "100", "5", "100"),
        null, null,
        Set.of("active-task-g1"),
        ImmutableSet.of()
    );

    Assert.assertEquals(2, supervisor.getActiveTaskGroupsCount());

    final Method checkPendingMethod = SeekableStreamSupervisor.class.getDeclaredMethod("checkPendingCompletionTasks");
    checkPendingMethod.setAccessible(true);
    checkPendingMethod.invoke(supervisor);

    final String notSetMarker = supervisor.getNotSetMarker();
    Assert.assertEquals(notSetMarker, offsets.get("0"));
    Assert.assertEquals("100", offsets.get("1"));
    Assert.assertEquals("100", offsets.get("2"));
    Assert.assertEquals("100", offsets.get("3"));
    Assert.assertEquals("100", offsets.get("4"));
    Assert.assertEquals("100", offsets.get("5"));

    // Only overlapping active group should be removed.
    Assert.assertEquals(
        1,
        supervisor.getActiveTaskGroupsCount()
    );
    Assert.assertNotNull(supervisor.getActivelyReadingTaskGroup(0));
    Assert.assertNull(supervisor.getActivelyReadingTaskGroup(1));
  }

  /**
   * Sets the {@code status} field on the {@code TaskData} (private inner class)
   * for the given task within the task group.
   */
  private static void setTaskStatus(TaskGroup group, String taskId, TaskStatus status)
      throws Exception
  {
    final Object taskData = group.tasks.get(taskId);
    Assert.assertNotNull("TaskData should exist for " + taskId, taskData);
    final Field statusField = taskData.getClass().getDeclaredField("status");
    statusField.setAccessible(true);
    statusField.set(taskData, status);
  }

  @Test
  public void test_checkPendingCompletionTimeout_withPendingPartitionsMappedToDifferentActiveGroups()
      throws Exception
  {
    final SeekableStreamSupervisorIOConfig ioConfig = createIOConfig(2, null);
    setupSpecExpectations(ioConfig);
    EasyMock.expect(spec.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.replay(taskMaster);

    taskQueue.shutdown(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(taskQueue);

    final AutoscalingTestSupervisor supervisor = new AutoscalingTestSupervisor(6);

    final ConcurrentHashMap<String, String> offsets = supervisor.getPartitionOffsets();
    offsets.put("0", "100");
    offsets.put("1", "100");
    offsets.put("2", "100");
    offsets.put("3", "100");
    offsets.put("4", "100");
    offsets.put("5", "100");

    // Pending group with partitions {0, 3} - before scaling with taskCount=3, these were both in groupId 0
    // After scaling to taskCount=2:
    //   partition 0 -> 0 % 2 = 0 (groupId 0)
    //   partition 3 -> 3 % 2 = 1 (groupId 1)
    final TaskGroup oldPending0 = supervisor.addTaskGroupToPendingCompletionTaskGroup(
        0,
        ImmutableMap.of("0", "50", "3", "50"),
        null, null,
        Set.of("pending-task-g0"),
        ImmutableSet.of()
    );
    oldPending0.completionTimeout = DateTimes.EPOCH;

    setTaskStatus(oldPending0, "pending-task-g0", TaskStatus.running("pending-task-g0"));

    // After scaling, active groups:
    //   groupId 0: partitions {0, 2, 4}
    //   groupId 1: partitions {1, 3, 5}
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        0,
        ImmutableMap.of("0", "100", "2", "100", "4", "100"),
        null, null,
        Set.of("active-task-g0"),
        ImmutableSet.of()
    );
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        1,
        ImmutableMap.of("1", "100", "3", "100", "5", "100"),
        null, null,
        Set.of("active-task-g1"),
        ImmutableSet.of()
    );

    Assert.assertEquals(2, supervisor.getActiveTaskGroupsCount());

    final Method checkPendingMethod = SeekableStreamSupervisor.class.getDeclaredMethod("checkPendingCompletionTasks");
    checkPendingMethod.setAccessible(true);
    checkPendingMethod.invoke(supervisor);

    final String notSetMarker = supervisor.getNotSetMarker();
    // Both partitions 0 and 3 should be reset (they were in the pending group)
    Assert.assertEquals(notSetMarker, offsets.get("0"));
    Assert.assertEquals("100", offsets.get("1"));
    Assert.assertEquals("100", offsets.get("2"));
    Assert.assertEquals(notSetMarker, offsets.get("3"));
    Assert.assertEquals("100", offsets.get("4"));
    Assert.assertEquals("100", offsets.get("5"));

    // Both active groups should be killed because they both have partitions overlapping with pending group
    Assert.assertEquals(0, supervisor.getActiveTaskGroupsCount());
  }

  /**
   * Test case: pending group has partition that maps to an active group that no longer exists
   * (already completed and removed). The offset should still be reset correctly.
   */
  @Test
  public void test_checkPendingCompletionTimeout_withPartitionAlreadyHandledByCompletedGroup()
      throws Exception
  {
    final SeekableStreamSupervisorIOConfig ioConfig = createIOConfig(2, null);
    setupSpecExpectations(ioConfig);
    EasyMock.expect(spec.createAutoscaler(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(spec);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.replay(taskMaster);

    taskQueue.shutdown(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(taskQueue);

    final AutoscalingTestSupervisor supervisor = new AutoscalingTestSupervisor(6);

    final ConcurrentHashMap<String, String> offsets = supervisor.getPartitionOffsets();
    offsets.put("0", "100");
    offsets.put("1", "100");
    offsets.put("2", "100");

    // Pending group with partition {0}
    final TaskGroup oldPending0 = supervisor.addTaskGroupToPendingCompletionTaskGroup(
        0,
        ImmutableMap.of("0", "50"),
        null, null,
        Set.of("pending-task-g0"),
        ImmutableSet.of()
    );
    oldPending0.completionTimeout = DateTimes.EPOCH;

    setTaskStatus(oldPending0, "pending-task-g0", TaskStatus.running("pending-task-g0"));

    // Only one active group that does NOT overlap with partition 0
    // (groupId 1 handles partitions {1, 2} with taskCount=2)
    supervisor.addTaskGroupToActivelyReadingTaskGroup(
        1,
        ImmutableMap.of("1", "100", "2", "100"),
        null, null,
        Set.of("active-task-g1"),
        ImmutableSet.of()
    );

    Assert.assertEquals(1, supervisor.getActiveTaskGroupsCount());

    final Method checkPendingMethod = SeekableStreamSupervisor.class.getDeclaredMethod("checkPendingCompletionTasks");
    checkPendingMethod.setAccessible(true);
    checkPendingMethod.invoke(supervisor);

    final String notSetMarker = supervisor.getNotSetMarker();
    // Partition 0 should be reset even though no overlapping active group exists
    Assert.assertEquals(notSetMarker, offsets.get("0"));
    Assert.assertEquals("100", offsets.get("1"));
    Assert.assertEquals("100", offsets.get("2"));

    // Active group should still exist (no overlap)
    Assert.assertEquals(1, supervisor.getActiveTaskGroupsCount());
  }

  private void setupSpecExpectations(SeekableStreamSupervisorIOConfig ioConfig)
  {
    EasyMock.expect(spec.getId()).andReturn(SUPERVISOR).anyTimes();
    EasyMock.expect(spec.getSupervisorStateManagerConfig()).andReturn(supervisorConfig).anyTimes();
    EasyMock.expect(spec.getDataSchema()).andReturn(getDataSchema(DATASOURCE)).anyTimes();
    EasyMock.expect(spec.getIoConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.expect(spec.getTuningConfig()).andReturn(getTuningConfig()).anyTimes();
    EasyMock.expect(spec.getEmitter()).andReturn(emitter).anyTimes();
    EasyMock.expect(spec.isSuspended()).andReturn(false).anyTimes();
  }

  /**
   * A test supervisor where {@code getTaskGroupIdForPartition} derives the
   * group from the current (mutable) taskCount, mimicking the real Kafka/Kinesis
   * implementations: {@code partition % taskCount}.
   */
  class AutoscalingTestSupervisor extends TestSeekableStreamSupervisor
  {
    AutoscalingTestSupervisor(int partitionCount)
    {
      super(partitionCount);
    }

    @Override
    protected int getTaskGroupIdForPartition(String partition)
    {
      return Integer.parseInt(partition) % spec.getIoConfig().getTaskCount();
    }

    @Override
    public String getNotSetMarker()
    {
      return super.getNotSetMarker();
    }

    /**
     * Exposes the active task group for the given groupId for test assertions.
     */
    TaskGroup getActivelyReadingTaskGroup(int groupId)
    {
      try {
        final Field field = SeekableStreamSupervisor.class.getDeclaredField("activelyReadingTaskGroups");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        final ConcurrentHashMap<Integer, TaskGroup> map = (ConcurrentHashMap<Integer, TaskGroup>) field.get(this);
        return map.get(groupId);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
