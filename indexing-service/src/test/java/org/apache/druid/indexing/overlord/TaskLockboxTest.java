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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.SegmentLock;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskLockbox.TaskLockPosse;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.DerbyMetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedPartialShardSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NumberedOverwritePartialShardSpec;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartialShardSpec;
import org.apache.druid.timeline.partition.PartitionIds;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TaskLockboxTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derby = new TestDerbyConnector.DerbyConnectorRule();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private ObjectMapper objectMapper;
  private TaskStorage taskStorage;
  private IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private TaskLockbox lockbox;

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setup()
  {
    objectMapper = TestHelper.makeJsonMapper();
    objectMapper.registerSubtypes(NumberedShardSpec.class, HashBasedNumberedShardSpec.class);

    final TestDerbyConnector derbyConnector = derby.getConnector();
    derbyConnector.createTaskTables();
    derbyConnector.createPendingSegmentsTable();
    derbyConnector.createSegmentTable();
    final MetadataStorageTablesConfig tablesConfig = derby.metadataTablesConfigSupplier().get();
    taskStorage = new MetadataTaskStorage(
        derbyConnector,
        new TaskStorageConfig(null),
        new DerbyMetadataStorageActionHandlerFactory(
            derbyConnector,
            tablesConfig,
            objectMapper
        )
    );
    ServiceEmitter emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    EasyMock.replay(emitter);

    metadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(objectMapper, tablesConfig, derbyConnector);

    lockbox = new TaskLockbox(taskStorage, metadataStorageCoordinator);
  }

  private LockResult acquireTimeChunkLock(TaskLockType lockType, Task task, Interval interval, long timeoutMs)
      throws InterruptedException
  {
    return lockbox.lock(task, new TimeChunkLockRequest(lockType, task, interval, null), timeoutMs);
  }

  private LockResult acquireTimeChunkLock(TaskLockType lockType, Task task, Interval interval)
      throws InterruptedException
  {
    return lockbox.lock(task, new TimeChunkLockRequest(lockType, task, interval, null));
  }

  private LockResult tryTimeChunkLock(TaskLockType lockType, Task task, Interval interval)
  {
    return lockbox.tryLock(task, new TimeChunkLockRequest(lockType, task, interval, null));
  }

  @Test
  public void testLock() throws InterruptedException
  {
    Task task = NoopTask.create();
    lockbox.add(task);
    Assert.assertNotNull(acquireTimeChunkLock(TaskLockType.EXCLUSIVE, task, Intervals.of("2015-01-01/2015-01-02")));
  }

  @Test(expected = IllegalStateException.class)
  public void testLockForInactiveTask() throws InterruptedException
  {
    acquireTimeChunkLock(TaskLockType.EXCLUSIVE, NoopTask.create(), Intervals.of("2015-01-01/2015-01-02"));
  }

  @Test
  public void testLockAfterTaskComplete() throws InterruptedException
  {
    Task task = NoopTask.create();
    exception.expect(ISE.class);
    exception.expectMessage("Unable to grant lock to inactive Task");
    lockbox.add(task);
    lockbox.remove(task);
    acquireTimeChunkLock(TaskLockType.EXCLUSIVE, task, Intervals.of("2015-01-01/2015-01-02"));
  }

  @Test
  public void testTrySharedLock() throws EntryExistsException
  {
    final Interval interval = Intervals.of("2017-01/2017-02");
    final List<Task> tasks = new ArrayList<>();
    final Set<TaskLock> activeLocks = new HashSet<>();

    // Add an exclusive lock entry of the highest priority
    Task exclusiveHigherPriorityRevokedLockTask = NoopTask.create(100);
    tasks.add(exclusiveHigherPriorityRevokedLockTask);
    taskStorage.insert(
        exclusiveHigherPriorityRevokedLockTask,
        TaskStatus.running(exclusiveHigherPriorityRevokedLockTask.getId())
    );
    lockbox.add(exclusiveHigherPriorityRevokedLockTask);
    final TaskLock exclusiveRevokedLock = tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        exclusiveHigherPriorityRevokedLockTask,
        interval
    ).getTaskLock();

    // Any equal or lower priority shared lock must fail
    final Task sharedLockTask = NoopTask.create(100);
    lockbox.add(sharedLockTask);
    Assert.assertFalse(tryTimeChunkLock(TaskLockType.SHARED, sharedLockTask, interval).isOk());

    // Revoke existing active exclusive lock
    lockbox.revokeLock(exclusiveHigherPriorityRevokedLockTask.getId(), exclusiveRevokedLock);
    Assert.assertEquals(1, getAllLocks(tasks).size());
    Assert.assertEquals(0, getAllActiveLocks(tasks).size());
    Assert.assertEquals(activeLocks, getAllActiveLocks(tasks));

    // test creating new shared locks
    for (int i = 0; i < 3; i++) {
      final Task task = NoopTask.create(Math.max(0, (i - 1) * 10)); // the first two tasks have the same priority
      tasks.add(task);
      taskStorage.insert(task, TaskStatus.running(task.getId()));
      lockbox.add(task);
      final TaskLock lock = tryTimeChunkLock(TaskLockType.SHARED, task, interval).getTaskLock();
      Assert.assertNotNull(lock);
      activeLocks.add(lock);
    }
    Assert.assertEquals(4, getAllLocks(tasks).size());
    Assert.assertEquals(3, getAllActiveLocks(tasks).size());
    Assert.assertEquals(activeLocks, getAllActiveLocks(tasks));

    // Adding an exclusive task lock of priority 15 should revoke all existing active locks
    Task exclusiveLowerPriorityLockTask = NoopTask.create(15);
    tasks.add(exclusiveLowerPriorityLockTask);
    taskStorage.insert(exclusiveLowerPriorityLockTask, TaskStatus.running(exclusiveLowerPriorityLockTask.getId()));
    lockbox.add(exclusiveLowerPriorityLockTask);
    final TaskLock lowerPriorityExclusiveLock = tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        exclusiveLowerPriorityLockTask,
        interval
    ).getTaskLock();
    activeLocks.clear();
    activeLocks.add(lowerPriorityExclusiveLock);
    Assert.assertEquals(5, getAllLocks(tasks).size());
    Assert.assertEquals(1, getAllActiveLocks(tasks).size());
    Assert.assertEquals(activeLocks, getAllActiveLocks(tasks));

    // Add new shared locks which revoke the active exclusive task lock
    activeLocks.clear();
    for (int i = 3; i < 5; i++) {
      final Task task = NoopTask.create(Math.max(0, (i - 1) * 10)); // the first two tasks have the same priority
      tasks.add(task);
      taskStorage.insert(task, TaskStatus.running(task.getId()));
      lockbox.add(task);
      final TaskLock lock = tryTimeChunkLock(TaskLockType.SHARED, task, interval).getTaskLock();
      Assert.assertNotNull(lock);
      activeLocks.add(lock);
    }
    Assert.assertEquals(7, getAllLocks(tasks).size());
    Assert.assertEquals(2, getAllActiveLocks(tasks).size());
    Assert.assertEquals(activeLocks, getAllActiveLocks(tasks));
  }

  @Test
  public void testTryMixedLocks() throws EntryExistsException
  {
    final Task lowPriorityTask = NoopTask.create(0);
    final Task lowPriorityTask2 = NoopTask.create(0);
    final Task highPiorityTask = NoopTask.create(10);
    final Interval interval1 = Intervals.of("2017-01-01/2017-01-02");
    final Interval interval2 = Intervals.of("2017-01-02/2017-01-03");
    final Interval interval3 = Intervals.of("2017-01-03/2017-01-04");

    taskStorage.insert(lowPriorityTask, TaskStatus.running(lowPriorityTask.getId()));
    taskStorage.insert(lowPriorityTask2, TaskStatus.running(lowPriorityTask2.getId()));
    taskStorage.insert(highPiorityTask, TaskStatus.running(highPiorityTask.getId()));

    lockbox.add(lowPriorityTask);
    lockbox.add(lowPriorityTask2);
    Assert.assertTrue(tryTimeChunkLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval1).isOk());
    Assert.assertTrue(tryTimeChunkLock(TaskLockType.SHARED, lowPriorityTask, interval2).isOk());
    Assert.assertTrue(tryTimeChunkLock(TaskLockType.SHARED, lowPriorityTask2, interval2).isOk());
    Assert.assertTrue(tryTimeChunkLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval3).isOk());

    lockbox.add(highPiorityTask);
    Assert.assertTrue(tryTimeChunkLock(TaskLockType.SHARED, highPiorityTask, interval1).isOk());
    Assert.assertTrue(tryTimeChunkLock(TaskLockType.EXCLUSIVE, highPiorityTask, interval2).isOk());
    Assert.assertTrue(tryTimeChunkLock(TaskLockType.EXCLUSIVE, highPiorityTask, interval3).isOk());

    Assert.assertTrue(lockbox.findLocksForTask(lowPriorityTask).stream().allMatch(TaskLock::isRevoked));
    Assert.assertTrue(lockbox.findLocksForTask(lowPriorityTask2).stream().allMatch(TaskLock::isRevoked));

    lockbox.remove(lowPriorityTask);
    lockbox.remove(lowPriorityTask2);
    lockbox.remove(highPiorityTask);

    lockbox.add(highPiorityTask);
    Assert.assertTrue(tryTimeChunkLock(TaskLockType.EXCLUSIVE, highPiorityTask, interval1).isOk());
    Assert.assertTrue(tryTimeChunkLock(TaskLockType.SHARED, highPiorityTask, interval2).isOk());
    Assert.assertTrue(tryTimeChunkLock(TaskLockType.EXCLUSIVE, highPiorityTask, interval3).isOk());

    lockbox.add(lowPriorityTask);
    Assert.assertFalse(tryTimeChunkLock(TaskLockType.SHARED, lowPriorityTask, interval1).isOk());
    Assert.assertFalse(tryTimeChunkLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval2).isOk());
    Assert.assertFalse(tryTimeChunkLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval3).isOk());
  }

  @Test
  public void testTryExclusiveLock()
  {
    Task task = NoopTask.create();
    lockbox.add(task);
    Assert.assertTrue(tryTimeChunkLock(TaskLockType.EXCLUSIVE, task, Intervals.of("2015-01-01/2015-01-03")).isOk());

    // try to take lock for task 2 for overlapping interval
    Task task2 = NoopTask.create();
    lockbox.add(task2);
    Assert.assertFalse(tryTimeChunkLock(TaskLockType.EXCLUSIVE, task2, Intervals.of("2015-01-01/2015-01-02")).isOk());

    // task 1 unlocks the lock
    lockbox.remove(task);

    // Now task2 should be able to get the lock
    Assert.assertTrue(tryTimeChunkLock(TaskLockType.EXCLUSIVE, task2, Intervals.of("2015-01-01/2015-01-02")).isOk());
  }

  @Test(expected = IllegalStateException.class)
  public void testTryLockForInactiveTask()
  {
    Assert.assertFalse(tryTimeChunkLock(TaskLockType.EXCLUSIVE, NoopTask.create(), Intervals.of("2015-01-01/2015-01-02")).isOk());
  }

  @Test
  public void testTryLockAfterTaskComplete()
  {
    Task task = NoopTask.create();
    exception.expect(ISE.class);
    exception.expectMessage("Unable to grant lock to inactive Task");
    lockbox.add(task);
    lockbox.remove(task);
    Assert.assertFalse(tryTimeChunkLock(TaskLockType.EXCLUSIVE, task, Intervals.of("2015-01-01/2015-01-02")).isOk());
  }

  @Test
  public void testTimeoutForLock() throws InterruptedException
  {
    Task task1 = NoopTask.create();
    Task task2 = NoopTask.create();

    lockbox.add(task1);
    lockbox.add(task2);
    Assert.assertTrue(acquireTimeChunkLock(TaskLockType.EXCLUSIVE, task1, Intervals.of("2015-01-01/2015-01-02"), 5000).isOk());
    Assert.assertFalse(acquireTimeChunkLock(TaskLockType.EXCLUSIVE, task2, Intervals.of("2015-01-01/2015-01-15"), 1000).isOk());
  }

  @Test
  public void testSyncFromStorage() throws EntryExistsException
  {
    final TaskLockbox originalBox = new TaskLockbox(taskStorage, metadataStorageCoordinator);
    for (int i = 0; i < 5; i++) {
      final Task task = NoopTask.create();
      taskStorage.insert(task, TaskStatus.running(task.getId()));
      originalBox.add(task);
      Assert.assertTrue(
          originalBox.tryLock(
              task,
              new TimeChunkLockRequest(
                  TaskLockType.EXCLUSIVE,
                  task,
                  Intervals.of(StringUtils.format("2017-01-0%d/2017-01-0%d", (i + 1), (i + 2))),
                  null
              )
          ).isOk()
      );
    }

    final List<TaskLock> beforeLocksInStorage = taskStorage.getActiveTasks().stream()
                                                           .flatMap(task -> taskStorage.getLocks(task.getId()).stream())
                                                           .collect(Collectors.toList());

    final TaskLockbox newBox = new TaskLockbox(taskStorage, metadataStorageCoordinator);
    newBox.syncFromStorage();

    Assert.assertEquals(originalBox.getAllLocks(), newBox.getAllLocks());
    Assert.assertEquals(originalBox.getActiveTasks(), newBox.getActiveTasks());

    final List<TaskLock> afterLocksInStorage = taskStorage.getActiveTasks().stream()
                                                          .flatMap(task -> taskStorage.getLocks(task.getId()).stream())
                                                          .collect(Collectors.toList());

    Assert.assertEquals(beforeLocksInStorage, afterLocksInStorage);
  }

  @Test
  public void testSyncFromStorageWithMissingTaskLockPriority() throws EntryExistsException
  {
    final Task task = NoopTask.create();
    taskStorage.insert(task, TaskStatus.running(task.getId()));
    taskStorage.addLock(
        task.getId(),
        new IntervalLockWithoutPriority(task.getGroupId(), task.getDataSource(), Intervals.of("2017/2018"), "v1")
    );

    final List<TaskLock> beforeLocksInStorage = taskStorage.getActiveTasks().stream()
                                                           .flatMap(t -> taskStorage.getLocks(t.getId()).stream())
                                                           .collect(Collectors.toList());

    final TaskLockbox lockbox = new TaskLockbox(taskStorage, metadataStorageCoordinator);
    lockbox.syncFromStorage();

    final List<TaskLock> afterLocksInStorage = taskStorage.getActiveTasks().stream()
                                                          .flatMap(t -> taskStorage.getLocks(t.getId()).stream())
                                                          .collect(Collectors.toList());

    Assert.assertEquals(beforeLocksInStorage, afterLocksInStorage);
  }

  @Test
  public void testSyncFromStorageWithMissingTaskPriority() throws EntryExistsException
  {
    final Task task = NoopTask.create();
    taskStorage.insert(task, TaskStatus.running(task.getId()));
    taskStorage.addLock(
        task.getId(),
        new TimeChunkLock(
            TaskLockType.EXCLUSIVE,
            task.getGroupId(),
            task.getDataSource(),
            Intervals.of("2017/2018"),
            "v1",
            task.getPriority()
        )
    );

    final List<TaskLock> beforeLocksInStorage = taskStorage.getActiveTasks().stream()
                                                           .flatMap(t -> taskStorage.getLocks(t.getId()).stream())
                                                           .collect(Collectors.toList());

    final TaskLockbox lockbox = new TaskLockbox(taskStorage, metadataStorageCoordinator);
    lockbox.syncFromStorage();

    final List<TaskLock> afterLocksInStorage = taskStorage.getActiveTasks().stream()
                                                          .flatMap(t -> taskStorage.getLocks(t.getId()).stream())
                                                          .collect(Collectors.toList());

    Assert.assertEquals(beforeLocksInStorage, afterLocksInStorage);
  }

  @Test
  public void testSyncFromStorageWithInvalidPriority() throws EntryExistsException
  {
    final Task task = NoopTask.create();
    taskStorage.insert(task, TaskStatus.running(task.getId()));
    taskStorage.addLock(
        task.getId(),
        new TimeChunkLock(
            TaskLockType.EXCLUSIVE,
            task.getGroupId(),
            task.getDataSource(),
            Intervals.of("2017/2018"),
            "v1",
            10
        )
    );

    final TaskLockbox lockbox = new TaskLockbox(taskStorage, metadataStorageCoordinator);
    TaskLockboxSyncResult result = lockbox.syncFromStorage();
    Assert.assertEquals(1, result.getTasksToFail().size());
    Assert.assertTrue(result.getTasksToFail().contains(task));
  }

  @Test
  public void testSyncWithUnknownTaskTypesFromModuleNotLoaded() throws Exception
  {
    // ensure that if we don't know how to deserialize a task it won't explode the lockbox
    // (or anything else that uses taskStorage.getActiveTasks() and doesn't expect null which is most things)
    final TestDerbyConnector derbyConnector = derby.getConnector();
    ObjectMapper loadedMapper = new DefaultObjectMapper().registerModule(new TheModule());
    TaskStorage loadedTaskStorage = new MetadataTaskStorage(
        derbyConnector,
        new TaskStorageConfig(null),
        new DerbyMetadataStorageActionHandlerFactory(
            derbyConnector,
            derby.metadataTablesConfigSupplier().get(),
            loadedMapper
        )
    );
    IndexerMetadataStorageCoordinator loadedMetadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        loadedMapper,
        derby.metadataTablesConfigSupplier().get(),
        derbyConnector
    );

    TaskLockbox theBox = new TaskLockbox(taskStorage, metadataStorageCoordinator);
    TaskLockbox loadedBox = new TaskLockbox(loadedTaskStorage, loadedMetadataStorageCoordinator);

    Task aTask = NoopTask.create();
    taskStorage.insert(aTask, TaskStatus.running(aTask.getId()));
    theBox.add(aTask);
    loadedBox.add(aTask);

    Task theTask = new MyModuleIsntLoadedTask("1", "yey", null, "foo");
    loadedTaskStorage.insert(theTask, TaskStatus.running(theTask.getId()));
    theBox.add(theTask);
    loadedBox.add(theTask);

    List<Task> tasks = taskStorage.getActiveTasks();
    List<Task> tasksFromLoaded = loadedTaskStorage.getActiveTasks();

    theBox.syncFromStorage();
    loadedBox.syncFromStorage();

    Assert.assertEquals(1, tasks.size());
    Assert.assertEquals(2, tasksFromLoaded.size());
  }

  @Test
  public void testRevokedLockSyncFromStorage() throws EntryExistsException
  {
    final TaskLockbox originalBox = new TaskLockbox(taskStorage, metadataStorageCoordinator);

    final Task task1 = NoopTask.create("task1", 10);
    taskStorage.insert(task1, TaskStatus.running(task1.getId()));
    originalBox.add(task1);
    Assert.assertTrue(originalBox.tryLock(task1, new TimeChunkLockRequest(TaskLockType.EXCLUSIVE, task1, Intervals.of("2017/2018"), null)).isOk());

    // task2 revokes task1
    final Task task2 = NoopTask.create("task2", 100);
    taskStorage.insert(task2, TaskStatus.running(task2.getId()));
    originalBox.add(task2);
    Assert.assertTrue(originalBox.tryLock(task2, new TimeChunkLockRequest(TaskLockType.EXCLUSIVE, task2, Intervals.of("2017/2018"), null)).isOk());

    final Map<String, List<TaskLock>> beforeLocksInStorage = taskStorage
        .getActiveTasks()
        .stream()
        .collect(Collectors.toMap(Task::getId, task -> taskStorage.getLocks(task.getId())));

    final List<TaskLock> task1Locks = beforeLocksInStorage.get("task1");
    Assert.assertEquals(1, task1Locks.size());
    Assert.assertTrue(task1Locks.get(0).isRevoked());

    final List<TaskLock> task2Locks = beforeLocksInStorage.get("task1");
    Assert.assertEquals(1, task2Locks.size());
    Assert.assertTrue(task2Locks.get(0).isRevoked());

    final TaskLockbox newBox = new TaskLockbox(taskStorage, metadataStorageCoordinator);
    newBox.syncFromStorage();

    final Set<TaskLock> afterLocksInStorage = taskStorage.getActiveTasks().stream()
                                                          .flatMap(task -> taskStorage.getLocks(task.getId()).stream())
                                                          .collect(Collectors.toSet());

    Assert.assertEquals(
        beforeLocksInStorage.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
        afterLocksInStorage
    );
  }

  @Test
  public void testDoInCriticalSectionWithSharedLock() throws Exception
  {
    final Interval interval = Intervals.of("2017-01-01/2017-01-02");
    final Task task = NoopTask.create();
    lockbox.add(task);
    Assert.assertTrue(tryTimeChunkLock(TaskLockType.SHARED, task, interval).isOk());

    Assert.assertTrue(
        lockbox.doInCriticalSection(
            task,
            Collections.singletonList(interval),
            CriticalAction.<Boolean>builder().onValidLocks(() -> true).onInvalidLocks(() -> false).build()
        )
    );
  }

  @Test
  public void testDoInCriticalSectionWithExclusiveLock() throws Exception
  {
    final Interval interval = Intervals.of("2017-01-01/2017-01-02");
    final Task task = NoopTask.create();
    lockbox.add(task);
    final TaskLock lock = tryTimeChunkLock(TaskLockType.EXCLUSIVE, task, interval).getTaskLock();
    Assert.assertNotNull(lock);

    Assert.assertTrue(
        lockbox.doInCriticalSection(
            task,
            Collections.singletonList(interval),
            CriticalAction.<Boolean>builder().onValidLocks(() -> true).onInvalidLocks(() -> false).build()
        )
    );
  }

  @Test
  public void testDoInCriticalSectionWithSmallerInterval() throws Exception
  {
    final Interval interval = Intervals.of("2017-01-01/2017-02-01");
    final Interval smallInterval = Intervals.of("2017-01-10/2017-01-11");
    final Task task = NoopTask.create();
    lockbox.add(task);
    final TaskLock lock = tryTimeChunkLock(TaskLockType.EXCLUSIVE, task, interval).getTaskLock();
    Assert.assertNotNull(lock);

    Assert.assertTrue(
        lockbox.doInCriticalSection(
            task,
            Collections.singletonList(interval),
            CriticalAction.<Boolean>builder().onValidLocks(() -> true).onInvalidLocks(() -> false).build()
        )
    );
  }

  @Test
  public void testPreemptionAndDoInCriticalSection() throws Exception
  {
    final Interval interval = Intervals.of("2017-01-01/2017-01-02");
    for (int i = 0; i < 5; i++) {
      final Task task = NoopTask.create();
      lockbox.add(task);
      taskStorage.insert(task, TaskStatus.running(task.getId()));
      Assert.assertTrue(tryTimeChunkLock(TaskLockType.SHARED, task, interval).isOk());
    }

    final Task highPriorityTask = NoopTask.create(100);
    lockbox.add(highPriorityTask);
    taskStorage.insert(highPriorityTask, TaskStatus.running(highPriorityTask.getId()));
    final TaskLock lock = tryTimeChunkLock(TaskLockType.EXCLUSIVE, highPriorityTask, interval).getTaskLock();
    Assert.assertNotNull(lock);

    Assert.assertTrue(
        lockbox.doInCriticalSection(
            highPriorityTask,
            Collections.singletonList(interval),
            CriticalAction.<Boolean>builder().onValidLocks(() -> true).onInvalidLocks(() -> false).build()
        )
    );
  }

  @Test
  public void testDoInCriticalSectionWithRevokedLock() throws Exception
  {
    final Interval interval = Intervals.of("2017-01-01/2017-01-02");
    final Task lowPriorityTask = NoopTask.create("task1", 0);
    final Task highPriorityTask = NoopTask.create("task2", 10);
    lockbox.add(lowPriorityTask);
    lockbox.add(highPriorityTask);
    taskStorage.insert(lowPriorityTask, TaskStatus.running(lowPriorityTask.getId()));
    taskStorage.insert(highPriorityTask, TaskStatus.running(highPriorityTask.getId()));

    final TaskLock lowPriorityLock = tryTimeChunkLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval).getTaskLock();
    Assert.assertNotNull(lowPriorityLock);
    Assert.assertTrue(tryTimeChunkLock(TaskLockType.EXCLUSIVE, highPriorityTask, interval).isOk());
    Assert.assertTrue(Iterables.getOnlyElement(lockbox.findLocksForTask(lowPriorityTask)).isRevoked());

    Assert.assertFalse(
        lockbox.doInCriticalSection(
            lowPriorityTask,
            Collections.singletonList(interval),
            CriticalAction.<Boolean>builder().onValidLocks(() -> true).onInvalidLocks(() -> false).build()
        )
    );
  }

  @Test(timeout = 60_000L)
  public void testAcquireLockAfterRevoked() throws EntryExistsException, InterruptedException
  {
    final Interval interval = Intervals.of("2017-01-01/2017-01-02");
    final Task lowPriorityTask = NoopTask.create("task1", 0);
    final Task highPriorityTask = NoopTask.create("task2", 10);
    lockbox.add(lowPriorityTask);
    lockbox.add(highPriorityTask);
    taskStorage.insert(lowPriorityTask, TaskStatus.running(lowPriorityTask.getId()));
    taskStorage.insert(highPriorityTask, TaskStatus.running(highPriorityTask.getId()));

    final TaskLock lowPriorityLock = acquireTimeChunkLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval).getTaskLock();
    Assert.assertNotNull(lowPriorityLock);
    Assert.assertTrue(tryTimeChunkLock(TaskLockType.EXCLUSIVE, highPriorityTask, interval).isOk());
    Assert.assertTrue(Iterables.getOnlyElement(lockbox.findLocksForTask(lowPriorityTask)).isRevoked());

    lockbox.unlock(highPriorityTask, interval);

    // Acquire again
    final LockResult lockResult = acquireTimeChunkLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval);
    Assert.assertFalse(lockResult.isOk());
    Assert.assertTrue(lockResult.isRevoked());
    Assert.assertTrue(Iterables.getOnlyElement(lockbox.findLocksForTask(lowPriorityTask)).isRevoked());
  }

  @Test
  public void testUnlock() throws EntryExistsException
  {
    final List<Task> lowPriorityTasks = new ArrayList<>();
    final List<Task> highPriorityTasks = new ArrayList<>();

    for (int i = 0; i < 8; i++) {
      final Task task = NoopTask.create(10);
      lowPriorityTasks.add(task);
      taskStorage.insert(task, TaskStatus.running(task.getId()));
      lockbox.add(task);
      Assert.assertTrue(
          tryTimeChunkLock(
              TaskLockType.EXCLUSIVE,
              task,
              Intervals.of(StringUtils.format("2017-01-0%d/2017-01-0%d", (i + 1), (i + 2)))
          ).isOk()
      );
    }

    // Revoke some locks
    for (int i = 0; i < 4; i++) {
      final Task task = NoopTask.create(100);
      highPriorityTasks.add(task);
      taskStorage.insert(task, TaskStatus.running(task.getId()));
      lockbox.add(task);
      Assert.assertTrue(
          tryTimeChunkLock(
              TaskLockType.EXCLUSIVE,
              task,
              Intervals.of(StringUtils.format("2017-01-0%d/2017-01-0%d", (i + 1), (i + 2)))
          ).isOk()
      );
    }

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(taskStorage.getLocks(lowPriorityTasks.get(i).getId()).stream().allMatch(TaskLock::isRevoked));
      Assert.assertFalse(taskStorage.getLocks(highPriorityTasks.get(i).getId()).stream().allMatch(TaskLock::isRevoked));
    }

    for (int i = 4; i < 8; i++) {
      Assert.assertFalse(taskStorage.getLocks(lowPriorityTasks.get(i).getId()).stream().allMatch(TaskLock::isRevoked));
    }

    for (int i = 0; i < 4; i++) {
      lockbox.unlock(
          lowPriorityTasks.get(i),
          Intervals.of(StringUtils.format("2017-01-0%d/2017-01-0%d", (i + 1), (i + 2)))
      );
      lockbox.unlock(
          highPriorityTasks.get(i),
          Intervals.of(StringUtils.format("2017-01-0%d/2017-01-0%d", (i + 1), (i + 2)))
      );
    }

    for (int i = 4; i < 8; i++) {
      lockbox.unlock(
          lowPriorityTasks.get(i),
          Intervals.of(StringUtils.format("2017-01-0%d/2017-01-0%d", (i + 1), (i + 2)))
      );
    }

    Assert.assertTrue(lockbox.getAllLocks().isEmpty());
  }

  @Test
  public void testFindLockPosseAfterRevokeWithDifferentLockIntervals() throws EntryExistsException
  {
    final Task lowPriorityTask = NoopTask.create(0);
    final Task highPriorityTask = NoopTask.create(10);

    taskStorage.insert(lowPriorityTask, TaskStatus.running(lowPriorityTask.getId()));
    taskStorage.insert(highPriorityTask, TaskStatus.running(highPriorityTask.getId()));
    lockbox.add(lowPriorityTask);
    lockbox.add(highPriorityTask);

    Assert.assertTrue(
        tryTimeChunkLock(
            TaskLockType.EXCLUSIVE,
            lowPriorityTask,
            Intervals.of("2018-12-16T09:00:00/2018-12-16T10:00:00")
        ).isOk()
    );

    Assert.assertTrue(
        tryTimeChunkLock(
            TaskLockType.EXCLUSIVE,
            highPriorityTask,
            Intervals.of("2018-12-16T09:00:00/2018-12-16T09:30:00")
        ).isOk()
    );

    final List<TaskLockPosse> highLockPosses = lockbox.getOnlyTaskLockPosseContainingInterval(
        highPriorityTask,
        Intervals.of("2018-12-16T09:00:00/2018-12-16T09:30:00")
    );

    Assert.assertEquals(1, highLockPosses.size());
    Assert.assertTrue(highLockPosses.get(0).containsTask(highPriorityTask));
    Assert.assertFalse(highLockPosses.get(0).getTaskLock().isRevoked());

    final List<TaskLockPosse> lowLockPosses = lockbox.getOnlyTaskLockPosseContainingInterval(
        lowPriorityTask,
        Intervals.of("2018-12-16T09:00:00/2018-12-16T10:00:00")
    );

    Assert.assertEquals(1, lowLockPosses.size());
    Assert.assertTrue(lowLockPosses.get(0).containsTask(lowPriorityTask));
    Assert.assertTrue(lowLockPosses.get(0).getTaskLock().isRevoked());
  }

  @Test
  public void testSegmentLock() throws InterruptedException
  {
    final Task task = NoopTask.create();
    lockbox.add(task);
    final LockResult lockResult = lockbox.lock(
        task,
        new SpecificSegmentLockRequest(
            TaskLockType.EXCLUSIVE,
            task,
            Intervals.of("2015-01-01/2015-01-02"),
            "v1",
            3
        )
    );
    Assert.assertTrue(lockResult.isOk());
    Assert.assertNull(lockResult.getNewSegmentId());
    Assert.assertTrue(lockResult.getTaskLock() instanceof SegmentLock);
    final SegmentLock segmentLock = (SegmentLock) lockResult.getTaskLock();
    Assert.assertEquals(TaskLockType.EXCLUSIVE, segmentLock.getType());
    Assert.assertEquals(task.getGroupId(), segmentLock.getGroupId());
    Assert.assertEquals(task.getDataSource(), segmentLock.getDataSource());
    Assert.assertEquals(Intervals.of("2015-01-01/2015-01-02"), segmentLock.getInterval());
    Assert.assertEquals("v1", segmentLock.getVersion());
    Assert.assertEquals(3, segmentLock.getPartitionId());
    Assert.assertEquals(task.getPriority(), segmentLock.getPriority().intValue());
    Assert.assertFalse(segmentLock.isRevoked());
  }

  @Test
  public void testSegmentAndTimeChunkLockForSameInterval()
  {
    final Task task1 = NoopTask.create();
    lockbox.add(task1);

    final Task task2 = NoopTask.create();
    lockbox.add(task2);

    Assert.assertTrue(
        lockbox.tryLock(
            task1,
            new SpecificSegmentLockRequest(
                TaskLockType.EXCLUSIVE,
                task1,
                Intervals.of("2015-01-01/2015-01-02"),
                "v1",
                3
            )
        ).isOk()
    );

    Assert.assertFalse(
        lockbox.tryLock(
            task2,
            new TimeChunkLockRequest(
                TaskLockType.EXCLUSIVE,
                task2,
                Intervals.of("2015-01-01/2015-01-02"),
                "v1"
            )
        ).isOk()
    );
  }

  @Test
  public void testSegmentAndTimeChunkLockForSameIntervalWithDifferentPriority() throws EntryExistsException
  {
    final Task task1 = NoopTask.create(10);
    lockbox.add(task1);
    taskStorage.insert(task1, TaskStatus.running(task1.getId()));

    final Task task2 = NoopTask.create(100);
    lockbox.add(task2);
    taskStorage.insert(task2, TaskStatus.running(task2.getId()));

    Assert.assertTrue(
        lockbox.tryLock(
            task1,
            new SpecificSegmentLockRequest(
                TaskLockType.EXCLUSIVE,
                task1,
                Intervals.of("2015-01-01/2015-01-02"),
                "v1",
                3
            )
        ).isOk()
    );

    Assert.assertTrue(
        lockbox.tryLock(
            task2,
            new TimeChunkLockRequest(
                TaskLockType.EXCLUSIVE,
                task2,
                Intervals.of("2015-01-01/2015-01-02"),
                "v1"
            )
        ).isOk()
    );

    final LockResult resultOfTask1 = lockbox.tryLock(
        task1,
        new SpecificSegmentLockRequest(
            TaskLockType.EXCLUSIVE,
            task1,
            Intervals.of("2015-01-01/2015-01-02"),
            "v1",
            3
        )
    );
    Assert.assertFalse(resultOfTask1.isOk());
    Assert.assertTrue(resultOfTask1.isRevoked());
  }

  @Test
  public void testSegmentLockForSameIntervalAndSamePartition()
  {
    final Task task1 = NoopTask.create();
    lockbox.add(task1);

    final Task task2 = NoopTask.create();
    lockbox.add(task2);

    Assert.assertTrue(
        lockbox.tryLock(
            task1,
            new SpecificSegmentLockRequest(
                TaskLockType.EXCLUSIVE,
                task1,
                Intervals.of("2015-01-01/2015-01-02"),
                "v1",
                3
            )
        ).isOk()
    );

    Assert.assertFalse(
        lockbox.tryLock(
            task2,
            new SpecificSegmentLockRequest(
                TaskLockType.EXCLUSIVE,
                task2,
                Intervals.of("2015-01-01/2015-01-02"),
                "v1",
                3
            )
        ).isOk()
    );
  }

  @Test
  public void testSegmentLockForSameIntervalDifferentPartition()
  {
    final Task task1 = NoopTask.create();
    lockbox.add(task1);

    final Task task2 = NoopTask.create();
    lockbox.add(task2);

    Assert.assertTrue(
        lockbox.tryLock(
            task1,
            new SpecificSegmentLockRequest(
                TaskLockType.EXCLUSIVE,
                task1,
                Intervals.of("2015-01-01/2015-01-02"),
                "v1",
                3
            )
        ).isOk()
    );

    Assert.assertTrue(
        lockbox.tryLock(
            task2,
            new SpecificSegmentLockRequest(
                TaskLockType.EXCLUSIVE,
                task2,
                Intervals.of("2015-01-01/2015-01-02"),
                "v1",
                2
            )
        ).isOk()
    );
  }

  @Test
  public void testSegmentLockForOverlappedIntervalDifferentPartition()
  {
    final Task task1 = NoopTask.create();
    lockbox.add(task1);

    final Task task2 = NoopTask.create();
    lockbox.add(task2);

    Assert.assertTrue(
        lockbox.tryLock(
            task1,
            new SpecificSegmentLockRequest(
                TaskLockType.EXCLUSIVE,
                task1,
                Intervals.of("2015-01-01/2015-01-05"),
                "v1",
                3
            )
        ).isOk()
    );

    Assert.assertFalse(
        lockbox.tryLock(
            task2,
            new SpecificSegmentLockRequest(
                TaskLockType.EXCLUSIVE,
                task2,
                Intervals.of("2015-01-03/2015-01-08"),
                "v1",
                2
            )
        ).isOk()
    );
  }

  @Test
  public void testRequestForNewSegmentWithSegmentLock()
  {
    final Task task = NoopTask.create();
    lockbox.add(task);
    allocateSegmentsAndAssert(task, "seq", 3, NumberedPartialShardSpec.instance());
    allocateSegmentsAndAssert(task, "seq2", 2, new NumberedOverwritePartialShardSpec(0, 3, (short) 1));

    final List<TaskLock> locks = lockbox.findLocksForTask(task);
    Assert.assertEquals(5, locks.size());
    int expectedPartitionId = 0;
    for (TaskLock lock : locks) {
      Assert.assertTrue(lock instanceof SegmentLock);
      final SegmentLock segmentLock = (SegmentLock) lock;
      Assert.assertEquals(expectedPartitionId++, segmentLock.getPartitionId());
      if (expectedPartitionId == 3) {
        expectedPartitionId = PartitionIds.NON_ROOT_GEN_START_PARTITION_ID;
      }
    }
  }

  @Test
  public void testRequestForNewSegmentWithHashPartition()
  {
    final Task task = NoopTask.create();
    lockbox.add(task);

    allocateSegmentsAndAssert(task, "seq", 3, new HashBasedNumberedPartialShardSpec(null, 1, 3, null));
    allocateSegmentsAndAssert(task, "seq2", 5, new HashBasedNumberedPartialShardSpec(null, 3, 5, null));
  }

  private void allocateSegmentsAndAssert(
      Task task,
      String baseSequenceName,
      int numSegmentsToAllocate,
      PartialShardSpec partialShardSpec
  )
  {
    for (int i = 0; i < numSegmentsToAllocate; i++) {
      final LockRequestForNewSegment request = new LockRequestForNewSegment(
          LockGranularity.SEGMENT,
          TaskLockType.EXCLUSIVE,
          task,
          Intervals.of("2015-01-01/2015-01-05"),
          partialShardSpec,
          StringUtils.format("%s_%d", baseSequenceName, i),
          null,
          true
      );
      assertAllocatedSegments(request, lockbox.tryLock(task, request));
    }
  }

  private void assertAllocatedSegments(
      LockRequestForNewSegment lockRequest,
      LockResult result
  )
  {
    Assert.assertTrue(result.isOk());
    Assert.assertNotNull(result.getTaskLock());
    Assert.assertTrue(result.getTaskLock() instanceof SegmentLock);
    Assert.assertNotNull(result.getNewSegmentId());
    final SegmentLock segmentLock = (SegmentLock) result.getTaskLock();
    final SegmentIdWithShardSpec segmentId = result.getNewSegmentId();

    Assert.assertEquals(lockRequest.getType(), segmentLock.getType());
    Assert.assertEquals(lockRequest.getGroupId(), segmentLock.getGroupId());
    Assert.assertEquals(lockRequest.getDataSource(), segmentLock.getDataSource());
    Assert.assertEquals(lockRequest.getInterval(), segmentLock.getInterval());
    Assert.assertEquals(lockRequest.getPartialShardSpec().getShardSpecClass(), segmentId.getShardSpec().getClass());
    Assert.assertEquals(lockRequest.getPriority(), lockRequest.getPriority());
  }

  @Test
  public void testLockPosseEquals()
  {
    final Task task1 = NoopTask.create();
    final Task task2 = NoopTask.create();

    TaskLock taskLock1 = new TimeChunkLock(
        TaskLockType.EXCLUSIVE,
        task1.getGroupId(),
        task1.getDataSource(),
        Intervals.of("2018/2019"),
        "v1",
        task1.getPriority()
    );

    TaskLock taskLock2 = new TimeChunkLock(
        TaskLockType.EXCLUSIVE,
        task2.getGroupId(),
        task2.getDataSource(),
        Intervals.of("2018/2019"),
        "v2",
        task2.getPriority()
    );

    TaskLockPosse taskLockPosse1 = new TaskLockPosse(taskLock1);
    TaskLockPosse taskLockPosse2 = new TaskLockPosse(taskLock2);
    TaskLockPosse taskLockPosse3 = new TaskLockPosse(taskLock1);

    Assert.assertNotEquals(taskLockPosse1, null);
    Assert.assertNotEquals(null, taskLockPosse1);
    Assert.assertNotEquals(taskLockPosse1, taskLockPosse2);
    Assert.assertEquals(taskLockPosse1, taskLockPosse3);
  }

  @Test
  public void testGetTimeChunkAndSegmentLockForSameGroup()
  {
    final Task task1 = NoopTask.withGroupId("groupId");
    final Task task2 = NoopTask.withGroupId("groupId");

    lockbox.add(task1);
    lockbox.add(task2);

    Assert.assertTrue(
        lockbox.tryLock(
            task1,
            new TimeChunkLockRequest(TaskLockType.EXCLUSIVE, task1, Intervals.of("2017/2018"), null)
        ).isOk()
    );

    Assert.assertTrue(
        lockbox.tryLock(
            task2,
            new SpecificSegmentLockRequest(TaskLockType.EXCLUSIVE, task2, Intervals.of("2017/2018"), "version", 0)
        ).isOk()
    );

    final List<TaskLockPosse> posses = lockbox
        .getAllLocks()
        .get(task1.getDataSource())
        .get(DateTimes.of("2017"))
        .get(Intervals.of("2017/2018"));
    Assert.assertEquals(2, posses.size());

    Assert.assertEquals(LockGranularity.TIME_CHUNK, posses.get(0).getTaskLock().getGranularity());
    final TimeChunkLock timeChunkLock = (TimeChunkLock) posses.get(0).getTaskLock();
    Assert.assertEquals("none", timeChunkLock.getDataSource());
    Assert.assertEquals("groupId", timeChunkLock.getGroupId());
    Assert.assertEquals(Intervals.of("2017/2018"), timeChunkLock.getInterval());

    Assert.assertEquals(LockGranularity.SEGMENT, posses.get(1).getTaskLock().getGranularity());
    final SegmentLock segmentLock = (SegmentLock) posses.get(1).getTaskLock();
    Assert.assertEquals("none", segmentLock.getDataSource());
    Assert.assertEquals("groupId", segmentLock.getGroupId());
    Assert.assertEquals(Intervals.of("2017/2018"), segmentLock.getInterval());
    Assert.assertEquals(0, segmentLock.getPartitionId());
  }

  @Test
  public void testGetTimeChunkAndSegmentLockForDifferentGroup()
  {
    final Task task1 = NoopTask.withGroupId("groupId");
    final Task task2 = NoopTask.withGroupId("groupId2");

    lockbox.add(task1);
    lockbox.add(task2);

    Assert.assertTrue(
        lockbox.tryLock(
            task1,
            new TimeChunkLockRequest(TaskLockType.EXCLUSIVE, task1, Intervals.of("2017/2018"), null)
        ).isOk()
    );

    Assert.assertFalse(
        lockbox.tryLock(
            task2,
            new SpecificSegmentLockRequest(TaskLockType.EXCLUSIVE, task2, Intervals.of("2017/2018"), "version", 0)
        ).isOk()
    );
  }

  @Test
  public void testGetLockedIntervals()
  {
    // Acquire locks for task1
    final Task task1 = NoopTask.create("ds1");
    lockbox.add(task1);

    tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        task1,
        Intervals.of("2017-01-01/2017-02-01")
    );
    tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        task1,
        Intervals.of("2017-04-01/2017-05-01")
    );

    // Acquire locks for task2
    final Task task2 = NoopTask.create("ds2");
    lockbox.add(task2);
    tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        task2,
        Intervals.of("2017-03-01/2017-04-01")
    );

    // Verify the locked intervals
    final Map<String, Integer> minTaskPriority = new HashMap<>();
    minTaskPriority.put(task1.getDataSource(), 10);
    minTaskPriority.put(task2.getDataSource(), 10);
    final Map<String, List<Interval>> lockedIntervals = lockbox.getLockedIntervals(minTaskPriority);
    Assert.assertEquals(2, lockedIntervals.size());

    Assert.assertEquals(
        Arrays.asList(
            Intervals.of("2017-01-01/2017-02-01"),
            Intervals.of("2017-04-01/2017-05-01")
        ),
        lockedIntervals.get(task1.getDataSource())
    );

    Assert.assertEquals(
        Collections.singletonList(
            Intervals.of("2017-03-01/2017-04-01")),
        lockedIntervals.get(task2.getDataSource())
    );
  }

  @Test
  public void testGetLockedIntervalsForLowPriorityTask() throws Exception
  {
    // Acquire lock for a low priority task
    final Task lowPriorityTask = NoopTask.create(5);
    lockbox.add(lowPriorityTask);
    taskStorage.insert(lowPriorityTask, TaskStatus.running(lowPriorityTask.getId()));
    tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        lowPriorityTask,
        Intervals.of("2017/2018")
    );

    final Map<String, Integer> minTaskPriority = new HashMap<>();
    minTaskPriority.put(lowPriorityTask.getDataSource(), 10);

    Map<String, List<Interval>> lockedIntervals = lockbox.getLockedIntervals(minTaskPriority);
    Assert.assertTrue(lockedIntervals.isEmpty());
  }

  @Test
  public void testGetLockedIntervalsForEqualPriorityTask() throws Exception
  {
    // Acquire lock for a low priority task
    final Task task = NoopTask.create(5);
    lockbox.add(task);
    taskStorage.insert(task, TaskStatus.running(task.getId()));
    tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        task,
        Intervals.of("2017/2018")
    );

    final Map<String, Integer> minTaskPriority = new HashMap<>();
    minTaskPriority.put(task.getDataSource(), 5);

    Map<String, List<Interval>> lockedIntervals = lockbox.getLockedIntervals(minTaskPriority);
    Assert.assertEquals(1, lockedIntervals.size());
    Assert.assertEquals(
        Collections.singletonList(Intervals.of("2017/2018")),
        lockedIntervals.get(task.getDataSource())
    );
  }

  @Test
  public void testExclusiveLockCompatibility() throws Exception
  {
    final List<Task> tasks = new ArrayList<>();

    final Task exclusiveTask = NoopTask.create(10);
    tasks.add(exclusiveTask);
    lockbox.add(exclusiveTask);
    taskStorage.insert(exclusiveTask, TaskStatus.running(exclusiveTask.getId()));
    TaskLock theLock = tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        exclusiveTask,
        Intervals.of("2017/2018")
    ).getTaskLock();
    Assert.assertNotNull(theLock);
    Assert.assertFalse(theLock.isRevoked());

    // Another exclusive lock cannot be created for an overlapping interval
    final Task otherExclusiveTask = NoopTask.create(10);
    tasks.add(otherExclusiveTask);
    lockbox.add(otherExclusiveTask);
    taskStorage.insert(otherExclusiveTask, TaskStatus.running(otherExclusiveTask.getId()));
    Assert.assertNull(
        tryTimeChunkLock(
            TaskLockType.EXCLUSIVE,
            otherExclusiveTask,
            Intervals.of("2017-05-01/2017-06-01")
        ).getTaskLock()
    );

    // A shared lock cannot be created for an overlapping interval
    final Task otherSharedTask = NoopTask.create(10);
    tasks.add(otherSharedTask);
    lockbox.add(otherSharedTask);
    taskStorage.insert(otherSharedTask, TaskStatus.running(otherSharedTask.getId()));
    Assert.assertNull(
        tryTimeChunkLock(
        TaskLockType.SHARED,
        otherSharedTask,
        Intervals.of("2016/2019")
        ).getTaskLock()
    );

    // A replace lock cannot be created for an overlapping interval
    final Task otherReplaceTask = NoopTask.create(10);
    tasks.add(otherReplaceTask);
    lockbox.add(otherReplaceTask);
    taskStorage.insert(otherReplaceTask, TaskStatus.running(otherReplaceTask.getId()));
    Assert.assertNull(
        tryTimeChunkLock(
            TaskLockType.REPLACE,
            otherReplaceTask,
            Intervals.of("2017/2018")
        ).getTaskLock()
    );

    // An append lock cannot be created for an overlapping interval
    final Task otherAppendTask = NoopTask.create(10);
    tasks.add(otherAppendTask);
    lockbox.add(otherAppendTask);
    taskStorage.insert(otherAppendTask, TaskStatus.running(otherAppendTask.getId()));
    Assert.assertNull(
        tryTimeChunkLock(
            TaskLockType.APPEND,
            otherAppendTask,
            Intervals.of("2017-05-01/2018-05-01")
        ).getTaskLock()
    );

    final Set<TaskLock> expectedLocks = ImmutableSet.of(
        theLock
    );
    Assert.assertEquals(expectedLocks, getAllLocks(tasks));
  }

  @Test
  public void testExclusiveLockCanRevokeAllIncompatible() throws Exception
  {
    final List<Task> tasks = new ArrayList<>();

    // Revoked shared lock of higher priority -> revoked locks are not incompatible
    final Task otherSharedTask = NoopTask.create(15);
    tasks.add(otherSharedTask);
    lockbox.add(otherSharedTask);
    taskStorage.insert(otherSharedTask, TaskStatus.running(otherSharedTask.getId()));
    TaskLock sharedLock = tryTimeChunkLock(
        TaskLockType.SHARED,
        otherSharedTask,
        Intervals.of("2016/2019")
    ).getTaskLock();
    Assert.assertNotNull(sharedLock);
    lockbox.revokeLock(otherSharedTask.getId(), sharedLock);

    // Active Exclusive lock of lower priority -> will be revoked
    final Task otherExclusiveTask = NoopTask.create(5);
    tasks.add(otherExclusiveTask);
    lockbox.add(otherExclusiveTask);
    taskStorage.insert(otherExclusiveTask, TaskStatus.running(otherExclusiveTask.getId()));
    TaskLock exclusiveLock = tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        otherExclusiveTask,
        Intervals.of("2017-01-01/2017-02-01")
    ).getTaskLock();
    Assert.assertNotNull(exclusiveLock);
    Assert.assertFalse(exclusiveLock.isRevoked());

    // Active replace lock of lower priority -> will be revoked
    final Task otherReplaceTask = NoopTask.create(5);
    tasks.add(otherReplaceTask);
    lockbox.add(otherReplaceTask);
    taskStorage.insert(otherReplaceTask, TaskStatus.running(otherReplaceTask.getId()));
    TaskLock replaceLock = tryTimeChunkLock(
        TaskLockType.REPLACE,
        otherReplaceTask,
        Intervals.of("2017-07-01/2018-01-01")
    ).getTaskLock();
    Assert.assertNotNull(replaceLock);
    Assert.assertFalse(replaceLock.isRevoked());

    // Active append lock of lower priority -> will be revoked
    final Task otherAppendTask = NoopTask.create(5);
    tasks.add(otherAppendTask);
    lockbox.add(otherAppendTask);
    taskStorage.insert(otherAppendTask, TaskStatus.running(otherAppendTask.getId()));
    TaskLock appendLock = tryTimeChunkLock(
        TaskLockType.APPEND,
        otherAppendTask,
        Intervals.of("2017-09-01/2017-10-01")
    ).getTaskLock();
    Assert.assertNotNull(appendLock);
    Assert.assertFalse(appendLock.isRevoked());

    final Task exclusiveTask = NoopTask.create(10);
    tasks.add(exclusiveTask);
    lockbox.add(exclusiveTask);
    taskStorage.insert(exclusiveTask, TaskStatus.running(exclusiveTask.getId()));
    TaskLock theLock = tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        exclusiveTask,
        Intervals.of("2017/2018")
    ).getTaskLock();
    Assert.assertNotNull(theLock);
    Assert.assertFalse(theLock.isRevoked());

    final Set<TaskLock> expectedActiveLocks = ImmutableSet.of(
        theLock
    );
    Assert.assertEquals(expectedActiveLocks, getAllActiveLocks(tasks));
    final Set<TaskLock> expectedLocks = ImmutableSet.of(
        theLock,
        sharedLock.revokedCopy(),
        exclusiveLock.revokedCopy(),
        appendLock.revokedCopy(),
        replaceLock.revokedCopy()
    );
    Assert.assertEquals(expectedLocks, getAllLocks(tasks));
  }

  @Test
  public void testSharedLockCompatibility() throws Exception
  {
    final List<Task> tasks = new ArrayList<>();

    final Task sharedTask = NoopTask.create(10);
    tasks.add(sharedTask);
    lockbox.add(sharedTask);
    taskStorage.insert(sharedTask, TaskStatus.running(sharedTask.getId()));
    TaskLock theLock = tryTimeChunkLock(
        TaskLockType.SHARED,
        sharedTask,
        Intervals.of("2017/2018")
    ).getTaskLock();
    Assert.assertNotNull(theLock);
    Assert.assertFalse(theLock.isRevoked());

    // No overlapping exclusive lock is compatible
    final Task otherExclusiveTask = NoopTask.create(10);
    tasks.add(otherExclusiveTask);
    lockbox.add(otherExclusiveTask);
    taskStorage.insert(otherExclusiveTask, TaskStatus.running(otherExclusiveTask.getId()));
    Assert.assertNull(
        tryTimeChunkLock(
            TaskLockType.EXCLUSIVE,
            otherExclusiveTask,
            Intervals.of("2017-05-01/2017-06-01")
        ).getTaskLock()
    );

    // Enclosing shared lock of lower priority - compatible
    final Task otherSharedTask0 = NoopTask.create(5);
    tasks.add(otherSharedTask0);
    lockbox.add(otherSharedTask0);
    taskStorage.insert(otherSharedTask0, TaskStatus.running(otherSharedTask0.getId()));
    TaskLock sharedLock0 = tryTimeChunkLock(
            TaskLockType.SHARED,
            otherSharedTask0,
            Intervals.of("2016/2019")
    ).getTaskLock();
    Assert.assertNotNull(sharedLock0);
    Assert.assertFalse(sharedLock0.isRevoked());

    // Enclosed shared lock of higher priority - compatible
    final Task otherSharedTask1 = NoopTask.create(15);
    tasks.add(otherSharedTask1);
    lockbox.add(otherSharedTask1);
    taskStorage.insert(otherSharedTask1, TaskStatus.running(otherSharedTask1.getId()));
    TaskLock sharedLock1 = tryTimeChunkLock(
        TaskLockType.SHARED,
        otherSharedTask1,
        Intervals.of("2017-06-01/2017-07-01")
    ).getTaskLock();
    Assert.assertNotNull(sharedLock1);
    Assert.assertFalse(sharedLock1.isRevoked());

    // Partially Overlapping shared lock of equal priority - compatible
    final Task otherSharedTask2 = NoopTask.create(10);
    tasks.add(otherSharedTask2);
    lockbox.add(otherSharedTask2);
    taskStorage.insert(otherSharedTask2, TaskStatus.running(otherSharedTask2.getId()));
    TaskLock sharedLock2 = tryTimeChunkLock(
        TaskLockType.SHARED,
        otherSharedTask2,
        Intervals.of("2017-05-01/2018-05-01")
    ).getTaskLock();
    Assert.assertNotNull(sharedLock2);
    Assert.assertFalse(sharedLock2.isRevoked());

    // Conficting replace locks are incompatible
    final Task otherReplaceTask = NoopTask.create(10);
    tasks.add(otherReplaceTask);
    lockbox.add(otherReplaceTask);
    taskStorage.insert(otherReplaceTask, TaskStatus.running(otherReplaceTask.getId()));
    Assert.assertNull(
        tryTimeChunkLock(
            TaskLockType.REPLACE,
            otherReplaceTask,
            Intervals.of("2017/2018")
        ).getTaskLock()
    );

    // Conflicting append locks are incompatible
    final Task otherAppendTask = NoopTask.create(10);
    tasks.add(otherAppendTask);
    lockbox.add(otherAppendTask);
    taskStorage.insert(otherAppendTask, TaskStatus.running(otherAppendTask.getId()));
    Assert.assertNull(
        tryTimeChunkLock(
            TaskLockType.APPEND,
            otherAppendTask,
            Intervals.of("2017-05-01/2018-05-01")
        ).getTaskLock()
    );

    final Set<TaskLock> expectedLocks = ImmutableSet.of(
        theLock,
        sharedLock0,
        sharedLock1,
        sharedLock2
    );
    Assert.assertEquals(expectedLocks, getAllLocks(tasks));
  }

  @Test
  public void testSharedLockCanRevokeAllIncompatible() throws Exception
  {
    final List<Task> tasks = new ArrayList<>();

    // Revoked Exclusive lock of higher priority -> revoked locks are not incompatible
    final Task otherExclusiveTask = NoopTask.create(15);
    tasks.add(otherExclusiveTask);
    lockbox.add(otherExclusiveTask);
    taskStorage.insert(otherExclusiveTask, TaskStatus.running(otherExclusiveTask.getId()));
    TaskLock exclusiveLock = tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        otherExclusiveTask,
        Intervals.of("2016/2019")
    ).getTaskLock();
    Assert.assertNotNull(exclusiveLock);
    lockbox.revokeLock(otherExclusiveTask.getId(), exclusiveLock);

    // Active Shared lock of same priority -> will not be affected
    final Task otherSharedTask = NoopTask.create(10);
    tasks.add(otherSharedTask);
    lockbox.add(otherSharedTask);
    taskStorage.insert(otherSharedTask, TaskStatus.running(otherSharedTask.getId()));
    TaskLock sharedLock = tryTimeChunkLock(
        TaskLockType.SHARED,
        otherExclusiveTask,
        Intervals.of("2017-01-01/2017-02-01")
    ).getTaskLock();
    Assert.assertNotNull(sharedLock);
    Assert.assertFalse(sharedLock.isRevoked());

    // Active replace lock of lower priority -> will be revoked
    final Task otherReplaceTask = NoopTask.create(5);
    tasks.add(otherReplaceTask);
    lockbox.add(otherReplaceTask);
    taskStorage.insert(otherReplaceTask, TaskStatus.running(otherReplaceTask.getId()));
    TaskLock replaceLock = tryTimeChunkLock(
        TaskLockType.REPLACE,
        otherReplaceTask,
        Intervals.of("2017-07-01/2018-07-01")
    ).getTaskLock();
    Assert.assertNotNull(replaceLock);
    Assert.assertFalse(replaceLock.isRevoked());

    // Active append lock of lower priority -> will be revoked
    final Task otherAppendTask = NoopTask.create(5);
    tasks.add(otherAppendTask);
    lockbox.add(otherAppendTask);
    taskStorage.insert(otherAppendTask, TaskStatus.running(otherAppendTask.getId()));
    TaskLock appendLock = tryTimeChunkLock(
        TaskLockType.APPEND,
        otherAppendTask,
        Intervals.of("2017-02-01/2017-03-01")
    ).getTaskLock();
    Assert.assertNotNull(appendLock);
    Assert.assertFalse(appendLock.isRevoked());

    final Task sharedTask = NoopTask.create(10);
    tasks.add(sharedTask);
    lockbox.add(sharedTask);
    taskStorage.insert(sharedTask, TaskStatus.running(sharedTask.getId()));
    TaskLock theLock = tryTimeChunkLock(
        TaskLockType.SHARED,
        sharedTask,
        Intervals.of("2017/2018")
    ).getTaskLock();
    Assert.assertNotNull(theLock);
    Assert.assertFalse(theLock.isRevoked());

    final Set<TaskLock> expectedLocks = ImmutableSet.of(
        theLock,
        sharedLock,
        exclusiveLock.revokedCopy(),
        replaceLock.revokedCopy(),
        appendLock.revokedCopy()
    );
    Assert.assertEquals(expectedLocks, getAllLocks(tasks));

    final Set<TaskLock> expectedActiveLocks = ImmutableSet.of(
        theLock,
        sharedLock
    );
    Assert.assertEquals(expectedActiveLocks, getAllActiveLocks(tasks));
  }

  @Test
  public void testAppendLockCompatibility() throws Exception
  {
    final List<Task> tasks = new ArrayList<>();

    final Task appendTask = NoopTask.create(10);
    tasks.add(appendTask);
    lockbox.add(appendTask);
    taskStorage.insert(appendTask, TaskStatus.running(appendTask.getId()));
    TaskLock theLock = tryTimeChunkLock(
        TaskLockType.APPEND,
        appendTask,
        Intervals.of("2017/2018")
    ).getTaskLock();
    Assert.assertNotNull(theLock);
    Assert.assertFalse(theLock.isRevoked());

    // An exclusive lock cannot be created for an overlapping interval
    final Task otherExclusiveTask = NoopTask.create(10);
    tasks.add(otherExclusiveTask);
    lockbox.add(otherExclusiveTask);
    taskStorage.insert(otherExclusiveTask, TaskStatus.running(otherExclusiveTask.getId()));
    Assert.assertNull(
        tryTimeChunkLock(
            TaskLockType.EXCLUSIVE,
            otherExclusiveTask,
            Intervals.of("2017-05-01/2017-06-01")
        ).getTaskLock()
    );

    // A shared lock cannot be created for an overlapping interval
    final Task otherSharedTask = NoopTask.create(10);
    tasks.add(otherSharedTask);
    lockbox.add(otherSharedTask);
    taskStorage.insert(otherSharedTask, TaskStatus.running(otherSharedTask.getId()));
    Assert.assertNull(
        tryTimeChunkLock(
            TaskLockType.SHARED,
            otherSharedTask,
            Intervals.of("2016/2019")
        ).getTaskLock()
    );

    // A replace lock cannot be created for a non-enclosing interval
    final Task otherReplaceTask0 = NoopTask.create(10);
    tasks.add(otherReplaceTask0);
    lockbox.add(otherReplaceTask0);
    taskStorage.insert(otherReplaceTask0, TaskStatus.running(otherReplaceTask0.getId()));
    TaskLock replaceLock0 = tryTimeChunkLock(
        TaskLockType.REPLACE,
        otherReplaceTask0,
        Intervals.of("2017-05-01/2018-01-01")
    ).getTaskLock();
    Assert.assertNull(replaceLock0);

    // A replace lock can be created for an enclosing interval
    final Task otherReplaceTask1 = NoopTask.create(10);
    tasks.add(otherReplaceTask1);
    lockbox.add(otherReplaceTask1);
    taskStorage.insert(otherReplaceTask1, TaskStatus.running(otherReplaceTask1.getId()));
    TaskLock replaceLock1 = tryTimeChunkLock(
        TaskLockType.REPLACE,
        otherReplaceTask1,
        Intervals.of("2017/2018")
    ).getTaskLock();
    Assert.assertNotNull(replaceLock1);
    Assert.assertFalse(replaceLock1.isRevoked());

    // Another replace lock cannot be created
    final Task otherReplaceTask2 = NoopTask.create(10);
    tasks.add(otherReplaceTask2);
    lockbox.add(otherReplaceTask2);
    taskStorage.insert(otherReplaceTask2, TaskStatus.running(otherReplaceTask2.getId()));
    TaskLock replaceLock2 = tryTimeChunkLock(
        TaskLockType.REPLACE,
        otherReplaceTask2,
        Intervals.of("2016/2019")
    ).getTaskLock();
    Assert.assertNull(replaceLock2);


    // Any append lock can be created, provided that it lies within the interval of the previously created replace lock
    // This should not revoke any of the existing locks even with a higher priority
    final Task otherAppendTask0 = NoopTask.create(15);
    tasks.add(otherAppendTask0);
    lockbox.add(otherAppendTask0);
    taskStorage.insert(otherAppendTask0, TaskStatus.running(otherAppendTask0.getId()));
    TaskLock appendLock0 = tryTimeChunkLock(
        TaskLockType.APPEND,
        otherAppendTask0,
        Intervals.of("2017-05-01/2017-06-01")
    ).getTaskLock();
    Assert.assertNotNull(appendLock0);
    Assert.assertFalse(appendLock0.isRevoked());

    // Append lock with a lower priority can be created as well
    final Task otherAppendTask1 = NoopTask.create(5);
    tasks.add(otherAppendTask1);
    lockbox.add(otherAppendTask1);
    taskStorage.insert(otherAppendTask1, TaskStatus.running(otherAppendTask1.getId()));
    TaskLock appendLock1 = tryTimeChunkLock(
        TaskLockType.APPEND,
        otherAppendTask1,
        Intervals.of("2017-05-01/2017-06-01")
        //Intervals.of("2017-05-01/2018-06-01")
    ).getTaskLock();
    Assert.assertNotNull(appendLock1);
    Assert.assertFalse(appendLock1.isRevoked());

    final Set<TaskLock> expectedLocks = ImmutableSet.of(
        theLock,
        replaceLock1,
        appendLock0,
        appendLock1
    );
    Assert.assertEquals(expectedLocks, getAllLocks(tasks));
  }

  @Test
  public void testAppendLockCanRevokeAllIncompatible() throws Exception
  {
    final List<Task> tasks = new ArrayList<>();

    // Revoked Shared lock of higher priority -> revoked locks are not incompatible
    final Task otherSharedTask = NoopTask.create(15);
    tasks.add(otherSharedTask);
    lockbox.add(otherSharedTask);
    taskStorage.insert(otherSharedTask, TaskStatus.running(otherSharedTask.getId()));
    TaskLock sharedLock = tryTimeChunkLock(
        TaskLockType.SHARED,
        otherSharedTask,
        Intervals.of("2016/2019")
    ).getTaskLock();
    Assert.assertNotNull(sharedLock);
    lockbox.revokeLock(otherSharedTask.getId(), sharedLock);

    // Active Exclusive lock of lower priority -> will be revoked
    final Task otherExclusiveTask = NoopTask.create(5);
    tasks.add(otherExclusiveTask);
    lockbox.add(otherExclusiveTask);
    taskStorage.insert(otherExclusiveTask, TaskStatus.running(otherExclusiveTask.getId()));
    TaskLock exclusiveLock = tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        otherExclusiveTask,
        Intervals.of("2017-01-01/2017-02-01")
    ).getTaskLock();
    Assert.assertNotNull(exclusiveLock);

    // Active replace lock of lower priority which doesn't enclose the interval -> will be revoked
    final Task otherReplaceTask = NoopTask.create(5);
    tasks.add(otherReplaceTask);
    lockbox.add(otherReplaceTask);
    taskStorage.insert(otherReplaceTask, TaskStatus.running(otherReplaceTask.getId()));
    TaskLock replaceLock = tryTimeChunkLock(
        TaskLockType.REPLACE,
        otherReplaceTask,
        Intervals.of("2017-07-01/2018-07-01")
    ).getTaskLock();
    Assert.assertNotNull(replaceLock);
    Assert.assertFalse(replaceLock.isRevoked());

    // Active append lock of lower priority -> will not be revoked
    final Task otherAppendTask0 = NoopTask.create(5);
    tasks.add(otherAppendTask0);
    lockbox.add(otherAppendTask0);
    taskStorage.insert(otherAppendTask0, TaskStatus.running(otherAppendTask0.getId()));
    TaskLock appendLock0 = tryTimeChunkLock(
        TaskLockType.APPEND,
        otherAppendTask0,
        Intervals.of("2017-02-01/2017-03-01")
    ).getTaskLock();
    Assert.assertNotNull(appendLock0);
    Assert.assertFalse(appendLock0.isRevoked());

    // Active append lock of higher priority -> will not revoke or be revoked
    final Task otherAppendTask1 = NoopTask.create(15);
    tasks.add(otherAppendTask1);
    lockbox.add(otherAppendTask1);
    taskStorage.insert(otherAppendTask1, TaskStatus.running(otherAppendTask1.getId()));
    TaskLock appendLock1 = tryTimeChunkLock(
        TaskLockType.APPEND,
        otherAppendTask1,
        Intervals.of("2017-02-01/2017-05-01")
    ).getTaskLock();
    Assert.assertNotNull(appendLock1);
    Assert.assertFalse(appendLock1.isRevoked());

    final Task appendTask = NoopTask.create(10);
    tasks.add(appendTask);
    lockbox.add(appendTask);
    taskStorage.insert(appendTask, TaskStatus.running(appendTask.getId()));
    TaskLock theLock = tryTimeChunkLock(
        TaskLockType.APPEND,
        appendTask,
        Intervals.of("2017/2018")
    ).getTaskLock();
    Assert.assertNotNull(theLock);
    Assert.assertFalse(theLock.isRevoked());

    final Set<TaskLock> expectedLocks = ImmutableSet.of(
        theLock,
        sharedLock.revokedCopy(),
        exclusiveLock.revokedCopy(),
        replaceLock.revokedCopy(),
        appendLock0,
        appendLock1
    );
    Assert.assertEquals(expectedLocks, getAllLocks(tasks));

    final Set<TaskLock> expectedActiveLocks = ImmutableSet.of(
        theLock,
        appendLock0,
        appendLock1
    );
    Assert.assertEquals(expectedActiveLocks, getAllActiveLocks(tasks));
  }


  @Test
  public void testReplaceLockCompatibility() throws Exception
  {
    final List<Task> tasks = new ArrayList<>();

    final Task replaceTask = NoopTask.create(10);
    tasks.add(replaceTask);
    lockbox.add(replaceTask);
    taskStorage.insert(replaceTask, TaskStatus.running(replaceTask.getId()));
    TaskLock theLock = tryTimeChunkLock(
        TaskLockType.REPLACE,
        replaceTask,
        Intervals.of("2017/2018")
    ).getTaskLock();
    Assert.assertNotNull(theLock);
    Assert.assertFalse(theLock.isRevoked());

    // An exclusive lock cannot be created for an overlapping interval
    final Task otherExclusiveTask = NoopTask.create(10);
    tasks.add(otherExclusiveTask);
    lockbox.add(otherExclusiveTask);
    taskStorage.insert(otherExclusiveTask, TaskStatus.running(otherExclusiveTask.getId()));
    Assert.assertNull(
        tryTimeChunkLock(
            TaskLockType.EXCLUSIVE,
            otherExclusiveTask,
            Intervals.of("2017-05-01/2017-06-01")
        ).getTaskLock()
    );

    // A shared lock cannot be created for an overlapping interval
    final Task otherSharedTask = NoopTask.create(10);
    tasks.add(otherSharedTask);
    lockbox.add(otherSharedTask);
    taskStorage.insert(otherSharedTask, TaskStatus.running(otherSharedTask.getId()));
    Assert.assertNull(
        tryTimeChunkLock(
            TaskLockType.SHARED,
            otherSharedTask,
            Intervals.of("2016/2019")
        ).getTaskLock()
    );

    // A replace lock cannot be created for an overlapping interval
    final Task otherReplaceTask = NoopTask.create(10);
    tasks.add(otherReplaceTask);
    lockbox.add(otherReplaceTask);
    taskStorage.insert(otherReplaceTask, TaskStatus.running(otherReplaceTask.getId()));
    Assert.assertNull(
        tryTimeChunkLock(
            TaskLockType.REPLACE,
            otherReplaceTask,
            Intervals.of("2017/2018")
        ).getTaskLock()
    );

    // An append lock can be created for an interval enclosed within the replace lock's.
    // Also note that the append lock has a higher priority but doesn't revoke the replace lock as it can coexist.
    final Task otherAppendTask0 = NoopTask.create(15);
    tasks.add(otherAppendTask0);
    lockbox.add(otherAppendTask0);
    taskStorage.insert(otherAppendTask0, TaskStatus.running(otherAppendTask0.getId()));
    TaskLock appendLock0 = tryTimeChunkLock(
        TaskLockType.APPEND,
        otherAppendTask0,
        Intervals.of("2017-05-01/2017-06-01")
    ).getTaskLock();
    Assert.assertNotNull(appendLock0);
    Assert.assertFalse(appendLock0.isRevoked());

    // An append lock cannot coexist when its interval is not enclosed within the replace lock's.
    final Task otherAppendTask1 = NoopTask.create(10);
    tasks.add(otherAppendTask1);
    lockbox.add(otherAppendTask1);
    taskStorage.insert(otherAppendTask1, TaskStatus.running(otherAppendTask1.getId()));
    TaskLock appendLock1 = tryTimeChunkLock(
        TaskLockType.APPEND,
        otherAppendTask1,
        Intervals.of("2016-05-01/2017-06-01")
    ).getTaskLock();
    Assert.assertNull(appendLock1);

    final Set<TaskLock> expectedLocks = ImmutableSet.of(
        theLock,
        appendLock0
    );
    Assert.assertEquals(expectedLocks, getAllLocks(tasks));
  }

  @Test
  public void testReplaceLockCanRevokeAllIncompatible() throws Exception
  {
    final List<Task> tasks = new ArrayList<>();

    // Revoked Append lock of higher priority -> revoked locks are not incompatible
    final Task otherAppendTask0 = NoopTask.create(15);
    tasks.add(otherAppendTask0);
    lockbox.add(otherAppendTask0);
    taskStorage.insert(otherAppendTask0, TaskStatus.running(otherAppendTask0.getId()));
    TaskLock appendLock0 = tryTimeChunkLock(
        TaskLockType.APPEND,
        otherAppendTask0,
        Intervals.of("2016/2019")
    ).getTaskLock();
    Assert.assertNotNull(appendLock0);
    lockbox.revokeLock(otherAppendTask0.getId(), appendLock0);

    // Active append lock of higher priority within replace interval -> unaffected
    final Task otherAppendTask1 = NoopTask.create(15);
    tasks.add(otherAppendTask1);
    lockbox.add(otherAppendTask1);
    taskStorage.insert(otherAppendTask1, TaskStatus.running(otherAppendTask1.getId()));
    TaskLock appendLock1 = tryTimeChunkLock(
        TaskLockType.APPEND,
        otherAppendTask1,
        Intervals.of("2017-02-01/2017-03-01")
    ).getTaskLock();
    Assert.assertNotNull(appendLock1);
    Assert.assertFalse(appendLock1.isRevoked());

    // Active Append lock of lower priority which is not enclosed -> will be revoked
    final Task otherAppendTask2 = NoopTask.create(5);
    tasks.add(otherAppendTask2);
    lockbox.add(otherAppendTask2);
    taskStorage.insert(otherAppendTask2, TaskStatus.running(otherAppendTask2.getId()));
    TaskLock appendLock2 = tryTimeChunkLock(
        TaskLockType.APPEND,
        otherAppendTask2,
        Intervals.of("2017-09-01/2018-03-01")
    ).getTaskLock();
    Assert.assertNotNull(appendLock2);
    Assert.assertFalse(appendLock2.isRevoked());

    // Active Exclusive lock of lower priority -> will be revoked
    final Task otherExclusiveTask = NoopTask.create(5);
    tasks.add(otherExclusiveTask);
    lockbox.add(otherExclusiveTask);
    taskStorage.insert(otherExclusiveTask, TaskStatus.running(otherExclusiveTask.getId()));
    TaskLock exclusiveLock = tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        otherExclusiveTask,
        Intervals.of("2017-05-01/2017-06-01")
    ).getTaskLock();
    Assert.assertNotNull(exclusiveLock);

    // Active replace lock of lower priority which doesn't enclose the interval -> will be revoked
    final Task otherReplaceTask = NoopTask.create(5);
    tasks.add(otherReplaceTask);
    lockbox.add(otherReplaceTask);
    taskStorage.insert(otherReplaceTask, TaskStatus.running(otherReplaceTask.getId()));
    TaskLock replaceLock = tryTimeChunkLock(
        TaskLockType.REPLACE,
        otherReplaceTask,
        Intervals.of("2016-09-01/2017-03-01")
    ).getTaskLock();
    Assert.assertNotNull(replaceLock);
    Assert.assertFalse(replaceLock.isRevoked());

    // Active shared lock of lower priority -> will be revoked
    final Task otherSharedTask = NoopTask.create(5);
    tasks.add(otherSharedTask);
    lockbox.add(otherSharedTask);
    taskStorage.insert(otherSharedTask, TaskStatus.running(otherSharedTask.getId()));
    TaskLock sharedLock = tryTimeChunkLock(
        TaskLockType.SHARED,
        otherSharedTask,
        Intervals.of("2017-04-01/2017-05-01")
    ).getTaskLock();
    Assert.assertNotNull(sharedLock);
    Assert.assertFalse(sharedLock.isRevoked());

    final Task replaceTask = NoopTask.create(10);
    tasks.add(replaceTask);
    lockbox.add(replaceTask);
    taskStorage.insert(replaceTask, TaskStatus.running(replaceTask.getId()));
    TaskLock theLock = tryTimeChunkLock(
        TaskLockType.REPLACE,
        replaceTask,
        Intervals.of("2017/2018")
    ).getTaskLock();
    Assert.assertNotNull(theLock);
    Assert.assertFalse(theLock.isRevoked());

    final Set<TaskLock> expectedLocks = ImmutableSet.of(
        theLock,
        sharedLock.revokedCopy(),
        exclusiveLock.revokedCopy(),
        replaceLock.revokedCopy(),
        appendLock0.revokedCopy(),
        appendLock1,
        appendLock2.revokedCopy()
    );
    Assert.assertEquals(expectedLocks, getAllLocks(tasks));

    final Set<TaskLock> expectedActiveLocks = ImmutableSet.of(
        theLock,
        appendLock1
    );
    Assert.assertEquals(expectedActiveLocks, getAllActiveLocks(tasks));
  }

  @Test
  public void testGetLockedIntervalsForRevokedLocks() throws Exception
  {
    // Acquire lock for a low priority task
    final Task lowPriorityTask = NoopTask.create(5);
    lockbox.add(lowPriorityTask);
    taskStorage.insert(lowPriorityTask, TaskStatus.running(lowPriorityTask.getId()));
    tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        lowPriorityTask,
        Intervals.of("2017/2018")
    );

    final Map<String, Integer> minTaskPriority = new HashMap<>();
    minTaskPriority.put(lowPriorityTask.getDataSource(), 1);

    Map<String, List<Interval>> lockedIntervals = lockbox.getLockedIntervals(minTaskPriority);
    Assert.assertEquals(1, lockedIntervals.size());
    Assert.assertEquals(
        Collections.singletonList(
            Intervals.of("2017/2018")),
        lockedIntervals.get(lowPriorityTask.getDataSource())
    );

    // Revoke the lowPriorityTask
    final Task highPriorityTask = NoopTask.create(10);
    lockbox.add(highPriorityTask);
    tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        highPriorityTask,
        Intervals.of("2017-05-01/2017-06-01")
    );

    // Verify the locked intervals
    minTaskPriority.put(highPriorityTask.getDataSource(), 1);
    lockedIntervals = lockbox.getLockedIntervals(minTaskPriority);
    Assert.assertEquals(1, lockedIntervals.size());
    Assert.assertEquals(
        Collections.singletonList(
            Intervals.of("2017-05-01/2017-06-01")),
        lockedIntervals.get(highPriorityTask.getDataSource())
    );
  }

  @Test
  public void testConflictsWithOverlappingSharedLocks() throws Exception
  {
    final List<Task> tasks = new ArrayList<>();

    final Task conflictingTask = NoopTask.create(10);
    tasks.add(conflictingTask);
    lockbox.add(conflictingTask);
    taskStorage.insert(conflictingTask, TaskStatus.running(conflictingTask.getId()));
    TaskLock conflictingLock = tryTimeChunkLock(
        TaskLockType.SHARED,
        conflictingTask,
        Intervals.of("2023-05-01/2023-06-01")
    ).getTaskLock();
    Assert.assertNotNull(conflictingLock);
    Assert.assertFalse(conflictingLock.isRevoked());

    final Task floorTask = NoopTask.create(10);
    tasks.add(floorTask);
    lockbox.add(floorTask);
    taskStorage.insert(floorTask, TaskStatus.running(floorTask.getId()));
    TaskLock floorLock = tryTimeChunkLock(
        TaskLockType.SHARED,
        floorTask,
        Intervals.of("2023-05-26/2023-05-27")
    ).getTaskLock();
    Assert.assertNotNull(floorLock);
    Assert.assertFalse(floorLock.isRevoked());

    final Task rightOverlapTask = NoopTask.create(10);
    tasks.add(rightOverlapTask);
    lockbox.add(rightOverlapTask);
    taskStorage.insert(rightOverlapTask, TaskStatus.running(rightOverlapTask.getId()));
    TaskLock rightOverlapLock = tryTimeChunkLock(
        TaskLockType.EXCLUSIVE,
        rightOverlapTask,
        Intervals.of("2023-05-28/2023-06-03")
    ).getTaskLock();
    Assert.assertNull(rightOverlapLock);

    Assert.assertEquals(
        ImmutableSet.of(conflictingLock, floorLock),
        getAllActiveLocks(tasks)
    );
  }

  @Test
  public void testFailedToReacquireTaskLock() throws Exception
  {
    // Tasks to be failed have a group id with the substring "FailingLockAcquisition"
    // Please refer to NullLockPosseTaskLockbox
    final Task taskWithFailingLockAcquisition0 = NoopTask.withGroupId("FailingLockAcquisition");
    final Task taskWithFailingLockAcquisition1 = NoopTask.withGroupId("FailingLockAcquisition");
    final Task taskWithSuccessfulLockAcquisition = NoopTask.create();
    taskStorage.insert(taskWithFailingLockAcquisition0, TaskStatus.running(taskWithFailingLockAcquisition0.getId()));
    taskStorage.insert(taskWithFailingLockAcquisition1, TaskStatus.running(taskWithFailingLockAcquisition1.getId()));
    taskStorage.insert(taskWithSuccessfulLockAcquisition, TaskStatus.running(taskWithSuccessfulLockAcquisition.getId()));

    TaskLockbox testLockbox = new NullLockPosseTaskLockbox(taskStorage, metadataStorageCoordinator);
    testLockbox.add(taskWithFailingLockAcquisition0);
    testLockbox.add(taskWithFailingLockAcquisition1);
    testLockbox.add(taskWithSuccessfulLockAcquisition);

    testLockbox.tryLock(taskWithFailingLockAcquisition0,
                        new TimeChunkLockRequest(TaskLockType.EXCLUSIVE,
                                                 taskWithFailingLockAcquisition0,
                                                 Intervals.of("2017-07-01/2017-08-01"),
                                                 null
                        )
    );

    testLockbox.tryLock(taskWithSuccessfulLockAcquisition,
                        new TimeChunkLockRequest(TaskLockType.EXCLUSIVE,
                                                 taskWithSuccessfulLockAcquisition,
                                                 Intervals.of("2017-07-01/2017-08-01"),
                                                 null
                        )
    );

    Assert.assertEquals(3, taskStorage.getActiveTasks().size());

    // The tasks must be marked for failure
    TaskLockboxSyncResult result = testLockbox.syncFromStorage();
    Assert.assertEquals(ImmutableSet.of(taskWithFailingLockAcquisition0, taskWithFailingLockAcquisition1),
                        result.getTasksToFail());
  }

  private Set<TaskLock> getAllActiveLocks(List<Task> tasks)
  {
    return tasks.stream()
                .flatMap(task -> taskStorage.getLocks(task.getId()).stream())
                .filter(taskLock -> !taskLock.isRevoked())
                .collect(Collectors.toSet());
  }

  private Set<TaskLock> getAllLocks(List<Task> tasks)
  {
    return tasks.stream()
                .flatMap(task -> taskStorage.getLocks(task.getId()).stream())
                .collect(Collectors.toSet());
  }

  private static class IntervalLockWithoutPriority extends TimeChunkLock
  {
    @JsonCreator
    IntervalLockWithoutPriority(
        String groupId,
        String dataSource,
        Interval interval,
        String version
    )
    {
      super(null, groupId, dataSource, interval, version, null, false);
    }

    @Override
    @JsonProperty
    public TaskLockType getType()
    {
      return super.getType();
    }

    @Override
    @JsonProperty
    public String getGroupId()
    {
      return super.getGroupId();
    }

    @Override
    @JsonProperty
    public String getDataSource()
    {
      return super.getDataSource();
    }

    @Override
    @JsonProperty
    public Interval getInterval()
    {
      return super.getInterval();
    }

    @Override
    @JsonProperty
    public String getVersion()
    {
      return super.getVersion();
    }

    @JsonIgnore
    @Override
    public Integer getPriority()
    {
      return super.getPriority();
    }

    @JsonIgnore
    @Override
    public boolean isRevoked()
    {
      return super.isRevoked();
    }
  }

  private static String TASK_NAME = "myModuleIsntLoadedTask";

  private static class TheModule extends SimpleModule
  {
    public TheModule()
    {
      registerSubtypes(new NamedType(MyModuleIsntLoadedTask.class, TASK_NAME));
    }
  }

  private static class MyModuleIsntLoadedTask extends AbstractTask
  {
    private String someProp;

    @JsonCreator
    protected MyModuleIsntLoadedTask(
        @JsonProperty("id") String id,
        @JsonProperty("dataSource") String dataSource,
        @JsonProperty("context") Map<String, Object> context,
        @JsonProperty("someProp") String someProp
    )
    {
      super(id, dataSource, context);
      this.someProp = someProp;
    }

    @JsonProperty
    public String getSomeProp()
    {
      return someProp;
    }

    @Override
    public String getType()
    {
      return TASK_NAME;
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient)
    {
      return true;
    }

    @Override
    public void stopGracefully(TaskConfig taskConfig)
    {
    }

    @Override
    public TaskStatus runTask(TaskToolbox toolbox)
    {
      return TaskStatus.failure("how?", "Dummy task status err msg");
    }
  }

  /**
   * Extends TaskLockbox to return a null TaskLockPosse when the task's group name contains "FailingLockAcquisition".
   */
  private static class NullLockPosseTaskLockbox extends TaskLockbox
  {
    public NullLockPosseTaskLockbox(
        TaskStorage taskStorage,
        IndexerMetadataStorageCoordinator metadataStorageCoordinator
    )
    {
      super(taskStorage, metadataStorageCoordinator);
    }

    @Override
    protected TaskLockPosse verifyAndCreateOrFindLockPosse(Task task, TaskLock taskLock)
    {
      return task.getGroupId()
                 .contains("FailingLockAcquisition") ? null : super.verifyAndCreateOrFindLockPosse(task, taskLock);
    }
  }
}
