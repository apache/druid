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
import com.google.common.collect.Iterables;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskLockbox.TaskLockPosse;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.DerbyMetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.metadata.TestDerbyConnector;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  private TaskStorage taskStorage;
  private TaskLockbox lockbox;

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setup()
  {
    final TestDerbyConnector derbyConnector = derby.getConnector();
    derbyConnector.createTaskTables();
    taskStorage = new MetadataTaskStorage(
        derbyConnector,
        new TaskStorageConfig(null),
        new DerbyMetadataStorageActionHandlerFactory(
            derbyConnector,
            derby.metadataTablesConfigSupplier().get(),
            objectMapper
        )
    );
    ServiceEmitter emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    EasyMock.replay(emitter);

    lockbox = new TaskLockbox(taskStorage);
  }

  @Test
  public void testLock() throws InterruptedException
  {
    Task task = NoopTask.create();
    lockbox.add(task);
    Assert.assertNotNull(lockbox.lock(TaskLockType.EXCLUSIVE, task, Intervals.of("2015-01-01/2015-01-02")));
  }

  @Test(expected = IllegalStateException.class)
  public void testLockForInactiveTask() throws InterruptedException
  {
    lockbox.lock(TaskLockType.EXCLUSIVE, NoopTask.create(), Intervals.of("2015-01-01/2015-01-02"));
  }

  @Test
  public void testLockAfterTaskComplete() throws InterruptedException
  {
    Task task = NoopTask.create();
    exception.expect(ISE.class);
    exception.expectMessage("Unable to grant lock to inactive Task");
    lockbox.add(task);
    lockbox.remove(task);
    lockbox.lock(TaskLockType.EXCLUSIVE, task, Intervals.of("2015-01-01/2015-01-02"));
  }

  @Test
  public void testTrySharedLock()
  {
    final Interval interval = Intervals.of("2017-01/2017-02");
    final List<Task> tasks = new ArrayList<>();
    final Set<TaskLock> actualLocks = new HashSet<>();

    // test creating new locks
    for (int i = 0; i < 5; i++) {
      final Task task = NoopTask.create(Math.min(0, (i - 1) * 10)); // the first two tasks have the same priority
      tasks.add(task);
      lockbox.add(task);
      final TaskLock lock = lockbox.tryLock(TaskLockType.SHARED, task, interval).getTaskLock();
      Assert.assertNotNull(lock);
      actualLocks.add(lock);
    }

    Assert.assertEquals(5, getAllLocks(tasks).size());
    Assert.assertEquals(getAllLocks(tasks), actualLocks);
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
    Assert.assertTrue(lockbox.tryLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval1).isOk());
    Assert.assertTrue(lockbox.tryLock(TaskLockType.SHARED, lowPriorityTask, interval2).isOk());
    Assert.assertTrue(lockbox.tryLock(TaskLockType.SHARED, lowPriorityTask2, interval2).isOk());
    Assert.assertTrue(lockbox.tryLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval3).isOk());

    lockbox.add(highPiorityTask);
    Assert.assertTrue(lockbox.tryLock(TaskLockType.SHARED, highPiorityTask, interval1).isOk());
    Assert.assertTrue(lockbox.tryLock(TaskLockType.EXCLUSIVE, highPiorityTask, interval2).isOk());
    Assert.assertTrue(lockbox.tryLock(TaskLockType.EXCLUSIVE, highPiorityTask, interval3).isOk());

    Assert.assertTrue(lockbox.findLocksForTask(lowPriorityTask).stream().allMatch(TaskLock::isRevoked));
    Assert.assertTrue(lockbox.findLocksForTask(lowPriorityTask2).stream().allMatch(TaskLock::isRevoked));

    lockbox.remove(lowPriorityTask);
    lockbox.remove(lowPriorityTask2);
    lockbox.remove(highPiorityTask);

    lockbox.add(highPiorityTask);
    Assert.assertTrue(lockbox.tryLock(TaskLockType.EXCLUSIVE, highPiorityTask, interval1).isOk());
    Assert.assertTrue(lockbox.tryLock(TaskLockType.SHARED, highPiorityTask, interval2).isOk());
    Assert.assertTrue(lockbox.tryLock(TaskLockType.EXCLUSIVE, highPiorityTask, interval3).isOk());

    lockbox.add(lowPriorityTask);
    Assert.assertFalse(lockbox.tryLock(TaskLockType.SHARED, lowPriorityTask, interval1).isOk());
    Assert.assertFalse(lockbox.tryLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval2).isOk());
    Assert.assertFalse(lockbox.tryLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval3).isOk());
  }

  @Test
  public void testTryExclusiveLock()
  {
    Task task = NoopTask.create();
    lockbox.add(task);
    Assert.assertTrue(lockbox.tryLock(TaskLockType.EXCLUSIVE, task, Intervals.of("2015-01-01/2015-01-03")).isOk());

    // try to take lock for task 2 for overlapping interval
    Task task2 = NoopTask.create();
    lockbox.add(task2);
    Assert.assertFalse(lockbox.tryLock(TaskLockType.EXCLUSIVE, task2, Intervals.of("2015-01-01/2015-01-02")).isOk());

    // task 1 unlocks the lock
    lockbox.remove(task);

    // Now task2 should be able to get the lock
    Assert.assertTrue(lockbox.tryLock(TaskLockType.EXCLUSIVE, task2, Intervals.of("2015-01-01/2015-01-02")).isOk());
  }

  @Test(expected = IllegalStateException.class)
  public void testTryLockForInactiveTask()
  {
    Assert.assertFalse(lockbox.tryLock(TaskLockType.EXCLUSIVE, NoopTask.create(), Intervals.of("2015-01-01/2015-01-02")).isOk());
  }

  @Test
  public void testTryLockAfterTaskComplete()
  {
    Task task = NoopTask.create();
    exception.expect(ISE.class);
    exception.expectMessage("Unable to grant lock to inactive Task");
    lockbox.add(task);
    lockbox.remove(task);
    Assert.assertFalse(lockbox.tryLock(TaskLockType.EXCLUSIVE, task, Intervals.of("2015-01-01/2015-01-02")).isOk());
  }

  @Test
  public void testTimeoutForLock() throws InterruptedException
  {
    Task task1 = NoopTask.create();
    Task task2 = NoopTask.create();

    lockbox.add(task1);
    lockbox.add(task2);
    Assert.assertTrue(lockbox.lock(TaskLockType.EXCLUSIVE, task1, Intervals.of("2015-01-01/2015-01-02"), 5000).isOk());
    Assert.assertFalse(lockbox.lock(TaskLockType.EXCLUSIVE, task2, Intervals.of("2015-01-01/2015-01-15"), 1000).isOk());
  }

  @Test
  public void testSyncFromStorage() throws EntryExistsException
  {
    final TaskLockbox originalBox = new TaskLockbox(taskStorage);
    for (int i = 0; i < 5; i++) {
      final Task task = NoopTask.create();
      taskStorage.insert(task, TaskStatus.running(task.getId()));
      originalBox.add(task);
      Assert.assertTrue(
          originalBox.tryLock(
              TaskLockType.EXCLUSIVE,
              task,
              Intervals.of(StringUtils.format("2017-01-0%d/2017-01-0%d", (i + 1), (i + 2)))
          ).isOk()
      );
    }

    final List<TaskLock> beforeLocksInStorage = taskStorage.getActiveTasks().stream()
                                                           .flatMap(task -> taskStorage.getLocks(task.getId()).stream())
                                                           .collect(Collectors.toList());

    final TaskLockbox newBox = new TaskLockbox(taskStorage);
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
        new TaskLockWithoutPriority(task.getGroupId(), task.getDataSource(), Intervals.of("2017/2018"), "v1")
    );

    final List<TaskLock> beforeLocksInStorage = taskStorage.getActiveTasks().stream()
                                                           .flatMap(t -> taskStorage.getLocks(t.getId()).stream())
                                                           .collect(Collectors.toList());

    final TaskLockbox lockbox = new TaskLockbox(taskStorage);
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
        new TaskLock(
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

    final TaskLockbox lockbox = new TaskLockbox(taskStorage);
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
        new TaskLock(
            TaskLockType.EXCLUSIVE,
            task.getGroupId(),
            task.getDataSource(),
            Intervals.of("2017/2018"),
            "v1",
            10
        )
    );

    final TaskLockbox lockbox = new TaskLockbox(taskStorage);
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("lock priority[10] is different from task priority[50]");
    lockbox.syncFromStorage();
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

    TaskLockbox theBox = new TaskLockbox(taskStorage);
    TaskLockbox loadedBox = new TaskLockbox(loadedTaskStorage);

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
    final TaskLockbox originalBox = new TaskLockbox(taskStorage);

    final Task task1 = NoopTask.create("task1", 10);
    taskStorage.insert(task1, TaskStatus.running(task1.getId()));
    originalBox.add(task1);
    Assert.assertTrue(originalBox.tryLock(TaskLockType.EXCLUSIVE, task1, Intervals.of("2017/2018")).isOk());

    // task2 revokes task1
    final Task task2 = NoopTask.create("task2", 100);
    taskStorage.insert(task2, TaskStatus.running(task2.getId()));
    originalBox.add(task2);
    Assert.assertTrue(originalBox.tryLock(TaskLockType.EXCLUSIVE, task2, Intervals.of("2017/2018")).isOk());

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

    final TaskLockbox newBox = new TaskLockbox(taskStorage);
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
    Assert.assertTrue(lockbox.tryLock(TaskLockType.SHARED, task, interval).isOk());

    Assert.assertFalse(
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
    final TaskLock lock = lockbox.tryLock(TaskLockType.EXCLUSIVE, task, interval).getTaskLock();
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
    final TaskLock lock = lockbox.tryLock(TaskLockType.EXCLUSIVE, task, interval).getTaskLock();
    Assert.assertNotNull(lock);

    Assert.assertTrue(
        lockbox.doInCriticalSection(
            task,
            Collections.singletonList(smallInterval),
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
      Assert.assertTrue(lockbox.tryLock(TaskLockType.SHARED, task, interval).isOk());
    }

    final Task highPriorityTask = NoopTask.create(100);
    lockbox.add(highPriorityTask);
    taskStorage.insert(highPriorityTask, TaskStatus.running(highPriorityTask.getId()));
    final TaskLock lock = lockbox.tryLock(TaskLockType.EXCLUSIVE, highPriorityTask, interval).getTaskLock();
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

    final TaskLock lowPriorityLock = lockbox.tryLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval).getTaskLock();
    Assert.assertNotNull(lowPriorityLock);
    Assert.assertTrue(lockbox.tryLock(TaskLockType.EXCLUSIVE, highPriorityTask, interval).isOk());
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

    final TaskLock lowPriorityLock = lockbox.lock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval).getTaskLock();
    Assert.assertNotNull(lowPriorityLock);
    Assert.assertTrue(lockbox.tryLock(TaskLockType.EXCLUSIVE, highPriorityTask, interval).isOk());
    Assert.assertTrue(Iterables.getOnlyElement(lockbox.findLocksForTask(lowPriorityTask)).isRevoked());

    lockbox.unlock(highPriorityTask, interval);

    // Acquire again
    final LockResult lockResult = lockbox.lock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval);
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
          lockbox.tryLock(
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
          lockbox.tryLock(
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
        lockbox.tryLock(
            TaskLockType.EXCLUSIVE,
            lowPriorityTask, Intervals.of("2018-12-16T09:00:00/2018-12-16T10:00:00")
        ).isOk()
    );

    Assert.assertTrue(
        lockbox.tryLock(
            TaskLockType.EXCLUSIVE,
            highPriorityTask, Intervals.of("2018-12-16T09:00:00/2018-12-16T09:30:00")
        ).isOk()
    );

    final TaskLockPosse highLockPosse = lockbox.getOnlyTaskLockPosseContainingInterval(
        highPriorityTask,
        Intervals.of("2018-12-16T09:00:00/2018-12-16T09:30:00")
    );

    Assert.assertTrue(highLockPosse.containsTask(highPriorityTask));
    Assert.assertFalse(highLockPosse.getTaskLock().isRevoked());

    final TaskLockPosse lowLockPosse = lockbox.getOnlyTaskLockPosseContainingInterval(
        lowPriorityTask,
        Intervals.of("2018-12-16T09:00:00/2018-12-16T10:00:00")
    );

    Assert.assertTrue(lowLockPosse.containsTask(lowPriorityTask));
    Assert.assertTrue(lowLockPosse.getTaskLock().isRevoked());
  }

  @Test
  public void testLockPosseEquals()
  {
    final Task task1 = NoopTask.create();
    final Task task2 = NoopTask.create();

    TaskLock taskLock1 = new TaskLock(TaskLockType.EXCLUSIVE,
        task1.getGroupId(),
        task1.getDataSource(),
        Intervals.of("2018/2019"),
        "v1",
        task1.getPriority());

    TaskLock taskLock2 = new TaskLock(TaskLockType.EXCLUSIVE,
        task2.getGroupId(),
        task2.getDataSource(),
        Intervals.of("2018/2019"),
        "v2",
        task2.getPriority());

    TaskLockPosse taskLockPosse1 = new TaskLockPosse(taskLock1);
    TaskLockPosse taskLockPosse2 = new TaskLockPosse(taskLock2);
    TaskLockPosse taskLockPosse3 = new TaskLockPosse(taskLock1);

    Assert.assertNotEquals(taskLockPosse1, null);
    Assert.assertNotEquals(null, taskLockPosse1);
    Assert.assertNotEquals(taskLockPosse1, taskLockPosse2);
    Assert.assertEquals(taskLockPosse1, taskLockPosse3);
  }

  private Set<TaskLock> getAllLocks(List<Task> tasks)
  {
    return tasks.stream()
                .flatMap(task -> taskStorage.getLocks(task.getId()).stream())
                .collect(Collectors.toSet());
  }

  private static class TaskLockWithoutPriority extends TaskLock
  {
    @JsonCreator
    TaskLockWithoutPriority(
        String groupId,
        String dataSource,
        Interval interval,
        String version
    )
    {
      super(null, groupId, dataSource, interval, version, 0, false);
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
    public TaskStatus run(TaskToolbox toolbox)
    {
      return TaskStatus.failure("how?");
    }
  }
}
