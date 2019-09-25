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
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpecFactory;
import org.apache.druid.timeline.partition.NumberedOverwritingShardSpecFactory;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpecFactory;
import org.apache.druid.timeline.partition.PartitionIds;
import org.apache.druid.timeline.partition.ShardSpecFactory;
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
      final TaskLock lock = tryTimeChunkLock(TaskLockType.SHARED, task, interval).getTaskLock();
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
    allocateSegmentsAndAssert(task, "seq", 3, NumberedShardSpecFactory.instance());
    allocateSegmentsAndAssert(task, "seq2", 2, new NumberedOverwritingShardSpecFactory(0, 3, (short) 1));

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

    allocateSegmentsAndAssert(task, "seq", 3, new HashBasedNumberedShardSpecFactory(null, 3));
    allocateSegmentsAndAssert(task, "seq2", 5, new HashBasedNumberedShardSpecFactory(null, 5));
  }

  private void allocateSegmentsAndAssert(
      Task task,
      String baseSequenceName,
      int numSegmentsToAllocate,
      ShardSpecFactory shardSpecFactory
  )
  {
    for (int i = 0; i < numSegmentsToAllocate; i++) {
      final LockRequestForNewSegment request = new LockRequestForNewSegment(
          LockGranularity.SEGMENT,
          TaskLockType.EXCLUSIVE,
          task,
          Intervals.of("2015-01-01/2015-01-05"),
          shardSpecFactory,
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
    Assert.assertEquals(lockRequest.getShardSpecFactory().getShardSpecClass(), segmentId.getShardSpec().getClass());
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
    public TaskStatus run(TaskToolbox toolbox)
    {
      return TaskStatus.failure("how?");
    }
  }
}
