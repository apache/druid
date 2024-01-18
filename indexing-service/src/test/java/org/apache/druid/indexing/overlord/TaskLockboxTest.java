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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.apache.druid.indexing.common.task.Tasks;
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
import org.apache.druid.metadata.LockFilterPolicy;
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
  private TaskLockboxValidator validator;

  private final int HIGH_PRIORITY = 15;
  private final int MEDIUM_PRIORITY = 10;
  private final int LOW_PRIORITY = 5;

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
    validator = new TaskLockboxValidator(lockbox, taskStorage);
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
    return tryTimeChunkLock(lockType, task, interval, null);
  }

  private LockResult tryTimeChunkLock(TaskLockType lockType, Task task, Interval interval, String version)
  {
    return lockbox.tryLock(task, new TimeChunkLockRequest(lockType, task, interval, version));
  }

  @Test
  public void testLock()
  {
    validator.expectLockCreated(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2015-01-01/2015-01-02"),
        MEDIUM_PRIORITY
    );
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

    final TaskLock exclusiveRevokedLock = validator.expectLockCreated(
        TaskLockType.EXCLUSIVE,
        interval,
        HIGH_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.SHARED,
        interval,
        HIGH_PRIORITY
    );

    validator.revokeLock(exclusiveRevokedLock);
    validator.expectRevokedLocks(exclusiveRevokedLock);

    final TaskLock lowPrioritySharedLock = validator.expectLockCreated(
        TaskLockType.SHARED,
        interval,
        LOW_PRIORITY
    );

    final TaskLock mediumPriorityExclusiveLock = validator.expectLockCreated(
        TaskLockType.EXCLUSIVE,
        interval,
        MEDIUM_PRIORITY
    );
    validator.expectActiveLocks(mediumPriorityExclusiveLock);
    validator.expectRevokedLocks(exclusiveRevokedLock, lowPrioritySharedLock);

    final TaskLock highPrioritySharedLock = validator.expectLockCreated(
        TaskLockType.SHARED,
        interval,
        HIGH_PRIORITY
    );
    validator.expectActiveLocks(highPrioritySharedLock);
    validator.expectRevokedLocks(exclusiveRevokedLock, lowPrioritySharedLock, mediumPriorityExclusiveLock);
  }

  @Test
  public void testTryMixedLocks() throws EntryExistsException
  {
    final Task lowPriorityTask = NoopTask.ofPriority(0);
    final Task lowPriorityTask2 = NoopTask.ofPriority(0);
    final Task highPiorityTask = NoopTask.ofPriority(10);
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
  public void testSyncWithUnknownTaskTypesFromModuleNotLoaded()
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

    final Task task1 = NoopTask.ofPriority(10);
    taskStorage.insert(task1, TaskStatus.running(task1.getId()));
    originalBox.add(task1);
    Assert.assertTrue(originalBox.tryLock(task1, new TimeChunkLockRequest(TaskLockType.EXCLUSIVE, task1, Intervals.of("2017/2018"), null)).isOk());

    // task2 revokes task1
    final Task task2 = NoopTask.ofPriority(100);
    taskStorage.insert(task2, TaskStatus.running(task2.getId()));
    originalBox.add(task2);
    Assert.assertTrue(originalBox.tryLock(task2, new TimeChunkLockRequest(TaskLockType.EXCLUSIVE, task2, Intervals.of("2017/2018"), null)).isOk());

    final Map<String, List<TaskLock>> beforeLocksInStorage = taskStorage
        .getActiveTasks()
        .stream()
        .collect(Collectors.toMap(Task::getId, task -> taskStorage.getLocks(task.getId())));

    final List<TaskLock> task1Locks = beforeLocksInStorage.get(task1.getId());
    Assert.assertEquals(1, task1Locks.size());
    Assert.assertTrue(task1Locks.get(0).isRevoked());

    final List<TaskLock> task2Locks = beforeLocksInStorage.get(task1.getId());
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
            Collections.singleton(interval),
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
            Collections.singleton(interval),
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
            Collections.singleton(interval),
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

    final Task highPriorityTask = NoopTask.ofPriority(100);
    lockbox.add(highPriorityTask);
    taskStorage.insert(highPriorityTask, TaskStatus.running(highPriorityTask.getId()));
    final TaskLock lock = tryTimeChunkLock(TaskLockType.EXCLUSIVE, highPriorityTask, interval).getTaskLock();
    Assert.assertNotNull(lock);

    Assert.assertTrue(
        lockbox.doInCriticalSection(
            highPriorityTask,
            Collections.singleton(interval),
            CriticalAction.<Boolean>builder().onValidLocks(() -> true).onInvalidLocks(() -> false).build()
        )
    );
  }

  @Test
  public void testDoInCriticalSectionWithRevokedLock() throws Exception
  {
    final Interval interval = Intervals.of("2017-01-01/2017-01-02");
    final Task lowPriorityTask = NoopTask.ofPriority(0);
    final Task highPriorityTask = NoopTask.ofPriority(10);
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
            Collections.singleton(interval),
            CriticalAction.<Boolean>builder().onValidLocks(() -> true).onInvalidLocks(() -> false).build()
        )
    );
  }

  @Test(timeout = 60_000L)
  public void testAcquireLockAfterRevoked() throws EntryExistsException, InterruptedException
  {
    final Interval interval = Intervals.of("2017-01-01/2017-01-02");
    final Task lowPriorityTask = NoopTask.ofPriority(0);
    final Task highPriorityTask = NoopTask.ofPriority(10);
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
      final Task task = NoopTask.ofPriority(10);
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
      final Task task = NoopTask.ofPriority(100);
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
    final Task lowPriorityTask = NoopTask.ofPriority(0);
    final Task highPriorityTask = NoopTask.ofPriority(10);

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
    final Task task1 = NoopTask.ofPriority(10);
    lockbox.add(task1);
    taskStorage.insert(task1, TaskStatus.running(task1.getId()));

    final Task task2 = NoopTask.ofPriority(100);
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
    Assert.assertEquals(lockRequest.getPriority(), segmentLock.getPriority().intValue());
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
    final Task task1 = new NoopTask(null, "groupId", null, 0, 0, null);
    final Task task2 = new NoopTask(null, "groupId", null, 0, 0, null);

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
    final Task task1 = new NoopTask(null, "group1", "wiki", 0, 0, null);
    final Task task2 = new NoopTask(null, "group2", "wiki", 0, 0, null);

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
    final Task task1 = NoopTask.forDatasource("ds1");
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
    final Task task2 = NoopTask.forDatasource("ds2");
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
  public void testGetLockedIntervalsForLowPriorityTask()
  {
    // Acquire lock for a low priority task
    final Task lowPriorityTask = NoopTask.ofPriority(5);
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
  public void testGetLockedIntervalsForEqualPriorityTask()
  {
    // Acquire lock for a low priority task
    final Task task = NoopTask.ofPriority(5);
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
  public void testGetLockedIntervalsForHigherPriorityExclusiveLock()
  {
    final Task task = NoopTask.ofPriority(50);
    lockbox.add(task);
    taskStorage.insert(task, TaskStatus.running(task.getId()));
    tryTimeChunkLock(
        TaskLockType.APPEND,
        task,
        Intervals.of("2017/2018")
    );

    LockFilterPolicy requestForExclusiveLowerPriorityLock = new LockFilterPolicy(
        task.getDataSource(),
        75,
        null
    );

    Map<String, List<Interval>> conflictingIntervals =
        lockbox.getLockedIntervals(ImmutableList.of(requestForExclusiveLowerPriorityLock));
    Assert.assertTrue(conflictingIntervals.isEmpty());
  }

  @Test
  public void testGetLockedIntervalsForLowerPriorityExclusiveLock()
  {
    final Task task = NoopTask.ofPriority(50);
    lockbox.add(task);
    taskStorage.insert(task, TaskStatus.running(task.getId()));
    tryTimeChunkLock(
        TaskLockType.APPEND,
        task,
        Intervals.of("2017/2018")
    );

    LockFilterPolicy requestForExclusiveLowerPriorityLock = new LockFilterPolicy(
        task.getDataSource(),
        25,
        null
    );

    Map<String, List<Interval>> conflictingIntervals =
        lockbox.getLockedIntervals(ImmutableList.of(requestForExclusiveLowerPriorityLock));
    Assert.assertEquals(1, conflictingIntervals.size());
    Assert.assertEquals(
        Collections.singletonList(Intervals.of("2017/2018")),
        conflictingIntervals.get(task.getDataSource())
    );
  }

  @Test
  public void testGetLockedIntervalsForLowerPriorityReplaceLock()
  {
    final Task task = NoopTask.ofPriority(50);
    lockbox.add(task);
    taskStorage.insert(task, TaskStatus.running(task.getId()));
    tryTimeChunkLock(
        TaskLockType.APPEND,
        task,
        Intervals.of("2017/2018")
    );

    LockFilterPolicy requestForReplaceLowerPriorityLock = new LockFilterPolicy(
        task.getDataSource(),
        25,
        ImmutableMap.of(Tasks.TASK_LOCK_TYPE, TaskLockType.REPLACE.name())
    );

    Map<String, List<Interval>> conflictingIntervals =
        lockbox.getLockedIntervals(ImmutableList.of(requestForReplaceLowerPriorityLock));
    Assert.assertTrue(conflictingIntervals.isEmpty());
  }


  @Test
  public void testExclusiveLockCompatibility()
  {
    final TaskLock theLock = validator.expectLockCreated(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2017/2018"),
        MEDIUM_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2017-05-01/2017-06-01"),
        MEDIUM_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.SHARED,
        Intervals.of("2016/2019"),
        MEDIUM_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.REPLACE,
        Intervals.of("2017/2018"),
        MEDIUM_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.APPEND,
        Intervals.of("2017-05-01/2018-05-01"),
        MEDIUM_PRIORITY
    );

    validator.expectActiveLocks(theLock);
    validator.expectRevokedLocks();
  }

  @Test
  public void testExclusiveLockCanRevokeAllIncompatible()
  {
    final TaskLockboxValidator validator = new TaskLockboxValidator(lockbox, taskStorage);

    final TaskLock sharedLock = validator.tryTaskLock(
        TaskLockType.SHARED,
        Intervals.of("2016/2019"),
        HIGH_PRIORITY
    );
    validator.revokeLock(sharedLock);

    final TaskLock exclusiveLock = validator.expectLockCreated(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2017-01-01/2017-02-01"),
        LOW_PRIORITY
    );

    final TaskLock replaceLock = validator.expectLockCreated(
        TaskLockType.REPLACE,
        Intervals.of("2017-07-01/2018-01-01"),
        LOW_PRIORITY
    );

    final TaskLock appendLock = validator.expectLockCreated(
        TaskLockType.APPEND,
        Intervals.of("2017-09-01/2017-10-01"),
        LOW_PRIORITY
    );

    validator.expectActiveLocks(exclusiveLock, replaceLock, appendLock);
    validator.expectRevokedLocks(sharedLock);

    final TaskLock theLock = validator.expectLockCreated(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2017/2018"),
        MEDIUM_PRIORITY
    );

    validator.expectActiveLocks(theLock);
    validator.expectRevokedLocks(sharedLock, exclusiveLock, appendLock, replaceLock);
  }

  @Test
  public void testSharedLockCompatibility()
  {
    final TaskLock theLock = validator.expectLockCreated(
        TaskLockType.SHARED,
        Intervals.of("2017/2018"),
        MEDIUM_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2017-05-01/2017-06-01"),
        MEDIUM_PRIORITY
    );

    final TaskLock sharedLock0 = validator.expectLockCreated(
        TaskLockType.SHARED,
        Intervals.of("2016/2019"),
        LOW_PRIORITY
    );

    final TaskLock sharedLock1 = validator.expectLockCreated(
        TaskLockType.SHARED,
        Intervals.of("2017-06-01/2017-07-01"),
        LOW_PRIORITY
    );

    final TaskLock sharedLock2 = validator.expectLockCreated(
        TaskLockType.SHARED,
        Intervals.of("2017-05-01/2018-05-01"),
        MEDIUM_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.REPLACE,
        Intervals.of("2017/2018"),
        MEDIUM_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.APPEND,
        Intervals.of("2017-05-01/2018-05-01"),
        MEDIUM_PRIORITY
    );

    validator.expectActiveLocks(theLock, sharedLock0, sharedLock1, sharedLock2);
    validator.expectRevokedLocks();
  }

  @Test
  public void testSharedLockCanRevokeAllIncompatible()
  {
    final TaskLock exclusiveLock = validator.expectLockCreated(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2016/2019"),
        HIGH_PRIORITY
    );
    validator.revokeLock(exclusiveLock);

    final TaskLock sharedLock = validator.expectLockCreated(
        TaskLockType.SHARED,
        Intervals.of("2017-01-01/2017-02-01"),
        MEDIUM_PRIORITY
    );

    final TaskLock replaceLock = validator.expectLockCreated(
        TaskLockType.REPLACE,
        Intervals.of("2017-07-01/2018-07-01"),
        LOW_PRIORITY
    );

    final TaskLock appendLock = validator.expectLockCreated(
        TaskLockType.APPEND,
        Intervals.of("2017-02-01/2017-03-01"),
        LOW_PRIORITY
    );

    validator.expectActiveLocks(sharedLock, replaceLock, appendLock);
    validator.expectRevokedLocks(exclusiveLock);

    final TaskLock theLock = validator.expectLockCreated(
        TaskLockType.SHARED,
        Intervals.of("2017/2018"),
        MEDIUM_PRIORITY
    );

    validator.expectActiveLocks(theLock, sharedLock);
    validator.expectRevokedLocks(exclusiveLock, replaceLock, appendLock);
  }

  @Test
  public void testAppendLockCompatibility()
  {
    final TaskLock theLock = validator.expectLockCreated(
        TaskLockType.APPEND,
        Intervals.of("2017/2018"),
        MEDIUM_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2017-05-01/2017-06-01"),
        MEDIUM_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.SHARED,
        Intervals.of("2016/2019"),
        MEDIUM_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.REPLACE,
        Intervals.of("2017-05-01/2018-01-01"),
        MEDIUM_PRIORITY
    );

    final TaskLock replaceLock = validator.expectLockCreated(
        TaskLockType.REPLACE,
        Intervals.of("2017/2018"),
        MEDIUM_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.REPLACE,
        Intervals.of("2016/2019"),
        MEDIUM_PRIORITY
    );


    // Any append lock can be created, provided that it lies within the interval of the previously created replace lock
    // This should not revoke any of the existing locks even with a higher priority
    final TaskLock appendLock0 = validator.expectLockCreated(
        TaskLockType.APPEND,
        Intervals.of("2017-05-01/2017-06-01"),
        HIGH_PRIORITY
    );

    final TaskLock appendLock1 = validator.expectLockCreated(
        TaskLockType.APPEND,
        Intervals.of("2017-05-01/2017-06-01"),
        LOW_PRIORITY
    );

    validator.expectActiveLocks(theLock, replaceLock, appendLock0, appendLock1);
    validator.expectRevokedLocks();
  }

  @Test
  public void testAppendLockCanRevokeAllIncompatible()
  {
    final TaskLock sharedLock = validator.expectLockCreated(
        TaskLockType.SHARED,
        Intervals.of("2016/2019"),
        HIGH_PRIORITY
    );
    validator.revokeLock(sharedLock);

    final TaskLock exclusiveLock = validator.expectLockCreated(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2017-01-01/2017-02-01"),
        LOW_PRIORITY
    );

    final TaskLock replaceLock = validator.expectLockCreated(
        TaskLockType.REPLACE,
        Intervals.of("2017-07-01/2018-07-01"),
        LOW_PRIORITY
    );

    final TaskLock appendLock0 = validator.expectLockCreated(
        TaskLockType.APPEND,
        Intervals.of("2017-02-01/2017-03-01"),
        LOW_PRIORITY
    );

    final TaskLock appendLock1 = validator.expectLockCreated(
        TaskLockType.APPEND,
        Intervals.of("2017-02-01/2017-05-01"),
        HIGH_PRIORITY
    );

    validator.expectActiveLocks(exclusiveLock, replaceLock, appendLock0, appendLock1);
    validator.expectRevokedLocks(sharedLock);

    final TaskLock theLock = validator.expectLockCreated(
        TaskLockType.APPEND,
        Intervals.of("2017/2018"),
        MEDIUM_PRIORITY
    );

    validator.expectActiveLocks(theLock, appendLock0, appendLock1);
    validator.expectRevokedLocks(sharedLock, exclusiveLock, replaceLock);
  }


  @Test
  public void testReplaceLockCompatibility()
  {
    final TaskLock theLock = validator.expectLockCreated(
        TaskLockType.REPLACE,
        Intervals.of("2017/2018"),
        MEDIUM_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2017-05-01/2017-06-01"),
        MEDIUM_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.SHARED,
        Intervals.of("2016/2019"),
        MEDIUM_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.REPLACE,
        Intervals.of("2017/2018"),
        MEDIUM_PRIORITY
    );

    // An append lock can be created for an interval enclosed within the replace lock's.
    // Also note that the append lock has a higher priority but doesn't revoke the replace lock as it can coexist.
    final TaskLock appendLock = validator.expectLockCreated(
        TaskLockType.APPEND,
        Intervals.of("2017-05-01/2017-06-01"),
        HIGH_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.APPEND,
        Intervals.of("2016-05-01/2017-06-01"),
        MEDIUM_PRIORITY
    );

    validator.expectActiveLocks(theLock, appendLock);
    validator.expectRevokedLocks();
  }

  @Test
  public void testReplaceLockCanRevokeAllIncompatible()
  {
    final TaskLock appendLock0 = validator.expectLockCreated(
        TaskLockType.APPEND,
        Intervals.of("2016/2019"),
        HIGH_PRIORITY
    );
    validator.revokeLock(appendLock0);

    final TaskLock appendLock1 = validator.expectLockCreated(
        TaskLockType.APPEND,
        Intervals.of("2017-02-01/2017-03-01"),
        HIGH_PRIORITY
    );

    final TaskLock appendLock2 = validator.expectLockCreated(
        TaskLockType.APPEND,
        Intervals.of("2017-09-01/2018-03-01"),
        LOW_PRIORITY
    );

    final TaskLock exclusiveLock = validator.expectLockCreated(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2017-05-01/2017-06-01"),
        LOW_PRIORITY
    );

    final TaskLock replaceLock = validator.expectLockCreated(
        TaskLockType.REPLACE,
        Intervals.of("2016-09-01/2017-03-01"),
        LOW_PRIORITY
    );

    final TaskLock sharedLock = validator.expectLockCreated(
        TaskLockType.SHARED,
        Intervals.of("2017-04-01/2017-05-01"),
        LOW_PRIORITY
    );

    validator.expectActiveLocks(appendLock1, appendLock2, exclusiveLock, replaceLock, sharedLock);
    validator.expectRevokedLocks(appendLock0);

    final TaskLock theLock = validator.expectLockCreated(
        TaskLockType.REPLACE,
        Intervals.of("2017/2018"),
        MEDIUM_PRIORITY
    );

    validator.expectActiveLocks(appendLock1, theLock);
    validator.expectRevokedLocks(appendLock0, appendLock2, exclusiveLock, replaceLock, sharedLock);
  }

  @Test
  public void testGetLockedIntervalsForRevokedLocks()
  {
    // Acquire lock for a low priority task
    final Task lowPriorityTask = NoopTask.ofPriority(5);
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
    final Task highPriorityTask = NoopTask.ofPriority(10);
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
  public void testFailedToReacquireTaskLock()
  {
    // Tasks to be failed have a group id with the substring "FailingLockAcquisition"
    // Please refer to NullLockPosseTaskLockbox
    final Task taskWithFailingLockAcquisition0 = new NoopTask(null, "FailingLockAcquisition", null, 0, 0, null);
    final Task taskWithFailingLockAcquisition1 = new NoopTask(null, "FailingLockAcquisition", null, 0, 0, null);
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

  @Test
  public void testConflictsWithOverlappingSharedLocks()
  {
    TaskLock conflictingLock = validator.expectLockCreated(
        TaskLockType.SHARED,
        Intervals.of("2023-05-01/2023-06-01"),
        MEDIUM_PRIORITY
    );

    TaskLock floorLock = validator.expectLockCreated(
        TaskLockType.SHARED,
        Intervals.of("2023-05-26/2023-05-27"),
        MEDIUM_PRIORITY
    );

    validator.expectLockNotGranted(
        TaskLockType.EXCLUSIVE,
        Intervals.of("2023-05-28/2023-06-03"),
        MEDIUM_PRIORITY
    );

    validator.expectActiveLocks(conflictingLock, floorLock);
  }

  @Test
  public void testUnlockSupersededLocks()
  {
    final Task task = NoopTask.create();
    taskStorage.insert(task, TaskStatus.running(task.getId()));
    lockbox.add(task);
    final Task otherTask = NoopTask.create();
    taskStorage.insert(otherTask, TaskStatus.running(otherTask.getId()));
    lockbox.add(otherTask);

    // Can coexist and is superseded. Will be unlocked
    final TaskLock supersededLock = tryTimeChunkLock(
        TaskLockType.APPEND,
        task,
        Intervals.of("2024-01-01/2024-01-02"),
        "v0"
    ).getTaskLock();
    Assert.assertEquals(
        ImmutableSet.copyOf(taskStorage.getLocks(task.getId())),
        ImmutableSet.of(supersededLock)
    );

    // Can coexist, but is not superseded as the task doesn't belong to this posse
    final TaskLock taskNotInPosse = tryTimeChunkLock(
        TaskLockType.APPEND,
        otherTask,
        Intervals.of("2024-01-01/2024-01-02"),
        "v0"
    ).getTaskLock();
    Assert.assertEquals(
        ImmutableSet.copyOf(taskStorage.getLocks(otherTask.getId())),
        ImmutableSet.of(taskNotInPosse)
    );

    // Can coexist, but is not superseded as it is not an APPEND lock
    final TaskLock replaceLock = tryTimeChunkLock(
        TaskLockType.REPLACE,
        task,
        Intervals.of("2024-01-01/2025-01-01"),
        "v0"
    ).getTaskLock();
    Assert.assertEquals(
        ImmutableSet.copyOf(taskStorage.getLocks(task.getId())),
        ImmutableSet.of(supersededLock, replaceLock)
    );

    // Can coexist, but is not superseded due to higher version
    final TaskLock higherVersion = tryTimeChunkLock(
        TaskLockType.APPEND,
        task,
        Intervals.of("2024-01-11/2024-01-12"),
        "v1"
    ).getTaskLock();
    Assert.assertEquals(
        ImmutableSet.copyOf(taskStorage.getLocks(task.getId())),
        ImmutableSet.of(supersededLock, replaceLock, higherVersion)
    );

    // Can coexist, but is not superseded as interval is not fully contained
    final TaskLock uncontainedInterval = tryTimeChunkLock(
        TaskLockType.APPEND,
        task,
        Intervals.of("2024-01-28/2024-02-04"),
        "v0"
    ).getTaskLock();
    Assert.assertEquals(
        ImmutableSet.copyOf(taskStorage.getLocks(task.getId())),
        ImmutableSet.of(supersededLock, replaceLock, higherVersion, uncontainedInterval)
    );

    final TaskLock theLock = tryTimeChunkLock(
        TaskLockType.APPEND,
        task,
        Intervals.of("2024-01-01/2024-02-01"),
        "v0"
    ).getTaskLock();
    Assert.assertEquals(
        ImmutableSet.copyOf(taskStorage.getLocks(task.getId())),
        ImmutableSet.of(theLock, replaceLock, higherVersion, uncontainedInterval)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(taskStorage.getLocks(otherTask.getId())),
        ImmutableSet.of(taskNotInPosse)
    );
  }

  @Test
  public void testUpgradeSegmentsCleanupOnUnlock()
  {
    final Task replaceTask = NoopTask.create();
    final Task appendTask = NoopTask.create();
    final IndexerSQLMetadataStorageCoordinator coordinator
        = EasyMock.createMock(IndexerSQLMetadataStorageCoordinator.class);
    // Only the replaceTask should attempt a delete on the upgradeSegments table
    EasyMock.expect(coordinator.deleteUpgradeSegmentsForTask(replaceTask.getId())).andReturn(0).once();
    EasyMock.replay(coordinator);

    final TaskLockbox taskLockbox = new TaskLockbox(taskStorage, coordinator);

    taskLockbox.add(replaceTask);
    taskLockbox.tryLock(
        replaceTask,
        new TimeChunkLockRequest(TaskLockType.REPLACE, replaceTask, Intervals.of("2024/2025"), "v0")
    );

    taskLockbox.add(appendTask);
    taskLockbox.tryLock(
        appendTask,
        new TimeChunkLockRequest(TaskLockType.APPEND, appendTask, Intervals.of("2024/2025"), "v0")
    );

    taskLockbox.remove(replaceTask);
    taskLockbox.remove(appendTask);

    EasyMock.verify(coordinator);
  }

  private class TaskLockboxValidator
  {

    private final List<Task> tasks;
    private final TaskLockbox lockbox;
    private final TaskStorage taskStorage;
    private final Map<TaskLock, String> lockToTaskIdMap;

    TaskLockboxValidator(TaskLockbox lockbox, TaskStorage taskStorage)
    {
      lockToTaskIdMap = new HashMap<>();
      tasks = new ArrayList<>();
      this.lockbox = lockbox;
      this.taskStorage = taskStorage;
    }

    public TaskLock expectLockCreated(TaskLockType type, Interval interval, int priority)
    {
      final TaskLock lock = tryTaskLock(type, interval, priority);
      Assert.assertNotNull(lock);
      Assert.assertFalse(lock.isRevoked());
      return lock;
    }

    public void revokeLock(TaskLock lock)
    {
      lockbox.revokeLock(lockToTaskIdMap.get(lock), lock);
    }

    public void expectLockNotGranted(TaskLockType type, Interval interval, int priority)
    {
      final TaskLock lock = tryTaskLock(type, interval, priority);
      Assert.assertNull(lock);
    }

    public void expectRevokedLocks(TaskLock... locks)
    {
      final Set<TaskLock> allLocks = getAllLocks();
      final Set<TaskLock> activeLocks = getAllActiveLocks();
      Assert.assertEquals(allLocks.size() - activeLocks.size(), locks.length);
      for (TaskLock lock : locks) {
        Assert.assertTrue(allLocks.contains(lock.revokedCopy()));
        Assert.assertFalse(activeLocks.contains(lock));
      }
    }

    public void expectActiveLocks(TaskLock... locks)
    {
      final Set<TaskLock> allLocks = getAllLocks();
      final Set<TaskLock> activeLocks = getAllActiveLocks();
      Assert.assertEquals(activeLocks.size(), locks.length);
      for (TaskLock lock : locks) {
        Assert.assertTrue(allLocks.contains(lock));
        Assert.assertTrue(activeLocks.contains(lock));
      }
    }

    private TaskLock tryTaskLock(TaskLockType type, Interval interval, int priority)
    {
      final Task task = NoopTask.ofPriority(priority);
      tasks.add(task);
      lockbox.add(task);
      taskStorage.insert(task, TaskStatus.running(task.getId()));
      TaskLock lock = tryTimeChunkLock(type, task, interval).getTaskLock();
      if (lock != null) {
        lockToTaskIdMap.put(lock, task.getId());
      }
      return lock;
    }

    private Set<TaskLock> getAllActiveLocks()
    {
      return tasks.stream()
                  .flatMap(task -> taskStorage.getLocks(task.getId()).stream())
                  .filter(taskLock -> !taskLock.isRevoked())
                  .collect(Collectors.toSet());
    }

    private Set<TaskLock> getAllLocks()
    {
      return tasks.stream()
                  .flatMap(task -> taskStorage.getLocks(task.getId()).stream())
                  .collect(Collectors.toSet());
    }
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
