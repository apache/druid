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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.DerbyMetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class TaskLockBoxConcurrencyTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derby = new TestDerbyConnector.DerbyConnectorRule();

  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  private ExecutorService service;
  private TaskStorage taskStorage;
  private TaskLockbox lockbox;
  private IndexerSQLMetadataStorageCoordinator coordinator;

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

    lockbox = new TaskLockbox(taskStorage);
    service = Executors.newFixedThreadPool(2);

    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    final AtomicLong metadataUpdateCounter = new AtomicLong();

    mapper.registerSubtypes(LinearShardSpec.class, NumberedShardSpec.class);
    derbyConnector.createDataSourceTable();
    derbyConnector.createTaskTables();
    derbyConnector.createSegmentTable();
    derbyConnector.createPendingSegmentsTable();
    metadataUpdateCounter.set(0);
    coordinator = new IndexerSQLMetadataStorageCoordinator(
        mapper,
        derby.metadataTablesConfigSupplier().get(),
        derbyConnector
    );
  }

  @After
  public void teardown()
  {
    service.shutdownNow();
  }

  @Test(timeout = 60_000L)
  public void testDoInCriticalSectionWithDifferentTasks()
      throws ExecutionException, InterruptedException, EntryExistsException
  {
    final Interval interval = Intervals.of("2017-01-01/2017-01-02");
    final Task lowPriorityTask = NoopTask.create(10);
    final Task highPriorityTask = NoopTask.create(100);
    lockbox.add(lowPriorityTask);
    lockbox.add(highPriorityTask);
    taskStorage.insert(lowPriorityTask, TaskStatus.running(lowPriorityTask.getId()));
    taskStorage.insert(highPriorityTask, TaskStatus.running(highPriorityTask.getId()));

    final SettableSupplier<Integer> intSupplier = new SettableSupplier<>(0);
    final CountDownLatch latch = new CountDownLatch(1);

    // lowPriorityTask acquires a lock first and increases the int of intSupplier in the critical section
    final Future<Integer> lowPriorityFuture = service.submit(() -> {
      final LockResult result = lockbox.tryLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval);
      Assert.assertTrue(result.isOk());
      Assert.assertFalse(result.isRevoked());

      return lockbox.doInCriticalSection(
          lowPriorityTask,
          Collections.singletonList(interval),
          CriticalAction.<Integer>builder()
              .onValidLocks(
                  () -> {
                    latch.countDown();
                    Thread.sleep(100);
                    intSupplier.set(intSupplier.get() + 1);
                    return intSupplier.get();
                  }
              )
              .onInvalidLocks(
                  () -> {
                    Assert.fail();
                    return null;
                  }
              )
              .build()
      );
    });

    // highPriorityTask awaits for the latch, acquires a lock, and increases the int of intSupplier in the critical
    // section
    final Future<Integer> highPriorityFuture = service.submit(() -> {
      latch.await();
      final LockResult result = lockbox.lock(TaskLockType.EXCLUSIVE, highPriorityTask, interval);
      Assert.assertTrue(result.isOk());
      Assert.assertFalse(result.isRevoked());

      return lockbox.doInCriticalSection(
          highPriorityTask,
          Collections.singletonList(interval),
          CriticalAction.<Integer>builder()
              .onValidLocks(
                  () -> {
                    Thread.sleep(100);
                    intSupplier.set(intSupplier.get() + 1);
                    return intSupplier.get();
                  }
              )
              .onInvalidLocks(
                  () -> {
                    Assert.fail();
                    return null;
                  }
              )
              .build()
      );
    });

    Assert.assertEquals(1, lowPriorityFuture.get().intValue());
    Assert.assertEquals(2, highPriorityFuture.get().intValue());

    // the lock for lowPriorityTask must be revoked by the highPriorityTask after its work is done in critical section
    final LockResult result = lockbox.tryLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval);
    Assert.assertFalse(result.isOk());
    Assert.assertTrue(result.isRevoked());
  }

  @Test(timeout = 60_000L)
  public void testDoInCriticalSectionWithOverlappedIntervals() throws Exception
  {
    final List<Interval> intervals = ImmutableList.of(
        Intervals.of("2017-01-01/2017-01-02"),
        Intervals.of("2017-01-02/2017-01-03"),
        Intervals.of("2017-01-03/2017-01-04")
    );
    final Task task = NoopTask.create();
    lockbox.add(task);
    taskStorage.insert(task, TaskStatus.running(task.getId()));

    for (Interval interval : intervals) {
      final LockResult result = lockbox.tryLock(TaskLockType.EXCLUSIVE, task, interval);
      Assert.assertTrue(result.isOk());
    }

    final SettableSupplier<Integer> intSupplier = new SettableSupplier<>(0);
    final CountDownLatch latch = new CountDownLatch(1);

    final Future<Integer> future1 = service.submit(() -> lockbox.doInCriticalSection(
        task,
        ImmutableList.of(intervals.get(0), intervals.get(1)),
        CriticalAction.<Integer>builder()
            .onValidLocks(
                () -> {
                  latch.countDown();
                  Thread.sleep(100);
                  intSupplier.set(intSupplier.get() + 1);
                  return intSupplier.get();
                }
            )
            .onInvalidLocks(
                () -> {
                  Assert.fail();
                  return null;
                }
            )
            .build()
    ));

    final Future<Integer> future2 = service.submit(() -> {
      latch.await();
      return lockbox.doInCriticalSection(
          task,
          ImmutableList.of(intervals.get(1), intervals.get(2)),
          CriticalAction.<Integer>builder()
              .onValidLocks(
                  () -> {
                    Thread.sleep(100);
                    intSupplier.set(intSupplier.get() + 1);
                    return intSupplier.get();
                  }
              )
              .onInvalidLocks(
                  () -> {
                    Assert.fail();
                    return null;
                  }
              )
              .build()
      );
    });

    Assert.assertEquals(1, future1.get().intValue());
    Assert.assertEquals(2, future2.get().intValue());
  }

  @Test(timeout = 60_000L)
  public void testAllocatePendingSegment() throws Exception
  {
    final String dataSource = "myDataSource";
    final Interval interval = Intervals.of("2017-01-01/2017-01-02");
    final Task task = NoopTask.create(dataSource);
    lockbox.add(task);
    final LockResult result = lockbox.tryLock(TaskLockType.EXCLUSIVE, task, interval);
    Assert.assertFalse(result.isRevoked());
    Assert.assertTrue(result.isOk());

    List<Future<SegmentIdentifier>> list = new ArrayList<>();
    final CountDownLatch latch = new CountDownLatch(1);

    for (int i = 0; i < 100; i++) {
      list.add(service.submit(() -> {
        latch.await();
        String sequnceName = "sequnceName" + ThreadLocalRandom.current().nextLong(10000);
        return coordinator.allocatePendingSegment(
            dataSource,
            sequnceName,
            null,
            interval,
            result.getTaskLock().getVersion(),
            true,
            result.getTaskLock());
      }));
    }

    latch.countDown();
    for (Future<SegmentIdentifier> future : list) {
      future.get();
    }
  }
}
