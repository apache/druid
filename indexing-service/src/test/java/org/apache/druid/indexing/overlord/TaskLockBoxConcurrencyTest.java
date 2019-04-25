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
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.DerbyMetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.TestDerbyConnector;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TaskLockBoxConcurrencyTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derby = new TestDerbyConnector.DerbyConnectorRule();

  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  private ExecutorService service;
  private TaskStorage taskStorage;
  private TaskLockbox lockbox;

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

    lockbox = new TaskLockbox(
        taskStorage,
        new IndexerSQLMetadataStorageCoordinator(objectMapper, derby.metadataTablesConfigSupplier().get(), derbyConnector)
    );
    service = Executors.newFixedThreadPool(2);
  }

  @After
  public void teardown()
  {
    service.shutdownNow();
  }

  private LockResult tryTimeChunkLock(TaskLockType lockType, Task task, Interval interval)
  {
    return lockbox.tryLock(task, new TimeChunkLockRequest(lockType, task, interval, null));
  }

  private LockResult acquireTimeChunkLock(TaskLockType lockType, Task task, Interval interval)
      throws InterruptedException
  {
    return lockbox.lock(task, new TimeChunkLockRequest(lockType, task, interval, null));
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
      final LockResult result = tryTimeChunkLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval);
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
      final LockResult result = acquireTimeChunkLock(TaskLockType.EXCLUSIVE, highPriorityTask, interval);
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
    final LockResult result = tryTimeChunkLock(TaskLockType.EXCLUSIVE, lowPriorityTask, interval);
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
      final LockResult result = tryTimeChunkLock(TaskLockType.EXCLUSIVE, task, interval);
      Assert.assertTrue(result.isOk());
    }

    final SettableSupplier<Integer> intSupplier = new SettableSupplier<>(0);
    final CountDownLatch latch = new CountDownLatch(1);

    final Future<Integer> future1 = service.submit(() -> lockbox.doInCriticalSection(
        task,
        intervals.subList(0, 2),
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
          intervals.subList(1, 3),
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
}
