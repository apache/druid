/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskLockType;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.metadata.EntryExistsException;
import io.druid.metadata.SQLMetadataStorageActionHandlerFactory;
import io.druid.metadata.TestDerbyConnector;
import io.druid.server.initialization.ServerConfig;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

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
  private ServerConfig serverConfig;
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
        new SQLMetadataStorageActionHandlerFactory(
            derbyConnector,
            derby.metadataTablesConfigSupplier().get(),
            objectMapper
        )
    );
    serverConfig = EasyMock.niceMock(ServerConfig.class);
    EasyMock.expect(serverConfig.getMaxIdleTime()).andReturn(new Period(5000)).anyTimes();
    EasyMock.replay(serverConfig);

    lockbox = new TaskLockbox(taskStorage, serverConfig);
    service = Executors.newFixedThreadPool(2);
  }

  @After
  public void teardown()
  {
    service.shutdownNow();
  }

  @Test(timeout = 5000L)
  public void testTryExclusiveLock() throws ExecutionException, InterruptedException, EntryExistsException
  {
    final Interval interval = new Interval("2017-01-01/2017-01-02");
    final Task lowPriorityTask = NoopTask.create(10);
    final Task highPriorityTask = NoopTask.create(100);
    lockbox.add(lowPriorityTask);
    lockbox.add(highPriorityTask);
    taskStorage.insert(lowPriorityTask, TaskStatus.running(lowPriorityTask.getId()));
    taskStorage.insert(highPriorityTask, TaskStatus.running(highPriorityTask.getId()));

    final CountDownLatch latch = new CountDownLatch(1);
    final Future<LockResult> lowPriorityFuture = service.submit(() -> {
      final LockResult lock = lockbox.tryLock(
          TaskLockType.EXCLUSIVE,
          lowPriorityTask,
          interval
      );
      latch.countDown();
      return lock;
    });
    final Future<LockResult> highPriorityFuture = service.submit(() -> {
      latch.await();
      return lockbox.tryLock(
          TaskLockType.EXCLUSIVE,
          highPriorityTask,
          interval
      );
    });

    final TaskLock lowLock = lowPriorityFuture.get().getTaskLock();
    final TaskLock highLock = highPriorityFuture.get().getTaskLock();

    Assert.assertNotNull(lowLock);
    Assert.assertNotNull(highLock);

    final Future<LockResult> lowUpgradeFuture = service.submit(
        () -> lockbox.upgrade(lowPriorityTask, interval)
    );
    final Future<LockResult> highUpgradeFuture = service.submit(
        () -> lockbox.upgrade(highPriorityTask, interval)
    );

    final LockResult resultOfHighPriorityLock = highUpgradeFuture.get();
    Assert.assertTrue(resultOfHighPriorityLock.isOk());
    Assert.assertTrue(resultOfHighPriorityLock.getTaskLock().isUpgraded());
    assertEqualsExceptUpgraded(highLock, resultOfHighPriorityLock.getTaskLock());

    final LockResult resultOfLowPriorityLock = lowUpgradeFuture.get();
    Assert.assertFalse(resultOfLowPriorityLock.isOk());
    Assert.assertTrue(resultOfLowPriorityLock.isRevoked());
  }

  private static void assertEqualsExceptUpgraded(TaskLock expected, TaskLock actual)
  {
    Assert.assertEquals(expected.getGroupId(), actual.getGroupId());
    Assert.assertEquals(expected.getDataSource(), actual.getDataSource());
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getInterval(), actual.getInterval());
    Assert.assertEquals(expected.getVersion(), actual.getVersion());
    Assert.assertEquals(expected.getPriority(), actual.getPriority());
  }
}
