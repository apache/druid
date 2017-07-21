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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.data.input.FirehoseFactory;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.metadata.EntryExistsException;
import io.druid.metadata.SQLMetadataStorageActionHandlerFactory;
import io.druid.metadata.TestDerbyConnector;
import io.druid.server.initialization.ServerConfig;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TaskLockboxTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derby = new TestDerbyConnector.DerbyConnectorRule();

  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  private ServerConfig serverConfig;
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
        new SQLMetadataStorageActionHandlerFactory(
            derbyConnector,
            derby.metadataTablesConfigSupplier().get(),
            objectMapper
        )
    );
    serverConfig = EasyMock.niceMock(ServerConfig.class);
    EasyMock.expect(serverConfig.getMaxIdleTime()).andReturn(new Period(100)).anyTimes();
    EasyMock.replay(serverConfig);

    ServiceEmitter emitter  = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    EasyMock.replay(emitter);

    lockbox = new TaskLockbox(taskStorage, serverConfig);
  }

  @Test
  public void testLock() throws InterruptedException
  {
    Task task = NoopTask.create();
    lockbox.add(task);
    Assert.assertNotNull(lockbox.lock(task, new Interval("2015-01-01/2015-01-02")));
  }

  @Test(expected = IllegalStateException.class)
  public void testLockForInactiveTask() throws InterruptedException
  {
    lockbox.lock(NoopTask.create(), new Interval("2015-01-01/2015-01-02"));
  }

  @Test
  public void testLockAfterTaskComplete() throws InterruptedException
  {
    Task task = NoopTask.create();
    exception.expect(ISE.class);
    exception.expectMessage("Unable to grant lock to inactive Task");
    lockbox.add(task);
    lockbox.remove(task);
    lockbox.lock(task, new Interval("2015-01-01/2015-01-02"));
  }

  @Test
  public void testTryLock()
  {
    Task task = NoopTask.create();
    lockbox.add(task);
    Assert.assertTrue(lockbox.tryLock(task, new Interval("2015-01-01/2015-01-03")).isPresent());

    // try to take lock for task 2 for overlapping interval
    Task task2 = NoopTask.create();
    lockbox.add(task2);
    Assert.assertFalse(lockbox.tryLock(task2, new Interval("2015-01-01/2015-01-02")).isPresent());

    // task 1 unlocks the lock
    lockbox.remove(task);

    // Now task2 should be able to get the lock
    Assert.assertTrue(lockbox.tryLock(task2, new Interval("2015-01-01/2015-01-02")).isPresent());
  }

  @Test
  public void testTrySmallerLock()
  {
    Task task = NoopTask.create();
    lockbox.add(task);
    Optional<TaskLock> lock1 = lockbox.tryLock(task, new Interval("2015-01-01/2015-01-03"));
    Assert.assertTrue(lock1.isPresent());
    Assert.assertEquals(new Interval("2015-01-01/2015-01-03"), lock1.get().getInterval());

    // same task tries to take partially overlapping interval; should fail
    Assert.assertFalse(lockbox.tryLock(task, new Interval("2015-01-02/2015-01-04")).isPresent());

    // same task tries to take contained interval; should succeed and should match the original lock
    Optional<TaskLock> lock2 = lockbox.tryLock(task, new Interval("2015-01-01/2015-01-02"));
    Assert.assertTrue(lock2.isPresent());
    Assert.assertEquals(new Interval("2015-01-01/2015-01-03"), lock2.get().getInterval());

    // only the first lock should actually exist
    Assert.assertEquals(
        ImmutableList.of(lock1.get()),
        lockbox.findLocksForTask(task)
    );
  }


  @Test(expected = IllegalStateException.class)
  public void testTryLockForInactiveTask()
  {
    Assert.assertFalse(lockbox.tryLock(NoopTask.create(), new Interval("2015-01-01/2015-01-02")).isPresent());
  }

  @Test
  public void testTryLockAfterTaskComplete()
  {
    Task task = NoopTask.create();
    exception.expect(ISE.class);
    exception.expectMessage("Unable to grant lock to inactive Task");
    lockbox.add(task);
    lockbox.remove(task);
    Assert.assertFalse(lockbox.tryLock(task, new Interval("2015-01-01/2015-01-02")).isPresent());
  }

  @Test
  public void testTimeoutForLock() throws InterruptedException
  {
    Task task1 = NoopTask.create();
    Task task2 = new SomeTask(null, 0, 0, null, null, null);

    lockbox.add(task1);
    lockbox.add(task2);
    exception.expect(InterruptedException.class);
    exception.expectMessage("can not acquire lock for interval");
    lockbox.lock(task1, new Interval("2015-01-01/2015-01-02"));
    lockbox.lock(task2, new Interval("2015-01-01/2015-01-15"));
  }

  @Test
  public void testSyncFromStorage() throws EntryExistsException
  {
    final TaskLockbox originalBox = new TaskLockbox(taskStorage, serverConfig);
    for (int i = 0; i < 5; i++) {
      final Task task = NoopTask.create();
      taskStorage.insert(task, TaskStatus.running(task.getId()));
      originalBox.add(task);
      Assert.assertTrue(
          originalBox.tryLock(task, new Interval(StringUtils.format("2017-01-0%d/2017-01-0%d", (i + 1), (i + 2))))
                     .isPresent()
      );
    }

    final List<TaskLock> beforeLocksInStorage = taskStorage.getActiveTasks().stream()
                                                           .flatMap(task -> taskStorage.getLocks(task.getId()).stream())
                                                           .collect(Collectors.toList());

    final TaskLockbox newBox = new TaskLockbox(taskStorage, serverConfig);
    newBox.syncFromStorage();

    Assert.assertEquals(originalBox.getAllLocks(), newBox.getAllLocks());
    Assert.assertEquals(originalBox.getActiveTasks(), newBox.getActiveTasks());

    final List<TaskLock> afterLocksInStorage = taskStorage.getActiveTasks().stream()
                                                          .flatMap(task -> taskStorage.getLocks(task.getId()).stream())
                                                          .collect(Collectors.toList());

    Assert.assertEquals(beforeLocksInStorage, afterLocksInStorage);
  }

  public static class SomeTask extends NoopTask
  {

    public SomeTask(
        @JsonProperty("id") String id,
        @JsonProperty("runTime") long runTime,
        @JsonProperty("isReadyTime") long isReadyTime,
        @JsonProperty("isReadyResult") String isReadyResult,
        @JsonProperty("firehose") FirehoseFactory firehoseFactory,
        @JsonProperty("context") Map<String, Object> context
    )
    {
      super(id, runTime, isReadyTime, isReadyResult, firehoseFactory, context);
    }

    @Override
    public String getType()
    {
      return "someTask";
    }

    @Override
    public  String getGroupId()
    {
      return "someGroupId";
    }
  }
}
