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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.metadata.EntryExistsException;
import io.druid.metadata.SQLMetadataStorageActionHandlerFactory;
import io.druid.metadata.TestDerbyConnector;
import junit.framework.Assert;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TaskLockboxTest
{
  private TaskStorage taskStorage;
  private TaskLockbox lockbox;
  private String taskStorageType;
  private String taskLockboxVersion;

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private static final String HEAP_TASK_STORAGE = "HeapMemoryTaskStorage";
  private static final String METADATA_TASK_STORAGE = "MetadataTaskStorage";
  private static final String TASKLOCKBOX_V1 = "v1";
  private static final String TASKLOCKBOX_V2 = "v2";

  @Parameterized.Parameters(name = "taskStorageType={0}, taskLockboxVersion={1}")
  public static Collection<String[]> constructFeed()
  {
    return Arrays.asList(new String[][]{
        {HEAP_TASK_STORAGE, TASKLOCKBOX_V1},
        {METADATA_TASK_STORAGE, TASKLOCKBOX_V1},
        {HEAP_TASK_STORAGE, TASKLOCKBOX_V2},
        {METADATA_TASK_STORAGE, TASKLOCKBOX_V2}
    });
  }

  public TaskLockboxTest(String taskStorageType, String taskLockboxVersion)
  {
    this.taskStorageType = taskStorageType;
    this.taskLockboxVersion = taskLockboxVersion;
  }

  @Before
  public void setUp(){
    if (taskStorageType.equals(HEAP_TASK_STORAGE)) {
      taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(null));
    } else {
      TestDerbyConnector testDerbyConnector = derbyConnectorRule.getConnector();
      testDerbyConnector.createTaskTables();
      testDerbyConnector.createSegmentTable();
      taskStorage = new MetadataTaskStorage(
          testDerbyConnector,
          new TaskStorageConfig(null),
          new SQLMetadataStorageActionHandlerFactory(
              testDerbyConnector,
              derbyConnectorRule.metadataTablesConfigSupplier().get(),
              new DefaultObjectMapper()
          )
      );
    }
    if (taskLockboxVersion.equals(TASKLOCKBOX_V2)) {
      lockbox = new TaskLockboxV2(taskStorage);
    } else {
      lockbox = new TaskLockboxV1(taskStorage);
    }
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

  @Test(expected = IllegalStateException.class)
  public void testLockAfterTaskComplete() throws InterruptedException
  {
    Task task = NoopTask.create();
    lockbox.add(task);
    lockbox.remove(task);
    lockbox.lock(task, new Interval("2015-01-01/2015-01-02"));
  }

  @Test
  public void testTryLock() throws InterruptedException, EntryExistsException
  {
    Task task = NoopTask.create();
    // add task to TaskStorage as well otherwise the task will be considered a zombie task as there will be
    // an active lock without associate entry in TaskStorage. Thus, unit test will fail
    taskStorage.insert(task, TaskStatus.running(task.getId()));
    lockbox.add(task);
    Assert.assertTrue(lockbox.tryLock(task, new Interval("2015-01-01/2015-01-03")).isPresent());

    // try to take lock for task 2 for overlapping interval
    Task task2 = NoopTask.create();
    // add task to TaskStorage as well otherwise the task will be considered a zombie task and unit test will fail
    taskStorage.insert(task2, TaskStatus.running(task2.getId()));
    lockbox.add(task2);
    Assert.assertFalse(lockbox.tryLock(task2, new Interval("2015-01-01/2015-01-02")).isPresent());

    // task 1 unlocks the lock
    lockbox.remove(task);

    // Now task2 should be able to get the lock
    Assert.assertTrue(lockbox.tryLock(task2, new Interval("2015-01-01/2015-01-02")).isPresent());
  }

  @Test
  public void testTrySmallerLock() throws InterruptedException
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
  public void testTryLockForInactiveTask() throws InterruptedException
  {
    Assert.assertFalse(lockbox.tryLock(NoopTask.create(), new Interval("2015-01-01/2015-01-02")).isPresent());
  }

  @Test(expected = IllegalStateException.class)
  public void testTryLockAfterTaskComplete() throws InterruptedException
  {
    Task task = NoopTask.create();
    lockbox.add(task);
    lockbox.remove(task);
    Assert.assertFalse(lockbox.tryLock(task, new Interval("2015-01-01/2015-01-02")).isPresent());
  }


}
