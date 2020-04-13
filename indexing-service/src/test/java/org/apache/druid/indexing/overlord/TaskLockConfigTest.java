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

import com.google.common.base.Optional;
import org.apache.druid.indexing.common.actions.LocalTaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.indexing.test.TestIndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

public class TaskLockConfigTest
{
  private TaskStorage taskStorage;

  @Before
  public void setup()
  {
    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(null));
  }

  @Test
  public void testDefault() throws EntryExistsException
  {
    final TaskQueue taskQueue = createTaskQueue(null);
    taskQueue.start();
    final Task task = NoopTask.create();
    Assert.assertTrue(taskQueue.add(task));
    taskQueue.stop();
    final Optional<Task> optionalTask = taskStorage.getTask(task.getId());
    Assert.assertTrue(optionalTask.isPresent());
    final Task fromTaskStorage = optionalTask.get();
    Assert.assertTrue(fromTaskStorage.getContextValue(Tasks.FORCE_TIME_CHUNK_LOCK_KEY));
  }

  @Test
  public void testNotForceTimeChunkLock() throws EntryExistsException
  {
    final TaskQueue taskQueue = createTaskQueue(false);
    taskQueue.start();
    final Task task = NoopTask.create();
    Assert.assertTrue(taskQueue.add(task));
    taskQueue.stop();
    final Optional<Task> optionalTask = taskStorage.getTask(task.getId());
    Assert.assertTrue(optionalTask.isPresent());
    final Task fromTaskStorage = optionalTask.get();
    Assert.assertFalse(fromTaskStorage.getContextValue(Tasks.FORCE_TIME_CHUNK_LOCK_KEY));
  }

  @Test
  public void testOverwriteDefault() throws EntryExistsException
  {
    final TaskQueue taskQueue = createTaskQueue(null);
    taskQueue.start();
    final Task task = NoopTask.create();
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, false);
    Assert.assertTrue(taskQueue.add(task));
    taskQueue.stop();
    final Optional<Task> optionalTask = taskStorage.getTask(task.getId());
    Assert.assertTrue(optionalTask.isPresent());
    final Task fromTaskStorage = optionalTask.get();
    Assert.assertFalse(fromTaskStorage.getContextValue(Tasks.FORCE_TIME_CHUNK_LOCK_KEY));
  }

  private TaskQueue createTaskQueue(@Nullable Boolean forceTimeChunkLock)
  {
    final TaskLockConfig lockConfig;
    if (forceTimeChunkLock != null) {
      lockConfig = new TaskLockConfig()
      {
        @Override
        public boolean isForceTimeChunkLock()
        {
          return forceTimeChunkLock;
        }
      };
    } else {
      lockConfig = new TaskLockConfig();
    }
    final TaskQueueConfig queueConfig = new TaskQueueConfig(null, null, null, null);
    final TaskRunner taskRunner = EasyMock.createNiceMock(RemoteTaskRunner.class);
    final TaskActionClientFactory actionClientFactory = EasyMock.createNiceMock(LocalTaskActionClientFactory.class);
    final TaskLockbox lockbox = new TaskLockbox(taskStorage, new TestIndexerMetadataStorageCoordinator());
    final ServiceEmitter emitter = new NoopServiceEmitter();
    return new TaskQueue(lockConfig, queueConfig, taskStorage, taskRunner, actionClientFactory, lockbox, emitter);
  }
}
