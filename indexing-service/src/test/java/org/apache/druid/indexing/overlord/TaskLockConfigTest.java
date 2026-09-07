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
import org.apache.druid.indexing.common.task.NoopTaskContextEnricher;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunner;
import org.apache.druid.indexing.test.TestIndexerMetadataStorageCoordinator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

public class TaskLockConfigTest
{
  private TaskStorage taskStorage;

  @BeforeEach
  public void setup()
  {
    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(null));
  }

  @Test
  public void testDefault()
  {
    final TaskQueue taskQueue = createTaskQueue(null);
    taskQueue.start();
    final Task task = NoopTask.create();
    Assertions.assertTrue(taskQueue.add(task));
    taskQueue.stop();
    final Optional<Task> optionalTask = taskStorage.getTask(task.getId());
    Assertions.assertTrue(optionalTask.isPresent());
    final Task fromTaskStorage = optionalTask.get();
    Assertions.assertTrue(Boolean.TRUE.equals(fromTaskStorage.getContextValue(Tasks.FORCE_TIME_CHUNK_LOCK_KEY)));
  }

  @Test
  public void testNotForceTimeChunkLock()
  {
    final TaskQueue taskQueue = createTaskQueue(false);
    taskQueue.start();
    final Task task = NoopTask.create();
    Assertions.assertTrue(taskQueue.add(task));
    taskQueue.stop();
    final Optional<Task> optionalTask = taskStorage.getTask(task.getId());
    Assertions.assertTrue(optionalTask.isPresent());
    final Task fromTaskStorage = optionalTask.get();
    Assertions.assertFalse(Boolean.TRUE.equals(fromTaskStorage.getContextValue(Tasks.FORCE_TIME_CHUNK_LOCK_KEY)));
  }

  @Test
  public void testOverwriteDefault()
  {
    final TaskQueue taskQueue = createTaskQueue(null);
    taskQueue.start();
    final Task task = NoopTask.create();
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, false);
    Assertions.assertTrue(taskQueue.add(task));
    taskQueue.stop();
    final Optional<Task> optionalTask = taskStorage.getTask(task.getId());
    Assertions.assertTrue(optionalTask.isPresent());
    final Task fromTaskStorage = optionalTask.get();
    Assertions.assertFalse(Boolean.TRUE.equals(fromTaskStorage.getContextValue(Tasks.FORCE_TIME_CHUNK_LOCK_KEY)));
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
    final TaskQueueConfig queueConfig = new TaskQueueConfig(null, null, null, null, null, null);
    final TaskRunner taskRunner = EasyMock.createNiceMock(HttpRemoteTaskRunner.class);
    final TaskActionClientFactory actionClientFactory = EasyMock.createNiceMock(LocalTaskActionClientFactory.class);
    final GlobalTaskLockbox lockbox = new GlobalTaskLockbox(taskStorage, new TestIndexerMetadataStorageCoordinator());
    final ServiceEmitter emitter = new NoopServiceEmitter();
    return new TaskQueue(
        lockConfig,
        queueConfig,
        new DefaultTaskConfig(),
        taskStorage,
        taskRunner,
        actionClientFactory,
        lockbox,
        emitter,
        new DefaultObjectMapper(),
        new NoopTaskContextEnricher()
    );
  }
}
