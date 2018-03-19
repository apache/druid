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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.indexing.common.Counters;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.LocalTaskActionClient;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.overlord.HeapMemoryTaskStorage;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.metadata.EntryExistsException;
import io.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import io.druid.metadata.TestDerbyConnector;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.server.metrics.NoopServiceEmitter;
import org.junit.Rule;

public abstract class IngestionTestBase
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private final TestUtils testUtils = new TestUtils();
  private final ObjectMapper objectMapper = testUtils.getTestObjectMapper();
  private final TaskStorage taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(null));
  private final TaskLockbox lockbox = new TaskLockbox(taskStorage);

  public IngestionTestBase()
  {
  }

  public TaskActionClient createActionClient(Task task)
  {
    return new LocalTaskActionClient(task, taskStorage, createTaskActionToolbox());
  }

  public void prepareTaskForLocking(Task task) throws EntryExistsException
  {
    lockbox.add(task);
    taskStorage.insert(task, TaskStatus.running(task.getId()));
  }

  public ObjectMapper getObjectMapper()
  {
    return objectMapper;
  }

  public TaskStorage getTaskStorage()
  {
    return taskStorage;
  }

  public TaskLockbox getLockbox()
  {
    return lockbox;
  }

  public TaskActionToolbox createTaskActionToolbox()
  {
    final IndexerSQLMetadataStorageCoordinator storageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        objectMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnectorRule.getConnector()
    );
    storageCoordinator.start();
    return new TaskActionToolbox(
        lockbox,
        taskStorage,
        storageCoordinator,
        new NoopServiceEmitter(),
        null,
        new Counters()
    );
  }

  public IndexIO getIndexIO()
  {
    return testUtils.getTestIndexIO();
  }

  public IndexMergerV9 getIndexMerger()
  {
    return testUtils.getTestIndexMergerV9();
  }
}
