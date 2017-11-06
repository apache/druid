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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.overlord.HeapMemoryTaskStorage;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.supervisor.SupervisorManager;
import io.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.metadata.MetadataSegmentManagerConfig;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataSegmentManager;
import io.druid.metadata.TestDerbyConnector;
import io.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.rules.ExternalResource;

public class TaskActionTestKit extends ExternalResource
{
  private final MetadataStorageTablesConfig metadataStorageTablesConfig = MetadataStorageTablesConfig.fromBase("druid");

  private TaskStorage taskStorage;
  private TaskLockbox taskLockbox;
  private TestDerbyConnector testDerbyConnector;
  private IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private MetadataSegmentManager metadataSegmentManager;
  private TaskActionToolbox taskActionToolbox;

  public MetadataStorageTablesConfig getMetadataStorageTablesConfig()
  {
    return metadataStorageTablesConfig;
  }

  public TaskStorage getTaskStorage()
  {
    return taskStorage;
  }

  public TaskLockbox getTaskLockbox()
  {
    return taskLockbox;
  }

  public TestDerbyConnector getTestDerbyConnector()
  {
    return testDerbyConnector;
  }

  public IndexerMetadataStorageCoordinator getMetadataStorageCoordinator()
  {
    return metadataStorageCoordinator;
  }

  public MetadataSegmentManager getMetadataSegmentManager()
  {
    return metadataSegmentManager;
  }

  public TaskActionToolbox getTaskActionToolbox()
  {
    return taskActionToolbox;
  }

  @Override
  public void before()
  {
    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(new Period("PT24H")));
    taskLockbox = new TaskLockbox(taskStorage);
    testDerbyConnector = new TestDerbyConnector(
        Suppliers.ofInstance(new MetadataStorageConnectorConfig()),
        Suppliers.ofInstance(metadataStorageTablesConfig)
    );
    final ObjectMapper objectMapper = new TestUtils().getTestObjectMapper();
    metadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        objectMapper,
        metadataStorageTablesConfig,
        testDerbyConnector
    );
    metadataSegmentManager = new SQLMetadataSegmentManager(
        objectMapper,
        Suppliers.ofInstance(new MetadataSegmentManagerConfig()),
        Suppliers.ofInstance(metadataStorageTablesConfig),
        testDerbyConnector
    );
    taskActionToolbox = new TaskActionToolbox(
        taskLockbox,
        metadataStorageCoordinator,
        new NoopServiceEmitter(),
        EasyMock.createMock(SupervisorManager.class)
    );
    testDerbyConnector.createDataSourceTable();
    testDerbyConnector.createPendingSegmentsTable();
    testDerbyConnector.createSegmentTable();
    testDerbyConnector.createRulesTable();
    testDerbyConnector.createConfigTable();
    testDerbyConnector.createTaskTables();
    testDerbyConnector.createAuditTable();
  }

  @Override
  public void after()
  {
    testDerbyConnector.tearDown();
    taskStorage = null;
    taskLockbox = null;
    testDerbyConnector = null;
    metadataStorageCoordinator = null;
    metadataSegmentManager = null;
    taskActionToolbox = null;
  }
}
