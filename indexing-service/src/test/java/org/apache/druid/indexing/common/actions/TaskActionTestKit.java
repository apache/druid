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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.indexing.overlord.HeapMemoryTaskStorage;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.metadata.segment.SqlSegmentMetadataTransactionFactory;
import org.apache.druid.metadata.segment.cache.HeapMemorySegmentMetadataCache;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.HeapMemoryIndexingStateStorage;
import org.apache.druid.segment.metadata.IndexingStateCache;
import org.apache.druid.segment.metadata.NoopSegmentSchemaCache;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.TestDruidLeaderSelector;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.joda.time.Period;
import org.junit.rules.ExternalResource;

import java.util.concurrent.atomic.AtomicBoolean;

public class TaskActionTestKit extends ExternalResource
{
  private final MetadataStorageTablesConfig metadataStorageTablesConfig = MetadataStorageTablesConfig.fromBase("druid");

  private TaskStorage taskStorage;
  private GlobalTaskLockbox taskLockbox;
  private StubServiceEmitter emitter;
  private TestDerbyConnector testDerbyConnector;
  private IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private TaskActionToolbox taskActionToolbox;
  private SegmentMetadataCache segmentMetadataCache;
  private BlockingExecutorService metadataCachePollExec;

  private boolean useSegmentMetadataCache = false;
  private boolean useCentralizedDatasourceSchema = false;
  private boolean batchSegmentAllocation = true;
  private boolean skipSegmentPayloadFetchForAllocation = new TaskLockConfig().isBatchAllocationReduceMetadataIO();
  private AtomicBoolean configFinalized = new AtomicBoolean();

  public TaskActionTestKit setUseSegmentMetadataCache(boolean useSegmentMetadataCache)
  {
    if (configFinalized.get()) {
      throw new IllegalStateException("Test config already finalized");
    }
    this.useSegmentMetadataCache = useSegmentMetadataCache;
    return this;
  }

  public TaskActionTestKit setUseCentralizedDatasourceSchema(boolean useCentralizedDatasourceSchema)
  {
    if (configFinalized.get()) {
      throw new IllegalStateException("Test config already finalized");
    }
    this.useCentralizedDatasourceSchema = useCentralizedDatasourceSchema;
    return this;
  }

  public TaskActionTestKit setBatchSegmentAllocation(boolean batchSegmentAllocation)
  {
    if (configFinalized.get()) {
      throw new IllegalStateException("Test config already finalized");
    }
    this.batchSegmentAllocation = batchSegmentAllocation;
    return this;
  }

  public TaskActionTestKit setSkipSegmentPayloadFetchForAllocation(boolean skipSegmentPayloadFetchForAllocation)
  {
    if (configFinalized.get()) {
      throw new IllegalStateException("Test config already finalized");
    }
    this.skipSegmentPayloadFetchForAllocation = skipSegmentPayloadFetchForAllocation;
    return this;
  }

  public StubServiceEmitter getServiceEmitter()
  {
    return emitter;
  }

  public TestDerbyConnector getTestDerbyConnector()
  {
    return testDerbyConnector;
  }

  public MetadataStorageTablesConfig getMetadataStorageTablesConfig()
  {
    return metadataStorageTablesConfig;
  }

  public GlobalTaskLockbox getTaskLockbox()
  {
    return taskLockbox;
  }

  public TaskStorage getTaskStorage()
  {
    return taskStorage;
  }

  public IndexerMetadataStorageCoordinator getMetadataStorageCoordinator()
  {
    return metadataStorageCoordinator;
  }

  public TaskActionToolbox getTaskActionToolbox()
  {
    return taskActionToolbox;
  }

  public void syncSegmentMetadataCache()
  {
    metadataCachePollExec.finishNextPendingTasks(4);
  }

  @Override
  public void before()
  {
    Preconditions.checkState(configFinalized.compareAndSet(false, true));
    emitter = new StubServiceEmitter();
    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(new Period("PT24H")));
    testDerbyConnector = new TestDerbyConnector(
        new MetadataStorageConnectorConfig(),
        metadataStorageTablesConfig,
        CentralizedDatasourceSchemaConfig.enabled(useCentralizedDatasourceSchema)
    );
    final ObjectMapper objectMapper = new TestUtils().getTestObjectMapper();
    final SegmentSchemaManager segmentSchemaManager = new SegmentSchemaManager(
        metadataStorageTablesConfig,
        objectMapper,
        testDerbyConnector
    );

    final SqlSegmentMetadataTransactionFactory transactionFactory = setupTransactionFactory(objectMapper, emitter);
    metadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        transactionFactory,
        objectMapper,
        metadataStorageTablesConfig,
        testDerbyConnector,
        segmentSchemaManager,
        CentralizedDatasourceSchemaConfig.enabled(useCentralizedDatasourceSchema),
        new HeapMemoryIndexingStateStorage()
    );
    taskLockbox = new GlobalTaskLockbox(taskStorage, metadataStorageCoordinator);
    taskLockbox.syncFromStorage();
    final TaskLockConfig taskLockConfig = new TaskLockConfig()
    {
      @Override
      public boolean isBatchSegmentAllocation()
      {
        return batchSegmentAllocation;
      }

      @Override
      public long getBatchAllocationWaitTime()
      {
        return 10L;
      }

      @Override
      public boolean isBatchAllocationReduceMetadataIO()
      {
        return skipSegmentPayloadFetchForAllocation;
      }
    };

    SupervisorManager supervisorManager = new SupervisorManager(objectMapper, null);
    SegmentAllocationQueue segmentAllocationQueue = new SegmentAllocationQueue(
        taskLockbox,
        taskLockConfig,
        metadataStorageCoordinator,
        emitter,
        ScheduledExecutors::fixed
    );
    if (segmentAllocationQueue.isEnabled()) {
      segmentAllocationQueue.becomeLeader();
    }
    taskActionToolbox = new TaskActionToolbox(
        taskLockbox,
        taskStorage,
        metadataStorageCoordinator,
        segmentAllocationQueue,
        emitter,
        supervisorManager,
        objectMapper
    );
    testDerbyConnector.createDataSourceTable();
    testDerbyConnector.createUpgradeSegmentsTable();
    testDerbyConnector.createPendingSegmentsTable();
    testDerbyConnector.createSegmentSchemasTable();
    testDerbyConnector.createSegmentTable();
    testDerbyConnector.createRulesTable();
    testDerbyConnector.createConfigTable();
    testDerbyConnector.createTaskTables();
    testDerbyConnector.createAuditTable();
    testDerbyConnector.createIndexingStatesTable();

    segmentMetadataCache.start();
    segmentMetadataCache.becomeLeader();
    syncSegmentMetadataCache();
  }

  private SqlSegmentMetadataTransactionFactory setupTransactionFactory(
      ObjectMapper objectMapper,
      ServiceEmitter emitter
  )
  {
    metadataCachePollExec = new BlockingExecutorService("test-cache-poll-exec");
    SegmentMetadataCache.UsageMode cacheMode
        = useSegmentMetadataCache
          ? SegmentMetadataCache.UsageMode.ALWAYS
          : SegmentMetadataCache.UsageMode.NEVER;

    segmentMetadataCache = new HeapMemorySegmentMetadataCache(
        objectMapper,
        Suppliers.ofInstance(new SegmentsMetadataManagerConfig(Period.seconds(1), cacheMode, null)),
        Suppliers.ofInstance(metadataStorageTablesConfig),
        new NoopSegmentSchemaCache(),
        new IndexingStateCache(),
        testDerbyConnector,
        (poolSize, name) -> new WrappingScheduledExecutorService(name, metadataCachePollExec, false),
        emitter
    );

    final TestDruidLeaderSelector leaderSelector = new TestDruidLeaderSelector();
    leaderSelector.becomeLeader();

    return new SqlSegmentMetadataTransactionFactory(
        objectMapper,
        metadataStorageTablesConfig,
        testDerbyConnector,
        leaderSelector,
        segmentMetadataCache,
        emitter
    )
    {
      @Override
      public int getMaxRetries()
      {
        return 2;
      }
    };
  }

  @Override
  public void after()
  {
    testDerbyConnector.tearDown();
    taskStorage = null;
    taskLockbox = null;
    testDerbyConnector = null;
    metadataStorageCoordinator = null;
    taskActionToolbox = null;
    segmentMetadataCache.stopBeingLeader();
    segmentMetadataCache.stop();
    useSegmentMetadataCache = false;
  }
}
