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

package org.apache.druid.metadata;

import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.ObjectMetadata;
import org.apache.druid.indexing.overlord.SegmentCreateRequest;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.segment.SegmentMetadataTransactionFactory;
import org.apache.druid.metadata.segment.SqlSegmentMetadataReadOnlyTransactionFactory;
import org.apache.druid.metadata.segment.SqlSegmentMetadataTransactionFactory;
import org.apache.druid.metadata.segment.cache.HeapMemorySegmentMetadataCache;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.HeapMemoryIndexingStateStorage;
import org.apache.druid.segment.metadata.NoopIndexingStateCache;
import org.apache.druid.segment.metadata.NoopSegmentSchemaCache;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.TestDruidLeaderSelector;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests to verify behaviour of {@link IndexerSQLMetadataStorageCoordinator}
 * on the Coordinator for read-only purposes.
 */
public class IndexerSQLMetadataStorageCoordinatorReadOnlyTest extends IndexerSqlMetadataStorageCoordinatorTestBase
{
  @RegisterExtension
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule
      = new TestDerbyConnector.DerbyConnectorRule();

  private IndexerMetadataStorageCoordinator readOnlyStorage;
  private IndexerMetadataStorageCoordinator readWriteStorage;

  private TestDruidLeaderSelector leaderSelector;
  private SegmentMetadataCache segmentMetadataCache;
  private StubServiceEmitter emitter;
  private BlockingExecutorService cachePollExecutor;

  private SegmentMetadataCache.UsageMode cacheMode;

  public static Object[][] testParameters()
  {
    return new Object[][]{
        {SegmentMetadataCache.UsageMode.ALWAYS},
        {SegmentMetadataCache.UsageMode.NEVER},
        {SegmentMetadataCache.UsageMode.IF_SYNCED}
    };
  }

  public void initIndexerSQLMetadataStorageCoordinatorReadOnlyTest(SegmentMetadataCache.UsageMode cacheMode)
  {
    this.cacheMode = cacheMode;
  }

  @BeforeEach
  public void setup()
  {
    derbyConnector = derbyConnectorRule.getConnector();

    leaderSelector = new TestDruidLeaderSelector();
    emitter = new StubServiceEmitter();
    cachePollExecutor = new BlockingExecutorService("test-cache-poll-exec");
    segmentMetadataCache = new HeapMemorySegmentMetadataCache(
        mapper,
        () -> new SegmentsMetadataManagerConfig(null, cacheMode, null),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        new NoopSegmentSchemaCache(),
        new NoopIndexingStateCache(),
        derbyConnector,
        (corePoolSize, nameFormat) -> new WrappingScheduledExecutorService(
            nameFormat,
            cachePollExecutor,
            false
        ),
        emitter
    );

    readOnlyStorage = createStorageCoordinator(NodeRole.COORDINATOR);
    readWriteStorage = createStorageCoordinator(NodeRole.OVERLORD);

    derbyConnector.createSegmentTable();
    derbyConnector.createPendingSegmentsTable();

    leaderSelector.becomeLeader();

    // Get the cache ready if required
    if (isCacheEnabled()) {
      segmentMetadataCache.start();
      segmentMetadataCache.becomeLeader();
      syncCache();
      syncCache();
    }
  }

  @AfterEach
  public void tearDown()
  {
    segmentMetadataCache.stopBeingLeader();
    segmentMetadataCache.stop();
    leaderSelector.stopBeingLeader();
  }

  private void syncCache()
  {
    if (isCacheEnabled()) {
      cachePollExecutor.finishNextPendingTasks(2);
    }
  }

  private boolean isCacheEnabled()
  {
    return cacheMode != SegmentMetadataCache.UsageMode.NEVER;
  }

  private IndexerSQLMetadataStorageCoordinator createStorageCoordinator(
      NodeRole nodeRole
  )
  {
    final SegmentMetadataTransactionFactory transactionFactory;
    if (nodeRole.equals(NodeRole.COORDINATOR)) {
      transactionFactory = new SqlSegmentMetadataReadOnlyTransactionFactory(
          mapper,
          derbyConnectorRule.metadataTablesConfigSupplier().get(),
          derbyConnector
      );
    } else {
      transactionFactory = new SqlSegmentMetadataTransactionFactory(
          mapper,
          derbyConnectorRule.metadataTablesConfigSupplier().get(),
          derbyConnector,
          leaderSelector,
          segmentMetadataCache,
          emitter
      );
    }

    return new IndexerSQLMetadataStorageCoordinator(
        transactionFactory,
        TestHelper.JSON_MAPPER,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector,
        null,
        CentralizedDatasourceSchemaConfig.enabled(false),
        new HeapMemoryIndexingStateStorage()
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void test_markSegmentsAsUnused_throwsException(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorReadOnlyTest(cacheMode);
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.markSegmentAsUnused(defaultSegment.getId())
    );
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.markAllSegmentsAsUnused(TestDataSource.WIKI)
    );
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.markSegmentsAsUnused(TestDataSource.WIKI, Set.of(defaultSegment.getId()))
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void test_commitSegments_throwsException(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorReadOnlyTest(cacheMode);
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.commitSegments(Set.of(defaultSegment), null)
    );
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.commitSegmentsAndMetadata(Set.of(defaultSegment), null, null, null, null)
    );
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.commitAppendSegments(
            Set.of(defaultSegment),
            Map.of(),
            "allocator",
            null
        )
    );
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.commitAppendSegmentsAndMetadata(
            Set.of(defaultSegment),
            Map.of(),
            null,
            null,
            null,
            "allocator",
            null
        )
    );
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.commitReplaceSegments(Set.of(defaultSegment), Set.of(), null)
    );
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.commitMetadataOnly(
            TestDataSource.WIKI,
            TestDataSource.WIKI,
            new ObjectMetadata("A"),
            new ObjectMetadata("B")
        )
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void test_deleteSegments_throwsException(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorReadOnlyTest(cacheMode);
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.deleteSegments(Set.of(defaultSegment))
    );
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.deletePendingSegments(TestDataSource.WIKI)
    );
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.deletePendingSegmentsCreatedInInterval(TestDataSource.WIKI, Intervals.ETERNITY)
    );
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.deletePendingSegmentsForTaskAllocatorId(TestDataSource.WIKI, "allocator")
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void test_allocatePendingSegment_throwsException(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorReadOnlyTest(cacheMode);
    final SegmentCreateRequest createRequest =
        new SegmentCreateRequest("seq1", null, "v1", NumberedPartialShardSpec.instance(), "allocator1");
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.allocatePendingSegment(
            TestDataSource.WIKI,
            Intervals.ETERNITY,
            true,
            createRequest
        )
    );
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.allocatePendingSegments(
            TestDataSource.WIKI,
            Intervals.ETERNITY,
            false,
            List.of(createRequest),
            true
        )
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void test_retrieveSegmentForId_returnsSegment_ifPresent(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorReadOnlyTest(cacheMode);
    Assertions.assertNull(
        readOnlyStorage.retrieveSegmentForId(defaultSegment.getId())
    );

    readWriteStorage.commitSegments(Set.of(defaultSegment), null);
    Assertions.assertEquals(
        defaultSegment,
        readOnlyStorage.retrieveSegmentForId(defaultSegment.getId())
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void test_retrieveUsedSegmentForId_returnsSegment_ifPresent(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorReadOnlyTest(cacheMode);
    Assertions.assertNull(
        readOnlyStorage.retrieveUsedSegmentForId(defaultSegment.getId())
    );

    readWriteStorage.commitSegments(Set.of(defaultSegment), null);
    Assertions.assertEquals(
        defaultSegment,
        readOnlyStorage.retrieveUsedSegmentForId(defaultSegment.getId())
    );
  }

  @MethodSource("testParameters")
  @ParameterizedTest(name = "cacheMode = {0}")
  public void test_retrieveAllUsedSegments_returnsSegments_ifPresent(SegmentMetadataCache.UsageMode cacheMode)
  {
    initIndexerSQLMetadataStorageCoordinatorReadOnlyTest(cacheMode);
    Assertions.assertEquals(
        Set.of(),
        readOnlyStorage.retrieveAllUsedSegments(defaultSegment.getDataSource(), Segments.INCLUDING_OVERSHADOWED)
    );

    readWriteStorage.commitSegments(Set.of(defaultSegment), null);
    Assertions.assertEquals(
        Set.of(defaultSegment),
        readOnlyStorage.retrieveAllUsedSegments(defaultSegment.getDataSource(), Segments.INCLUDING_OVERSHADOWED)
    );
  }

  private static void verifyThrowsDefensiveException(Executable runnable)
  {
    org.apache.druid.error.DruidExceptionAssertions.assertMatches(
        Assertions.assertThrows(DruidException.class, runnable),
        DruidExceptionMatcher.defensive().expectMessageIs(
            "Only Overlord can perform write transactions on segment metadata."
        )
    );
  }
}
