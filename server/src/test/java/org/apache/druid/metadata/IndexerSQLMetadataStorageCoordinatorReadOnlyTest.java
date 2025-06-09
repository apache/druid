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
import org.apache.druid.segment.metadata.NoopSegmentSchemaCache;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.TestDruidLeaderSelector;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests to verify behaviour of {@link IndexerSQLMetadataStorageCoordinator}
 * on the Coordinator for read-only purposes.
 */
@RunWith(Parameterized.class)
public class IndexerSQLMetadataStorageCoordinatorReadOnlyTest extends IndexerSqlMetadataStorageCoordinatorTestBase
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule
      = new TestDerbyConnector.DerbyConnectorRule();

  private IndexerMetadataStorageCoordinator readOnlyStorage;
  private IndexerMetadataStorageCoordinator readWriteStorage;

  private TestDruidLeaderSelector leaderSelector;
  private SegmentMetadataCache segmentMetadataCache;
  private StubServiceEmitter emitter;
  private BlockingExecutorService cachePollExecutor;

  private final SegmentMetadataCache.UsageMode cacheMode;

  @Parameterized.Parameters(name = "cacheMode = {0}")
  public static Object[][] testParameters()
  {
    return new Object[][]{
        {SegmentMetadataCache.UsageMode.ALWAYS},
        {SegmentMetadataCache.UsageMode.NEVER},
        {SegmentMetadataCache.UsageMode.IF_SYNCED}
    };
  }

  public IndexerSQLMetadataStorageCoordinatorReadOnlyTest(SegmentMetadataCache.UsageMode cacheMode)
  {
    this.cacheMode = cacheMode;
  }

  @Before
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

  @After
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
        CentralizedDatasourceSchemaConfig.enabled(false)
    );
  }

  @Test
  public void test_markSegmentsAsUnused_throwsException()
  {
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

  @Test
  public void test_commitSegments_throwsException()
  {
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.commitSegments(Set.of(defaultSegment), null)
    );
    verifyThrowsDefensiveException(
        () -> readOnlyStorage.commitSegmentsAndMetadata(Set.of(defaultSegment), null, null, null)
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
            new ObjectMetadata("A"),
            new ObjectMetadata("B")
        )
    );
  }

  @Test
  public void test_deleteSegments_throwsException()
  {
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

  @Test
  public void test_allocatePendingSegment_throwsException()
  {
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

  @Test
  public void test_retrieveSegmentForId_returnsSegment_ifPresent()
  {
    Assert.assertNull(
        readOnlyStorage.retrieveSegmentForId(defaultSegment.getId())
    );

    readWriteStorage.commitSegments(Set.of(defaultSegment), null);
    Assert.assertEquals(
        defaultSegment,
        readOnlyStorage.retrieveSegmentForId(defaultSegment.getId())
    );
  }

  @Test
  public void test_retrieveUsedSegmentForId_returnsSegment_ifPresent()
  {
    Assert.assertNull(
        readOnlyStorage.retrieveUsedSegmentForId(defaultSegment.getId())
    );

    readWriteStorage.commitSegments(Set.of(defaultSegment), null);
    Assert.assertEquals(
        defaultSegment,
        readOnlyStorage.retrieveUsedSegmentForId(defaultSegment.getId())
    );
  }

  @Test
  public void test_retrieveAllUsedSegments_returnsSegments_ifPresent()
  {
    Assert.assertEquals(
        Set.of(),
        readOnlyStorage.retrieveAllUsedSegments(defaultSegment.getDataSource(), Segments.INCLUDING_OVERSHADOWED)
    );

    readWriteStorage.commitSegments(Set.of(defaultSegment), null);
    Assert.assertEquals(
        Set.of(defaultSegment),
        readOnlyStorage.retrieveAllUsedSegments(defaultSegment.getDataSource(), Segments.INCLUDING_OVERSHADOWED)
    );
  }

  private static void verifyThrowsDefensiveException(ThrowingRunnable runnable)
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(DruidException.class, runnable),
        DruidExceptionMatcher.defensive().expectMessageIs(
            "Only Overlord can perform write transactions on segment metadata."
        )
    );
  }
}
