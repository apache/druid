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

package org.apache.druid.metadata.segment.cache;

import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.IndexerSqlMetadataStorageCoordinatorTestBase;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class HeapMemorySegmentMetadataCacheTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule
      = new TestDerbyConnector.DerbyConnectorRule();

  private BlockingExecutorService pollExecutor;
  private ScheduledExecutorFactory executorFactory;
  private TestDerbyConnector derbyConnector;
  private StubServiceEmitter serviceEmitter;

  private HeapMemorySegmentMetadataCache cache;

  @Before
  public void setup()
  {
    pollExecutor = new BlockingExecutorService("test-poll-exec");
    executorFactory = (poolSize, name) -> new WrappingScheduledExecutorService(name, pollExecutor, false);
    derbyConnector = derbyConnectorRule.getConnector();
    serviceEmitter = new StubServiceEmitter();

    derbyConnector.createSegmentTable();
    derbyConnector.createPendingSegmentsTable();

    EmittingLogger.registerEmitter(serviceEmitter);
  }

  @After
  public void tearDown()
  {
    if (cache != null) {
      cache.stopBeingLeader();
      cache.stop();
    }
  }

  /**
   * Creates the target {@link #cache} to be tested in the current test.
   */
  private void setupTargetWithCaching(boolean enabled)
  {
    if (cache != null) {
      throw new ISE("Test target has already been initialized with caching[%s]", cache.isEnabled());
    }
    final SegmentsMetadataManagerConfig metadataManagerConfig
        = new SegmentsMetadataManagerConfig(null, enabled);
    cache = new HeapMemorySegmentMetadataCache(
        TestHelper.JSON_MAPPER,
        () -> metadataManagerConfig,
        derbyConnectorRule.metadataTablesConfigSupplier(),
        derbyConnector,
        executorFactory,
        serviceEmitter
    );
  }

  private void setupAndSyncCache()
  {
    setupTargetWithCaching(true);
    cache.start();
    cache.becomeLeader();
    syncCache();
  }

  private void syncCache()
  {
    pollExecutor.finishNextPendingTask();
  }

  @Test
  public void testStart_schedulesDbPoll_ifCacheIsEnabled()
  {
    setupTargetWithCaching(true);
    Assert.assertTrue(cache.isEnabled());

    cache.start();
    Assert.assertTrue(pollExecutor.hasPendingTasks());

    syncCache();
    serviceEmitter.verifyEmitted(Metric.SYNC_DURATION_MILLIS, 1);

    Assert.assertTrue(pollExecutor.hasPendingTasks());
  }

  @Test
  public void testStart_doesNotScheduleDbPoll_ifCacheIsDisabled()
  {
    setupTargetWithCaching(false);
    Assert.assertFalse(cache.isEnabled());

    cache.start();
    Assert.assertFalse(cache.isEnabled());
    Assert.assertFalse(pollExecutor.hasPendingTasks());
  }

  @Test
  public void testStop_stopsDbPoll_ifCacheIsEnabled()
  {
    setupTargetWithCaching(true);
    Assert.assertTrue(cache.isEnabled());

    cache.start();
    Assert.assertTrue(pollExecutor.hasPendingTasks());

    cache.stop();
    Assert.assertTrue(pollExecutor.isShutdown());
    Assert.assertFalse(pollExecutor.hasPendingTasks());
  }

  @Test
  public void testBecomeLeader_isNoop_ifCacheIsDisabled()
  {
    setupTargetWithCaching(false);

    cache.start();
    Assert.assertFalse(pollExecutor.hasPendingTasks());

    cache.becomeLeader();
    Assert.assertFalse(pollExecutor.hasPendingTasks());
  }

  @Test
  public void testBecomeLeader_throwsException_ifCacheIsStopped()
  {
    setupTargetWithCaching(true);

    verifyThrowsException(
        () -> cache.becomeLeader(),
        "Cache has not been started yet"
    );
  }

  @Test
  public void testGetDataSource_throwsException_ifCacheIsDisabled()
  {
    setupTargetWithCaching(false);
    verifyThrowsException(
        () -> cache.getDatasource(TestDataSource.WIKI),
        "Segment metadata cache is not enabled."
    );
  }

  @Test
  public void testGetDataSource_throwsException_ifCacheIsStoppedOrNotLeader()
  {
    setupTargetWithCaching(true);
    Assert.assertTrue(cache.isEnabled());

    verifyThrowsException(
        () -> cache.getDatasource(TestDataSource.WIKI),
        "Segment metadata cache has not been started yet."
    );

    cache.start();
    verifyThrowsException(
        () -> cache.getDatasource(TestDataSource.WIKI),
        "Not leader yet. Segment metadata cache is not usable."
    );
  }

  @Test(timeout = 60_000)
  public void testGetDataSource_waitsForOneSync_afterBecomingLeader() throws InterruptedException
  {
    setupTargetWithCaching(true);
    cache.start();
    cache.becomeLeader();

    final List<String> observedEventOrder = new ArrayList<>();

    // Invoke getDatasource in Thread 1
    final Thread getDatasourceThread = new Thread(() -> {
      cache.getDatasource(TestDataSource.WIKI);
      observedEventOrder.add("getDatasource completed");
    });
    getDatasourceThread.start();

    // Invoke becomeLeader in Thread 2 after a wait period
    Thread.sleep(100);
    final Thread syncCompleteThread = new Thread(() -> {
      observedEventOrder.add("before first sync");
      syncCache();
    });
    syncCompleteThread.start();

    // Verify that the getDatasource call finishes only after the first sync
    getDatasourceThread.join();
    syncCompleteThread.join();
    Assert.assertEquals(
        List.of("before first sync", "getDatasource completed"),
        observedEventOrder
    );

    // Verify that subsequent calls to getDatasource do not wait
    final Thread getDatasourceThread2 = new Thread(() -> {
      cache.getDatasource(TestDataSource.WIKI);
      observedEventOrder.add("getDatasource 2 completed");
    });
    getDatasourceThread2.start();
    getDatasourceThread2.join();

    Assert.assertEquals(
        List.of("before first sync", "getDatasource completed", "getDatasource 2 completed"),
        observedEventOrder
    );
  }

  @Test
  public void testAddSegmentsToCache()
  {
    setupAndSyncCache();

    final DatasourceSegmentCache wikiCache = cache.getDatasource(TestDataSource.WIKI);

    final DataSegmentPlus segment = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                                                      .markUsed().asPlus();
    final String segmentId = segment.getDataSegment().getId().toString();

    Assert.assertNull(wikiCache.findUsedSegment(segmentId));

    Assert.assertEquals(1, wikiCache.insertSegments(Set.of(segment)));
    Assert.assertEquals(segment.getDataSegment(), wikiCache.findUsedSegment(segmentId));
  }

  @Test
  public void testSync_addsUsedSegment_ifNotPresentInCache()
  {
    setupAndSyncCache();

    final DataSegmentPlus usedSegmentPlus
        = CreateDataSegments.ofDatasource(TestDataSource.WIKI).updatedNow().markUsed().asPlus();
    insertSegmentsInMetadataStore(Set.of(usedSegmentPlus));

    final DatasourceSegmentCache wikiCache = cache.getDatasource(TestDataSource.WIKI);
    Assert.assertTrue(
        wikiCache.findUsedSegmentsPlusOverlappingAnyOf(List.of()).isEmpty()
    );

    syncCache();
    serviceEmitter.verifyValue(Metric.STALE_USED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.USED_SEGMENTS_UPDATED, 1L);
    serviceEmitter.verifyValue(Metric.POLLED_SEGMENTS, 1L);

    Assert.assertEquals(
        usedSegmentPlus.getDataSegment(),
        wikiCache.findUsedSegment(usedSegmentPlus.getDataSegment().getId().toString())
    );
  }

  @Test
  public void testSync_addsUnusedSegment_ifNotPresentInCache()
  {
    setupAndSyncCache();

    final DataSegmentPlus unusedSegmentPlus
        = CreateDataSegments.ofDatasource(TestDataSource.WIKI).updatedNow().markUnused().asPlus();
    insertSegmentsInMetadataStore(Set.of(unusedSegmentPlus));

    final SegmentId segmentId = unusedSegmentPlus.getDataSegment().getId();

    final DatasourceSegmentCache wikiCache = cache.getDatasource(TestDataSource.WIKI);
    Assert.assertNull(
        wikiCache.findHighestUnusedSegmentId(segmentId.getInterval(), segmentId.getVersion())
    );

    syncCache();
    serviceEmitter.verifyValue(Metric.UNUSED_SEGMENTS_UPDATED, 1L);
    serviceEmitter.verifyValue(Metric.POLLED_SEGMENTS, 1L);

    Assert.assertEquals(
        segmentId,
        wikiCache.findHighestUnusedSegmentId(segmentId.getInterval(), segmentId.getVersion())
    );
  }

  @Test
  public void testSync_doesNotUsedRefreshUnnecessarily()
  {

  }

  @Test
  public void testSync_updatesUsedSegment_ifCacheHasOlderEntry()
  {
    setupAndSyncCache();
    final DatasourceSegmentCache wikiCache = cache.getDatasource(TestDataSource.WIKI);

    // Add a used segment to both metadata store and cache
    final DateTime updateTime = DateTimes.nowUtc();
    final DataSegmentPlus usedSegmentPlus =
        CreateDataSegments.ofDatasource(TestDataSource.WIKI).markUsed()
                          .lastUpdatedOn(updateTime).asPlus();
    insertSegmentsInMetadataStore(Set.of(usedSegmentPlus));
    wikiCache.insertSegments(Set.of(usedSegmentPlus));

    Assert.assertEquals(
        Set.of(usedSegmentPlus),
        wikiCache.findUsedSegmentsPlusOverlappingAnyOf(List.of())
    );

    // Add a newer version of segment to metadata store
    final DataSegmentPlus updatedSegment = new DataSegmentPlus(
        usedSegmentPlus.getDataSegment(),
        null,
        updateTime.plus(1),
        true,
        null,
        null,
        null
    );
    updateSegmentInMetadataStore(updatedSegment);

    syncCache();
    serviceEmitter.verifyValue(Metric.POLLED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.STALE_USED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.USED_SEGMENTS_UPDATED, 1L);

    Assert.assertEquals(
        Set.of(updatedSegment),
        wikiCache.findUsedSegmentsPlusOverlappingAnyOf(List.of())
    );
  }

  @Test
  public void testSync_updatesUnusedSegment_ifCacheHasOlderEntry()
  {
    setupAndSyncCache();
    final DatasourceSegmentCache wikiCache = cache.getDatasource(TestDataSource.WIKI);

    final DataSegmentPlus unusedSegment =
        CreateDataSegments.ofDatasource(TestDataSource.WIKI).markUnused().asPlus();
    insertSegmentsInMetadataStore(Set.of(unusedSegment));

    syncCache();
    serviceEmitter.verifyValue(Metric.POLLED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.UNUSED_SEGMENTS_UPDATED, 1L);

    final SegmentId segmentId = unusedSegment.getDataSegment().getId();
    Assert.assertEquals(
        segmentId,
        wikiCache.findHighestUnusedSegmentId(segmentId.getInterval(), segmentId.getVersion())
    );
  }

  @Test
  public void testSync_removesUsedSegment_ifNotPresentInMetadataStore()
  {
    setupAndSyncCache();
    final DatasourceSegmentCache wikiCache = cache.getDatasource(TestDataSource.WIKI);

    final DataSegmentPlus unpersistedSegmentPlus =
        CreateDataSegments.ofDatasource(TestDataSource.WIKI).markUsed().asPlus();
    wikiCache.insertSegments(Set.of(unpersistedSegmentPlus));

    final DataSegment unpersistedSegment = unpersistedSegmentPlus.getDataSegment();
    Assert.assertEquals(
        unpersistedSegment,
        wikiCache.findUsedSegment(unpersistedSegment.getId().toString())
    );

    syncCache();
    serviceEmitter.verifyValue(Metric.DELETED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.POLLED_SEGMENTS, 0L);

    Assert.assertNull(
        wikiCache.findUsedSegment(unpersistedSegment.getId().toString())
    );
  }

  @Test
  public void testSync_removesUnusedSegment_ifNotPresentInMetadataStore()
  {
    setupAndSyncCache();
    final DatasourceSegmentCache wikiCache = cache.getDatasource(TestDataSource.WIKI);

    final DataSegmentPlus unpersistedSegmentPlus =
        CreateDataSegments.ofDatasource(TestDataSource.WIKI).markUnused().asPlus();
    wikiCache.insertSegments(Set.of(unpersistedSegmentPlus));

    final SegmentId unusedSegmentId = unpersistedSegmentPlus.getDataSegment().getId();
    Assert.assertEquals(
        unusedSegmentId,
        wikiCache.findHighestUnusedSegmentId(
            unusedSegmentId.getInterval(),
            unusedSegmentId.getVersion()
        )
    );

    syncCache();
    serviceEmitter.verifyValue(Metric.DELETED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.POLLED_SEGMENTS, 0L);

    Assert.assertNull(
        wikiCache.findHighestUnusedSegmentId(
            unusedSegmentId.getInterval(),
            unusedSegmentId.getVersion()
        )
    );
  }

  @Test
  public void testSync_addsPendingSegment_ifNotPresentInCache()
  {
    setupAndSyncCache();

    // Create a pending segment and add it only to the metadata store
    final PendingSegmentRecord pendingSegment = createPendingSegment();
    derbyConnectorRule.pendingSegments().insert(
        List.of(pendingSegment),
        false,
        TestHelper.JSON_MAPPER
    );

    final SegmentIdWithShardSpec segmentId = pendingSegment.getId();
    final DatasourceSegmentCache wikiCache = cache.getDatasource(TestDataSource.WIKI);
    Assert.assertTrue(
        wikiCache.findPendingSegmentIdsWithExactInterval(
            pendingSegment.getSequenceName(),
            segmentId.getInterval()
        ).isEmpty()
    );

    syncCache();
    serviceEmitter.verifyValue(Metric.POLLED_PENDING_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.PENDING_SEGMENTS_UPDATED, 1L);

    Assert.assertEquals(
        List.of(segmentId),
        wikiCache.findPendingSegmentIdsWithExactInterval(
            pendingSegment.getSequenceName(),
            segmentId.getInterval()
        )
    );
  }

  @Test
  public void testSync_removesPendingSegment_ifNotPresentInMetadataStore()
  {
    setupAndSyncCache();

    // Create a pending segment and add it only to the cache
    final PendingSegmentRecord pendingSegment = createPendingSegment();
    final DatasourceSegmentCache wikiCache = cache.getDatasource(TestDataSource.WIKI);
    wikiCache.insertPendingSegment(pendingSegment, false);

    final SegmentIdWithShardSpec segmentId = pendingSegment.getId();
    Assert.assertEquals(
        List.of(segmentId),
        wikiCache.findPendingSegmentIdsWithExactInterval(
            pendingSegment.getSequenceName(),
            segmentId.getInterval()
        )
    );

    // Verify that sync removes the pending segment from the cache
    syncCache();
    serviceEmitter.verifyValue(Metric.POLLED_PENDING_SEGMENTS, 0L);
    serviceEmitter.verifyValue(Metric.DELETED_PENDING_SEGMENTS, 1L);

    Assert.assertTrue(
        wikiCache.findPendingSegmentIdsWithExactInterval(
            pendingSegment.getSequenceName(),
            segmentId.getInterval()
        ).isEmpty()
    );
  }

  private void insertSegmentsInMetadataStore(Set<DataSegmentPlus> segments)
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();

    IndexerSqlMetadataStorageCoordinatorTestBase
        .insertSegments(segments, derbyConnector, table, TestHelper.JSON_MAPPER);
  }

  private void updateSegmentInMetadataStore(DataSegmentPlus segment)
  {
    int updatedRows = derbyConnectorRule.segments().update(
        "UPDATE %1$s SET used = ?, used_status_last_updated = ? WHERE id = ?",
        Boolean.TRUE.equals(segment.getUsed()),
        segment.getUsedStatusLastUpdatedDate().toString(),
        segment.getDataSegment().getId().toString()
    );
    Assert.assertEquals(1, updatedRows);
  }

  private static PendingSegmentRecord createPendingSegment()
  {
    SegmentIdWithShardSpec segmentId = new SegmentIdWithShardSpec(
        TestDataSource.WIKI,
        Intervals.of("2021-01-01/P1D"),
        "v1",
        new NumberedShardSpec(0, 1)
    );
    return new PendingSegmentRecord(
        segmentId,
        "sequence1", null, null, "allocator1", DateTimes.nowUtc()
    );
  }

  private static void verifyThrowsException(ThrowingRunnable runnable, String expectedMessage)
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(DruidException.class, runnable),
        DruidExceptionMatcher.defensive().expectMessageIs(expectedMessage)
    );
  }

  private static class Metric
  {
    static final String SYNC_DURATION_MILLIS = "segment/metadataCache/sync/time";

    static final String POLLED_SEGMENTS = "segment/metadataCache/polled/total";
    static final String POLLED_PENDING_SEGMENTS = "segment/metadataCache/polled/pending";

    static final String STALE_USED_SEGMENTS = "segment/metadataCache/stale/used";

    static final String DELETED_SEGMENTS = "segment/metadataCache/deleted/total";
    static final String DELETED_PENDING_SEGMENTS = "segment/metadataCache/deleted/pending";

    static final String USED_SEGMENTS_UPDATED = "segment/metadataCache/updated/used";
    static final String UNUSED_SEGMENTS_UPDATED = "segment/metadataCache/updated/unused";
    static final String PENDING_SEGMENTS_UPDATED = "segment/metadataCache/updated/pending";
  }

}
