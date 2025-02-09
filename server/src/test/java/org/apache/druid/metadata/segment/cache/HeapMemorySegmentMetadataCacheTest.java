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

import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.AlertEvent;
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
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

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
    syncCacheAfterBecomingLeader();
  }

  /**
   * Completes the cancelled sync and the fresh sync after becoming leader.
   */
  private void syncCacheAfterBecomingLeader()
  {
    syncCache();
    syncCache();
  }

  /**
   * Executes a sync and its callback.
   */
  private void syncCache()
  {
    pollExecutor.finishNextPendingTasks(2);
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
    DruidExceptionMatcher.defensive().expectMessageIs(
        "Cache has not been started yet"
    ).assertThrowsAndMatches(
        () -> cache.becomeLeader()
    );
  }

  @Test
  public void testGetDataSource_throwsException_ifCacheIsDisabled()
  {
    setupTargetWithCaching(false);
    DruidExceptionMatcher.defensive().expectMessageIs(
        "Segment metadata cache is not enabled."
    ).assertThrowsAndMatches(
        () -> cache.getDatasource(TestDataSource.WIKI)
    );
  }

  @Test
  public void testGetDataSource_throwsException_ifCacheIsStoppedOrNotLeader()
  {
    setupTargetWithCaching(true);
    Assert.assertTrue(cache.isEnabled());

    DruidExceptionMatcher.internalServerError().expectMessageIs(
        "Segment metadata cache has not been started yet."
    ).assertThrowsAndMatches(
        () -> cache.getDatasource(TestDataSource.WIKI)
    );

    cache.start();
    DruidExceptionMatcher.internalServerError().expectMessageIs(
        "Not leader yet. Segment metadata cache is not usable."
    ).assertThrowsAndMatches(
        () -> cache.getDatasource(TestDataSource.WIKI)
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

    // Invoke syncCache in Thread 2 after a wait period
    Thread.sleep(100);
    final Thread syncCompleteThread = new Thread(() -> {
      observedEventOrder.add("before first sync");
      syncCacheAfterBecomingLeader();
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
    final SegmentId segmentId = segment.getDataSegment().getId();

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
        wikiCache.findUsedSegment(usedSegmentPlus.getDataSegment().getId())
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
  public void testSync_emitsAlert_ifErrorOccurs()
  {
    setupAndSyncCache();
    serviceEmitter.verifyEmitted(Metric.SYNC_DURATION_MILLIS, 1);

    // Tear down the connector to cause sync to fail
    derbyConnector.tearDown();

    syncCache();

    final List<AlertEvent> alerts = serviceEmitter.getAlerts();
    Assert.assertEquals(1, alerts.size());
    Assert.assertEquals(
        "Could not sync segment metadata cache with metadata store",
        alerts.get(0).getDescription()
    );

    // Verify that sync duration is not emitted again
    serviceEmitter.verifyEmitted(Metric.SYNC_DURATION_MILLIS, 1);
  }

  @Test
  public void testSync_doesNotFail_ifSegmentRecordIsBad()
  {
    // Insert 2 segments into the metadata store
    final DataSegmentPlus validSegment = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                                                           .updatedNow().markUsed().asPlus();
    final DataSegmentPlus invalidSegment = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                                                             .updatedNow().markUsed().asPlus();
    insertSegmentsInMetadataStore(Set.of(validSegment, invalidSegment));

    // Update the second segment to have an invalid payload
    derbyConnectorRule.segments().update(
        "UPDATE %1$s SET id = 'invalid' WHERE id = ?",
        invalidSegment.getDataSegment().getId().toString()
    );

    // Verify that sync completes successfully and updates the valid segment
    setupAndSyncCache();
    serviceEmitter.verifyEmitted(Metric.SYNC_DURATION_MILLIS, 1);
    serviceEmitter.verifyValue(Metric.USED_SEGMENTS_UPDATED, 1L);
    serviceEmitter.verifyValue(Metric.IGNORED_SEGMENTS, 1L);

    final DatasourceSegmentCache wikiCache = cache.getDatasource(TestDataSource.WIKI);
    Assert.assertNull(wikiCache.findUsedSegment(invalidSegment.getDataSegment().getId()));
    Assert.assertEquals(
        validSegment.getDataSegment(),
        wikiCache.findUsedSegment(validSegment.getDataSegment().getId())
    );
  }

  @Test
  public void testSync_doesNotFail_ifPendingSegmentRecordIsBad()
  {
    // Insert an invalid pending segment record into the metadata store
    derbyConnector.retryWithHandle(
        handle -> handle
            .createStatement(
                StringUtils.format(
                    "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, "
                    + "sequence_name, sequence_prev_id, sequence_name_prev_id_sha1, payload) "
                    + "VALUES (:id, :dataSource, :created_date, :start, :end, "
                    + ":sequence_name, :sequence_prev_id, :sequence_name_prev_id_sha1, :payload)",
                    derbyConnectorRule.metadataTablesConfigSupplier().get().getPendingSegmentsTable(),
                    derbyConnector.getQuoteString()
                )
            )
            .bind("id", "1")
            .bind("dataSource", "wiki")
            .bind("created_date", "1")
            .bind("start", "-start-")
            .bind("end", "-end-")
            .bind("sequence_name", "s1")
            .bind("sequence_prev_id", "")
            .bind("sequence_name_prev_id_sha1", "abcdef")
            .bind("payload", new byte[0])
            .execute()
    );

    // Insert a valid segment record into the metadata store
    final DataSegmentPlus segment = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                                                      .updatedNow().markUsed().asPlus();
    insertSegmentsInMetadataStore(Set.of(segment));

    // Verify that sync has completed successfully and has updated the segment
    setupAndSyncCache();
    serviceEmitter.verifyEmitted(Metric.SYNC_DURATION_MILLIS, 1);
    serviceEmitter.verifyValue(Metric.IGNORED_PENDING_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.USED_SEGMENTS_UPDATED, 1L);
    serviceEmitter.verifyValue(Metric.POLLED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.POLLED_PENDING_SEGMENTS, 0L);

    final DatasourceSegmentCache wikiCache = cache.getDatasource(TestDataSource.WIKI);
    Assert.assertEquals(segment.getDataSegment(), wikiCache.findUsedSegment(segment.getDataSegment().getId()));
    Assert.assertTrue(
        wikiCache.findPendingSegmentsOverlapping(Intervals.ETERNITY)
                 .isEmpty()
    );
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
        wikiCache.findUsedSegment(unpersistedSegment.getId())
    );

    syncCache();
    serviceEmitter.verifyValue(Metric.DELETED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.POLLED_SEGMENTS, 0L);

    Assert.assertNull(
        wikiCache.findUsedSegment(unpersistedSegment.getId())
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
    final PendingSegmentRecord pendingSegment = createPendingSegment(DateTimes.nowUtc());
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
    final PendingSegmentRecord pendingSegment = createPendingSegment(DateTimes.nowUtc().minusHours(1));
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

  private static PendingSegmentRecord createPendingSegment(DateTime createdTime)
  {
    SegmentIdWithShardSpec segmentId = new SegmentIdWithShardSpec(
        TestDataSource.WIKI,
        Intervals.of("2021-01-01/P1D"),
        "v1",
        new NumberedShardSpec(0, 1)
    );
    return new PendingSegmentRecord(
        segmentId,
        "sequence1", null, null, "allocator1", createdTime
    );
  }

}
