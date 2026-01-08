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
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SegmentMetadata;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.FingerprintGenerator;
import org.apache.druid.segment.metadata.NoopSegmentSchemaCache;
import org.apache.druid.segment.metadata.SegmentSchemaCache;
import org.apache.druid.segment.metadata.SegmentSchemaTestUtils;
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HeapMemorySegmentMetadataCacheTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule
      = new TestDerbyConnector.DerbyConnectorRule(CentralizedDatasourceSchemaConfig.enabled(true));

  private BlockingExecutorService pollExecutor;
  private ScheduledExecutorFactory executorFactory;
  private TestDerbyConnector derbyConnector;
  private StubServiceEmitter serviceEmitter;

  private HeapMemorySegmentMetadataCache cache;
  private SegmentSchemaCache schemaCache;
  private SegmentSchemaTestUtils schemaTestUtils;

  @Before
  public void setup()
  {
    pollExecutor = new BlockingExecutorService("test-poll-exec");
    executorFactory = (poolSize, name) -> new WrappingScheduledExecutorService(name, pollExecutor, false);
    derbyConnector = derbyConnectorRule.getConnector();
    serviceEmitter = new StubServiceEmitter();

    derbyConnector.createSegmentTable();
    derbyConnector.createSegmentSchemasTable();
    derbyConnector.createPendingSegmentsTable();

    schemaTestUtils = new SegmentSchemaTestUtils(derbyConnectorRule, derbyConnector, TestHelper.JSON_MAPPER);
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

  private void setupTargetWithCaching(SegmentMetadataCache.UsageMode cacheMode)
  {
    setupTargetWithCaching(cacheMode, false);
  }

  /**
   * Creates the target {@link #cache} to be tested in the current test.
   */
  private void setupTargetWithCaching(SegmentMetadataCache.UsageMode cacheMode, boolean useSchemaCache)
  {
    if (cache != null) {
      throw new ISE("Test target has already been initialized with caching[%s]", cache.isEnabled());
    }
    final SegmentsMetadataManagerConfig metadataManagerConfig
        = new SegmentsMetadataManagerConfig(null, cacheMode, null);
    schemaCache = useSchemaCache ? new SegmentSchemaCache() : new NoopSegmentSchemaCache();
    cache = new HeapMemorySegmentMetadataCache(
        TestHelper.JSON_MAPPER,
        () -> metadataManagerConfig,
        derbyConnectorRule.metadataTablesConfigSupplier(),
        schemaCache,
        derbyConnector,
        executorFactory,
        serviceEmitter
    );
  }

  private void setupAndSyncCacheWithSchema()
  {
    setupTargetWithCaching(SegmentMetadataCache.UsageMode.ALWAYS, true);
    cache.start();
    cache.becomeLeader();
    syncCacheAfterBecomingLeader();
  }

  private void setupAndSyncCache()
  {
    setupTargetWithCaching(SegmentMetadataCache.UsageMode.ALWAYS);
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
    serviceEmitter.flush();
    pollExecutor.finishNextPendingTasks(2);
  }

  @Test
  public void testStart_schedulesDbPoll_ifCacheIsEnabled()
  {
    setupTargetWithCaching(SegmentMetadataCache.UsageMode.ALWAYS);
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
    setupTargetWithCaching(SegmentMetadataCache.UsageMode.NEVER);
    Assert.assertFalse(cache.isEnabled());

    cache.start();
    Assert.assertFalse(cache.isEnabled());
    Assert.assertFalse(pollExecutor.hasPendingTasks());
  }

  @Test
  public void testStop_stopsDbPoll_ifCacheIsEnabled()
  {
    setupTargetWithCaching(SegmentMetadataCache.UsageMode.ALWAYS);
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
    setupTargetWithCaching(SegmentMetadataCache.UsageMode.NEVER);

    cache.start();
    Assert.assertFalse(pollExecutor.hasPendingTasks());

    cache.becomeLeader();
    Assert.assertFalse(pollExecutor.hasPendingTasks());
  }

  @Test
  public void testBecomeLeader_throwsException_ifCacheIsStopped()
  {
    setupTargetWithCaching(SegmentMetadataCache.UsageMode.ALWAYS);
    DruidExceptionMatcher.defensive().expectMessageIs(
        "Cache has not been started yet"
    ).assertThrowsAndMatches(
        () -> cache.becomeLeader()
    );
  }

  @Test
  public void testReadCacheForDataSource_throwsException_ifCacheIsDisabled()
  {
    setupTargetWithCaching(SegmentMetadataCache.UsageMode.NEVER);
    DruidExceptionMatcher.defensive().expectMessageIs(
        "Segment metadata cache is not enabled."
    ).assertThrowsAndMatches(
        () -> cache.readCacheForDataSource(TestDataSource.WIKI, d -> 0)
    );
  }

  @Test
  public void testReadCacheForDataSource_throwsException_ifCacheIsStoppedOrNotLeader()
  {
    setupTargetWithCaching(SegmentMetadataCache.UsageMode.ALWAYS);
    Assert.assertTrue(cache.isEnabled());

    DruidExceptionMatcher.internalServerError().expectMessageIs(
        "Segment metadata cache has not been started yet."
    ).assertThrowsAndMatches(
        () -> cache.readCacheForDataSource(TestDataSource.WIKI, d -> 0)
    );

    cache.start();
    DruidExceptionMatcher.internalServerError().expectMessageIs(
        "Not leader yet. Segment metadata cache is not usable."
    ).assertThrowsAndMatches(
        () -> cache.readCacheForDataSource(TestDataSource.WIKI, d -> 0)
    );
  }

  @Test(timeout = 60_000)
  public void testReadCacheForDataSource_waitsForOneSync_afterBecomingLeader() throws InterruptedException
  {
    setupTargetWithCaching(SegmentMetadataCache.UsageMode.ALWAYS);
    cache.start();
    cache.becomeLeader();

    final List<String> observedEventOrder = new ArrayList<>();

    // Invoke getDatasource in Thread 1
    final Thread getDatasourceThread = new Thread(() -> {
      cache.readCacheForDataSource(TestDataSource.WIKI, d -> 0);
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
      cache.readCacheForDataSource(TestDataSource.WIKI, d -> 0);
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

    final DataSegmentPlus segment = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                                                      .markUsed().asPlus();
    final SegmentId segmentId = segment.getDataSegment().getId();

    Assert.assertNull(
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findUsedSegment(segmentId)
        )
    );

    final int numInsertedSegments = cache.writeCacheForDataSource(
        TestDataSource.WIKI,
        wikiCache -> wikiCache.insertSegments(Set.of(segment))
    );
    Assert.assertEquals(1, numInsertedSegments);
    Assert.assertEquals(
        segment.getDataSegment(),
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findUsedSegment(segmentId)
        )
    );
  }

  @Test
  public void testSync_addsUsedSegment_ifNotPresentInCache()
  {
    setupAndSyncCache();

    final DataSegmentPlus usedSegmentPlus
        = CreateDataSegments.ofDatasource(TestDataSource.WIKI).updatedNow().markUsed().asPlus();
    insertSegmentsInMetadataStore(Set.of(usedSegmentPlus));

    Assert.assertTrue(
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findUsedSegmentsPlusOverlappingAnyOf(List.of())
        ).isEmpty()
    );

    syncCache();
    serviceEmitter.verifyValue(Metric.STALE_USED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.UPDATED_USED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.PERSISTED_USED_SEGMENTS, 1L);

    Assert.assertEquals(
        usedSegmentPlus.getDataSegment(),
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findUsedSegment(usedSegmentPlus.getDataSegment().getId())
        )
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
    serviceEmitter.verifyNotEmitted(Metric.SYNC_DURATION_MILLIS);
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

    // Update the second segment to have an invalid id and payload
    derbyConnectorRule.segments().update(
        "UPDATE %1$s SET id = 'invalid', payload = ? WHERE id = ?",
        "invalid".getBytes(StandardCharsets.UTF_8),
        invalidSegment.getDataSegment().getId().toString()
    );

    // Verify that sync completes successfully and updates the valid segment
    setupAndSyncCache();
    serviceEmitter.verifyEmitted(Metric.SYNC_DURATION_MILLIS, 1);
    serviceEmitter.verifyValue(Metric.CACHED_USED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.SKIPPED_SEGMENTS, 1L);

    Assert.assertNull(
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findUsedSegment(invalidSegment.getDataSegment().getId())
        )
    );
    Assert.assertEquals(
        validSegment.getDataSegment(),
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findUsedSegment(validSegment.getDataSegment().getId())
        )
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
    serviceEmitter.verifyValue(Metric.SKIPPED_PENDING_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.CACHED_USED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.PERSISTED_USED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.PERSISTED_PENDING_SEGMENTS, 0L);

    Assert.assertEquals(
        segment.getDataSegment(),
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findUsedSegment(segment.getDataSegment().getId())
        )
    );
    Assert.assertTrue(
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findPendingSegmentsOverlapping(Intervals.ETERNITY)
        ).isEmpty()
    );
  }

  @Test
  public void testSync_updatesUsedSegment_ifCacheHasOlderEntry()
  {
    setupAndSyncCache();

    // Add a used segment to both metadata store and cache
    final DateTime updateTime = DateTimes.nowUtc();
    final DataSegmentPlus usedSegmentPlus =
        CreateDataSegments.ofDatasource(TestDataSource.WIKI).markUsed()
                          .lastUpdatedOn(updateTime).asPlus();
    insertSegmentsInMetadataStore(Set.of(usedSegmentPlus));
    cache.writeCacheForDataSource(
        TestDataSource.WIKI,
        wikiCache -> wikiCache.insertSegments(Set.of(usedSegmentPlus))
    );

    Assert.assertEquals(
        Set.of(usedSegmentPlus),
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findUsedSegmentsPlusOverlappingAnyOf(List.of())
        )
    );

    // Add a newer version of segment to metadata store
    final DataSegmentPlus updatedSegment = new DataSegmentPlus(
        usedSegmentPlus.getDataSegment(),
        usedSegmentPlus.getCreatedDate(),
        updateTime.plus(1),
        true,
        null,
        null,
        null
    );
    updateSegmentInMetadataStore(updatedSegment);

    syncCache();
    serviceEmitter.verifyValue(Metric.PERSISTED_USED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.STALE_USED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.UPDATED_USED_SEGMENTS, 1L);

    Assert.assertEquals(
        Set.of(updatedSegment),
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findUsedSegmentsPlusOverlappingAnyOf(List.of())
        )
    );
  }

  @Test
  public void testSync_removesUsedSegment_ifNotPresentInMetadataStore()
  {
    setupAndSyncCache();

    final DataSegmentPlus unpersistedSegmentPlus =
        CreateDataSegments.ofDatasource(TestDataSource.WIKI).markUsed().asPlus();
    cache.writeCacheForDataSource(
        TestDataSource.WIKI,
        wikiCache -> wikiCache.insertSegments(Set.of(unpersistedSegmentPlus))
    );

    final DataSegment unpersistedSegment = unpersistedSegmentPlus.getDataSegment();
    Assert.assertEquals(
        unpersistedSegment,
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findUsedSegment(unpersistedSegment.getId())
        )
    );

    syncCache();
    serviceEmitter.verifyValue(Metric.DELETED_SEGMENTS, 1L);
    serviceEmitter.verifyNotEmitted(Metric.PERSISTED_USED_SEGMENTS);

    Assert.assertNull(
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findUsedSegment(unpersistedSegment.getId())
        )
    );
  }

  @Test
  public void testSync_removesUnusedSegment_ifCacheHasOlderEntry()
  {
    setupAndSyncCache();

    final DateTime now = DateTimes.nowUtc();
    final DataSegmentPlus unpersistedSegmentPlus =
        CreateDataSegments.ofDatasource(TestDataSource.WIKI).markUsed().asPlus();
    cache.writeCacheForDataSource(
        TestDataSource.WIKI,
        wikiCache -> wikiCache.insertSegments(Set.of(unpersistedSegmentPlus))
    );
    cache.writeCacheForDataSource(
        TestDataSource.WIKI,
        wikiCache -> wikiCache.markSegmentAsUnused(
            unpersistedSegmentPlus.getDataSegment().getId(),
            now.minusMinutes(1)
        )
    );

    syncCache();
    serviceEmitter.verifyValue(Metric.DELETED_SEGMENTS, 1L);
    serviceEmitter.verifyNotEmitted(Metric.PERSISTED_USED_SEGMENTS);
    serviceEmitter.verifyNotEmitted(Metric.CACHED_USED_SEGMENTS);
    serviceEmitter.verifyNotEmitted(Metric.CACHED_UNUSED_SEGMENTS);
  }

  @Test
  public void testSync_doesNotRemoveIntervalWithOnlyUnusedSegments()
  {
    setupAndSyncCache();

    final DataSegmentPlus usedSegment =
        CreateDataSegments.ofDatasource(TestDataSource.WIKI).updatedNow().markUsed().asPlus();

    final DateTime now = DateTimes.nowUtc();
    cache.writeCacheForDataSource(
        TestDataSource.WIKI,
        wikiCache -> wikiCache.insertSegments(Set.of(usedSegment))
    );
    cache.writeCacheForDataSource(
        TestDataSource.WIKI,
        wikiCache -> wikiCache.markSegmentAsUnused(usedSegment.getDataSegment().getId(), now.plusMinutes(1))
    );

    syncCache();
    serviceEmitter.verifyValue(Metric.CACHED_UNUSED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.CACHED_INTERVALS, 1L);

    // Perform another sync
    serviceEmitter.flush();
    syncCache();
    serviceEmitter.verifyValue(Metric.CACHED_UNUSED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.CACHED_INTERVALS, 1L);
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
    Assert.assertTrue(
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findPendingSegmentIdsWithExactInterval(
                pendingSegment.getSequenceName(),
                segmentId.getInterval()
            )
        ).isEmpty()
    );

    syncCache();
    serviceEmitter.verifyValue(Metric.PERSISTED_PENDING_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.UPDATED_PENDING_SEGMENTS, 1L);

    Assert.assertEquals(
        List.of(segmentId),
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findPendingSegmentIdsWithExactInterval(
                pendingSegment.getSequenceName(),
                segmentId.getInterval()
            )
        )
    );
  }

  @Test
  public void testSync_removesPendingSegment_ifNotPresentInMetadataStore()
  {
    setupAndSyncCache();

    // Create a pending segment and add it only to the cache
    final PendingSegmentRecord pendingSegment = createPendingSegment(DateTimes.nowUtc().minusHours(1));
    cache.writeCacheForDataSource(
        TestDataSource.WIKI,
        wikiCache -> wikiCache.insertPendingSegment(pendingSegment, false)
    );

    final SegmentIdWithShardSpec segmentId = pendingSegment.getId();
    Assert.assertEquals(
        List.of(segmentId),
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findPendingSegmentIdsWithExactInterval(
                pendingSegment.getSequenceName(),
                segmentId.getInterval()
            )
        )
    );

    // Verify that sync removes the pending segment from the cache
    syncCache();
    serviceEmitter.verifyNotEmitted(Metric.PERSISTED_PENDING_SEGMENTS);
    serviceEmitter.verifyValue(Metric.DELETED_PENDING_SEGMENTS, 1L);

    Assert.assertTrue(
        cache.readCacheForDataSource(
            TestDataSource.WIKI,
            wikiCache -> wikiCache.findPendingSegmentIdsWithExactInterval(
                pendingSegment.getSequenceName(),
                segmentId.getInterval()
            )
        ).isEmpty()
    );
  }

  @Test
  public void testSync_cleansUpDataSourceCache_ifEmptyAndNotInUse()
  {
    setupAndSyncCache();

    final DateTime now = DateTimes.nowUtc();
    final DataSegmentPlus segment = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                                                      .markUsed().lastUpdatedOn(now.minusHours(1))
                                                      .asPlus();
    final int numInsertedSegments = cache.writeCacheForDataSource(
        TestDataSource.WIKI,
        wikiCache -> wikiCache.insertSegments(Set.of(segment))
    );
    Assert.assertEquals(1, numInsertedSegments);

    // Verify that sync removes the extra entry from the cache and also cleans it up
    syncCache();
    serviceEmitter.verifyValue(Metric.DELETED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.DELETED_DATASOURCES, 1L);
  }

  @Test
  public void test_sync_addsUsedSegmentSchema_ifNotPresentInCache()
  {
    setupAndSyncCacheWithSchema();

    Assert.assertTrue(
        schemaCache.getPublishedSchemaPayloadMap().isEmpty()
    );

    final SchemaPayload payload = new SchemaPayload(
        RowSignature.builder().add("col1", null).build()
    );
    final String fingerprint = getSchemaFingerprint(payload);
    schemaTestUtils.insertSegmentSchema(
        TestDataSource.WIKI,
        Map.of(fingerprint, payload),
        Set.of(fingerprint)
    );

    syncCache();
    serviceEmitter.verifyValue("segment/schemaCache/usedFingerprint/count", 1L);

    Assert.assertEquals(
        Map.of(fingerprint, payload),
        schemaCache.getPublishedSchemaPayloadMap()
    );
  }

  @Test
  public void test_sync_removesUsedSegmentSchema_ifNotPresentInMetadataStore()
  {
    setupAndSyncCacheWithSchema();

    final SchemaPayload payload = new SchemaPayload(
        RowSignature.builder().add("col1", null).build()
    );
    final String fingerprint = getSchemaFingerprint(payload);

    schemaCache.resetSchemaForPublishedSegments(
        Map.of(),
        Map.of(fingerprint, payload)
    );
    Assert.assertEquals(
        Map.of(fingerprint, payload),
        schemaCache.getPublishedSchemaPayloadMap()
    );

    syncCache();
    serviceEmitter.verifyValue("segment/schemaCache/usedFingerprint/count", 0L);

    Assert.assertTrue(
        schemaCache.getPublishedSchemaPayloadMap().isEmpty()
    );
  }

  @Test
  public void test_sync_addsUsedSegmentMetadata_ifNotPresentInCache()
  {
    setupAndSyncCacheWithSchema();

    Assert.assertTrue(
        schemaCache.getPublishedSegmentMetadataMap().isEmpty()
    );

    final SchemaPayload payload = new SchemaPayload(
        RowSignature.builder().add("col1", null).build()
    );
    final String fingerprint = getSchemaFingerprint(payload);

    final DataSegmentPlus usedSegmentPlus
        = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                            .withNumRows(10).withSchemaFingerprint(fingerprint)
                            .updatedNow().markUsed().asPlus();
    insertSegmentsInMetadataStoreWithSchema(usedSegmentPlus);

    syncCache();
    serviceEmitter.verifyValue(Metric.PERSISTED_USED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.CACHED_USED_SEGMENTS, 1L);
    serviceEmitter.verifyValue(Metric.UPDATED_USED_SEGMENTS, 1L);
    serviceEmitter.verifyValue("segment/schemaCache/used/count", 1L);

    Assert.assertEquals(
        Map.of(usedSegmentPlus.getDataSegment().getId(), new SegmentMetadata(10L, fingerprint)),
        schemaCache.getPublishedSegmentMetadataMap()
    );
  }

  @Test
  public void test_sync_removesUsedSegmentMetadata_ifNotPresentInMetadataStore()
  {
    setupAndSyncCacheWithSchema();

    final SchemaPayload payload = new SchemaPayload(
        RowSignature.builder().add("col1", null).build()
    );
    final String fingerprint = getSchemaFingerprint(payload);
    final SegmentId segmentId = SegmentId.dummy(TestDataSource.WIKI);
    final SegmentMetadata metadata = new SegmentMetadata(10L, fingerprint);

    schemaCache.resetSchemaForPublishedSegments(
        Map.of(segmentId, metadata),
        Map.of()
    );
    Assert.assertEquals(
        Map.of(segmentId, metadata),
        schemaCache.getPublishedSegmentMetadataMap()
    );

    syncCache();
    serviceEmitter.verifyValue("segment/schemaCache/used/count", 0L);

    Assert.assertTrue(
        schemaCache.getPublishedSegmentMetadataMap().isEmpty()
    );
  }

  private static String getSchemaFingerprint(SchemaPayload payload)
  {
    return new FingerprintGenerator(TestHelper.JSON_MAPPER).generateFingerprint(
        payload,
        TestDataSource.WIKI,
        CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
    );
  }

  private void insertSegmentsInMetadataStore(Set<DataSegmentPlus> segments)
  {
    IndexerSqlMetadataStorageCoordinatorTestBase
        .insertSegments(segments, false, derbyConnectorRule, TestHelper.JSON_MAPPER);
  }

  private void insertSegmentsInMetadataStoreWithSchema(DataSegmentPlus... segments)
  {
    IndexerSqlMetadataStorageCoordinatorTestBase
        .insertSegments(Set.of(segments), true, derbyConnectorRule, TestHelper.JSON_MAPPER);
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
