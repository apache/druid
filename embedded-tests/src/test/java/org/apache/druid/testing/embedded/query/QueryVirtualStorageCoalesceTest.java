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

package org.apache.druid.testing.embedded.query;

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.Druids;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.server.metrics.LatchableEmitter;
import org.apache.druid.server.metrics.StorageMonitor;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Exercises range-read coalescing end to end: on-demand partial loads over MinIO S3 deep storage, asserting the
 * {@link StorageMonitor} read metrics reflect fewer requests than files loaded.
 */
class QueryVirtualStorageCoalesceTest extends EmbeddedClusterTestBase
{
  // Cache is sized to hold the whole dataset so a query's columns load once and eviction doesn't muddy the counts.
  private static final long CACHE_SIZE = HumanReadableBytes.parse("64MiB");
  private static final long MAX_SIZE = HumanReadableBytes.parse("1GiB");
  private static final long ESTIMATE_SIZE = HumanReadableBytes.parse("1MiB");

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedRouter router = new EmbeddedRouter();
  private final MinIOStorageResource storageResource = new MinIOStorageResource();

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    historical.setServerMemory(500_000_000)
              .addProperty("druid.segmentCache.virtualStorage", "true")
              .addProperty("druid.segmentCache.virtualStoragePartialDownloadsEnabled", "true")
              .addProperty("druid.segmentCache.virtualStorageMetadataReservationEstimate", String.valueOf(ESTIMATE_SIZE))
              .addProperty("druid.segmentCache.virtualStorageLoadThreads", String.valueOf(Runtime.getRuntime().availableProcessors()))
              .addBeforeStartHook(
                  (cluster, self) -> self.addProperty(
                      "druid.segmentCache.locations",
                      StringUtils.format(
                          "[{\"path\":\"%s\",\"maxSize\":\"%s\"}]",
                          cluster.getTestFolder().newFolder().getAbsolutePath(),
                          CACHE_SIZE
                      )
                  )
              )
              .addProperty("druid.server.maxSize", String.valueOf(MAX_SIZE));

    broker.setServerMemory(200_000_000);
    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "always");
    overlord.addProperty("druid.manager.segments.useIncrementalCache", "always")
            .addProperty("druid.manager.segments.pollDuration", "PT0.1s");
    indexer.setServerMemory(400_000_000)
           .addProperty("druid.worker.capacity", "4")
           .addProperty("druid.processing.numThreads", "3")
           .addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .useDefaultTimeoutForLatchableEmitter(20)
        .addResource(storageResource)
        // segments must be pushed unzipped (rangeable) and in V10 format for partial reads to engage
        .addCommonProperty("druid.storage.zip", "false")
        .addCommonProperty("druid.indexer.task.buildV10", "true")
        .addCommonProperty("druid.monitoring.emissionPeriod", "PT1s")
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(indexer)
        .addServer(historical)
        .addServer(broker)
        .addServer(router);
  }

  @BeforeAll
  void loadData() throws IOException
  {
    final EmbeddedMSQApis msqApis = new EmbeddedMSQApis(cluster, overlord);
    dataSource = "wiki-" + IdUtils.getRandomId();
    WikipediaVirtualStorageTable.ingestHourly(cluster, msqApis, broker, dataSource);
    // waitUntilSegmentsLoad only covers the historical mount; wait for the broker schema too so column queries resolve
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
  }

  @Override
  protected void refreshDatasourceName()
  {
    // set up once for all tests
  }

  @Test
  void testMultiColumnQueryCoalescesReads()
  {
    // no baseline wait here: nothing has loaded on the historical yet (partial loads only happen on query), so the
    // VSF metrics don't exist until the query below drives them
    final LatchableEmitter emitter = historical.latchableEmitter();
    emitter.flush();

    // Native scan, not SQL: the broker only knows __time for a metadata-mounted partial segment, so SQL can't resolve
    // data columns. A native query names its columns and the historical resolves + loads them on demand. Touching
    // several columns per segment lets contiguous files coalesce into far fewer range reads.
    final ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(new TableDataSource(dataSource))
        .eternityInterval()
        .columns("channel", "countryName", "page", "user", "added", "deleted", "delta")
        .limit(100)
        .build();
    final String result = cluster.callApi().onAnyBroker(b -> b.submitNativeQuery(query));
    Assertions.assertFalse(result.isEmpty());

    emitter.waitForNextEvent(event -> event.hasMetricName(StorageMonitor.VSF_READ_COUNT));
    final long reads = emitter.getMetricEventLongSum(StorageMonitor.VSF_READ_COUNT);
    final long loads = emitter.getMetricEventLongSum(StorageMonitor.VSF_LOAD_COUNT);

    Assertions.assertTrue(reads > 0, "expected some range reads, got " + reads);
    // the coalescing signal: multiple files land per read, so files loaded exceeds reads issued
    Assertions.assertTrue(loads > reads, "expected loads(" + loads + ") > reads(" + reads + ")");

    // gap-fill is emitted and bounded by the wire bytes; it quantifies over-fetch from coalescing
    emitter.waitForNextEvent(event -> event.hasMetricName(StorageMonitor.VSF_READ_GAP_BYTES));
    final long gapBytes = emitter.getMetricEventLongSum(StorageMonitor.VSF_READ_GAP_BYTES);
    final long readBytes = emitter.getMetricEventLongSum(StorageMonitor.VSF_READ_BYTES);
    Assertions.assertTrue(gapBytes >= 0, "gap-fill bytes should be non-negative, got " + gapBytes);
    Assertions.assertTrue(gapBytes <= readBytes, "gap-fill(" + gapBytes + ") cannot exceed read bytes(" + readBytes + ")");
  }

}
