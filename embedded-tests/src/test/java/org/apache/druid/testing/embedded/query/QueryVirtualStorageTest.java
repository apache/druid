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
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.server.metrics.LatchableEmitter;
import org.apache.druid.server.metrics.StorageMonitor;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.http.GetQueryReportResponse;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;
import org.apache.druid.testing.embedded.msq.EmbeddedDurableShuffleStorageTest;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.io.ByteStreams;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Virtual storage mode tests for classic native JSON queries
 */
class QueryVirtualStorageTest extends EmbeddedClusterTestBase
{
  // size of wiki segments (size here is size with uncompressed metadata as an upper bound since the zstd default
  // appears to make different sizes on different platforms) adjust this if segment size changes for some reason
  private static final long SIZE_BYTES = 3778338L;
  private static final long CACHE_SIZE = HumanReadableBytes.parse("1MiB");
  private static final long MAX_SIZE = HumanReadableBytes.parse("100MiB");
  private static final long ESTIMATE_SIZE = HumanReadableBytes.parse("2KiB");
  // Quiescence wait for the storage monitor: a few times its PT1s emission period, so a single missed tick doesn't
  // falsely read as "idle" while still bounding how long we wait once activity has actually stopped.
  private static final long MONITOR_QUIESCE_TIMEOUT_MILLIS = 3_000L;

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedRouter router = new EmbeddedRouter();
  private final MinIOStorageResource storageResource = new MinIOStorageResource();

  private EmbeddedMSQApis msqApis;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    historical.setServerMemory(500_000_000)
              .addProperty("druid.segmentCache.virtualStorage", "true")
              .addProperty("druid.segmentCache.virtualStoragePartialDownloadsEnabled", "true")
              .addProperty(
                  "druid.segmentCache.virtualStorageMetadataReservationEstimate",
                  String.valueOf(ESTIMATE_SIZE)
              )
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

    broker.setServerMemory(200_000_000)
          .addProperty("druid.msq.dart.controller.maxRetainedReportCount", "10")
          .addProperty("druid.query.default.context.maxConcurrentStages", "1")
          .addProperty("druid.sql.planner.enableSysQueriesTable", "true");

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
        .addCommonProperty("druid.msq.dart.enabled", "true")
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
    msqApis = new EmbeddedMSQApis(cluster, overlord);
    dataSource = createTestDatasourceName();
    loadWikiData();
  }

  @Override
  protected void refreshDatasourceName()
  {
    // don't change the datasource name for each run because we set things up before all tests
  }

  @Test
  void testQueryTooMuchData()
  {
    Throwable t = Assertions.assertThrows(
        RuntimeException.class,
        () -> cluster.runSql("select count(*) from \"%s\"", dataSource)
    );
    Assertions.assertTrue(t.getMessage().contains("Unable to reserve bundle"));
    Assertions.assertTrue(t.getMessage()
                           .contains(
                               "ensure enough disk space has been allocated to load all segments involved in the query"));
  }

  @Test
  void testQueryPartials()
  {
    // at the time this test was written, we can divide the segments up into these intervals and fit the required
    // segments in the cache, this is kind of brittle, but not quite sure what better to do and still expect exact
    // results..
    // "2015-09-12T00:00:00Z/2025-09-12T08:00:00Z"
    // "2015-09-12T08:00:00Z/2025-09-12T14:00:00Z"
    // "2015-09-12T14:00:00Z/2025-09-12T19:00:00Z"
    // "2015-09-12T19:00:00Z/2025-09-13T00:00:00Z"

    final String[] queries = new String[]{
        "select count(*) from \"%s\" WHERE __time >= TIMESTAMP '2015-09-12 00:00:00' and __time < TIMESTAMP '2015-09-12 08:00:00'",
        "select count(*) from \"%s\" WHERE __time >= TIMESTAMP '2015-09-12 08:00:00' and __time < TIMESTAMP '2015-09-12 14:00:00'",
        "select count(*) from \"%s\" WHERE __time >= TIMESTAMP '2015-09-12 14:00:00' and __time < TIMESTAMP '2015-09-12 19:00:00'",
        "select count(*) from \"%s\" WHERE __time >= TIMESTAMP '2015-09-12 19:00:00' and __time < TIMESTAMP '2015-09-13 00:00:00'"
    };
    final long[] expectedResults = new long[]{9770, 10524, 10267, 8683};
    final long[] expectedLoads = new long[]{8L, 6L, 5L, 5L};


    LatchableEmitter emitter = historical.latchableEmitter();
    LatchableEmitter coordinatorEmitter = coordinator.latchableEmitter();

    // Wait for any in-flight storage activity to settle before taking our baseline.
    emitter.awaitMetricQuiescent(StorageMonitor.VSF_LOAD_BEGIN_COUNT, MONITOR_QUIESCE_TIMEOUT_MILLIS);
    emitter.flush();

    // run the queries in order
    Assertions.assertEquals(expectedResults[0], Long.parseLong(cluster.runSql(queries[0], dataSource)));
    assertQueryMetrics(1, expectedLoads[0]);
    Assertions.assertEquals(expectedResults[1], Long.parseLong(cluster.runSql(queries[1], dataSource)));
    assertQueryMetrics(2, expectedLoads[1]);
    Assertions.assertEquals(expectedResults[2], Long.parseLong(cluster.runSql(queries[2], dataSource)));
    assertQueryMetrics(3, expectedLoads[2]);
    Assertions.assertEquals(expectedResults[3], Long.parseLong(cluster.runSql(queries[3], dataSource)));
    assertQueryMetrics(4, expectedLoads[3]);

    emitter.waitForNextEvent(event -> event.hasMetricName(StorageMonitor.VSF_LOAD_BEGIN_COUNT));
    long firstLoads = emitter.getMetricEventLongSum(StorageMonitor.VSF_LOAD_BEGIN_COUNT);
    Assertions.assertTrue(firstLoads >= 24, "expected " + 24 + " but only got " + firstLoads);

    long expectedTotalHits = 0;
    long expectedTotalLoad = 0;
    for (int i = 0; i < 1000; i++) {
      int nextQuery = ThreadLocalRandom.current().nextInt(queries.length);
      Assertions.assertEquals(expectedResults[nextQuery], Long.parseLong(cluster.runSql(queries[nextQuery], dataSource)));
      assertQueryMetrics(i + 5, null);
      long actualLoads = getMetricLatestValue(emitter, DefaultQueryMetrics.QUERY_ON_DEMAND_LOAD_COUNT, i + 5);
      expectedTotalLoad += actualLoads;
      expectedTotalHits += (expectedLoads[nextQuery] - actualLoads);
    }

    emitter.waitForNextEvent(event -> event.hasMetricName(StorageMonitor.VSF_HIT_COUNT));
    long hits = emitter.getMetricEventLongSum(StorageMonitor.VSF_HIT_COUNT);
    Assertions.assertTrue(hits >= expectedTotalHits, "expected " + expectedTotalHits + " but only got " + hits);
    if (expectedTotalHits > 0) {
      emitter.waitForNextEvent(event -> event.hasMetricName(StorageMonitor.VSF_HIT_BYTES));
      Assertions.assertTrue(emitter.getMetricEventLongSum(StorageMonitor.VSF_HIT_BYTES) > 0);
    }
    emitter.waitForNextEvent(event -> event.hasMetricName(StorageMonitor.VSF_LOAD_BEGIN_COUNT));
    long loads = emitter.getMetricEventLongSum(StorageMonitor.VSF_LOAD_BEGIN_COUNT);
    Assertions.assertTrue(loads >= expectedTotalLoad, "expected " + expectedTotalLoad + " but only got " + loads);
    Assertions.assertTrue(emitter.getMetricEventLongSum(StorageMonitor.VSF_LOAD_BEGIN_BYTES) > 0);
    Assertions.assertTrue(emitter.getMetricEventLongSum(StorageMonitor.VSF_LOAD_COUNT) > 0);
    Assertions.assertTrue(emitter.getMetricEventLongSum(StorageMonitor.VSF_LOAD_BYTES) > 0);
    emitter.waitForNextEvent(event -> event.hasMetricName(StorageMonitor.VSF_READ_COUNT));
    Assertions.assertTrue(emitter.getMetricEventLongSum(StorageMonitor.VSF_READ_COUNT) > 0);
    Assertions.assertTrue(emitter.getMetricEventLongSum(StorageMonitor.VSF_READ_BYTES) > 0);
    Assertions.assertTrue(emitter.getMetricEventLongSum(StorageMonitor.VSF_READ_TIME) >= 0);
    emitter.waitForNextEvent(event -> event.hasMetricName(StorageMonitor.VSF_EVICT_COUNT));
    Assertions.assertTrue(emitter.getMetricEventLongSum(StorageMonitor.VSF_EVICT_COUNT) > 0);
    Assertions.assertTrue(emitter.getMetricEventLongSum(StorageMonitor.VSF_EVICT_BYTES) > 0);
    Assertions.assertEquals(0, emitter.getMetricEventLongSum(StorageMonitor.VSF_REJECT_COUNT));
    Assertions.assertTrue(emitter.getLatestMetricEventValue(StorageMonitor.VSF_USED_BYTES, 0).longValue() > 0);

    coordinatorEmitter.waitForEvent(event -> event.hasMetricName(Stats.Tier.STORAGE_CAPACITY.getMetricName()));
    Assertions.assertEquals(
        CACHE_SIZE,
        coordinatorEmitter.getLatestMetricEventValue(Stats.Tier.STORAGE_CAPACITY.getMetricName())
    );
    coordinatorEmitter.waitForEvent(event -> event.hasMetricName(Stats.Tier.TOTAL_CAPACITY.getMetricName()));
    Assertions.assertEquals(
        MAX_SIZE,
        coordinatorEmitter.getLatestMetricEventValue(Stats.Tier.TOTAL_CAPACITY.getMetricName())
    );
  }

  @Test
  void testQueryTooMuchDataButWithDart()
  {
    // dart uses vsf in a totally rad way so it can query all of the segments at once due to how it chunks up and
    // fetches segments to do the work
    final String sqlQueryId = UUID.randomUUID().toString();
    final String resultString = cluster.callApi().onAnyBroker(
        b -> b.submitSqlQuery(
            new ClientSqlQuery(
                StringUtils.format("select count(*) from \"%s\"", dataSource),
                "CSV",
                false,
                false,
                false,
                Map.of(
                    QueryContexts.CTX_SQL_QUERY_ID, sqlQueryId,
                    QueryContexts.ENGINE, "msq-dart"
                ),
                null
            )
        )
    ).trim();

    final Long result = Long.parseLong(resultString);
    Assertions.assertEquals(39244L, result);

    // Now fetch the report using the SQL query ID
    final GetQueryReportResponse reportResponse = msqApis.getDartQueryReport(sqlQueryId, broker);
    Assertions.assertNotNull(reportResponse, "Report response should not be null");

    final MSQTaskReportPayload reportPayload =
        ((MSQTaskReport) reportResponse.getReportMap().get(MSQTaskReport.REPORT_KEY)).getPayload();

    // Verify stage 0 (segment read) input counters
    final EmbeddedMSQApis.ChannelSums inputChannelSums = msqApis.getInputChannelSums(reportPayload, 0);
    Assertions.assertEquals(24L, inputChannelSums.files());
    Assertions.assertEquals(24L, inputChannelSums.totalFiles());
    Assertions.assertEquals(0L, inputChannelSums.queries());
    Assertions.assertEquals(0L, inputChannelSums.totalQueries());
    Assertions.assertEquals(39244L, inputChannelSums.rows());
    MatcherAssert.assertThat(inputChannelSums.bytes(), Matchers.greaterThan(0L));
    MatcherAssert.assertThat(inputChannelSums.bytes(), Matchers.lessThanOrEqualTo(SIZE_BYTES));

    // Verify stage 0 (segment read) VSF load counters
    // partial loading is only partially metered at the moment, so depending on how stuff landed in and was evicted
    // from the cache,there can be 0 loads (because loads are currently only counted when the metadata entry is mounted)
    MatcherAssert.assertThat(inputChannelSums.loadFiles(), Matchers.greaterThanOrEqualTo(0L));
    MatcherAssert.assertThat(inputChannelSums.loadFiles(), Matchers.lessThanOrEqualTo(24L));
    MatcherAssert.assertThat(inputChannelSums.loadTime(), Matchers.greaterThanOrEqualTo(0L));
    MatcherAssert.assertThat(inputChannelSums.loadWait(), Matchers.greaterThanOrEqualTo(0L));
    MatcherAssert.assertThat(inputChannelSums.loadBytes(), Matchers.greaterThanOrEqualTo(0L));
    MatcherAssert.assertThat(inputChannelSums.loadBytes(), Matchers.lessThanOrEqualTo(SIZE_BYTES));
  }

  @Test
  void testQuerySysTables()
  {
    final String query = "SELECT curr_size, max_size, storage_size FROM sys.servers WHERE tier IS NOT NULL AND server_type = 'historical'";
    final String resultString = cluster.callApi().runSql(query);
    final String[] split = resultString.split(",");
    Assertions.assertTrue(Long.parseLong(split[0]) <= SIZE_BYTES);
    Assertions.assertEquals(MAX_SIZE, Long.parseLong(split[1]));
    Assertions.assertEquals(CACHE_SIZE, Long.parseLong(split[2]));
  }


  private void assertQueryMetrics(int expectedEventCount, @Nullable Long expectedLoadCount)
  {
    LatchableEmitter emitter = historical.latchableEmitter();

    long loadCount = getMetricLatestValue(emitter, DefaultQueryMetrics.QUERY_ON_DEMAND_LOAD_COUNT, expectedEventCount);
    if (expectedLoadCount != null) {
      MatcherAssert.assertThat(loadCount, Matchers.lessThanOrEqualTo(expectedLoadCount));
    }
    boolean hasLoads = loadCount > 0;

    long time = getMetricLatestValue(emitter, DefaultQueryMetrics.QUERY_ON_DEMAND_LOAD_BATCH_TIME, expectedEventCount);
    if (hasLoads) {
      Assertions.assertTrue(time > 0);
    } else {
      Assertions.assertEquals(0, time);
    }

    long maxTime = getMetricLatestValue(emitter, DefaultQueryMetrics.QUERY_ON_DEMAND_LOAD_TIME_MAX, expectedEventCount);
    if (hasLoads) {
      Assertions.assertTrue(maxTime > 0);
    } else {
      Assertions.assertEquals(0, maxTime);
    }

    long avgTime = getMetricLatestValue(emitter, DefaultQueryMetrics.QUERY_ON_DEMAND_LOAD_TIME_AVG, expectedEventCount);
    if (hasLoads) {
      Assertions.assertTrue(avgTime > 0);
    } else {
      Assertions.assertEquals(0, avgTime);
    }

    long maxWait = getMetricLatestValue(emitter, DefaultQueryMetrics.QUERY_ON_DEMAND_WAIT_TIME_MAX, expectedEventCount);
    if (hasLoads) {
      Assertions.assertTrue(maxWait >= 0);
    } else {
      Assertions.assertEquals(0, maxWait);
    }

    long avgWait = getMetricLatestValue(emitter, DefaultQueryMetrics.QUERY_ON_DEMAND_WAIT_TIME_AVG, expectedEventCount);
    if (hasLoads) {
      Assertions.assertTrue(avgWait >= 0);
    } else {
      Assertions.assertEquals(0, avgWait);
    }

    long bytes = getMetricLatestValue(emitter, DefaultQueryMetrics.QUERY_ON_DEMAND_LOAD_BYTES, expectedEventCount);
    if (hasLoads) {
      Assertions.assertTrue(bytes > 0);
    } else {
      Assertions.assertEquals(0, bytes);
    }
  }

  private long getMetricLatestValue(LatchableEmitter emitter, String metricName, int expectedCount)
  {
    Assertions.assertEquals(expectedCount, emitter.getMetricEventCount(metricName));
    return emitter.getLatestMetricEventValue(metricName, 0).longValue();
  }

  private String createTestDatasourceName()
  {
    return "wiki-" + IdUtils.getRandomId();
  }

  /**
   * Stolen from {@link EmbeddedDurableShuffleStorageTest#loadWikipediaTable()} but with hourly granularity and no
   * durable shuffle location
   */
  private void loadWikiData() throws IOException
  {
    final File tmpDir = cluster.getTestFolder().newFolder();
    final File wikiFile = new File(tmpDir, "wiki.gz");

    ByteStreams.copy(
        DruidProcessingConfigTest.class.getResourceAsStream("/wikipedia/wikiticker-2015-09-12-sampled.json.gz"),
        Files.newOutputStream(wikiFile.toPath())
    );
    final String sql = StringUtils.format(
        "SET waitUntilSegmentsLoad = TRUE;\n"
        + "REPLACE INTO \"%s\" OVERWRITE ALL\n"
        + "SELECT\n"
        + "  TIME_PARSE(\"time\") AS __time,\n"
        + "  channel,\n"
        + "  countryName,\n"
        + "  page,\n"
        + "  \"user\",\n"
        + "  added,\n"
        + "  deleted,\n"
        + "  delta\n"
        + "FROM TABLE(\n"
        + "    EXTERN(\n"
        + "      %s,\n"
        + "      '{\"type\":\"json\"}',\n"
        + "      '[{\"name\":\"isRobot\",\"type\":\"string\"},{\"name\":\"channel\",\"type\":\"string\"},{\"name\":\"time\",\"type\":\"string\"},{\"name\":\"flags\",\"type\":\"string\"},{\"name\":\"isUnpatrolled\",\"type\":\"string\"},{\"name\":\"page\",\"type\":\"string\"},{\"name\":\"diffUrl\",\"type\":\"string\"},{\"name\":\"added\",\"type\":\"long\"},{\"name\":\"comment\",\"type\":\"string\"},{\"name\":\"commentLength\",\"type\":\"long\"},{\"name\":\"isNew\",\"type\":\"string\"},{\"name\":\"isMinor\",\"type\":\"string\"},{\"name\":\"delta\",\"type\":\"long\"},{\"name\":\"isAnonymous\",\"type\":\"string\"},{\"name\":\"user\",\"type\":\"string\"},{\"name\":\"deltaBucket\",\"type\":\"long\"},{\"name\":\"deleted\",\"type\":\"long\"},{\"name\":\"namespace\",\"type\":\"string\"},{\"name\":\"cityName\",\"type\":\"string\"},{\"name\":\"countryName\",\"type\":\"string\"},{\"name\":\"regionIsoCode\",\"type\":\"string\"},{\"name\":\"metroCode\",\"type\":\"long\"},{\"name\":\"countryIsoCode\",\"type\":\"string\"},{\"name\":\"regionName\",\"type\":\"string\"}]'\n"
        + "    )\n"
        + "  )\n"
        + "PARTITIONED BY HOUR\n"
        + "CLUSTERED BY channel",
        dataSource,
        Calcites.escapeStringLiteral(
            broker.bindings()
                  .jsonMapper()
                  .writeValueAsString(new LocalInputSource(null, null, Collections.singletonList(wikiFile), null))
        )
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSqlAndGetReport(sql);
    Assertions.assertEquals(TaskState.SUCCESS, payload.getStatus().getStatus());
    Assertions.assertEquals(24, payload.getStatus().getSegmentLoadWaiterStatus().getTotalSegments());
    Assertions.assertNull(payload.getStatus().getErrorReport());
  }
}
