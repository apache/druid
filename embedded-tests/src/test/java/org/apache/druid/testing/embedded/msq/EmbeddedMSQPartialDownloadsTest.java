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

package org.apache.druid.testing.embedded.msq;

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.server.metrics.LatchableEmitter;
import org.apache.druid.server.metrics.StorageMonitor;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * End-to-end wiring test for on-demand partial segment downloads on MSQ worker tasks. Partial downloads default to
 * enabled for tasks (the {@code druid.indexer.task.virtualStoragePartialDownloadsEnabled} {@code TaskConfig} default is
 * true), so run 1 sets no property or context and verifies the default engages the partial path. Run 2 verifies the
 * per-query {@link MultiStageQueryContext#CTX_VIRTUAL_STORAGE_PARTIAL_DOWNLOADS} override can turn it back off.
 * <p>
 * The observable signal is the indexer's {@link StorageMonitor#VSF_READ_COUNT} metric: the partial path performs
 * on-demand deep-storage range reads (so the count is positive), while the full-download path does not (the count
 * stays zero because {@code virtualStoragePartialDownloadsEnabled=false} makes the worker mount segments as complete
 * cache entries and download them in full instead of via range reads).
 */
class EmbeddedMSQPartialDownloadsTest extends EmbeddedClusterTestBase
{
  // A few times the storage monitor's PT1s emission period, so a single missed tick doesn't falsely read as "idle".
  private static final long MONITOR_QUIESCE_TIMEOUT_MILLIS = 3_000L;

  private static final String SELECT_SQL = "SELECT channel, COUNT(*) AS c FROM \"%s\" GROUP BY channel";

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedRouter router = new EmbeddedRouter();

  private EmbeddedMSQApis msqApis;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    indexer.setServerMemory(400_000_000)
           .addProperty("druid.worker.capacity", "4")
           .addProperty("druid.processing.numThreads", "3");

    broker.setServerMemory(200_000_000);

    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "always");
    overlord.addProperty("druid.manager.segments.useIncrementalCache", "always")
            .addProperty("druid.manager.segments.pollDuration", "PT0.1s");

    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .useDefaultTimeoutForLatchableEmitter(20)
        // Partial downloads require unzipped, V10 segments (range-readable via LocalLoadSpec's directory reader).
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
    dataSource = "wiki-" + IdUtils.getRandomId();
    loadWikiData();
  }

  @Override
  protected void refreshDatasourceName()
  {
    // Keep the datasource stable: it is ingested once in loadData() before all tests.
  }

  @Test
  void testPartialDownloadsWiredViaRuntimePropertyAndQueryContext()
  {
    final LatchableEmitter emitter = indexer.latchableEmitter();

    // Run 1: no context override, so the query uses the indexer's TaskConfig default (partial enabled). The worker
    // reads the input segment's columns on demand, which shows up as deep-storage range reads.
    emitter.awaitMetricQuiescent(StorageMonitor.VSF_READ_COUNT, MONITOR_QUIESCE_TIMEOUT_MILLIS);
    emitter.flush();
    runMsqSelect(Collections.emptyMap());
    // The worker's StorageMonitor only emits while the (ephemeral) task is alive, so the VSF_READ_COUNT events land
    // during runMsqSelect. waitForEventAggregate (unlike waitForNextEvent) also considers events already emitted since
    // the last flush, so it sees them; it fails the test (timeout) if no on-demand range reads ever happened.
    emitter.waitForEventAggregate(
        matcher -> matcher.hasMetricName(StorageMonitor.VSF_READ_COUNT),
        aggregate -> aggregate.hasSumAtLeast(1L)
    );

    // Run 2: the query context disables partial downloads, so the worker downloads segments in full and performs no
    // range reads.
    emitter.awaitMetricQuiescent(StorageMonitor.VSF_READ_COUNT, MONITOR_QUIESCE_TIMEOUT_MILLIS);
    emitter.flush();
    runMsqSelect(Map.of(MultiStageQueryContext.CTX_VIRTUAL_STORAGE_PARTIAL_DOWNLOADS, false));
    emitter.awaitMetricQuiescent(StorageMonitor.VSF_READ_COUNT, MONITOR_QUIESCE_TIMEOUT_MILLIS);
    Assertions.assertEquals(
        0L,
        emitter.getMetricEventLongSum(StorageMonitor.VSF_READ_COUNT),
        "with partial downloads disabled via query context, the worker should download in full (no range reads)"
    );
  }

  private void runMsqSelect(Map<String, Object> queryContext)
  {
    final SqlTaskStatus status = msqApis.submitTaskSql(queryContext, SELECT_SQL, dataSource);
    cluster.callApi().waitForTaskToSucceed(status.getTaskId(), overlord);
  }

  private void loadWikiData() throws IOException
  {
    final File tmpDir = cluster.getTestFolder().newFolder();
    final File wikiFile = new File(tmpDir, "wiki.gz");
    try (var in = DruidProcessingConfigTest.class.getResourceAsStream("/wikipedia/wikiticker-2015-09-12-sampled.json.gz")) {
      Files.copy(
          Objects.requireNonNull(in, "wikiticker sample resource not found on the test classpath"),
          wikiFile.toPath()
      );
    }

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
        + "      '[{\"name\":\"channel\",\"type\":\"string\"},{\"name\":\"time\",\"type\":\"string\"},{\"name\":\"page\",\"type\":\"string\"},{\"name\":\"added\",\"type\":\"long\"},{\"name\":\"user\",\"type\":\"string\"},{\"name\":\"delta\",\"type\":\"long\"},{\"name\":\"deleted\",\"type\":\"long\"},{\"name\":\"countryName\",\"type\":\"string\"}]'\n"
        + "    )\n"
        + "  )\n"
        + "PARTITIONED BY DAY\n"
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
    Assertions.assertNull(payload.getStatus().getErrorReport());
  }
}
