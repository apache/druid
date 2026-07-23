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

import org.apache.druid.catalog.guice.CatalogClientModule;
import org.apache.druid.catalog.guice.CatalogCoordinatorModule;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.SegmentGranularitySpec;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.server.coordinator.rules.CannotMatchBehavior;
import org.apache.druid.server.coordinator.rules.ForeverPartialLoadRule;
import org.apache.druid.server.coordinator.rules.WildcardProjectionPartialLoadMatcher;
import org.apache.druid.server.metrics.LatchableEmitter;
import org.apache.druid.server.metrics.StorageMonitor;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.catalog.TestCatalogClient;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * End-to-end coverage for the partial-load-rule wiring: a segment with a clustered base + aggregate projection
 * loaded under a {@link ForeverPartialLoadRule} whose matcher selects only the projection. The coordinator emits
 * a {@code PartialProjectionLoadSpec} wrapper on the load request; the historical resolves the wrapper into the
 * projection's bundle name, eagerly downloads that one bundle, and pins it via a rule-hold. Queries that the
 * projection can serve are proven to run entirely off the pre-loaded bundle (zero on-demand loads); queries that
 * miss the projection are proven to trigger on-demand loads of the base bundles.
 * <p>
 * Exercises the full happy-path flow end-to-end: coordinator matcher &rarr; wire form &rarr; historical
 * {@code loadPartial} &rarr; {@code applyRule} + eager download &rarr; announced fingerprint &rarr; query with
 * projection hit vs miss &rarr; on-demand metric emission.
 */
class PartialProjectionLoadRuleQueryTest extends EmbeddedClusterTestBase
{
  private static final String PROJECTION_NAME = "country_delta";
  // A second projection ingested alongside country_delta but NOT selected by the rule. Under the partial-load rule
  // its container bytes stay off the historical's disk, so the historical's realized footprint is measurably less
  // than the full segment size — that inequality is what {@link #testCoordinatorSeesPartialLoadFootprint} asserts
  // against sys.servers. Uses a min-aggregator on delta so neither test query's SUM(delta) projection-select can
  // route through it (planner needs matching aggregator).
  private static final String UNMATCHED_PROJECTION_NAME = "country_min_delta";
  private static final long CACHE_SIZE = HumanReadableBytes.parse("1MiB");
  private static final long MAX_SIZE = HumanReadableBytes.parse("100MiB");
  private static final long ESTIMATE_SIZE = HumanReadableBytes.parse("2KiB");
  // Quiescence wait for the storage monitor: a few times its PT1s emission period so a single missed tick
  // doesn't falsely read as "idle" while still bounding how long we wait once activity has actually stopped.
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
              .addProperty(
                  "druid.segmentCache.virtualStorageLoadThreads",
                  String.valueOf(Runtime.getRuntime().availableProcessors())
              )
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
          .addProperty("druid.sql.planner.enableSysQueriesTable", "true")
          // Poll the catalog often so the broker's DruidSchema picks up the table definition quickly.
          // Under virtualStorage the historical exposes only __time via VirtualPlaceholderSegment, so
          // BrokerSegmentMetadataCache alone can't infer the real column schema — the catalog fills that gap.
          .addProperty("druid.catalog.client.pollingPeriod", "100")
          // Retain Dart reports long enough to fetch them after the query completes.
          .addProperty("druid.msq.dart.controller.maxRetainedReportCount", "10");

    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "always");

    overlord.addProperty("druid.manager.segments.useIncrementalCache", "always")
            .addProperty("druid.manager.segments.pollDuration", "PT0.1s")
            .addProperty("druid.catalog.client.maxSyncRetries", "0");

    indexer.setServerMemory(300_000_000)
           .addProperty("druid.worker.capacity", "2")
           .addProperty("druid.processing.numThreads", "2")
           .addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .useDefaultTimeoutForLatchableEmitter(60)
        .addResource(storageResource)
        // Catalog: table schema must be published explicitly since historical exposes only __time under
        // virtualStorage=true (see broker.druid.catalog.client.pollingPeriod above).
        .addExtensions(CatalogClientModule.class, CatalogCoordinatorModule.class)
        // Run queries via Dart so MSQ leaf processors take the PARTIAL acquire path (async cursor factory);
        // native SQL takes the FULL acquire path which pins every bundle for the reference lifetime, defeating
        // the memory savings of partial loads.
        .addCommonProperty("druid.msq.dart.enabled", "true")
        // Clustered base-table + projection segments require the V10 segment format.
        .addCommonProperty("druid.indexer.task.buildV10", "true")
        // Range reads over MinIO — required for partial loading to be functional.
        .addCommonProperty("druid.storage.zip", "false")
        .addCommonProperty("druid.monitoring.emissionPeriod", "PT1s")
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(indexer)
        .addServer(historical)
        .addServer(broker)
        .addServer(router);
  }

  @BeforeAll
  void loadDataAndConfigureRule() throws IOException
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);
    dataSource = "partial-projection-" + IdUtils.getRandomId();

    // Under virtualStorage=true the historical announces segments via VirtualPlaceholderSegment, which only exposes
    // __time. That means BrokerSegmentMetadataCache alone can't populate a full DruidSchema for SQL validation, so
    // the queries below would fail at plan-time with "column not found". Publishing an explicit catalog table
    // definition gives DruidSchema the real column list. Future work: derive a RowSignature from the partial-load
    // header so this catalog registration becomes unnecessary.
    final TestCatalogClient catalog = new TestCatalogClient(cluster);
    final TableMetadata table = TableBuilder.datasource(dataSource, "HOUR")
                                            .column(Columns.TIME_COLUMN, Columns.LONG)
                                            .column("channel", Columns.SQL_VARCHAR)
                                            .column("countryName", Columns.SQL_VARCHAR)
                                            .column("delta", Columns.SQL_BIGINT)
                                            .build();
    catalog.createTable(table, true);

    // Configure the partial-load rule BEFORE ingestion so the initial load applies the rule directly, rather than
    // needing a rule change afterward. The matcher selects only the country_delta projection; base-table bundles
    // (cluster groups) are NOT rule-loaded.
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateRulesForDatasource(
            dataSource,
            List.of(
                new ForeverPartialLoadRule(
                    Map.of("_default_tier", 1),
                    null,
                    new WildcardProjectionPartialLoadMatcher(List.of(PROJECTION_NAME), null),
                    CannotMatchBehavior.FALL_THROUGH
                )
            )
        )
    );

    ingestClusteredSegmentWithProjection();
  }

  @Override
  protected void refreshDatasourceName()
  {
    // Fixed datasource across tests — rule + ingest are one-time setup.
  }

  @Test
  void testProjectionHitLoadsNothingOnDemand()
  {
    // The country_delta projection can serve group-by-countryName / sum-delta. The projection bundle was
    // rule-loaded on ingest, so an MSQ leaf PARTIAL acquire should read it without triggering any downloads.
    // MSQ input channel counters don't (yet) observe partial-load on-demand downloads (those happen inside the
    // async cursor factory, downstream of the input channel), so we observe the historical's StorageMonitor
    // directly — VSF_LOAD_* are incremented as each file is actually pulled from deep storage.
    final LatchableEmitter emitter = quiesceAndFlushStorageMonitor();

    runDartQuery(
        "SELECT \"countryName\", SUM(\"delta\") FROM \"" + dataSource + "\" GROUP BY 1 ORDER BY 1",
        "CA,3\nFR,7\nUS,17"
    );

    // Wait for post-query storage activity (if any) to settle before we sum the counters.
    emitter.awaitMetricQuiescent(StorageMonitor.VSF_LOAD_BEGIN_COUNT, MONITOR_QUIESCE_TIMEOUT_MILLIS);

    Assertions.assertEquals(
        0L,
        emitter.getMetricEventLongSum(StorageMonitor.VSF_LOAD_COUNT),
        "expected no on-demand file loads for a projection-served Dart query, but the projection bundle was fetched at query time"
    );
    Assertions.assertEquals(
        0L,
        emitter.getMetricEventLongSum(StorageMonitor.VSF_LOAD_BYTES),
        "expected zero on-demand load bytes for a projection-served Dart query"
    );
  }

  @Test
  void testCoordinatorSeesPartialLoadFootprint()
  {
    // End-to-end check that PartialLoadedDataSegment reaches the coordinator's inventory accounting: the historical's
    // announcement stamps realizedBytes (metadata + selected projection + __base dep) as loadedBytes, so
    // sys.servers.curr_size for the historical reflects the partial footprint — strictly less than the segment's full
    // size (which includes the unmatched projection's bytes). Without the fix, forAnnouncement would stamp
    // segment.getSize() and curr_size would equal the full size.
    final long fullSize = Long.parseLong(
        cluster.callApi().runSql(
            "SELECT \"size\" FROM sys.segments WHERE datasource = '" + dataSource + "'"
        ).trim()
    );
    Assertions.assertTrue(fullSize > 0, "sys.segments.size must be populated for the ingested segment");

    final long currSize = Long.parseLong(
        cluster.callApi().runSql(
            "SELECT curr_size FROM sys.servers WHERE server_type = 'historical'"
        ).trim()
    );

    Assertions.assertTrue(
        currSize > 0,
        "coordinator's historical curr_size must reflect the partial-load footprint, got 0"
    );
    Assertions.assertTrue(
        currSize < fullSize,
        StringUtils.format(
            "coordinator should see the partial footprint; got curr_size=%d, full segment size=%d "
            + "(without PartialLoadedDataSegment, forAnnouncement would stamp segment.getSize() as loadedBytes and "
            + "these would be equal)",
            currSize,
            fullSize
        )
    );
  }

  @Test
  void testProjectionMissLoadsBaseBundleOnDemand()
  {
    // Group by the clustering column — the projection can't serve this, so MSQ must fall back to the clustered
    // base table, whose cluster-group bundles were NOT rule-loaded. Under PARTIAL acquire the async cursor
    // factory triggers per-file downloads on demand, which show up on StorageMonitor's VSF_LOAD_* counters.
    final LatchableEmitter emitter = quiesceAndFlushStorageMonitor();

    runDartQuery(
        "SELECT \"channel\", SUM(\"delta\") FROM \"" + dataSource + "\" GROUP BY 1 ORDER BY 1",
        "#en,18\n#fr,9"
    );

    emitter.awaitMetricQuiescent(StorageMonitor.VSF_LOAD_BEGIN_COUNT, MONITOR_QUIESCE_TIMEOUT_MILLIS);

    MatcherAssert.assertThat(
        "expected on-demand file loads for a projection-miss Dart query that needs the base-table bundles",
        emitter.getMetricEventLongSum(StorageMonitor.VSF_LOAD_COUNT),
        Matchers.greaterThan(0L)
    );
    MatcherAssert.assertThat(
        "expected non-zero on-demand load bytes for a projection-miss Dart query",
        emitter.getMetricEventLongSum(StorageMonitor.VSF_LOAD_BYTES),
        Matchers.greaterThan(0L)
    );
  }

  /**
   * Waits for any lingering storage-monitor activity from ingest / rule-application to settle, then flushes the
   * emitter's event queue so subsequent {@code getMetricEventLongSum(...)} calls only reflect activity that
   * happens after this point.
   */
  private LatchableEmitter quiesceAndFlushStorageMonitor()
  {
    final LatchableEmitter emitter = historical.latchableEmitter();
    emitter.awaitMetricQuiescent(StorageMonitor.VSF_LOAD_BEGIN_COUNT, MONITOR_QUIESCE_TIMEOUT_MILLIS);
    emitter.flush();
    return emitter;
  }

  /**
   * Submits {@code sql} via the Dart engine (MSQ leaves take the PARTIAL acquire path, so partial-load bundles
   * only download the files the query actually reads) and asserts the CSV result matches {@code expectedCsv}.
   */
  private void runDartQuery(String sql, String expectedCsv)
  {
    final String sqlQueryId = UUID.randomUUID().toString();
    final String result;
    try {
      result = msqApis.submitDartSqlAsync(
          sql,
          Map.of(QueryContexts.CTX_SQL_QUERY_ID, sqlQueryId),
          broker
      ).get();
    }
    catch (Exception e) {
      throw new RuntimeException("failed to run Dart query [" + sqlQueryId + "]: " + sql, e);
    }
    Assertions.assertEquals(expectedCsv, result.trim(), "unexpected result for Dart query [" + sqlQueryId + "]");
  }

  /**
   * Ingests a single clustered base-table segment (clustered by {@code channel}) with a {@code country_delta}
   * aggregate projection (group by {@code countryName}, sum {@code delta}). Copied from
   * {@link ClusteredSegmentProjectionQueryTest#ingestClusteredSegmentWithProjection}; kept local so this test
   * doesn't cross-depend on that class.
   */
  private void ingestClusteredSegmentWithProjection() throws IOException
  {
    final File tmpDir = cluster.getTestFolder().newFolder();
    final File inputFile = new File(tmpDir, "clustered-input.json");
    final String inputData =
        "{\"time\":\"2024-01-01T00:10:00Z\",\"channel\":\"#en\",\"countryName\":\"US\",\"delta\":10}\n"
        + "{\"time\":\"2024-01-01T00:20:00Z\",\"channel\":\"#en\",\"countryName\":\"US\",\"delta\":5}\n"
        + "{\"time\":\"2024-01-01T00:30:00Z\",\"channel\":\"#en\",\"countryName\":\"CA\",\"delta\":3}\n"
        + "{\"time\":\"2024-01-01T00:40:00Z\",\"channel\":\"#fr\",\"countryName\":\"FR\",\"delta\":7}\n"
        + "{\"time\":\"2024-01-01T00:50:00Z\",\"channel\":\"#fr\",\"countryName\":\"US\",\"delta\":2}\n";
    Files.write(inputFile.toPath(), inputData.getBytes(StandardCharsets.UTF_8));

    final ClusteredValueGroupsBaseTableProjectionSpec clusterSpec =
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .columns(
                new StringDimensionSchema("channel"),
                new StringDimensionSchema("countryName"),
                new LongDimensionSchema("delta"),
                new LongDimensionSchema("__time")
            )
            .clusteringColumns("channel")
            .build();

    final AggregateProjectionSpec projection =
        AggregateProjectionSpec.builder(PROJECTION_NAME)
            .groupingColumns(new StringDimensionSchema("countryName"))
            .aggregators(new LongSumAggregatorFactory("sumDelta", "delta"))
            .build();

    // Second projection ingested but NOT selected by the rule. Its containers stay off disk under the partial-load
    // rule, giving the historical a realized footprint measurably smaller than the full segment size. The
    // min-aggregator prevents the planner from routing SUM(delta) queries through this projection.
    final AggregateProjectionSpec unmatchedProjection =
        AggregateProjectionSpec.builder(UNMATCHED_PROJECTION_NAME)
            .groupingColumns(new StringDimensionSchema("countryName"))
            .aggregators(new LongMinAggregatorFactory("minDelta", "delta"))
            .build();

    final SegmentGranularitySpec segmentGranularitySpec = new SegmentGranularitySpec(
        Granularities.HOUR,
        List.of(Intervals.of("2024-01-01/2024-01-02"))
    );

    final String taskId = IdUtils.getRandomId();
    final ParallelIndexSupervisorTask task = TaskBuilder
        .ofTypeIndexParallel()
        .jsonInputFormat()
        .localInputSourceWithFiles(inputFile)
        .dataSchema(
            builder -> builder
                .withDataSource(dataSource)
                .withTimestamp(new TimestampSpec("time", "iso", null))
                .withSegmentGranularity(segmentGranularitySpec)
                .withBaseTable(clusterSpec)
                .withProjections(List.of(projection, unmatchedProjection))
        )
        .tuningConfig(t -> t.withMaxNumConcurrentSubTasks(1))
        .withId(taskId);

    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
  }
}
