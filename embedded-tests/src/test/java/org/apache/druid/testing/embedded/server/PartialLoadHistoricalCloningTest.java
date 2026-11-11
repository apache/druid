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

package org.apache.druid.testing.embedded.server;

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
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.rules.CannotMatchBehavior;
import org.apache.druid.server.coordinator.rules.ForeverPartialLoadRule;
import org.apache.druid.server.coordinator.rules.WildcardProjectionPartialLoadMatcher;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.utils.ITRetryUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for cloning a historical that loads segments partially. The source historical loads only the
 * bundles a {@link ForeverPartialLoadRule} selects, so it reports a footprint smaller than the full segment. Its
 * clone is expected to hold and report the same footprint: cloning copies the source's load state, and under a
 * partial-load rule that state includes which parts of the segment were loaded.
 * <p>
 * Both historicals are configured identically for partial downloads, so any difference in reported {@code curr_size}
 * comes from the load request itself rather than from node configuration.
 */
public class PartialLoadHistoricalCloningTest extends EmbeddedClusterTestBase
{
  private static final String PROJECTION_NAME = "country_delta";
  // Ingested alongside country_delta but not selected by the rule, so its container bytes stay off the historical's
  // disk. That is what makes the rule-loaded footprint measurably smaller than the full segment size.
  private static final String UNMATCHED_PROJECTION_NAME = "country_min_delta";

  private static final long CACHE_SIZE = HumanReadableBytes.parse("1MiB");
  private static final long MAX_SIZE = HumanReadableBytes.parse("100MiB");
  private static final long ESTIMATE_SIZE = HumanReadableBytes.parse("2KiB");

  private static final String CLONE_PORT = "7083";

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedRouter router = new EmbeddedRouter();

  private final EmbeddedHistorical sourceHistorical = new EmbeddedHistorical();
  private final EmbeddedHistorical cloneHistorical =
      new EmbeddedHistorical().addProperty("druid.plaintextPort", CLONE_PORT);

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    configureForPartialDownloads(sourceHistorical);
    configureForPartialDownloads(cloneHistorical);

    broker.setServerMemory(200_000_000)
          .addProperty("druid.sql.planner.enableSysQueriesTable", "true");

    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "always");

    overlord.addProperty("druid.manager.segments.useIncrementalCache", "always")
            .addProperty("druid.manager.segments.pollDuration", "PT0.1s");

    indexer.setServerMemory(300_000_000)
           .addProperty("druid.worker.capacity", "2")
           .addProperty("druid.processing.numThreads", "2")
           .addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .useDefaultTimeoutForLatchableEmitter(60)
        .addCommonProperty("druid.indexer.task.buildV10", "true")
        .addCommonProperty("druid.storage.type", "local")
        .addCommonProperty("druid.storage.zip", "false")
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(indexer)
        .addServer(sourceHistorical)
        .addServer(cloneHistorical)
        .addServer(broker)
        .addServer(router);
  }

  private void configureForPartialDownloads(EmbeddedHistorical historical)
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
  }

  @BeforeAll
  void loadDataAndConfigureCloning() throws IOException
  {
    dataSource = "partial-clone-" + IdUtils.getRandomId();

    // The rule and the clone mapping are both configured before ingestion so the first coordinator run already sees
    // the clone target as unmanaged: rule-driven assignment can only pick the source, and everything the clone gets
    // comes from the cloning duty.
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
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateCoordinatorDynamicConfig(
            CoordinatorDynamicConfig
                .builder()
                .withCloneServers(Map.of(cloneHost(), sourceHost()))
                .build()
        )
    );

    ingestClusteredSegmentWithProjection();
  }

  @Override
  protected void refreshDatasourceName()
  {
    // Fixed datasource across tests — rule, clone mapping and ingest are one-time setup.
  }

  @Test
  void testCloneReportsTheSamePartialFootprintAsItsSource()
  {
    coordinator.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/clone/assigned/count")
                      .hasDimension("server", cloneHost()),
        agg -> agg.hasSumAtLeast(1)
    );
    coordinator.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/loadQueue/success")
                      .hasDimension("server", cloneHost())
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );

    // The load announcement reaches the broker's inventory asynchronously; wait until both historicals have reported
    // a footprint before comparing them.
    ITRetryUtil.retryUntilTrue(
        () -> currSizeOf(sourceHost()) > 0 && currSizeOf(cloneHost()) > 0,
        "both historicals to report a non-zero curr_size"
    );

    final long fullSize = Long.parseLong(
        cluster.callApi().runSql(
            "SELECT \"size\" FROM sys.segments WHERE datasource = '" + dataSource + "'"
        ).trim()
    );
    final long sourceSize = currSizeOf(sourceHost());
    final long cloneSize = currSizeOf(cloneHost());

    Assertions.assertTrue(
        sourceSize < fullSize,
        StringUtils.format(
            "source should hold only the rule-selected parts; got curr_size=%d, full segment size=%d",
            sourceSize,
            fullSize
        )
    );
    Assertions.assertEquals(
        sourceSize,
        cloneSize,
        StringUtils.format(
            "clone should hold the same parts as its source; source curr_size=%d, clone curr_size=%d, "
            + "full segment size=%d (a clone loaded without the source's partial-load profile downloads the whole "
            + "segment and reports its full size)",
            sourceSize,
            cloneSize,
            fullSize
        )
    );
  }

  private long currSizeOf(String host)
  {
    final String result = cluster.callApi().runSql(
        "SELECT curr_size FROM sys.servers WHERE server_type = 'historical' AND server = '" + host + "'"
    ).trim();
    return result.isEmpty() ? 0L : Long.parseLong(result);
  }

  private String sourceHost()
  {
    return sourceHistorical.bindings().selfNode().getHostAndPort();
  }

  private String cloneHost()
  {
    return cloneHistorical.bindings().selfNode().getHostAndPort();
  }

  /**
   * Ingests a single clustered base-table segment (clustered by {@code channel}) with a {@code country_delta}
   * aggregate projection (group by {@code countryName}, sum {@code delta}) plus a second projection the rule does
   * not select.
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
