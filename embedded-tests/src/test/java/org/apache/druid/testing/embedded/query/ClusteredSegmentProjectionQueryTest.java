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
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.SegmentGranularitySpec;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.server.metrics.LatchableEmitter;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for native (index_parallel) ingestion of a CLUSTERED base-table segment that also carries an
 * aggregate projection, and for querying it both through the projection and directly against the clustered base table.
 * <p>
 * This exercises:
 * <ul>
 *   <li>writing a clustered base-table V10 segment via {@code index_parallel}</li>
 *   <li>an aggregate projection built on top of the clustered base table</li>
 *   <li>the projection being chosen to satisfy a matching aggregation (asserted via the {@code projection} dimension
 *       on the historical's {@code query/segment/time} metric)</li>
 *   <li>forcing direct base-table access with {@code noProjections=true} and proving identical results</li>
 *   <li>queries that the projection cannot satisfy (group-by / filter on the clustering column), which exercise the
 *       per-cluster-group cursor path on the clustered base table</li>
 * </ul>
 */
class ClusteredSegmentProjectionQueryTest extends EmbeddedClusterTestBase
{
  private static final String PROJECTION_NAME = "country_delta";
  private static final String SEGMENT_TIME_METRIC = "query/segment/time";

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedRouter router = new EmbeddedRouter();

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    historical.setServerMemory(300_000_000);

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
        // Clustered base-table segments can only be written in the V10 segment format.
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
    dataSource = "clustered-" + IdUtils.getRandomId();
    ingestClusteredSegmentWithProjection();
  }

  @Override
  protected void refreshDatasourceName()
  {
    // don't change the datasource name for each run because we set things up before all tests
  }

  @Test
  void testProjectionServesAggregationQuery()
  {
    // The aggregation matches the country_delta projection (group by countryName, sum delta).
    final String sql = "SELECT \"countryName\", SUM(\"delta\") FROM \"%s\" GROUP BY 1 ORDER BY 1";

    final LatchableEmitter emitter = historical.latchableEmitter();
    emitter.flush();

    cluster.callApi().verifySqlQuery(sql, dataSource, "CA,3\nFR,7\nUS,17");

    // When the projection is used, the segment-scan query metrics carry the projection name as a dimension.
    emitter.waitForEvent(
        event -> event.hasMetricName(SEGMENT_TIME_METRIC)
                      .hasDimension("projection", PROJECTION_NAME)
    );
  }

  /**
   * Forces the clustered base table to serve the {@code GROUP BY countryName, SUM(delta)} aggregation directly
   * (bypassing the projection via {@code noProjections=true}) and asserts the results match the projection-served
   * results exactly.
   * <p>
   * Regression test for a clustered read-path bug: {@code countryName} is a non-clustering column whose
   * per-cluster-group dictionaries assign different values the same local ID (group {@code #en}: {@code {US, CA}},
   * group {@code #fr}: {@code {FR, US}}). A dictionary-id-keyed group-by over the concatenated per-group cursors (see
   * {@link org.apache.druid.segment.ConcatenatingCursor}) would conflate those, returning {@code FR,10 / US,17}. The
   * fix makes the concatenating cursor report non-clustering columns as non-dictionary-encoded (see
   * {@link org.apache.druid.segment.projections.ClusteringColumnSelectorFactory#getColumnCapabilities}), forcing
   * value-based grouping, so the correct {@code CA,3 / FR,7 / US,17} is returned.
   */
  @Test
  void testNoProjectionsServesFromClusteredBaseTable()
  {
    final String sql = "SELECT \"countryName\", SUM(\"delta\") FROM \"" + dataSource + "\" GROUP BY 1 ORDER BY 1";

    final LatchableEmitter emitter = historical.latchableEmitter();
    emitter.flush();

    // Force the clustered base table to serve the query directly, bypassing the projection.
    final String result = cluster.callApi().onAnyBroker(
        b -> b.submitSqlQuery(
            new ClientSqlQuery(
                sql,
                "CSV",
                false,
                false,
                false,
                Map.of(QueryContexts.NO_PROJECTIONS, true),
                null
            )
        )
    ).trim();

    // Identical results, proving the clustered base table serves them.
    Assertions.assertEquals("CA,3\nFR,7\nUS,17", result);

    // Confirm a segment scan actually happened, and that no segment scan reported the projection dimension for this run.
    emitter.waitForEvent(event -> event.hasMetricName(SEGMENT_TIME_METRIC));
    for (ServiceMetricEvent event : emitter.getMetricEvents(SEGMENT_TIME_METRIC)) {
      Assertions.assertNull(
          event.getUserDims().get("projection"),
          "expected no projection dimension when noProjections=true, but found: " + event.getUserDims()
      );
    }
  }

  @Test
  void testGroupByClusteringColumnUsesBaseTable()
  {
    // Grouping on the clustering column cannot be satisfied by the country_delta projection, so it dispatches to the
    // clustered base table and exercises the per-cluster-group cursor path.
    final String sql = "SELECT \"channel\", SUM(\"delta\") FROM \"%s\" GROUP BY 1 ORDER BY 1";

    final LatchableEmitter emitter = historical.latchableEmitter();
    emitter.flush();

    cluster.callApi().verifySqlQuery(sql, dataSource, "#en,18\n#fr,9");

    // The projection cannot serve this query, so no segment scan should report the projection dimension.
    emitter.waitForEvent(event -> event.hasMetricName(SEGMENT_TIME_METRIC));
    for (ServiceMetricEvent event : emitter.getMetricEvents(SEGMENT_TIME_METRIC)) {
      Assertions.assertNull(
          event.getUserDims().get("projection"),
          "expected no projection dimension for a clustering-column group-by, but found: " + event.getUserDims()
      );
    }
  }

  @Test
  void testFilterOnClusteringColumnUsesBaseTable()
  {
    // Filtering on the clustering column exercises a single cluster group of the clustered base table.
    cluster.callApi().verifySqlQuery(
        "SELECT SUM(\"delta\") FROM \"%s\" WHERE \"channel\" = '#en'",
        dataSource,
        "18"
    );
    cluster.callApi().verifySqlQuery(
        "SELECT \"countryName\", SUM(\"delta\") FROM \"%s\" WHERE \"channel\" = '#en' GROUP BY 1 ORDER BY 1",
        dataSource,
        "CA,3\nUS,15"
    );
  }

  /**
   * Ingests a single clustered base-table segment (clustered by {@code channel}) that also defines a
   * {@code country_delta} aggregate projection (group by {@code countryName}, sum {@code delta}) using a native
   * {@code index_parallel} task.
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
                new StringDimensionSchema("channel"),       // clustering prefix
                new StringDimensionSchema("countryName"),
                new LongDimensionSchema("delta"),
                new LongDimensionSchema("__time")           // __time present, non-clustering
            )
            .clusteringColumns("channel")
            .build();

    final AggregateProjectionSpec projection =
        AggregateProjectionSpec.builder(PROJECTION_NAME)
            .groupingColumns(new StringDimensionSchema("countryName"))
            .aggregators(new LongSumAggregatorFactory("sumDelta", "delta"))
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
                .withProjections(List.of(projection))
        )
        .tuningConfig(t -> t.withMaxNumConcurrentSubTasks(1))
        .withId(taskId);

    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
  }
}
