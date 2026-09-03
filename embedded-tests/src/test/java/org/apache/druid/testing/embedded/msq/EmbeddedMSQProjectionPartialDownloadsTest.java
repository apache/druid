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
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.http.SqlTaskStatus;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

/**
 * End-to-end test that ingests a projection segment and runs the exact GROUP BY the projection serves through an
 * MSQ task, so its GroupBy leaf processor acquires the projection bundle (dependent on {@code __base}) under
 * {@code AcquireMode.PARTIAL} and releases it at cursor close under the ephemeral cache.
 */
class EmbeddedMSQProjectionPartialDownloadsTest extends EmbeddedClusterTestBase
{
  private static final String PROJECTION_NAME = "country_delta";
  private static final long MONITOR_QUIESCE_TIMEOUT_MILLIS = 3_000L;
  private static final String SELECT_SQL = "SELECT \"countryName\", SUM(\"delta\") AS s FROM \"%s\" GROUP BY \"countryName\"";

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
    dataSource = "projection-partial-" + IdUtils.getRandomId();
    ingestSegmentWithProjection();
  }

  @Override
  protected void refreshDatasourceName()
  {
    // Keep the datasource stable: it is ingested once in loadData() before all tests.
  }

  @Test
  void testProjectionServedGroupByDoesNotCrashWorker()
  {
    final LatchableEmitter emitter = indexer.latchableEmitter();

    emitter.awaitMetricQuiescent(StorageMonitor.VSF_READ_COUNT, MONITOR_QUIESCE_TIMEOUT_MILLIS);
    emitter.flush();

    final SqlTaskStatus status = msqApis.submitTaskSql(
        QueryContext.ofMap(QueryContextParameters.USE_PROJECTION, PROJECTION_NAME),
        SELECT_SQL,
        dataSource
    );
    cluster.callApi().waitForTaskToSucceed(status.getTaskId(), overlord);

    // Confirm the partial-load path actually engaged: on-demand deep-storage range reads. The worker's StorageMonitor
    // only emits while the ephemeral task is alive, so the events land during the query; waitForEventAggregate also
    // considers events already emitted since the last flush and fails the test on timeout if none ever arrived.
    emitter.waitForEventAggregate(
        matcher -> matcher.hasMetricName(StorageMonitor.VSF_READ_COUNT),
        aggregate -> aggregate.hasSumAtLeast(1L)
    );
  }

  private void ingestSegmentWithProjection() throws IOException
  {
    final File tmpDir = cluster.getTestFolder().newFolder();
    final File inputFile = new File(tmpDir, "projection-input.json");
    final String inputData =
        "{\"time\":\"2024-01-01T00:10:00Z\",\"channel\":\"#en\",\"countryName\":\"US\",\"delta\":10}\n"
        + "{\"time\":\"2024-01-01T00:20:00Z\",\"channel\":\"#en\",\"countryName\":\"US\",\"delta\":5}\n"
        + "{\"time\":\"2024-01-01T00:30:00Z\",\"channel\":\"#en\",\"countryName\":\"CA\",\"delta\":3}\n"
        + "{\"time\":\"2024-01-01T00:40:00Z\",\"channel\":\"#fr\",\"countryName\":\"FR\",\"delta\":7}\n"
        + "{\"time\":\"2024-01-01T00:50:00Z\",\"channel\":\"#fr\",\"countryName\":\"US\",\"delta\":2}\n";
    Files.write(inputFile.toPath(), inputData.getBytes(StandardCharsets.UTF_8));

    final AggregateProjectionSpec projection =
        AggregateProjectionSpec.builder(PROJECTION_NAME)
                               .groupingColumns(new StringDimensionSchema("countryName"))
                               .aggregators(new LongSumAggregatorFactory("sumDelta", "delta"))
                               .build();

    final UniformGranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.HOUR,
        Granularities.NONE,
        false,
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
                .withGranularity(granularitySpec)
                .withDimensions(
                    new StringDimensionSchema("channel"),
                    new StringDimensionSchema("countryName"),
                    new LongDimensionSchema("delta")
                )
                .withProjections(List.of(projection))
        )
        .tuningConfig(t -> t.withMaxNumConcurrentSubTasks(1))
        .withId(taskId);

    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
  }
}
