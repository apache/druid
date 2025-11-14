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

package org.apache.druid.testing.embedded.compact;

import org.apache.druid.catalog.guice.CatalogClientModule;
import org.apache.druid.catalog.guice.CatalogCoordinatorModule;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.msq.guice.IndexerMemoryManagementModule;
import org.apache.druid.msq.guice.MSQDurableStorageModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.guice.MSQSqlModule;
import org.apache.druid.msq.guice.SqlTaskModule;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.rpc.UpdateResponse;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

/**
 * Embedded test that runs compaction supervisors of various types.
 */
public class CompactionSupervisorTest extends EmbeddedClusterTestBase
{
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(2_000_000_000L)
      .addProperty("druid.worker.capacity", "20");
  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty("druid.manager.segments.pollDuration", "PT1s")
      .addProperty("druid.manager.segments.useIncrementalCache", "always");
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.manager.segments.useIncrementalCache", "always");

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .useDefaultTimeoutForLatchableEmitter(600)
                               .addCommonProperty("druid.auth.authorizers", "[\"allowAll\"]")
                               .addCommonProperty("druid.auth.authorizer.allowAll.type", "allowAll")
                               .addCommonProperty("druid.auth.authorizer.allowAll.policy.type", "noRestriction")
                               .addCommonProperty(
                                   "druid.policy.enforcer.allowedPolicies",
                                   "[\"org.apache.druid.query.policy.NoRestrictionPolicy\"]"
                               )
                               .addCommonProperty("druid.policy.enforcer.type", "restrictAllTables")
                               .addExtensions(
                                   CatalogClientModule.class,
                                   CatalogCoordinatorModule.class,
                                   IndexerMemoryManagementModule.class,
                                   MSQDurableStorageModule.class,
                                   MSQIndexingModule.class,
                                   MSQSqlModule.class,
                                   SqlTaskModule.class
                               )
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(indexer)
                               .addServer(historical)
                               .addServer(broker)
                               .addServer(new EmbeddedRouter());
  }


  private void configureCompaction(CompactionEngine compactionEngine)
  {
    final UpdateResponse updateResponse = cluster.callApi().onLeaderOverlord(
        o -> o.updateClusterCompactionConfig(new ClusterCompactionConfig(1.0, 100, null, true, compactionEngine))
    );
    Assertions.assertTrue(updateResponse.isSuccess());
  }

  @MethodSource("getEngine")
  @ParameterizedTest(name = "compactionEngine={0}")
  public void test_ingestDayGranularity_andCompactToMonthGranularity_withInlineConfig(CompactionEngine compactionEngine)
  {
    configureCompaction(compactionEngine);

    // Ingest data at DAY granularity and verify
    runIngestionAtGranularity(
        "DAY",
        "2025-06-01T00:00:00.000Z,shirt,105"
        + "\n2025-06-02T00:00:00.000Z,trousers,210"
        + "\n2025-06-03T00:00:00.000Z,jeans,150"
    );
    Assertions.assertEquals(3, getNumSegmentsWith(Granularities.DAY));

    // Create a compaction config with MONTH granularity
    InlineSchemaDataSourceCompactionConfig compactionConfig =
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withGranularitySpec(
                new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null)
            )
            .withTuningConfig(
                new UserCompactionTaskQueryTuningConfig(
                    null,
                    null,
                    null,
                    null,
                    null,
                    new DimensionRangePartitionsSpec(null, 5000, List.of("item"), false),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
            .build();

    runCompactionWithSpec(compactionConfig);
    waitForAllCompactionTasksToFinish();

    Assertions.assertEquals(0, getNumSegmentsWith(Granularities.DAY));
    Assertions.assertEquals(1, getNumSegmentsWith(Granularities.MONTH));
  }

  private void runCompactionWithSpec(DataSourceCompactionConfig config)
  {
    final CompactionSupervisorSpec compactionSupervisor
        = new CompactionSupervisorSpec(config, false, null);
    cluster.callApi().postSupervisor(compactionSupervisor);
  }

  private void waitForAllCompactionTasksToFinish()
  {
    // Wait for all intervals to be compacted
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("interval/waitCompact/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
                      .hasValueMatching(Matchers.equalTo(0L))
    );

    // Wait for all submitted compaction jobs to finish
    int numSubmittedTasks = overlord.latchableEmitter().getMetricValues(
        "compact/task/count",
        Map.of(DruidMetrics.DATASOURCE, dataSource)
    ).stream().mapToInt(Number::intValue).sum();

    final Matcher<Object> taskTypeMatcher = Matchers.anyOf(
        Matchers.equalTo("query_controller"),
        Matchers.equalTo("compact")
    );
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/run/time")
                      .hasDimensionMatching(DruidMetrics.TASK_TYPE, taskTypeMatcher)
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasCountAtLeast(numSubmittedTasks)
    );
  }

  private int getNumSegmentsWith(Granularity granularity)
  {
    return (int) overlord
        .bindings()
        .segmentsMetadataStorage()
        .retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE)
        .stream()
        .filter(segment -> granularity.isAligned(segment.getInterval()))
        .count();
  }

  private void runIngestionAtGranularity(
      String granularity,
      String inlineDataCsv
  )
  {
    final IndexTask task = MoreResources.Task.BASIC_INDEX
        .get()
        .segmentGranularity(granularity)
        .inlineInputSourceWithData(inlineDataCsv)
        .dataSource(dataSource)
        .withId(IdUtils.getRandomId());
    cluster.callApi().runTask(task, overlord);
  }

  public static List<CompactionEngine> getEngine()
  {
    return List.of(CompactionEngine.NATIVE, CompactionEngine.MSQ);
  }
}
