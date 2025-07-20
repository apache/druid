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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.dart.guice.DartControllerMemoryManagementModule;
import org.apache.druid.msq.dart.guice.DartControllerModule;
import org.apache.druid.msq.dart.guice.DartWorkerMemoryManagementModule;
import org.apache.druid.msq.dart.guice.DartWorkerModule;
import org.apache.druid.msq.guice.IndexerMemoryManagementModule;
import org.apache.druid.msq.guice.MSQDurableStorageModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.guice.MSQSqlModule;
import org.apache.druid.msq.guice.SqlTaskModule;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.List;

public class EmbeddedMSQRealtimeUnnestQueryTest extends BaseRealtimeQueryTest
{
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
    EmbeddedDruidCluster clusterWithKafka = super.createCluster();

    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "always");

    overlord.addProperty("druid.manager.segments.useIncrementalCache", "always")
            .addProperty("druid.manager.segments.pollDuration", "PT0.1s");

    broker.addProperty("druid.msq.dart.controller.heapFraction", "0.9")
          .addProperty("druid.query.default.context.maxConcurrentStages", "1");

    historical.addProperty("druid.msq.dart.worker.heapFraction", "0.9")
              .addProperty("druid.msq.dart.worker.concurrentQueries", "1");

    indexer.setServerMemory(300_000_000) // to run 2x realtime and 2x MSQ tasks
           .addProperty("druid.segment.handoff.pollDuration", "PT0.1s")
           // druid.processing.numThreads must be higher than # of MSQ tasks to avoid contention, because the realtime
           // server is contacted in such a way that the processing thread is blocked
           .addProperty("druid.processing.numThreads", "3")
           .addProperty("druid.worker.capacity", "4");

    return clusterWithKafka
        .addExtension(DartControllerModule.class)
        .addExtension(DartWorkerModule.class)
        .addExtension(DartControllerMemoryManagementModule.class)
        .addExtension(DartWorkerMemoryManagementModule.class)
        .addExtension(IndexerMemoryManagementModule.class)
        .addExtension(MSQDurableStorageModule.class)
        .addExtension(MSQIndexingModule.class)
        .addExtension(MSQSqlModule.class)
        .addExtension(SqlTaskModule.class)
        .addCommonProperty("druid.monitoring.emissionPeriod", "PT0.1s")
        .addCommonProperty("druid.msq.dart.enabled", "true")
        .useLatchableEmitter()
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(router)
        .addServer(broker)
        .addServer(historical)
        .addServer(indexer);
  }

  @BeforeEach
  void setUpEach()
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);

    QueryableIndex index = TestIndex.getMMappedTestIndex();

    submitSupervisor();
    publishToKafka(index);

    final int totalRows = index.getNumRows();

    // Wait for it to be loaded.
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, Collections.singletonList(dataSource)),
        agg -> agg.hasSumAtLeast(totalRows)
    );
  }

  @Test
  @Timeout(60)
  public void test_unnest_task_withRealtime()
  {
    final String sql = StringUtils.format(
        "SET includeSegmentSource = 'REALTIME';\n"
        + "SELECT d3 FROM \"%s\" CROSS JOIN UNNEST(MV_TO_ARRAY(\"placementish\")) AS d3\n"
        + "LIMIT 5",
        dataSource
    );
    final MSQTaskReportPayload payload = msqApis.runTaskSqlAndGetReport(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        List.of(
            new Object[]{"a"},
            new Object[]{"preferred"},
            new Object[]{"b"},
            new Object[]{"preferred"},
            new Object[]{"e"}
        ),
        payload.getResults().getResults()
    );
  }

  @Test
  @Timeout(60)
  public void test_unnest_dart()
  {
    final String sql = StringUtils.format(
        "SELECT d3 FROM \"%s\" CROSS JOIN UNNEST(MV_TO_ARRAY(\"placementish\")) AS d3\n"
        + "LIMIT 5",
        dataSource
    );
    final String result = msqApis.runDartSql(sql);

    Assertions.assertEquals(
        "a\n"
        + "preferred\n"
        + "b\n"
        + "preferred\n"
        + "e",
        result
    );
  }
}
