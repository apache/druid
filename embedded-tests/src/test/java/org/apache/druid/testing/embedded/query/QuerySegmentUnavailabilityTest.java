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
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Embedded test to ensure consistent query behavior in corner cases such as:
 * 1. Data node failures,
 * 2. Lapses in segment availability, etc.
 */
public class QuerySegmentUnavailabilityTest extends EmbeddedClusterTestBase
{
  private final EmbeddedHistorical historical1 = new EmbeddedHistorical();
  private final EmbeddedHistorical historical2 = new EmbeddedHistorical().addProperty("druid.plaintextPort", "7083");
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer().addProperty("druid.worker.capacity", "2");

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(indexer)
                               .addServer(historical1)
                               .addServer(historical2)
                               .addServer(broker);
  }

  /**
   * Asserts the behavior that when:
   * 1. Data is ingested and segments are loaded across N > 1 historicals
   * 2. M historicals, where M < N, go down suddenly (simulating crash/network partition/other failure) and Broker detects this
   * Then:
   * 1. Queries fail as there are partial/empty results instead of succeeding
   */
  @Test
  public void test_queryFailsWhenHistoricalNodeGoesDown() throws Exception
  {
    coordinator.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("coordinator/time")
                      .hasService("druid/coordinator")
    );

    cluster.callApi().onLeaderCoordinator(
        c -> c.updateRulesForDatasource(
            dataSource,
            List.of(new ForeverLoadRule(Map.of("_default_tier", 1), null))
        )
    );

    final String taskId = IdUtils.getRandomId();
    final IndexTask task = createIndexTaskForInlineData(taskId);
    cluster.callApi().runTask(task, overlord);

    final long totalSegments = 10;
    broker.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/schemaCache/refresh/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(totalSegments)
    );

    String fullResult = cluster.runSql("SELECT COUNT(*) FROM %s", dataSource);
    Assertions.assertEquals("10", fullResult, "Expected all 10 rows to be queryable");

    String fullData = cluster.runSql("SELECT * FROM %s ORDER BY __time", dataSource);
    Assertions.assertEquals(
        Resources.InlineData.CSV_10_DAYS,
        fullData,
        "Expected all data to be queryable"
    );

    // Pause coordination on Coordinator BEFORE killing historical
    // This simulates "slow" segment rebalancing to ensure a gap remains in the timeline
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateCoordinatorDynamicConfig(
            CoordinatorDynamicConfig.builder().withPauseCoordination(true).build()
        )
    );
    Thread.sleep(3000);

    historical1.stop();

    broker.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/schemaCache/refresh/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(5L)
    );
    try {
      String resultAfterFailure = cluster.runSql("SELECT COUNT(*) FROM %s", dataSource);
      Assertions.fail(StringUtils.format("Should have failed, instead got [%s] rows", resultAfterFailure));
    }
    catch (Exception e) {
      Assertions.assertTrue(
          e.getMessage().contains("segment") || e.getMessage().contains("unavailable"),
          "Error message should indicate segment unavailability"
      );
    }
  }

  private IndexTask createIndexTaskForInlineData(String taskId)
  {
    return TaskBuilder.ofTypeIndex()
                      .dataSource(dataSource)
                      .isoTimestampColumn("time")
                      .csvInputFormatWithColumns("time", "item", "value")
                      .inlineInputSourceWithData(Resources.InlineData.CSV_10_DAYS)
                      .segmentGranularity("DAY")
                      .dimensions()
                      .withId(taskId);
  }
}

