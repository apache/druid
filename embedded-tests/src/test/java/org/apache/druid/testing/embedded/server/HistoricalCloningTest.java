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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class HistoricalCloningTest extends EmbeddedClusterTestBase
{
  private final EmbeddedHistorical historical1 = new EmbeddedHistorical();
  private final EmbeddedHistorical historical2 = new EmbeddedHistorical()
      .addProperty("druid.plaintextPort", "7083");
  private final EmbeddedCoordinator coordinator1 = new EmbeddedCoordinator();
  private final EmbeddedCoordinator coordinator2 = new EmbeddedCoordinator()
      .addProperty("druid.plaintextPort", "7081");
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(overlord)
                               .addServer(coordinator1)
                               .addServer(coordinator2)
                               .addServer(new EmbeddedIndexer())
                               .addServer(historical1)
                               .addServer(new EmbeddedBroker())
                               .addServer(new EmbeddedRouter());
  }

  @Test
  public void test_cloneHistoricals_inTurboMode_duringCoordinatorLeaderSwitch() throws Exception
  {
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateRulesForDatasource(
            dataSource,
            List.of(new ForeverLoadRule(Map.of("_default_tier", 1), null))
        )
    );
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateCoordinatorDynamicConfig(
            CoordinatorDynamicConfig
                .builder()
                .withCloneServers(Map.of("localhost:7083", "localhost:8083"))
                .withTurboLoadingNodes(Set.of("localhost:7083"))
                .build()
        )
    );

    runIngestion();

    // Wait for segments to be loaded on historical1
    coordinator1.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/loadQueue/success")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(10)
    );
    coordinator1.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/loadQueue/success")
                      .hasDimension("server", historical1.bindings().selfNode().getHostAndPort())
                      .hasDimension("description", "LOAD: NORMAL"),
        agg -> agg.hasSumAtLeast(10)
    );

    // Switch coordinator leader to force syncer to reset
    coordinator1.stop();

    // Wait for a few coordinator runs so that the server views are refreshed
    coordinator2.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("coordinator/time")
                      .hasDimension("dutyGroup", "HistoricalManagementDuties"),
        agg -> agg.hasCountAtLeast(2)
    );

    // Add historical2 to the cluster
    cluster.addServer(historical2);
    historical2.start();

    // Wait for the clones to be loaded
    coordinator2.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/clone/assigned/count")
                      .hasDimension("server", historical2.bindings().selfNode().getHostAndPort()),
        agg -> agg.hasSumAtLeast(10)
    );
    coordinator2.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/loadQueue/success")
                      .hasDimension("server", historical2.bindings().selfNode().getHostAndPort())
                      .hasDimension("description", "LOAD: TURBO"),
        agg -> agg.hasSumAtLeast(10)
    );
  }

  private void runIngestion()
  {
    final String taskId = IdUtils.getRandomId();
    final Object task = createIndexTaskForInlineData(
        taskId,
        StringUtils.replace(Resources.CSV_DATA_10_DAYS, "\n", "\\n")
    );

    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
  }

  private Object createIndexTaskForInlineData(String taskId, String inlineDataCsv)
  {
    return EmbeddedClusterApis.createTaskFromPayload(
        taskId,
        StringUtils.format(Resources.INDEX_TASK_PAYLOAD_WITH_INLINE_DATA, inlineDataCsv, dataSource)
    );
  }
}
