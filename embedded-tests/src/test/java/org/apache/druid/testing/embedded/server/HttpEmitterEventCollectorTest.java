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
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.emitter.LatchableEmitterModule;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Test;

/**
 * Embedded test to verify basic functionality of {@code HttpPostEmitter} and
 * {@link EmbeddedEventCollector}.
 */
public class HttpEmitterEventCollectorTest extends EmbeddedClusterTestBase
{
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedEventCollector eventCollector = new EmbeddedEventCollector()
      .addProperty("druid.emitter", "latching");

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    // Event collector node uses a latchable emitter
    // All other servers use http emitter
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .addExtension(LatchableEmitterModule.class)
        .addCommonProperty("druid.emitter", "http")
        .addCommonProperty("druid.emitter.http.recipientBaseUrl", eventCollector.getMetricsUrl())
        .addCommonProperty("druid.emitter.http.flushMillis", "500")
        .addServer(overlord)
        .addServer(eventCollector)
        .addServer(coordinator)
        .addServer(broker)
        .addServer(new EmbeddedHistorical())
        .addServer(new EmbeddedIndexer());
  }

  @Test
  public void test_runIndexTask_andQueryData()
  {
    // Run a task
    final String taskId = IdUtils.getRandomId();
    final IndexTask task = MoreResources.Task.BASIC_INDEX.get().dataSource(dataSource).withId(taskId);
    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));

    // Wait for the task finish metric to be emitted by the Overlord
    eventCollector.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("task/run/time")
                      .hasService("druid/overlord")
                      .hasHost(overlord.bindings().selfNode().getHostAndPort())
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
                      .hasDimension(DruidMetrics.TASK_ID, taskId)
    );

    // Wait for the Broker to refresh schema
    eventCollector.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/schemaCache/refresh/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
                      .hasService("druid/broker"),
        agg -> agg.hasSumAtLeast(10)
    );

    cluster.callApi().verifySqlQuery("SELECT * FROM %s", dataSource, Resources.InlineData.CSV_10_DAYS);
  }
}
