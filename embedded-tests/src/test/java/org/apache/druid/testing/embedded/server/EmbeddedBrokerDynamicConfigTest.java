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

import org.apache.druid.audit.AuditInfo;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.server.QueryBlocklistRule;
import org.apache.druid.server.broker.BrokerDynamicConfig;
import org.apache.druid.server.http.BrokerDynamicConfigSyncer;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Integration test for broker dynamic configuration
 */
public class EmbeddedBrokerDynamicConfigTest extends EmbeddedClusterTestBase
{
  // Fixed datasource ingested once for all tests; restored before each test since the
  // base class @BeforeEach would otherwise assign a fresh name.
  private String fixedDataSource;

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedBroker broker = new EmbeddedBroker();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(indexer)
                               .addServer(historical)
                               .addServer(broker);
  }

  @BeforeAll
  @Override
  public void setup() throws Exception
  {
    fixedDataSource = EmbeddedClusterApis.createTestDatasourceName();
    dataSource = fixedDataSource;
    super.setup();
    ingestData();
    cluster.callApi().waitForAllSegmentsToBeAvailable(fixedDataSource, coordinator, broker);
  }

  @BeforeEach
  @Override
  protected void refreshDatasourceName()
  {
    dataSource = fixedDataSource;
  }

  @Test
  @Timeout(30)
  public void testQueryBlocklistBlocksMatchingQueries()
  {
    // Baseline: query succeeds before blocklist is applied
    String initialResult = cluster.callApi().runSql("SELECT COUNT(*) FROM %s", dataSource);
    Assertions.assertFalse(initialResult.isBlank());

    // Apply blocklist rule that matches all queries on this datasource
    QueryBlocklistRule blockRule = new QueryBlocklistRule(
        "block-test-datasource",
        Set.of(dataSource),
        null,
        null
    );
    updateBrokerDynamicConfig(
        BrokerDynamicConfig.builder()
                           .withQueryBlocklist(List.of(blockRule))
                           .build()
    );

    // Query should now throw due to FORBIDDEN blocklist rule
    Assertions.assertThrows(
        RuntimeException.class,
        () -> cluster.callApi().runSql("SELECT COUNT(*) FROM %s", dataSource)
    );

    // Clear the blocklist and verify queries resume
    updateBrokerDynamicConfig(BrokerDynamicConfig.builder().build());
    String finalResult = cluster.callApi().runSql("SELECT COUNT(*) FROM %s", dataSource);
    Assertions.assertFalse(finalResult.isBlank());
  }

  @Test
  @Timeout(30)
  public void testDynamicQueryContextTimeoutCausesQueryToFail()
  {
    // Baseline: query succeeds before a timeout context is applied
    String initialResult = cluster.callApi().runSql("SELECT COUNT(*) FROM %s", dataSource);
    Assertions.assertFalse(initialResult.isBlank());

    // Apply a 1ms timeout via dynamic query context — any real query will expire before responding
    updateBrokerDynamicConfig(
        BrokerDynamicConfig.builder()
                           .withQueryContext(Map.of("timeout", 1))
                           .build()
    );

    // Query should now throw due to the timeout being exceeded
    Assertions.assertThrows(
        RuntimeException.class,
        () -> cluster.callApi().runSql("SELECT COUNT(*) FROM %s", dataSource)
    );

    // Clear the dynamic context and verify queries resume
    updateBrokerDynamicConfig(BrokerDynamicConfig.builder().build());
    String finalResult = cluster.callApi().runSql("SELECT COUNT(*) FROM %s", dataSource);
    Assertions.assertFalse(finalResult.isBlank());
  }

  private void ingestData()
  {
    cluster.callApi().runTask(
        TaskBuilder.ofTypeIndex()
                   .dataSource(dataSource)
                   .isoTimestampColumn("time")
                   .csvInputFormatWithColumns("time", "item", "value")
                   .inlineInputSourceWithData(Resources.InlineData.CSV_10_DAYS)
                   .segmentGranularity("DAY")
                   .dimensions()
                   .withId(IdUtils.getRandomId()),
        overlord
    );
  }

  /**
   * Updates the broker dynamic config on the coordinator and synchronously broadcasts it
   * to all brokers.
   *
   * Uses {@link JacksonConfigManager} directly to avoid the HTTP endpoint's builder-merge
   * semantics, which cannot distinguish "clear to empty" from "not specified" when fields
   * are omitted via {@code @JsonInclude(NON_EMPTY)}.
   */
  private void updateBrokerDynamicConfig(BrokerDynamicConfig config)
  {
    coordinator.bindings()
               .getInstance(JacksonConfigManager.class)
               .set(BrokerDynamicConfig.CONFIG_KEY, config, new AuditInfo("test", "test@test.com", "Testing", "127.0.0.1"));
    coordinator.bindings()
               .getInstance(BrokerDynamicConfigSyncer.class)
               .broadcastConfigToBrokers();
  }
}
