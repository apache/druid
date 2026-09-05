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

import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

public class NativeSysServerPropertiesQueryTest extends EmbeddedClusterTestBase
{
  private static final String SERVICE_NAME = "native/mvp/broker";
  private static final String COORDINATOR_PROPERTY = "native.sys.server.properties.coordinator";
  private static final String OVERLORD_PROPERTY = "native.sys.server.properties.overlord";
  private static final String BROKER_PROPERTY = "native.sys.server.properties.broker";
  private static final String HISTORICAL_PROPERTY = "native.sys.server.properties.historical";
  private static final String INDEXER_PROPERTY = "native.sys.server.properties.indexer";
  private static final String ROUTER_PROPERTY = "native.sys.server.properties.router";

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty(COORDINATOR_PROPERTY, "enabled");

  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty(OVERLORD_PROPERTY, "enabled");

  private final EmbeddedBroker broker = new EmbeddedBroker()
      .addProperty("druid.service", SERVICE_NAME)
      .addProperty(BROKER_PROPERTY, "enabled");

  private final EmbeddedHistorical historical = new EmbeddedHistorical()
      .addProperty(HISTORICAL_PROPERTY, "enabled");

  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .addProperty(INDEXER_PROPERTY, "enabled");

  private final EmbeddedRouter router = new EmbeddedRouter()
      .addProperty(ROUTER_PROPERTY, "enabled");

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addCommonProperty("druid.centralizedDatasourceSchema.enabled", "true")
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(broker)
                               .addServer(historical)
                               .addServer(indexer)
                               .addServer(router);
  }

  /**
   * Verifies that a native query for {@code sys.server_properties} reaches every persistent node type in the
   * embedded cluster. In particular, the Router row proves that the Broker's native request included
   * {@code X-Druid-Native-Query-Route: local}: without that header the Router would forward the request back to the
   * Broker instead of reading its local system-table provider.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void testServerPropertiesFansOutToAllNodes(final String plannerStrategy)
  {
    final String result = cluster.runSql(
        "SELECT service_name, COUNT(*) "
        + "FROM sys.server_properties "
        + "WHERE property IN ('" + COORDINATOR_PROPERTY + "', '" + OVERLORD_PROPERTY + "', '" + BROKER_PROPERTY
        + "', '" + HISTORICAL_PROPERTY + "', '" + INDEXER_PROPERTY + "', '" + ROUTER_PROPERTY + "') "
        + "GROUP BY service_name ORDER BY service_name",
        nativeQueryContext(plannerStrategy)
    );

    Assertions.assertEquals(
        String.join(
            "\n",
            "druid/coordinator,1",
            "druid/historical,1",
            "druid/indexer,1",
            "druid/overlord,1",
            "druid/router,1",
            SERVICE_NAME + ",1"
        ),
        result
    );
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @ValueSource(strings = {
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED,
      QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
  })
  public void testNativeAggregationsSupportDistinctCount(final String plannerStrategy)
  {
    final String result = cluster.runSql(
        "SELECT COUNT(*), COUNT(DISTINCT service_name), COUNT(DISTINCT server), "
        + "COUNT(DISTINCT property), SUM(1) "
        + "FROM sys.server_properties "
        + "WHERE property IN ('" + COORDINATOR_PROPERTY + "', '" + OVERLORD_PROPERTY + "', '" + BROKER_PROPERTY
        + "', '" + HISTORICAL_PROPERTY + "', '" + INDEXER_PROPERTY + "', '" + ROUTER_PROPERTY + "')",
        nativeQueryContext(plannerStrategy)
    );

    Assertions.assertEquals("6,6,6,6,6", result);
  }

  private static Map<String, Object> nativeQueryContext(final String plannerStrategy)
  {
    return Map.of(
        QueryContexts.ENGINE,
        NativeSqlEngine.NAME,
        PlannerContext.CTX_USE_NATIVE_QUERY_FOR_SYSTEM_TABLES,
        true,
        QueryContexts.CTX_NATIVE_QUERY_SQL_PLANNING_MODE,
        plannerStrategy
    );
  }
}
