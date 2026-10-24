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

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.URIs;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.server.QueryResource;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedDruidServer;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.ws.rs.core.MediaType;

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
  /** Hidden by the default {@code druid.server.hiddenProperties}, which includes {@code password}. */
  private static final String HIDDEN_PROPERTY = "native.sys.server.properties.password";
  private static final String HIDDEN_VALUE = "native-sys-hidden-value";

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty(COORDINATOR_PROPERTY, "enabled");

  private final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty(OVERLORD_PROPERTY, "enabled");

  private final EmbeddedBroker broker = new EmbeddedBroker()
      .addProperty("druid.service", SERVICE_NAME)
      .addProperty(BROKER_PROPERTY, "enabled")
      .addProperty(HIDDEN_PROPERTY, HIDDEN_VALUE);

  private final EmbeddedHistorical historical = new EmbeddedHistorical()
      .addProperty(HISTORICAL_PROPERTY, "enabled");

  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .addProperty(INDEXER_PROPERTY, "enabled");

  private final EmbeddedRouter router = new EmbeddedRouter()
      .addProperty(ROUTER_PROPERTY, "enabled")
      .addProperty(HIDDEN_PROPERTY, HIDDEN_VALUE);

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
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testServerPropertiesFansOutToAllNodes(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT service_name, COUNT(*)
        FROM sys.server_properties
        WHERE property IN ('%s', '%s', '%s', '%s', '%s', '%s')
        GROUP BY service_name
        ORDER BY service_name
        """,
        nativeQueryContext(plannerStrategy),
        COORDINATOR_PROPERTY,
        OVERLORD_PROPERTY,
        BROKER_PROPERTY,
        HISTORICAL_PROPERTY,
        INDEXER_PROPERTY,
        ROUTER_PROPERTY
    );

    Assertions.assertEquals(
        StringUtils.format(
            """
            druid/coordinator,1
            druid/historical,1
            druid/indexer,1
            druid/overlord,1
            druid/router,1
            %s,1""",
            SERVICE_NAME
        ),
        result
    );
  }

  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testNativeAggregationsSupportDistinctCount(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT COUNT(*), COUNT(DISTINCT service_name), COUNT(DISTINCT server), COUNT(DISTINCT property), SUM(1)
        FROM sys.server_properties
        WHERE property IN ('%s', '%s', '%s', '%s', '%s', '%s')
        """,
        nativeQueryContext(plannerStrategy),
        COORDINATOR_PROPERTY,
        OVERLORD_PROPERTY,
        BROKER_PROPERTY,
        HISTORICAL_PROPERTY,
        INDEXER_PROPERTY,
        ROUTER_PROPERTY
    );

    Assertions.assertEquals("6,6,6,6,6", result);
  }

  /**
   * A filtered window-function query is planned as a {@code WindowOperatorQuery} over a system-table Scan, whose
   * filter is pushed into the node scans.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testNativeWindowFunction(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT service_name, ROW_NUMBER() OVER (ORDER BY service_name)
        FROM sys.server_properties
        WHERE property IN ('%s', '%s', '%s', '%s', '%s', '%s')
        ORDER BY service_name
        """,
        nativeQueryContext(plannerStrategy),
        COORDINATOR_PROPERTY,
        OVERLORD_PROPERTY,
        BROKER_PROPERTY,
        HISTORICAL_PROPERTY,
        INDEXER_PROPERTY,
        ROUTER_PROPERTY
    );

    Assertions.assertEquals(
        StringUtils.format(
            """
            druid/coordinator,1
            druid/historical,2
            druid/indexer,3
            druid/overlord,4
            druid/router,5
            %s,6""",
            SERVICE_NAME
        ),
        result
    );
  }

  /**
   * An IN subquery on the same system table is planned as a join of two system-table leaves, each fanned out
   * separately. This is the query reported in review to return no rows.
   */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testInSubqueryOnSameSystemTable(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    final String result = cluster.runSql(
        """
        SELECT property
        FROM sys.server_properties
        WHERE server IN (SELECT server FROM sys.server_properties WHERE service_name = '%s')
        LIMIT 5
        """,
        nativeQueryContext(plannerStrategy),
        SERVICE_NAME
    );

    Assertions.assertEquals(5, result.split("\n").length, result);
  }

  /** Hidden properties are removed by the native SQL path, as they are by the SQL interpreter on the Broker. */
  @ParameterizedTest(name = "plannerStrategy = {0}")
  @EnumSource(QueryContexts.PlannerStrategy.class)
  public void testHiddenPropertiesAreNotReturnedBySql(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    Assertions.assertEquals(
        "0",
        cluster.runSql(
            "SELECT COUNT(*) FROM sys.server_properties WHERE property = '%s' OR \"value\" = '%s'",
            nativeQueryContext(plannerStrategy),
            HIDDEN_PROPERTY,
            HIDDEN_VALUE
        )
    );
  }

  /**
   * Hidden properties are removed from native {@code /druid/v2/} queries, both for Broker fan-out and for
   * execution on the contacted node with {@code X-Druid-Native-Query-Route: local}.
   */
  @Test
  public void testHiddenPropertiesAreNotReturnedByNativeQueries()
  {
    final Map<String, String> local =
        Map.of(QueryResource.HEADER_NATIVE_QUERY_ROUTE, QueryResource.NATIVE_QUERY_ROUTE_LOCAL);

    // Each response must contain a visible property from the expected node, to show the query read that node.
    // Router forwarding isn't covered here: this cluster renames the Broker service, so the Router can't route to it.
    assertNativeQueryHidesProperties(broker, Map.of(), ROUTER_PROPERTY);
    assertNativeQueryHidesProperties(broker, local, BROKER_PROPERTY);
    assertNativeQueryHidesProperties(router, local, ROUTER_PROPERTY);
  }

  private void assertNativeQueryHidesProperties(
      final EmbeddedDruidServer<?> server,
      final Map<String, String> headers,
      final String expectedVisibleProperty
  )
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource("server_properties"))
                                  .eternityInterval()
                                  .columns("property", "value")
                                  .build();
    try {
      final Request request = new Request(HttpMethod.POST, URIs.toURL(getServerUrl(server) + "/druid/v2/"))
          .setContent(MediaType.APPLICATION_JSON, server.bindings().jsonMapper().writeValueAsBytes(query));
      headers.forEach(request::setHeader);
      final StatusResponseHolder response = server.bindings()
                                                  .globalHttpClient()
                                                  .go(request, StatusResponseHandler.getInstance())
                                                  .get();

      Assertions.assertEquals(HttpResponseStatus.OK, response.getStatus(), response.getContent());
      Assertions.assertTrue(response.getContent().contains(expectedVisibleProperty), response.getContent());
      Assertions.assertFalse(response.getContent().contains(HIDDEN_PROPERTY), response.getContent());
      Assertions.assertFalse(response.getContent().contains(HIDDEN_VALUE), response.getContent());
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<String, Object> nativeQueryContext(final QueryContexts.PlannerStrategy plannerStrategy)
  {
    return QueryContext.ofMap(
        QueryContextParameters.ENGINE,
        NativeSqlEngine.NAME,
        QueryContextParameters.USE_QUERY_ENGINE_FOR_SYSTEM_TABLES,
        true,
        QueryContextParameters.PLANNER_STRATEGY,
        plannerStrategy
    );
  }
}
