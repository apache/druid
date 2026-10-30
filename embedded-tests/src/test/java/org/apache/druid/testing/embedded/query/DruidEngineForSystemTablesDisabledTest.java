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
import org.apache.druid.java.util.common.URIs;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.server.QueryResource;
import org.apache.druid.server.system.module.SystemTableModule;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedDruidServer;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.MediaType;

import java.util.Map;

/**
 * Verifies that, without {@code druid.query.enableDruidEngineForSystemTables}, servers don't serve system-table
 * queries on the Druid engine, and that the {@code useDruidEngineForSystemTables} context parameter has no effect.
 */
public class DruidEngineForSystemTablesDisabledTest extends EmbeddedClusterTestBase
{
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedRouter router = new EmbeddedRouter();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(broker)
                               .addServer(historical)
                               .addServer(router);
  }

  @Test
  public void testContextParameterHasNoEffect()
  {
    final Map<String, Object> context = QueryContext.ofMap(
        QueryContextParameters.ENGINE,
        NativeSqlEngine.NAME,
        QueryContextParameters.USE_DRUID_ENGINE_FOR_SYSTEM_TABLES,
        true
    );

    final String plan = cluster.runSql("EXPLAIN PLAN FOR SELECT server FROM sys.server_properties", context);
    Assertions.assertTrue(plan.contains("BindableTableScan"), plan);
    Assertions.assertFalse(plan.contains("systemTable"), plan);
    // The Broker's SQL interpreter reads the table.
    Assertions.assertFalse(cluster.runSql("SELECT server FROM sys.server_properties LIMIT 1", context).isEmpty());
  }

  @Test
  public void testBrokerRejectsSystemTableDataSource()
  {
    assertSystemTableQueryRejected(broker, Map.of());
    assertSystemTableQueryRejected(broker, localRoute());
  }

  /** The Router ignores the local route, and forwards the query to the Broker. */
  @Test
  public void testRouterForwardsLocalRoute()
  {
    assertSystemTableQueryRejected(router, localRoute());
  }

  /** Servers that don't otherwise handle queries don't serve {@code /druid/v2/}. */
  @Test
  public void testNoNativeQueryEndpointOnCoordinatorAndOverlord()
  {
    Assertions.assertEquals(HttpResponseStatus.NOT_FOUND, postSystemTableQuery(coordinator, localRoute()).getStatus());
    Assertions.assertEquals(HttpResponseStatus.NOT_FOUND, postSystemTableQuery(overlord, localRoute()).getStatus());
  }

  private void assertSystemTableQueryRejected(final EmbeddedDruidServer<?> server, final Map<String, String> headers)
  {
    final StatusResponseHolder response = postSystemTableQuery(server, headers);
    Assertions.assertEquals(HttpResponseStatus.NOT_IMPLEMENTED, response.getStatus(), response.getContent());
    Assertions.assertTrue(
        response.getContent().contains(SystemTableModule.ENABLE_DRUID_ENGINE_FOR_SYSTEM_TABLES_PROPERTY),
        response.getContent()
    );
  }

  private static Map<String, String> localRoute()
  {
    return Map.of(QueryResource.HEADER_NATIVE_QUERY_ROUTE, QueryResource.NATIVE_QUERY_ROUTE_LOCAL);
  }

  private static StatusResponseHolder postSystemTableQuery(
      final EmbeddedDruidServer<?> server,
      final Map<String, String> headers
  )
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(new SystemTableDataSource("server_properties"))
                                  .eternityInterval()
                                  .build();
    try {
      final Request request = new Request(HttpMethod.POST, URIs.toURL(getServerUrl(server) + "/druid/v2/"))
          .setContent(MediaType.APPLICATION_JSON, server.bindings().jsonMapper().writeValueAsBytes(query));
      headers.forEach(request::setHeader);
      return server.bindings().globalHttpClient().go(request, StatusResponseHandler.getInstance()).get();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
