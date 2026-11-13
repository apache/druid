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

import com.google.common.base.Throwables;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Dart controllers here have too little memory, so every Dart query fails during execution with an operator fault.
 */
public class EmbeddedDartErrorResponseTransformTest extends EmbeddedClusterTestBase
{
  private final EmbeddedBroker personaBroker = new EmbeddedBroker();
  private final EmbeddedBroker defaultBroker = new EmbeddedBroker();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    personaBroker.addProperty("druid.msq.dart.controller.heapFraction", "0.000001")
                 .addProperty("druid.server.http.errorResponseTransform.strategy", "persona")
                 .addProperty("druid.plaintextPort", "7082");
    defaultBroker.addProperty("druid.msq.dart.controller.heapFraction", "0.000001")
                 .addProperty("druid.plaintextPort", "7083");

    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addCommonProperty("druid.msq.dart.enabled", "true")
                               .useLatchableEmitter()
                               .addServer(coordinator)
                               .addServer(personaBroker)
                               .addServer(defaultBroker)
                               .addServer(historical);
  }

  @Test
  public void test_dartExecutionFailure_isHiddenByPersonaStrategy()
  {
    final HttpResponseException e = runFailingDartQuery(personaBroker, "persona-query");

    Assertions.assertEquals(500, e.getResponse().getStatus().code());
    Assertions.assertTrue(e.getMessage().contains("Error ID [persona-query]"), e.getMessage());
    Assertions.assertFalse(e.getMessage().contains("NotEnoughMemory"), e.getMessage());
    Assertions.assertFalse(e.getMessage().contains("exceptionStackTrace"), e.getMessage());
  }

  @Test
  public void test_dartExecutionFailure_isReturnedUnchangedByDefaultStrategy()
  {
    final HttpResponseException e = runFailingDartQuery(defaultBroker, "default-query");

    Assertions.assertTrue(e.getMessage().contains("NotEnoughMemory"), e.getMessage());
  }

  private HttpResponseException runFailingDartQuery(final EmbeddedBroker broker, final String sqlQueryId)
  {
    final Exception e = Assertions.assertThrows(
        Exception.class,
        () -> cluster.callApi().onTargetBroker(
            broker,
            b -> b.submitSqlQuery(
                new ClientSqlQuery(
                    "SELECT 1",
                    ResultFormat.CSV.name(),
                    false,
                    false,
                    false,
                    Map.of(QueryContexts.ENGINE, DartSqlEngine.NAME, QueryContexts.CTX_SQL_QUERY_ID, sqlQueryId),
                    null
                )
            )
        )
    );
    return Assertions.assertInstanceOf(HttpResponseException.class, Throwables.getRootCause(e));
  }
}
