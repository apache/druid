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

package org.apache.druid.testing.embedded.catalog;

import org.apache.druid.catalog.guice.CatalogClientModule;
import org.apache.druid.catalog.guice.CatalogCoordinatorModule;
import org.apache.druid.error.ExceptionMatcher;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.Map;

public abstract class CatalogTestBase extends EmbeddedClusterTestBase
{
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord()
      .addProperty("druid.catalog.client.maxSyncRetries", "0");
  protected final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.manager.segments.useIncrementalCache", "always");
  protected final EmbeddedBroker broker = new EmbeddedBroker()
      .addProperty("druid.catalog.client.pollingPeriod", "100");
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(1_000_000_000)
      .addProperty("druid.worker.capacity", "2");

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addExtensions(
                                   CatalogClientModule.class,
                                   CatalogCoordinatorModule.class
                               )
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(broker)
                               .addServer(indexer)
                               .addServer(new EmbeddedHistorical());
  }

  void verifySubmitSqlTaskFailsWith400BadRequest(String sql, String expectedMessageSubstring)
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> cluster.callApi().onAnyBroker(
                b -> b.submitSqlTask(
                    new ClientSqlQuery(sql, null, false, false, false, Map.of(), List.of())
                )
            )
        ),
        ExceptionMatcher.of(HttpResponseException.class)
                        .expectMessageContains("400 Bad Request")
                        .expectMessageContains(expectedMessageSubstring)
    );
  }
}
