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

import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public abstract class QueryTestBase extends EmbeddedClusterTestBase
{
  protected static final String SQL_QUERY_ROUTE = "%s/druid/v2/sql/";
  public static List<Boolean> SHOULD_USE_BROKER_TO_QUERY = List.of(true, false);

  protected final EmbeddedBroker broker = new EmbeddedBroker();
  protected final EmbeddedRouter router = new EmbeddedRouter();
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord();
  protected final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  protected final EmbeddedIndexer indexer = new EmbeddedIndexer();
  protected final EmbeddedHistorical historical = new EmbeddedHistorical();

  protected HttpClient httpClientRef;
  protected String brokerEndpoint;
  protected String routerEndpoint;

  public static String getResourceAsString(String file) throws IOException
  {
    try (final InputStream inputStream = QueryTestBase.class.getResourceAsStream(file)) {
      if (inputStream == null) {
        throw new ISE("Failed to load resource: [%s]", file);
      }
      return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    }
  }

  /**
   * Hook for the additional setup that needs to be done before all tests.
   */
  protected void beforeAll()
  {
    // No-op dy default
  }

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    overlord.addProperty("druid.manager.segments.pollDuration", "PT0.1s");
    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "always");
    indexer.setServerMemory(100_000_000)
           .addProperty("druid.worker.capacity", "4")
           .addProperty("druid.processing.numThreads", "2")
           .addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(broker)
                               .addServer(router)
                               .addServer(indexer)
                               .addServer(historical);
  }

  @BeforeAll
  void setUp()
  {
    httpClientRef = router.bindings().globalHttpClient();
    brokerEndpoint = StringUtils.format(SQL_QUERY_ROUTE, getServerUrl(broker));
    routerEndpoint = StringUtils.format(SQL_QUERY_ROUTE, getServerUrl(router));
    try {
      beforeAll();
    }
    catch (Exception e) {
      throw new AssertionError(e);
    }
  }
}
