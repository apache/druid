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

package org.apache.druid.testing.embedded.auth;

import org.apache.druid.audit.AuditEntry;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests to verify audit logging done for basic authorizer.
 */
public class BasicAuthAuditTest extends EmbeddedClusterTestBase
{
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer().addProperty("druid.worker.capacity", "25");
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  private SecurityClient securityClient;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .addResource(new EmbeddedBasicAuthResource())
        .useLatchableEmitter()
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(indexer)
        .addServer(historical)
        .addServer(broker)
        .addServer(new EmbeddedRouter())
        .addCommonProperty("druid.auth.basic.common.pollingPeriod", "10");
  }

  @BeforeAll
  public void setupClient()
  {
    securityClient = new SecurityClient(cluster.callApi().serviceClient());
  }

  @Test
  public void test_createRole_createsSingleAuditEntry() throws Exception
  {
    securityClient.createAuthorizerRole(dataSource);

    // Wait for all services to be synced
    Thread.sleep(100L);

    final List<AuditEntry> entries = securityClient.getUpdateHistory();
    Assertions.assertEquals(1, entries.size());
    Assertions.assertEquals(
        StringUtils.format("\"Create role[%s]\"", dataSource),
        entries.get(0).getPayload().serialized()
    );
  }
}
