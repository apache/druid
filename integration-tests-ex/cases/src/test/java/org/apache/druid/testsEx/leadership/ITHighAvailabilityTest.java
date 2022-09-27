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

package org.apache.druid.testsEx.leadership;

import com.google.inject.Inject;
import org.apache.druid.cli.CliCustomNodeRole;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.apache.druid.testsEx.categories.HighAvailability;
import org.apache.druid.testsEx.cluster.DruidClusterClient;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.apache.druid.testsEx.config.Initializer;
import org.apache.druid.testsEx.utils.DruidClusterAdminClient;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(DruidTestRunner.class)
@Category(HighAvailability.class)
public class ITHighAvailabilityTest
{
  private static final Logger LOG = new Logger(ITHighAvailabilityTest.class);
  private static final String SYSTEM_QUERIES_RESOURCE = Initializer.queryFile(HighAvailability.class, "sys.json");
  private static final int NUM_LEADERSHIP_SWAPS = 3;

  @Inject
  private IntegrationTestingConfig config;

  @Inject
  private DruidClusterAdminClient druidClusterAdminClient;

  @Inject
  private DruidNodeDiscoveryProvider druidNodeDiscovery;

  @Inject
  private SqlTestQueryHelper queryHelper;

  @Inject
  @TestClient
  private HttpClient httpClient;

  @Inject
  private DruidClusterClient clusterClient;

  @Test
  public void testLeadershipChanges() throws Exception
  {
    int runCount = 0;
    String previousCoordinatorLeader = null;
    String previousOverlordLeader = null;
    // fetch current leaders, make sure queries work, then swap leaders and do it again
    do {
      String coordinatorLeader = getLeader("coordinator");
      String overlordLeader = getLeader("indexer");

      // we expect leadership swap to happen
      assertNotEquals(previousCoordinatorLeader, coordinatorLeader);
      assertNotEquals(previousOverlordLeader, overlordLeader);

      previousCoordinatorLeader = coordinatorLeader;
      previousOverlordLeader = overlordLeader;

      String queries = fillTemplate(
          AbstractIndexerTest.getResourceAsString(SYSTEM_QUERIES_RESOURCE),
          overlordLeader,
          coordinatorLeader
      );
      queryHelper.testQueriesFromString(queries);

      swapLeadersAndWait(coordinatorLeader, overlordLeader);
    } while (runCount++ < NUM_LEADERSHIP_SWAPS);
  }

  @Test
  public void testDiscoveryAndSelfDiscovery()
  {
    // The cluster used here has an abbreviated set of services.
    verifyRoleDiscovery(NodeRole.BROKER, 1);
    verifyRoleDiscovery(NodeRole.COORDINATOR, 2);
    verifyRoleDiscovery(NodeRole.OVERLORD, 2);
    verifyRoleDiscovery(NodeRole.ROUTER, 1);
  }

  public void verifyRoleDiscovery(NodeRole role, int expectedCount)
  {
    DruidNodeDiscovery discovered = druidNodeDiscovery.getForNodeRole(role);
    try {
      int count = 0;
      for (DiscoveryDruidNode node : discovered.getAllNodes()) {
        if (clusterClient.selfDiscovered(clusterClient.nodeUrl(node.getDruidNode()))) {
          count++;
        }
      }
      assertEquals(expectedCount, count);
    }
    catch (Exception e) {
      LOG.error(e, "node discovery failed");
      fail();
    }
  }

  @Test
  public void testCustomDiscovery()
  {
    verifyRoleDiscovery(CliCustomNodeRole.NODE_ROLE, 1);
    verifyCoordinatorCluster();
  }

  private void swapLeadersAndWait(String coordinatorLeader, String overlordLeader)
  {
    String coordUrl;
    String coordLabel;
    if (isCoordinatorOneLeader(coordinatorLeader)) {
      druidClusterAdminClient.restartCoordinatorContainer();
      coordUrl = config.getCoordinatorUrl();
      coordLabel = "coordinator one";
    } else {
      druidClusterAdminClient.restartCoordinatorTwoContainer();
      coordUrl = config.getCoordinatorTwoUrl();
      coordLabel = "coordinator two";
    }

    String overlordUrl;
    String overlordLabel;
    if (isOverlordOneLeader(overlordLeader)) {
      druidClusterAdminClient.restartOverlordContainer();
      overlordUrl = config.getOverlordUrl();
      overlordLabel = "overlord one";
    } else {
      druidClusterAdminClient.restartOverlordTwoContainer();
      overlordUrl = config.getOverlordTwoUrl();
      overlordLabel = "overlord two";
    }
    clusterClient.waitForNodeReady(coordLabel, coordUrl);
    clusterClient.waitForNodeReady(overlordLabel, overlordUrl);
  }

  private String getLeader(String service)
  {
    return clusterClient.getLeader(service);
  }

  private String fillTemplate(String template, String overlordLeader, String coordinatorLeader)
  {
    /*
      {"host":"%%BROKER%%","server_type":"broker", "is_leader": %%NON_LEADER%%},
      {"host":"%%COORDINATOR_ONE%%","server_type":"coordinator", "is_leader": %%COORDINATOR_ONE_LEADER%%},
      {"host":"%%COORDINATOR_TWO%%","server_type":"coordinator", "is_leader": %%COORDINATOR_TWO_LEADER%%},
      {"host":"%%OVERLORD_ONE%%","server_type":"overlord", "is_leader": %%OVERLORD_ONE_LEADER%%},
      {"host":"%%OVERLORD_TWO%%","server_type":"overlord", "is_leader": %%OVERLORD_TWO_LEADER%%},
      {"host":"%%ROUTER%%","server_type":"router", "is_leader": %%NON_LEADER%%},
     */
    String working = template;
    working = StringUtils.replace(working, "%%OVERLORD_ONE%%", config.getOverlordInternalHost());
    working = StringUtils.replace(working, "%%OVERLORD_TWO%%", config.getOverlordTwoInternalHost());
    working = StringUtils.replace(working, "%%COORDINATOR_ONE%%", config.getCoordinatorInternalHost());
    working = StringUtils.replace(working, "%%COORDINATOR_TWO%%", config.getCoordinatorTwoInternalHost());
    working = StringUtils.replace(working, "%%BROKER%%", config.getBrokerInternalHost());
    working = StringUtils.replace(working, "%%ROUTER%%", config.getRouterInternalHost());
    if (isOverlordOneLeader(overlordLeader)) {
      working = StringUtils.replace(working, "%%OVERLORD_ONE_LEADER%%", "1");
      working = StringUtils.replace(working, "%%OVERLORD_TWO_LEADER%%", "0");
    } else {
      working = StringUtils.replace(working, "%%OVERLORD_ONE_LEADER%%", "0");
      working = StringUtils.replace(working, "%%OVERLORD_TWO_LEADER%%", "1");
    }
    if (isCoordinatorOneLeader(coordinatorLeader)) {
      working = StringUtils.replace(working, "%%COORDINATOR_ONE_LEADER%%", "1");
      working = StringUtils.replace(working, "%%COORDINATOR_TWO_LEADER%%", "0");
    } else {
      working = StringUtils.replace(working, "%%COORDINATOR_ONE_LEADER%%", "0");
      working = StringUtils.replace(working, "%%COORDINATOR_TWO_LEADER%%", "1");
    }
    working = StringUtils.replace(working, "%%NON_LEADER%%", String.valueOf(NullHandling.defaultLongValue()));
    return working;
  }

  private boolean isCoordinatorOneLeader(String coordinatorLeader)
  {
    return coordinatorLeader.contains(transformHost(config.getCoordinatorInternalHost()));
  }

  private boolean isOverlordOneLeader(String overlordLeader)
  {
    return overlordLeader.contains(transformHost(config.getOverlordInternalHost()));
  }

  /**
   * host + ':' which should be enough to distinguish subsets, e.g. 'druid-coordinator:8081' from
   * 'druid-coordinator-two:8081' for example
   */
  private static String transformHost(String host)
  {
    return StringUtils.format("%s:", host);
  }

  private void verifyCoordinatorCluster()
  {
    // Verify the basics: 4 service types, excluding the custom node role.
    // One of the two-node services has a size of 2.
    // This endpoint includes an entry for historicals, even if none are running.
    Map<String, Object> results = clusterClient.coordinatorCluster();
    assertEquals(5, results.size());
    @SuppressWarnings("unchecked")
    List<Object> coordNodes = (List<Object>) results.get(NodeRole.COORDINATOR.getJsonName());
    assertEquals(2, coordNodes.size());
    @SuppressWarnings("unchecked")
    List<Object> histNodes = (List<Object>) results.get(NodeRole.HISTORICAL.getJsonName());
    assertTrue(histNodes.isEmpty());
  }
}
