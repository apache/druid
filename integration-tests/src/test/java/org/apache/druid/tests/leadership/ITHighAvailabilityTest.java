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

package org.apache.druid.tests.leadership;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.cli.CliCustomNodeRole;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.http.ClusterResource;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModule;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing.utils.AbstractDruidClusterAdminClient;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Test(groups = TestNGGroup.HIGH_AVAILABILTY)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITHighAvailabilityTest
{
  private static final Logger LOG = new Logger(ITHighAvailabilityTest.class);
  private static final String SYSTEM_QUERIES_RESOURCE = "/queries/high_availability_sys.json";
  private static final String K8S_SYSTEM_QUERIES_RESOURCE = "/queries/k8s_high_availability_sys.json";
  private static final int NUM_LEADERSHIP_SWAPS = 3;

  private static final int NUM_RETRIES = 120;
  private static final long RETRY_DELAY = TimeUnit.SECONDS.toMillis(5);

  @Inject
  private IntegrationTestingConfig config;

  @Inject
  private AbstractDruidClusterAdminClient druidClusterAdminClient;

  @Inject
  CoordinatorResourceTestClient coordinatorClient;

  @Inject
  SqlTestQueryHelper queryHelper;

  @Inject
  ObjectMapper jsonMapper;

  @Inject
  @TestClient
  HttpClient httpClient;

  @Test
  public void testLeadershipChanges() throws Exception
  {
    int runCount = 0;
    String previousCoordinatorLeader = null;
    String previousOverlordLeader = null;
    // fetch current leaders, make sure queries work, then swap leaders and do it again
    do {
      LOG.info("%dth round of leader testing.", runCount);

      String coordinatorLeader = getLeader("coordinator");
      LOG.info("Coordinator Leader previous[%s], current[%s]", previousCoordinatorLeader, coordinatorLeader);

      String overlordLeader = getLeader("indexer");
      LOG.info("Overlord Leader previous[%s], current[%s]", previousOverlordLeader, overlordLeader);

      // we expect leadership swap to happen
      Assert.assertNotEquals(previousCoordinatorLeader, coordinatorLeader);
      Assert.assertNotEquals(previousOverlordLeader, overlordLeader);

      previousCoordinatorLeader = coordinatorLeader;
      previousOverlordLeader = overlordLeader;

      String queries = fillTemplate(
          config,
          AbstractIndexerTest.getResourceAsString(
              config.getDruidDeploymentEnvType() == DruidTestModule.DruidDeploymentEnvType.K8S ?
              K8S_SYSTEM_QUERIES_RESOURCE : SYSTEM_QUERIES_RESOURCE
          ),
          overlordLeader,
          coordinatorLeader
      );

      RetryUtils.retry(
          () -> {
            queryHelper.testQueriesFromString(queries);
            return true;
          },
          (Throwable th) -> true,
          10
      );

      swapLeadersAndWait(coordinatorLeader, overlordLeader);
      LOG.info("Leaders swapped.");
    } while (runCount++ < NUM_LEADERSHIP_SWAPS);
  }

  @Test
  public void testDiscoveryAndSelfDiscovery()
  {
    ITRetryUtil.retryUntil(
        () -> {
          try {
            Map<String, List<ClusterResource.Node>> clusterNodes = getClusterNodes();
            if (clusterNodes.get(NodeRole.COORDINATOR.getJsonName()).size() < 2 ||
                clusterNodes.get(NodeRole.OVERLORD.getJsonName()).size() < 2 ||
                clusterNodes.get(NodeRole.BROKER.getJsonName()).size() < 1 ||
                clusterNodes.get(NodeRole.ROUTER.getJsonName()).size() < 1) {
              return false;
            }

            List<ClusterResource.Node> allNodes = new ArrayList<>();
            clusterNodes.values().forEach((nodes) -> allNodes.addAll(nodes));

            return allNodes.size() == testSelfDiscovery(allNodes);
          }
          catch (Throwable t) {
            return false;
          }
        },
        true,
        RETRY_DELAY,
        NUM_RETRIES,
        "Standard services discovered"
    );
  }

  @Test
  public void testCustomDiscovery()
  {
    if (config.getDruidDeploymentEnvType() == DruidTestModule.DruidDeploymentEnvType.K8S) {
      // Custom NodeRole is not deployed in k8s environment just yet
      return;
    }

    ITRetryUtil.retryUntil(
        () -> {
          try {
            int count = testSelfDiscovery(getClusterNodes(CliCustomNodeRole.SERVICE_NAME));
            return count > 0;
          }
          catch (Throwable t) {
            return false;
          }
        },
        true,
        RETRY_DELAY,
        NUM_RETRIES,
        "Custom service discovered"
    );
  }

  private int testSelfDiscovery(Collection<ClusterResource.Node> nodes)
      throws MalformedURLException, ExecutionException, InterruptedException
  {
    int count = 0;

    for (ClusterResource.Node node : nodes) {
      String host = config.getDruidDeploymentEnvType() == DruidTestModule.DruidDeploymentEnvType.UNKNOWN ?
                    node.getHost() : config.getDruidClusterHost();

      final String location = StringUtils.format(
          "http://%s:%s/status/selfDiscovered",
          host,
          node.getPlaintextPort()
      );
      LOG.info("testing self discovery %s", location);
      StatusResponseHolder response = httpClient.go(
          new Request(HttpMethod.GET, new URL(location)),
          StatusResponseHandler.getInstance()
      ).get();
      LOG.info("%s responded with %s", location, response.getStatus().getCode());
      Assert.assertEquals(response.getStatus(), HttpResponseStatus.OK);
      count++;
    }
    return count;
  }

  private void swapLeadersAndWait(String coordinatorLeader, String overlordLeader)
  {
    Runnable waitUntilCoordinatorSupplier;
    if (isCoordinatorOneLeader(config, coordinatorLeader)) {
      druidClusterAdminClient.restartCoordinatorContainer();
      waitUntilCoordinatorSupplier = () -> druidClusterAdminClient.waitUntilCoordinatorReady();
    } else {
      druidClusterAdminClient.restartCoordinatorTwoContainer();
      waitUntilCoordinatorSupplier = () -> druidClusterAdminClient.waitUntilCoordinatorTwoReady();
    }

    Runnable waitUntilOverlordSupplier;
    if (isOverlordOneLeader(config, overlordLeader)) {
      druidClusterAdminClient.restartOverlordContainer();
      waitUntilOverlordSupplier = () -> druidClusterAdminClient.waitUntilIndexerReady();
    } else {
      druidClusterAdminClient.restartOverlordTwoContainer();
      waitUntilOverlordSupplier = () -> druidClusterAdminClient.waitUntilOverlordTwoReady();
    }
    waitUntilCoordinatorSupplier.run();
    waitUntilOverlordSupplier.run();
  }

  private String getLeader(String service)
  {
    try {
      return RetryUtils.retry(
          () -> tryGetLeader(service),
          (Throwable t) -> true,
          5
      );
    }
    catch (RuntimeException e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String tryGetLeader(String service)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.GET,
              new URL(StringUtils.format(
                  "%s/druid/%s/v1/leader",
                  config.getRouterUrl(),
                  service
              ))
          ),
          StatusResponseHandler.getInstance()
      ).get();

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while fetching leader from[%s] status[%s] content[%s]",
            config.getRouterUrl(),
            response.getStatus(),
            response.getContent()
        );
      }
      return response.getContent();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, List<ClusterResource.Node>> getClusterNodes()
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.GET,
              new URL(StringUtils.format(
                  "%s/druid/coordinator/v1/cluster",
                  config.getRouterUrl()
              ))
          ),
          StatusResponseHandler.getInstance()
      ).get();

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while fetching cluster nodes from[%s] status[%s] content[%s]",
            config.getRouterUrl(),
            response.getStatus(),
            response.getContent()
        );
      }

      return jsonMapper.readValue(
          response.getContent(),
          new TypeReference<Map<String, List<ClusterResource.Node>>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<ClusterResource.Node> getClusterNodes(String nodeRole)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.GET,
              new URL(StringUtils.format(
                  "%s/druid/coordinator/v1/cluster/%s",
                  config.getRouterUrl(),
                  nodeRole
              ))
          ),
          StatusResponseHandler.getInstance()
      ).get();

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while fetching cluster nodes from[%s] status[%s] content[%s]",
            config.getRouterUrl(),
            response.getStatus(),
            response.getContent()
        );
      }

      return jsonMapper.readValue(
          response.getContent(),
          new TypeReference<List<ClusterResource.Node>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String fillTemplate(IntegrationTestingConfig config, String template, String overlordLeader, String coordinatorLeader)
  {
    /*
      {"host":"%%BROKER%%","server_type":"broker", "is_leader": %%NON_LEADER%%},
      {"host":"%%COORDINATOR_ONE%%","server_type":"coordinator", "is_leader": %%COORDINATOR_ONE_LEADER%%},
      {"host":"%%COORDINATOR_TWO%%","server_type":"coordinator", "is_leader": %%COORDINATOR_TWO_LEADER%%},
      {"host":"%%OVERLORD_ONE%%","server_type":"overlord", "is_leader": %%OVERLORD_ONE_LEADER%%},
      {"host":"%%OVERLORD_TWO%%","server_type":"overlord", "is_leader": %%OVERLORD_TWO_LEADER%%},
      {"host":"%%ROUTER%%","server_type":"router", "is_leader": %%NON_LEADER%%}
     */
    String working = template;

    working = StringUtils.replace(working, "%%OVERLORD_ONE%%", config.getOverlordInternalHost());
    working = StringUtils.replace(working, "%%OVERLORD_TWO%%", config.getOverlordTwoInternalHost());
    working = StringUtils.replace(working, "%%COORDINATOR_ONE%%", config.getCoordinatorInternalHost());
    working = StringUtils.replace(working, "%%COORDINATOR_TWO%%", config.getCoordinatorTwoInternalHost());
    working = StringUtils.replace(working, "%%BROKER%%", config.getBrokerInternalHost());
    working = StringUtils.replace(working, "%%ROUTER%%", config.getRouterInternalHost());
    if (isOverlordOneLeader(config, overlordLeader)) {
      working = StringUtils.replace(working, "%%OVERLORD_ONE_LEADER%%", "1");
      working = StringUtils.replace(working, "%%OVERLORD_TWO_LEADER%%", "0");
    } else {
      working = StringUtils.replace(working, "%%OVERLORD_ONE_LEADER%%", "0");
      working = StringUtils.replace(working, "%%OVERLORD_TWO_LEADER%%", "1");
    }
    if (isCoordinatorOneLeader(config, coordinatorLeader)) {
      working = StringUtils.replace(working, "%%COORDINATOR_ONE_LEADER%%", "1");
      working = StringUtils.replace(working, "%%COORDINATOR_TWO_LEADER%%", "0");
    } else {
      working = StringUtils.replace(working, "%%COORDINATOR_ONE_LEADER%%", "0");
      working = StringUtils.replace(working, "%%COORDINATOR_TWO_LEADER%%", "1");
    }
    working = StringUtils.replace(working, "%%NON_LEADER%%", String.valueOf(NullHandling.defaultLongValue()));
    return working;
  }

  private static boolean isCoordinatorOneLeader(IntegrationTestingConfig config, String coordinatorLeader)
  {
    return coordinatorLeader.contains(transformHost(config.getCoordinatorInternalHost()));
  }

  private static boolean isOverlordOneLeader(IntegrationTestingConfig config, String overlordLeader)
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
}
