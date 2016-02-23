/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.TestDruidServerDiscovery;
import io.druid.curator.CuratorTestBase;
import io.druid.curator.announcement.Announcer;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.ZooKeeperServerAnnouncer;
import io.druid.server.initialization.ZkPathsConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 */
public class ClusterInfoResourceTest extends CuratorTestBase
{

  private static final ZkPathsConfig ZK_PATHS_CONFIG = new ZkPathsConfig()
  {
    @Override
    public String getBase()
    {
      return "test/druid";
    }
  };

  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  private static Map<String, List<DruidServerMetadata>> expectedNodes;
  private static Map<DruidServerMetadata, ZooKeeperServerAnnouncer> announcers;

  private ClusterInfoResource clusterInfoResource;

  @BeforeClass
  public static void setupStatic() throws Exception
  {
    expectedNodes = ImmutableMap.<String, List<DruidServerMetadata>>builder().put(
        "historical",
        ImmutableList.of(
            new DruidServerMetadata("hist1", "localhost", 100, "historical", "1", 0, "service", "hostText", -1),
            new DruidServerMetadata("hist2", "localhost", 100, "historical", "1", 0, "service", "hostText", -1)
        )
    ).put(
        "broker",
        ImmutableList.of(
            new DruidServerMetadata("brok1", "localhost", 100, "broker", "1", 0, "service", "hostText", -1),
            new DruidServerMetadata("brok2", "localhost", 100, "broker", "1", 0, "service", "hostText", -1)
        )
    ).put(
        "overlord",
        ImmutableList.of(
            new DruidServerMetadata("over1", "localhost", 100, "overlord", "1", 0, "service", "hostText", -1),
            new DruidServerMetadata("over2", "localhost", 100, "overlord", "1", 0, "service", "hostText", -1)
        )
    ).put(
        "coordinator",
        ImmutableList.of(
            new DruidServerMetadata("coor1", "localhost", 100, "coordinator", "1", 0, "service", "hostText", -1),
            new DruidServerMetadata("coor2", "localhost", 100, "coordinator", "1", 0, "service", "hostText", -1)
        )
    ).put(
        "router",
        ImmutableList.of(
            new DruidServerMetadata("rout1", "localhost", 100, "router", "1", 0, "service", "hostText", -1),
            new DruidServerMetadata("rout2", "localhost", 100, "router", "1", 0, "service", "hostText", -1)
        )
    ).put(
        "realtime",
        ImmutableList.of(
            new DruidServerMetadata("real1", "localhost", 100, "realtime", "1", 0, "service", "hostText", -1),
            new DruidServerMetadata("real2", "localhost", 100, "realtime", "1", 0, "service", "hostText", -1)
        )
    ).build();
    announcers = new HashMap<>();
  }

  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();
    clusterInfoResource = new ClusterInfoResource(new TestDruidServerDiscovery(expectedNodes));
    final Announcer announcer = new Announcer(curator, MoreExecutors.sameThreadExecutor());
    announcer.start();

    for (List<DruidServerMetadata> servers : expectedNodes.values()) {
      for (DruidServerMetadata server : servers) {

        ZooKeeperServerAnnouncer serverAnnouncer = new ZooKeeperServerAnnouncer(
            server,
            OBJECT_MAPPER,
            ZK_PATHS_CONFIG,
            announcer,
            new HashSet<String>()
        );
        serverAnnouncer.start();
        announcers.put(server, serverAnnouncer);
      }

    }
  }

  @After
  public void tearDown() throws Exception
  {
    for (ZooKeeperServerAnnouncer announcer : announcers.values()) {
      announcer.stop();
    }
    tearDownServerAndCurator();
  }

  @Test
  public void testGetClusterInfo() throws Exception
  {
    Map<String, List<DruidServerMetadata>> actualNodes =
        (Map<String, List<DruidServerMetadata>>) clusterInfoResource.getClusterInfo().getEntity();

    Assert.assertEquals(expectedNodes.keySet(), actualNodes.keySet());

    for (String nodeType : actualNodes.keySet()) {
      verifyNodes(expectedNodes.get(nodeType), actualNodes.get(nodeType));
    }
  }

  private void verifyNodes(List<DruidServerMetadata> expectedNodes, List<DruidServerMetadata> actualNodes)
  {
    Assert.assertEquals(expectedNodes.size(), actualNodes.size());
    Assert.assertEquals(new HashSet<>(expectedNodes), new HashSet<>(actualNodes));
  }

  @Test
  public void testGetHistoricalInfo() throws Exception
  {
    verifyNodes(
        expectedNodes.get("historical"),
        ((Map<String, List<DruidServerMetadata>>) clusterInfoResource.getHistoricalInfo()
                                                                     .getEntity()).get("historical")
    );
  }

  @Test
  public void testGetOverlordInfo() throws Exception
  {
    verifyNodes(
        expectedNodes.get("overlord"),
        ((Map<String, List<DruidServerMetadata>>) clusterInfoResource.getOverlordInfo()
                                                                     .getEntity()).get("overlord")
    );
  }

  @Test
  public void testGetBrokerInfo() throws Exception
  {
    verifyNodes(
        expectedNodes.get("broker"),
        ((Map<String, List<DruidServerMetadata>>) clusterInfoResource.getBrokerInfo()
                                                                     .getEntity()).get("broker")
    );
  }

  @Test
  public void testGetCoordinatorInfo() throws Exception
  {
    verifyNodes(
        expectedNodes.get("coordinator"),
        ((Map<String, List<DruidServerMetadata>>) clusterInfoResource.getCoordinatorInfo()
                                                                     .getEntity()).get("coordinator")
    );
  }

  @Test
  public void testGetRouterInfo() throws Exception
  {
    verifyNodes(
        expectedNodes.get("router"),
        ((Map<String, List<DruidServerMetadata>>) clusterInfoResource.getRouterInfo()
                                                                     .getEntity()).get("router")
    );
  }

  @Test
  public void testGetRealtimeInfo() throws Exception
  {
    verifyNodes(
        expectedNodes.get("realtime"),
        ((Map<String, List<DruidServerMetadata>>) clusterInfoResource.getRealtimeInfo()
                                                                     .getEntity()).get("realtime")
    );
  }
}
