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

package io.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
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
import java.util.Map;
import java.util.Set;

/**
 */
public class ZookeeperDruidServerDiscoveryTest extends CuratorTestBase
{

  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final DruidServerMetadata COORDINATOR_LEADER = new DruidServerMetadata(
      "coor1",
      "localhost",
      100,
      "coordinator",
      "1",
      0,
      "coordinator",
      "hostText",
      -1
  );
  private static final DruidServerMetadata OVERLORD_LEADER = new DruidServerMetadata(
      "over1",
      "localhost",
      100,
      "overlord",
      "1",
      0,
      "overlord",
      "hostText",
      -1
  );

  private static Map<String, Set<DruidServerMetadata>> cluster;
  private static Map<DruidServerMetadata, ZooKeeperServerAnnouncer> announcers;
  private static ZkPathsConfig ZK_PATHS_CONFIG = new ZkPathsConfig()
  {
    @Override
    public String getBase()
    {
      return "test/druid";
    }
  };

  private ZookeeperDruidServerDiscovery discovery;
  private Announcer announcer;

  @BeforeClass
  public static void setupStatic() throws Exception
  {
    cluster = ImmutableMap.<String, Set<DruidServerMetadata>>builder()
        .put(
            "historical",
            ImmutableSet.of(
                new DruidServerMetadata("hist1", "localhost", 100, "historical", "1", 0, "historical", "hostText", -1),
                new DruidServerMetadata("hist2", "localhost", 100, "historical", "1", 0, "historical", "hostText", -1)
            )
        )
        .put(
            "broker",
            ImmutableSet.of(
                new DruidServerMetadata("brok1", "localhost", 100, "broker", "1", 0, "broker", "hostText", -1),
                new DruidServerMetadata("brok2", "localhost", 100, "broker", "1", 0, "broker", "hostText", -1)
            )
        )
        .put(
            "overlord",
            ImmutableSet.of(
                OVERLORD_LEADER,
                new DruidServerMetadata("over2", "localhost", 100, "overlord", "1", 0, "overlord", "hostText", -1)
            )
        )
        .put(
            "coordinator",
            ImmutableSet.of(
                COORDINATOR_LEADER,
                new DruidServerMetadata("coor2", "localhost", 100, "coordinator", "1", 0, "coordinator", "hostText", -1)
            )
        )
        .put(
            "router",
            ImmutableSet.of(
                new DruidServerMetadata("rout1", "localhost", 100, "router", "1", 0, "router", "hostText", -1),
                new DruidServerMetadata("rout2", "localhost", 100, "router", "1", 0, "router", "hostText", -1)
            )
        )
        .put(
            "realtime",
            ImmutableSet.of(
                new DruidServerMetadata("real1", "localhost", 100, "realtime", "1", 0, "realtime", "hostText", -1),
                new DruidServerMetadata("real2", "localhost", 100, "realtime", "1", 0, "realtime", "hostText", -1)
            )
        )
        .put(
            "middleManager",
            ImmutableSet.of(
                new DruidServerMetadata(
                    "mm1",
                    "localhost",
                    100,
                    "middleManager",
                    "1",
                    0,
                    "middleManager",
                    "hostText",
                    -1
                ),
                new DruidServerMetadata(
                    "mm2",
                    "localhost",
                    100,
                    "middleManager",
                    "1",
                    0,
                    "middleManager",
                    "hostText",
                    -1
                )
            )
        )
        .build();
    announcers = new HashMap<>();
  }

  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();
    announcer = new Announcer(curator, MoreExecutors.sameThreadExecutor());
    announcer.start();

    for (Set<DruidServerMetadata> servers : cluster.values()) {
      for (DruidServerMetadata server : servers) {
        final ZooKeeperServerAnnouncer serverAnnouncer = new ZooKeeperServerAnnouncer(
            server,
            MAPPER,
            ZK_PATHS_CONFIG,
            announcer,
            ((server.getType().equals("realtime") || server.getType().equals("historical")) ?
             ImmutableSet.of("segmentServer") : ImmutableSet.<String>of())
        );
        serverAnnouncer.start();
        announcers.put(server, serverAnnouncer);
      }
    }

    announcers.get(COORDINATOR_LEADER).announceLeadership();
    announcers.get(OVERLORD_LEADER).announceLeadership();
    discovery = new ZookeeperDruidServerDiscovery(curator, MAPPER, ZK_PATHS_CONFIG);
  }

  @Test
  public void testDiscovery() throws Exception
  {
    Assert.assertEquals(cluster.get("historical"), Sets.newHashSet(discovery.getServersForType("historical")));
    Assert.assertEquals(cluster.get("broker"), Sets.newHashSet(discovery.getServersForType("broker")));
    Assert.assertEquals(cluster.get("realtime"), Sets.newHashSet(discovery.getServersForType("realtime")));
    Assert.assertEquals(cluster.get("overlord"), Sets.newHashSet(discovery.getServersForType("overlord")));
    Assert.assertEquals(cluster.get("middleManager"), Sets.newHashSet(discovery.getServersForType("middleManager")));
    Assert.assertEquals(cluster.get("router"), Sets.newHashSet(discovery.getServersForType("router")));
    Assert.assertEquals(ImmutableList.of(), discovery.getServersForType("null"));

    Assert.assertEquals(COORDINATOR_LEADER, discovery.getLeaderForType("coordinator"));
    Assert.assertEquals(OVERLORD_LEADER, discovery.getLeaderForType("overlord"));
    Assert.assertEquals(null, discovery.getLeaderForType("null"));

    Assert.assertEquals(
        cluster.get("historical"),
        Sets.newHashSet(discovery.getServersForTypeWithService("historical", "historical"))
    );
    Assert.assertEquals(
        cluster.get("broker"),
        Sets.newHashSet(discovery.getServersForTypeWithService("broker", "broker"))
    );
    Assert.assertEquals(
        cluster.get("realtime"),
        Sets.newHashSet(discovery.getServersForTypeWithService("realtime", "realtime"))
    );
    Assert.assertEquals(
        cluster.get("overlord"),
        Sets.newHashSet(discovery.getServersForTypeWithService("overlord", "overlord"))
    );
    Assert.assertEquals(
        cluster.get("middleManager"),
        Sets.newHashSet(discovery.getServersForTypeWithService("middleManager", "middleManager"))
    );
    Assert.assertEquals(
        cluster.get("router"),
        Sets.newHashSet(discovery.getServersForTypeWithService("router", "router"))
    );
    Assert.assertEquals(ImmutableList.of(), discovery.getServersForType("null"));

    Assert.assertEquals(
        Sets.union(cluster.get("historical"), cluster.get("realtime")),
        Sets.newHashSet(discovery.getServersWithCapability("segmentServer"))
    );
  }

  @After
  public void tearDown() throws Exception
  {
    for (ZooKeeperServerAnnouncer announcer : announcers.values()) {
      announcer.stop();
    }
    announcer.stop();
    tearDownServerAndCurator();
  }

}