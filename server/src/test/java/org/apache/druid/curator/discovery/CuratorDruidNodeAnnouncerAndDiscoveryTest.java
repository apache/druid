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

package org.apache.druid.curator.discovery;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.curator.announcement.Announcer;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.NodeType;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class CuratorDruidNodeAnnouncerAndDiscoveryTest extends CuratorTestBase
{
  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
  }

  @Test(timeout = 60_000L)
  public void testAnnouncementAndDiscovery() throws Exception
  {
    ObjectMapper objectMapper = new DefaultObjectMapper();

    //additional setup to serde DruidNode
    objectMapper.setInjectableValues(new InjectableValues.Std()
                                         .addValue(ServerConfig.class, new ServerConfig())
                                         .addValue("java.lang.String", "dummy")
                                         .addValue("java.lang.Integer", 1234)
    );

    curator.start();
    curator.blockUntilConnected();

    Announcer announcer = new Announcer(
        curator,
        Execs.directExecutor()
    );
    announcer.start();

    CuratorDruidNodeAnnouncer druidNodeAnnouncer = new CuratorDruidNodeAnnouncer(
        announcer,
        new ZkPathsConfig(),
        objectMapper
    );

    DiscoveryDruidNode node1 = new DiscoveryDruidNode(
        new DruidNode("s1", "h1", false, 8080, null, true, false),
        NodeType.COORDINATOR,
        ImmutableMap.of()
    );

    DiscoveryDruidNode node2 = new DiscoveryDruidNode(
        new DruidNode("s2", "h2", false, 8080, null, true, false),
        NodeType.COORDINATOR,
        ImmutableMap.of()
    );

    DiscoveryDruidNode node3 = new DiscoveryDruidNode(
        new DruidNode("s3", "h3", false, 8080, null, true, false),
        NodeType.OVERLORD,
        ImmutableMap.of()
    );

    DiscoveryDruidNode node4 = new DiscoveryDruidNode(
        new DruidNode("s4", "h4", false, 8080, null, true, false),
        NodeType.OVERLORD,
        ImmutableMap.of()
    );

    druidNodeAnnouncer.announce(node1);
    druidNodeAnnouncer.announce(node3);

    CuratorDruidNodeDiscoveryProvider druidNodeDiscoveryProvider = new CuratorDruidNodeDiscoveryProvider(
        curator,
        new ZkPathsConfig(),
        objectMapper
    );
    druidNodeDiscoveryProvider.start();

    DruidNodeDiscovery coordDiscovery = druidNodeDiscoveryProvider.getForNodeType(NodeType.COORDINATOR);
    DruidNodeDiscovery overlordDiscovery = druidNodeDiscoveryProvider.getForNodeType(NodeType.OVERLORD);

    while (!checkNodes(ImmutableSet.of(node1), coordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    while (!checkNodes(ImmutableSet.of(node3), overlordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    HashSet<DiscoveryDruidNode> coordNodes = new HashSet<>();
    coordDiscovery.registerListener(
        new DruidNodeDiscovery.Listener()
        {
          @Override
          public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
          {
            coordNodes.addAll(nodes);
          }

          @Override
          public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
          {
            coordNodes.removeAll(nodes);
          }
        }
    );

    HashSet<DiscoveryDruidNode> overlordNodes = new HashSet<>();
    overlordDiscovery.registerListener(
        new DruidNodeDiscovery.Listener()
        {
          @Override
          public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
          {
            overlordNodes.addAll(nodes);
          }

          @Override
          public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
          {
            overlordNodes.removeAll(nodes);
          }
        }
    );

    while (!checkNodes(ImmutableSet.of(node1), coordNodes)) {
      Thread.sleep(100);
    }

    while (!checkNodes(ImmutableSet.of(node3), overlordNodes)) {
      Thread.sleep(100);
    }

    druidNodeAnnouncer.announce(node2);
    druidNodeAnnouncer.announce(node4);

    while (!checkNodes(ImmutableSet.of(node1, node2), coordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    while (!checkNodes(ImmutableSet.of(node3, node4), overlordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    while (!checkNodes(ImmutableSet.of(node1, node2), coordNodes)) {
      Thread.sleep(100);
    }

    while (!checkNodes(ImmutableSet.of(node3, node4), overlordNodes)) {
      Thread.sleep(100);
    }

    druidNodeAnnouncer.unannounce(node1);
    druidNodeAnnouncer.unannounce(node2);
    druidNodeAnnouncer.unannounce(node3);
    druidNodeAnnouncer.unannounce(node4);

    while (!checkNodes(ImmutableSet.of(), coordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    while (!checkNodes(ImmutableSet.of(), overlordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    while (!coordNodes.isEmpty()) {
      Thread.sleep(100);
    }

    while (!overlordNodes.isEmpty()) {
      Thread.sleep(100);
    }

    druidNodeDiscoveryProvider.stop();
    announcer.stop();
  }

  private boolean checkNodes(Set<DiscoveryDruidNode> expected, Collection<DiscoveryDruidNode> actual)
  {
    return expected.equals(ImmutableSet.copyOf(actual));
  }

  @After
  public void tearDown()
  {
    tearDownServerAndCurator();
  }
}
