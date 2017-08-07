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

package io.druid.curator.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.curator.CuratorTestBase;
import io.druid.curator.announcement.Announcer;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscovery;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.guice.GuiceInjectors;
import io.druid.initialization.Initialization;
import io.druid.server.DruidNode;
import io.druid.server.initialization.CuratorDiscoveryConfig;
import io.druid.server.initialization.ServerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;

/**
 */
public class CuratorDruidNodeAnnouncerAndDiscoveryTest extends CuratorTestBase
{
  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
  }

  @Test(timeout = 10000)
  public void testAnnouncementAndDiscovery() throws Exception
  {
    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bind(Key.get(String.class, Names.named("serviceName"))).toInstance("some service");
                binder.bind(Key.get(Integer.class, Names.named("servicePort"))).toInstance(0);
                binder.bind(Key.get(Integer.class, Names.named("tlsServicePort"))).toInstance(-1);
              }
            }
        )
    );
    ObjectMapper objectMapper = injector.getInstance(ObjectMapper.class);

    curator.start();
    curator.blockUntilConnected();

    Announcer announcer = new Announcer(
        curator,
        MoreExecutors.sameThreadExecutor()
    );
    announcer.start();

    CuratorDruidNodeAnnouncer druidNodeAnnouncer = new CuratorDruidNodeAnnouncer(
        announcer,
        new CuratorDiscoveryConfig(),
        objectMapper
    );

    DiscoveryDruidNode node1 = new DiscoveryDruidNode(
        new DruidNode("s1", "h1", 8080, null, new ServerConfig()),
        DruidNodeDiscoveryProvider.NODE_TYPE_COORDINATOR,
        ImmutableMap.of()
    );

    DiscoveryDruidNode node2 = new DiscoveryDruidNode(
        new DruidNode("s2", "h2", 8080, null, new ServerConfig()),
        DruidNodeDiscoveryProvider.NODE_TYPE_COORDINATOR,
        ImmutableMap.of()
    );

    DiscoveryDruidNode node3 = new DiscoveryDruidNode(
        new DruidNode("s3", "h3", 8080, null, new ServerConfig()),
        DruidNodeDiscoveryProvider.NODE_TYPE_OVERLORD,
        ImmutableMap.of()
    );

    DiscoveryDruidNode node4 = new DiscoveryDruidNode(
        new DruidNode("s4", "h4", 8080, null, new ServerConfig()),
        DruidNodeDiscoveryProvider.NODE_TYPE_OVERLORD,
        ImmutableMap.of()
    );

    druidNodeAnnouncer.announce(node1);
    druidNodeAnnouncer.announce(node3);

    CuratorDruidNodeDiscoveryProvider druidNodeDiscoveryProvider = new CuratorDruidNodeDiscoveryProvider(
        curator,
        new CuratorDiscoveryConfig(),
        objectMapper
    );
    druidNodeDiscoveryProvider.start();

    DruidNodeDiscovery coordDiscovery = druidNodeDiscoveryProvider.getForNodeType(DruidNodeDiscoveryProvider.NODE_TYPE_COORDINATOR);
    DruidNodeDiscovery overlordDiscovery = druidNodeDiscoveryProvider.getForNodeType(DruidNodeDiscoveryProvider.NODE_TYPE_OVERLORD);

    while (!ImmutableSet.of(node1).equals(coordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    while (!ImmutableSet.of(node3).equals(overlordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    HashSet<DiscoveryDruidNode> coordNodes = new HashSet<>();
    coordDiscovery.registerListener(
        new DruidNodeDiscovery.Listener()
        {
          @Override
          public void nodeAdded(DiscoveryDruidNode node)
          {
            coordNodes.add(node);
          }

          @Override
          public void nodeRemoved(DiscoveryDruidNode node)
          {
            coordNodes.remove(node);
          }
        }
    );

    HashSet<DiscoveryDruidNode> overlordNodes = new HashSet<>();
    overlordDiscovery.registerListener(
        new DruidNodeDiscovery.Listener()
        {
          @Override
          public void nodeAdded(DiscoveryDruidNode node)
          {
            overlordNodes.add(node);
          }

          @Override
          public void nodeRemoved(DiscoveryDruidNode node)
          {
            overlordNodes.remove(node);
          }
        }
    );

    while (!ImmutableSet.of(node1).equals(coordNodes)) {
      Thread.sleep(100);
    }

    while (!ImmutableSet.of(node3).equals(overlordNodes)) {
      Thread.sleep(100);
    }

    druidNodeAnnouncer.announce(node2);
    druidNodeAnnouncer.announce(node4);

    while (!ImmutableSet.of(node1, node2).equals(coordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    while (!ImmutableSet.of(node3, node4).equals(overlordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    while (!ImmutableSet.of(node1, node2).equals(coordNodes)) {
      Thread.sleep(100);
    }

    while (!ImmutableSet.of(node3, node4).equals(overlordNodes)) {
      Thread.sleep(100);
    }

    druidNodeAnnouncer.unannounce(node1);
    druidNodeAnnouncer.unannounce(node2);
    druidNodeAnnouncer.unannounce(node3);
    druidNodeAnnouncer.unannounce(node4);

    while (!ImmutableSet.of().equals(coordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    while (!ImmutableSet.of().equals(overlordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    while (!ImmutableSet.of().equals(coordNodes)) {
      Thread.sleep(100);
    }

    while (!ImmutableSet.of().equals(overlordNodes)) {
      Thread.sleep(100);
    }

    druidNodeDiscoveryProvider.stop();
    announcer.stop();
  }

  @After
  public void tearDown()
  {
    tearDownServerAndCurator();
  }
}
