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

package org.apache.druid.client;

import com.google.common.util.concurrent.Futures;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class BrokerViewOfCoordinatorConfigTest
{
  private BrokerViewOfCoordinatorConfig target;

  private CoordinatorClient coordinatorClient;
  private CoordinatorDynamicConfig config;


  @BeforeEach
  public void setUp() throws Exception
  {
    config = CoordinatorDynamicConfig.builder()
                                     .withCloneServers(Map.of("host1", "host2"))
                                     .build();
    coordinatorClient = Mockito.mock(CoordinatorClient.class);
    Mockito.when(coordinatorClient.getCoordinatorDynamicConfig()).thenReturn(Futures.immediateFuture(config));
    target = new BrokerViewOfCoordinatorConfig(coordinatorClient);
  }

  @Test
  public void testFetchesConfigOnStartup()
  {
    target.start();
    Mockito.verify(coordinatorClient, Mockito.times(1)).getCoordinatorDynamicConfig();
    Assertions.assertEquals(config, target.getDynamicConfig());
  }

  @Test
  public void testExcludeClonesFiltersTargetCloneServers()
  {
    target.start();
    Int2ObjectRBTreeMap<Set<QueryableDruidServer>> servers = makeServers("host1", "host2", "host3");

    Int2ObjectRBTreeMap<Set<QueryableDruidServer>> result =
        target.getQueryableServers(servers, CloneQueryMode.EXCLUDECLONES);

    Set<String> hosts = extractHosts(result);
    Assertions.assertFalse(hosts.contains("host1"), "target clone server host1 should be filtered");
    Assertions.assertTrue(hosts.contains("host2"), "source clone server host2 should remain");
    Assertions.assertTrue(hosts.contains("host3"), "non-clone server host3 should remain");
  }

  @Test
  public void testPreferClonesFiltersSourceCloneServers()
  {
    target.start();
    Int2ObjectRBTreeMap<Set<QueryableDruidServer>> servers = makeServers("host1", "host2", "host3");

    Int2ObjectRBTreeMap<Set<QueryableDruidServer>> result =
        target.getQueryableServers(servers, CloneQueryMode.PREFERCLONES);

    Set<String> hosts = extractHosts(result);
    Assertions.assertTrue(hosts.contains("host1"), "target clone server host1 should remain");
    Assertions.assertFalse(hosts.contains("host2"), "source clone server host2 should be filtered");
    Assertions.assertTrue(hosts.contains("host3"), "non-clone server host3 should remain");
  }

  @Test
  public void testIncludeClonesReturnsAll()
  {
    target.start();
    Int2ObjectRBTreeMap<Set<QueryableDruidServer>> servers = makeServers("host1", "host2", "host3");

    Int2ObjectRBTreeMap<Set<QueryableDruidServer>> result =
        target.getQueryableServers(servers, CloneQueryMode.INCLUDECLONES);

    Assertions.assertSame(servers, result, "INCLUDECLONES should return the original map");
  }

  @Test
  public void testConfigUpdateChangesFiltering()
  {
    target.start();

    CoordinatorDynamicConfig newConfig = CoordinatorDynamicConfig.builder()
                                                                 .withCloneServers(Map.of("host3", "host1"))
                                                                 .build();
    target.setDynamicConfig(newConfig);

    Int2ObjectRBTreeMap<Set<QueryableDruidServer>> servers = makeServers("host1", "host2", "host3");

    Int2ObjectRBTreeMap<Set<QueryableDruidServer>> result =
        target.getQueryableServers(servers, CloneQueryMode.EXCLUDECLONES);

    Set<String> hosts = extractHosts(result);
    Assertions.assertFalse(hosts.contains("host3"), "new target clone host3 should be filtered");
    Assertions.assertTrue(hosts.contains("host1"), "host1 is now source, should remain");
    Assertions.assertTrue(hosts.contains("host2"), "host2 is unrelated, should remain");
  }

  /**
   * Creates a priority-to-servers map with all servers at priority 0.
   *
   * @param hosts host names to create historical servers for
   * @return map of priority to queryable server set, matching the structure used by
   *         {@link BrokerViewOfCoordinatorConfig#getQueryableServers}
   */
  private static Int2ObjectRBTreeMap<Set<QueryableDruidServer>> makeServers(String... hosts)
  {
    Int2ObjectRBTreeMap<Set<QueryableDruidServer>> map = new Int2ObjectRBTreeMap<>();
    Set<QueryableDruidServer> serverSet = new HashSet<>();
    for (String host : hosts) {
      DruidServer druidServer = new DruidServer(host, host, null, 100, null, ServerType.HISTORICAL, "tier1", 0);
      serverSet.add(new QueryableDruidServer(druidServer, Mockito.mock(QueryRunner.class)));
    }
    map.put(0, serverSet);
    return map;
  }

  /**
   * Flattens the priority-to-servers map into a set of host names for easy assertion.
   *
   * @param servers priority-to-servers map returned by {@link BrokerViewOfCoordinatorConfig#getQueryabl
  eServers}
   * @return set of host names across all priority levels
   */
  private static Set<String> extractHosts(Int2ObjectRBTreeMap<Set<QueryableDruidServer>> servers)
  {
    Set<String> hosts = new HashSet<>();
    for (Set<QueryableDruidServer> serverSet : servers.values()) {
      for (QueryableDruidServer server : serverSet) {
        hosts.add(server.getServer().getHost());
      }
    }
    return hosts;
  }
}
