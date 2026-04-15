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
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public class BrokerViewOfCoordinatorConfigTest
{
  private BrokerViewOfCoordinatorConfig target;

  private CoordinatorClient coordinatorClient;
  private CoordinatorDynamicConfig config;


  @Before
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
    Assert.assertEquals(config, target.getDynamicConfig());
  }

  @Test
  public void testGetQueryableServers_noFiltersOrTurbo_returnsOriginalMapReference()
  {
    target.setDynamicConfig(CoordinatorDynamicConfig.builder().build());

    final QueryableDruidServer server = historical("h1:8080", 0);
    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> input =
        new Int2ObjectRBTreeMap<>(Comparator.reverseOrder());
    input.put(0, Set.of(server));

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> result =
        target.getQueryableServers(input, CloneQueryMode.INCLUDECLONES);

    // Fast path: no filtering needed, return the original map reference unchanged.
    Assert.assertSame(input, result);
  }

  @Test
  public void testGetQueryableServers_turboNodeDemoted_highestPriorityStrategy()
  {
    target.setDynamicConfig(
        CoordinatorDynamicConfig
            .builder()
            .withTurboLoadingNodes(Set.of("turbo:8080"))
            .build()
    );

    final QueryableDruidServer nonTurboHighTier = historical("normal-high:8080", 10);
    final QueryableDruidServer nonTurboLowTier = historical("normal-low:8080", 5);
    final QueryableDruidServer turbo = historical("turbo:8080", 10);

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> input =
        new Int2ObjectRBTreeMap<>(Comparator.reverseOrder());
    input.put(10, Set.of(nonTurboHighTier, turbo));
    input.put(5, Set.of(nonTurboLowTier));

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> result =
        target.getQueryableServers(input, CloneQueryMode.INCLUDECLONES);

    // Expected iteration order for highest-priority-first comparator:
    //   10 -> [nonTurboHighTier]
    //    5 -> [nonTurboLowTier]
    //  MIN -> [turbo] (dead last)
    Assert.assertEquals(
        Arrays.asList(10, 5, Integer.MIN_VALUE),
        new ArrayList<>(result.keySet())
    );
    Assert.assertEquals(Set.of(nonTurboHighTier), result.get(10));
    Assert.assertEquals(Set.of(nonTurboLowTier), result.get(5));
    Assert.assertEquals(Set.of(turbo), result.get(Integer.MIN_VALUE));
  }

  @Test
  public void testGetQueryableServers_turboNodeDemoted_lowestPriorityStrategy()
  {
    target.setDynamicConfig(
        CoordinatorDynamicConfig
            .builder()
            .withTurboLoadingNodes(Set.of("turbo:8080"))
            .build()
    );

    final QueryableDruidServer nonTurboLow = historical("normal-low:8080", 5);
    final QueryableDruidServer nonTurboHigh = historical("normal-high:8080", 10);
    final QueryableDruidServer turbo = historical("turbo:8080", 5);

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> input =
        new Int2ObjectRBTreeMap<>(Comparator.naturalOrder());
    input.put(5, Set.of(nonTurboLow, turbo));
    input.put(10, Set.of(nonTurboHigh));

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> result =
        target.getQueryableServers(input, CloneQueryMode.INCLUDECLONES);

    // Expected iteration order for lowest-priority-first comparator:
    //    5 -> [nonTurboLow]
    //   10 -> [nonTurboHigh]
    //  MAX -> [turbo] (dead last)
    Assert.assertEquals(
        Arrays.asList(5, 10, Integer.MAX_VALUE),
        new ArrayList<>(result.keySet())
    );
    Assert.assertEquals(Set.of(nonTurboLow), result.get(5));
    Assert.assertEquals(Set.of(nonTurboHigh), result.get(10));
    Assert.assertEquals(Set.of(turbo), result.get(Integer.MAX_VALUE));
  }

  @Test
  public void testGetQueryableServers_allTurbo_stillQueryableAsFallback()
  {
    target.setDynamicConfig(
        CoordinatorDynamicConfig
            .builder()
            .withTurboLoadingNodes(Set.of("turbo1:8080", "turbo2:8080"))
            .build()
    );

    final QueryableDruidServer turbo1 = historical("turbo1:8080", 10);
    final QueryableDruidServer turbo2 = historical("turbo2:8080", 5);

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> input =
        new Int2ObjectRBTreeMap<>(Comparator.reverseOrder());
    input.put(10, Set.of(turbo1));
    input.put(5, Set.of(turbo2));

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> result =
        target.getQueryableServers(input, CloneQueryMode.INCLUDECLONES);

    // Original tier buckets are emptied out and removed; all turbo servers collapse
    // into the dead-last bucket so queries still have somewhere to land.
    Assert.assertEquals(
        Collections.singletonList(Integer.MIN_VALUE),
        new ArrayList<>(result.keySet())
    );
    Assert.assertEquals(Set.of(turbo1, turbo2), result.get(Integer.MIN_VALUE));
  }

  @Test
  public void testGetQueryableServers_turboPlusCloneFilter_bothApplied()
  {
    target.setDynamicConfig(
        CoordinatorDynamicConfig
            .builder()
            .withCloneServers(Map.of("clone:8080", "source:8080"))
            .withTurboLoadingNodes(Set.of("turbo:8080"))
            .build()
    );

    final QueryableDruidServer source = historical("source:8080", 0);
    final QueryableDruidServer clone = historical("clone:8080", 0);
    final QueryableDruidServer turbo = historical("turbo:8080", 0);

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> input =
        new Int2ObjectRBTreeMap<>(Comparator.reverseOrder());
    input.put(0, Set.of(source, clone, turbo));

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> result =
        target.getQueryableServers(input, CloneQueryMode.EXCLUDECLONES);

    // EXCLUDECLONES drops the clone entirely; turbo is demoted to dead-last.
    Assert.assertEquals(Set.of(source), result.get(0));
    Assert.assertEquals(Set.of(turbo), result.get(Integer.MIN_VALUE));
    Assert.assertFalse(result.values().stream().flatMap(Set::stream).anyMatch(s -> s == clone));
  }

  @Test
  public void testGetQueryableServers_emptiedTier_priorityKeyOmitted()
  {
    target.setDynamicConfig(
        CoordinatorDynamicConfig
            .builder()
            .withCloneServers(Map.of("clone:8080", "source:8080"))
            .build()
    );

    final QueryableDruidServer clone = historical("clone:8080", 3);

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> input =
        new Int2ObjectRBTreeMap<>(Comparator.reverseOrder());
    input.put(3, Set.of(clone));

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> result =
        target.getQueryableServers(input, CloneQueryMode.EXCLUDECLONES);

    // Tier 3 was the only tier and its only server was clone-filtered; the priority
    // key should be absent rather than present with an empty set.
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testSetDynamicConfig_updatesCachedTurboSet()
  {
    target.setDynamicConfig(
        CoordinatorDynamicConfig
            .builder()
            .withTurboLoadingNodes(Set.of("turbo-v1:8080"))
            .build()
    );

    final QueryableDruidServer serverV1 = historical("turbo-v1:8080", 0);
    final QueryableDruidServer serverV2 = historical("turbo-v2:8080", 0);

    Int2ObjectRBTreeMap<Set<QueryableDruidServer>> input =
        new Int2ObjectRBTreeMap<>(Comparator.reverseOrder());
    input.put(0, Set.of(serverV1, serverV2));

    Int2ObjectRBTreeMap<Set<QueryableDruidServer>> result =
        target.getQueryableServers(input, CloneQueryMode.INCLUDECLONES);
    Assert.assertEquals(Set.of(serverV2), result.get(0));
    Assert.assertEquals(Set.of(serverV1), result.get(Integer.MIN_VALUE));

    // Swap which server is in turbo mode.
    target.setDynamicConfig(
        CoordinatorDynamicConfig.builder()
                                .withTurboLoadingNodes(Set.of("turbo-v2:8080"))
                                .build()
    );

    input = new Int2ObjectRBTreeMap<>(Comparator.reverseOrder());
    input.put(0, Set.of(serverV1, serverV2));

    result = target.getQueryableServers(input, CloneQueryMode.INCLUDECLONES);
    Assert.assertEquals(Set.of(serverV1), result.get(0));
    Assert.assertEquals(Set.of(serverV2), result.get(Integer.MIN_VALUE));
  }

  /**
   * Build a {@link QueryableDruidServer} representing a historical at the given host and priority.
   * The {@code hostAndPort} string doubles as the server name — the broker's turbo and clone
   * lookups both key off {@link DruidServer#getHost()}, so tests need the host string under
   * their control. The query runner is left null since these tests never dispatch queries;
   * they only exercise the routing/filtering logic in
   * {@link BrokerViewOfCoordinatorConfig#getQueryableServers}.
   */
  private static QueryableDruidServer historical(String hostAndPort, int priority)
  {
    final DruidServer server = new DruidServer(
        hostAndPort,
        hostAndPort,
        null,
        0L,
        null,
        ServerType.HISTORICAL,
        DruidServer.DEFAULT_TIER,
        priority
    );
    return new QueryableDruidServer(server, null);
  }

}
