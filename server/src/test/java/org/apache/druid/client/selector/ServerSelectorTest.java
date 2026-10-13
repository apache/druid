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

package org.apache.druid.client.selector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.BrokerViewOfCoordinatorConfig;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;


public class ServerSelectorTest
{
  @BeforeEach
  public void setUp()
  {
    TierSelectorStrategy tierSelectorStrategy = EasyMock.createMock(TierSelectorStrategy.class);
    EasyMock.expect(tierSelectorStrategy.getComparator()).andReturn(Integer::compare).anyTimes();
  }

  @Test
  public void testSegmentUpdate()
  {
    final ServerSelector selector = new ServerSelector(
        DataSegment.builder()
                   .dataSource("test_broker_server_view")
                   .interval(Intervals.of("2012/2013"))
                   .loadSpec(
                       ImmutableMap.of(
                           "type",
                           "local",
                           "path",
                           "somewhere"
                       )
                   )
                   .version("v1")
                   .dimensions(ImmutableList.of())
                   .metrics(ImmutableList.of())
                   .shardSpec(NoneShardSpec.instance())
                   .binaryVersion(9)
                   .size(0)
                   .build(),
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        HistoricalFilter.IDENTITY_FILTER
    );

    selector.addServerAndUpdateSegment(
        new QueryableDruidServer(
            new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
            EasyMock.createMock(DirectDruidClient.class)
        ),
        DataSegment.builder()
                   .dataSource(
                       "test_broker_server_view")
                   .interval(Intervals.of("2012/2013"))
                   .loadSpec(
                       ImmutableMap.of(
                           "type",
                           "local",
                           "path",
                           "somewhere"
                       )
                   )
                   .version("v1")
                   .dimensions(
                       ImmutableList.of(
                           "a",
                           "b",
                           "c"
                       ))
                   .metrics(
                       ImmutableList.of())
                   .shardSpec(NoneShardSpec.instance())
                   .binaryVersion(9)
                   .size(0)
                   .build()
    );

    Assertions.assertEquals(ImmutableList.of("a", "b", "c"), selector.getSegment().getDimensions());
  }

  @Test
  public void testSegmentCannotBeNull()
  {
    Assertions.assertThrows(NullPointerException.class, () -> {
      final ServerSelector selector = new ServerSelector(
          null,
          new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
          HistoricalFilter.IDENTITY_FILTER
      );
    });
  }

  @Test
  public void testSegmentWithNoData()
  {
    final ServerSelector selector = new ServerSelector(
        DataSegment.builder()
                   .dataSource("test_broker_server_view")
                   .interval(Intervals.of("2012/2013"))
                   .loadSpec(
                       ImmutableMap.of(
                           "type",
                           "tombstone"
                       )
                   )
                   .version("v1")
                   .dimensions(ImmutableList.of())
                   .metrics(ImmutableList.of())
                   .shardSpec(new TombstoneShardSpec())
                   .binaryVersion(9)
                   .size(0)
                   .build(),
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        HistoricalFilter.IDENTITY_FILTER
    );
    Assertions.assertFalse(selector.hasData());
  }

  @Test
  public void testSegmentWithData()
  {
    final ServerSelector selector = new ServerSelector(
        DataSegment.builder()
                   .dataSource("another segment") // fool the interner inside the selector
                   .interval(Intervals.of("2012/2013"))
                   .loadSpec(
                       ImmutableMap.of(
                           "type",
                           "local",
                           "path",
                           "somewhere"
                       )
                   )
                   .version("v1")
                   .dimensions(ImmutableList.of())
                   .metrics(ImmutableList.of())
                   .shardSpec(NoneShardSpec.instance())
                   .binaryVersion(9)
                   .size(0)
                   .build(),
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        HistoricalFilter.IDENTITY_FILTER
    );
    Assertions.assertTrue(selector.hasData());
  }

  @Test
  public void testPickFallsBackToRealtimeWhenEveryHistoricalIsExcluded()
  {
    final ServerSelector selector = makeSelector("test_clone_and_realtime");

    final QueryableDruidServer cloneTarget = addServer(selector, "clone:8083", ServerType.HISTORICAL);
    final QueryableDruidServer peon = addServer(selector, "peon:8100", ServerType.INDEXER_EXECUTOR);

    Assertions.assertEquals(peon, selector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assertions.assertEquals(
        List.of(peon.getServer().getMetadata()),
        selector.getCandidates(1, CloneQueryMode.EXCLUDECLONES)
    );

    // The clone target is queryable when clones are not excluded.
    Assertions.assertEquals(cloneTarget, selector.pick(null, CloneQueryMode.INCLUDECLONES));
  }

  @Test
  public void testPickReturnsNullWhenEveryHistoricalIsExcludedAndThereIsNoRealtimeServer()
  {
    final ServerSelector selector = makeSelector("test_clone_only");

    addServer(selector, "clone:8083", ServerType.HISTORICAL);

    Assertions.assertNull(selector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assertions.assertEquals(List.of(), selector.getCandidates(1, CloneQueryMode.EXCLUDECLONES));
  }

  @Test
  public void testPickPrefersHistoricalOverRealtimeWhenTheHistoricalIsNotExcluded()
  {
    final ServerSelector selector = makeSelector("test_source_and_realtime");

    final QueryableDruidServer cloneSource = addServer(selector, "source:8083", ServerType.HISTORICAL);
    addServer(selector, "peon:8100", ServerType.INDEXER_EXECUTOR);

    Assertions.assertEquals(cloneSource, selector.pick(null, CloneQueryMode.EXCLUDECLONES));
  }

  /**
   * Creates a selector whose {@link HistoricalFilter} treats "clone:8083" as a clone of "source:8083".
   */
  private static ServerSelector makeSelector(final String dataSource)
  {
    final BrokerViewOfCoordinatorConfig filter =
        new BrokerViewOfCoordinatorConfig(Mockito.mock(CoordinatorClient.class));
    filter.setDynamicConfig(
        CoordinatorDynamicConfig.builder()
                                .withCloneServers(Map.of("clone:8083", "source:8083"))
                                .build()
    );

    return new ServerSelector(
        DataSegment.builder(SegmentId.dummy(dataSource)).shardSpec(NoneShardSpec.instance()).build(),
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        filter
    );
  }

  private static QueryableDruidServer addServer(
      final ServerSelector selector,
      final String host,
      final ServerType serverType
  )
  {
    final QueryableDruidServer server = new QueryableDruidServer(
        new DruidServer(host, host, null, 0, null, serverType, DruidServer.DEFAULT_TIER, 0),
        EasyMock.createMock(DirectDruidClient.class)
    );
    selector.addServerAndUpdateSegment(server, selector.getSegment());
    return server;
  }
}
