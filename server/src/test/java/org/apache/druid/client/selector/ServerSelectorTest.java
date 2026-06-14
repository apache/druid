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
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ServerSelectorTest
{
  @Before
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

    Assert.assertEquals(ImmutableList.of("a", "b", "c"), selector.getSegment().getDimensions());
  }

  @Test(expected = NullPointerException.class)
  public void testSegmentCannotBeNull()
  {
    final ServerSelector selector = new ServerSelector(
        null,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        HistoricalFilter.IDENTITY_FILTER
    );
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
    Assert.assertFalse(selector.hasData());
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
    Assert.assertTrue(selector.hasData());
  }

  @Test
  public void testExcludeSourceDoesNotUseRealtimeFallback()
  {
    final DataSegment segment = dataSegment();
    final QueryableDruidServer realtimeServer = queryableServer("realtime", ServerType.INDEXER_EXECUTOR);
    final ServerSelector selector = new ServerSelector(
        segment,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        HistoricalFilter.IDENTITY_FILTER
    );
    selector.addServerAndUpdateSegment(realtimeServer, segment);

    Assert.assertNull(selector.pick(null, CloneQueryMode.EXCLUDESOURCE));
    Assert.assertEquals(List.of(), selector.getCandidates(1, CloneQueryMode.EXCLUDESOURCE));
    Assert.assertEquals(List.of(), selector.getAllServers(CloneQueryMode.EXCLUDESOURCE));
    Assert.assertEquals(realtimeServer, selector.pick(null, CloneQueryMode.EXCLUDECLONES));
  }

  @Test
  public void testExcludeSourceDoesNotAddRealtimeCandidates()
  {
    final DataSegment segment = dataSegment();
    final QueryableDruidServer cloneServer = queryableServer("clone", ServerType.HISTORICAL);
    final QueryableDruidServer sourceServer = queryableServer("source", ServerType.HISTORICAL);
    final QueryableDruidServer realtimeServer = queryableServer("realtime", ServerType.INDEXER_EXECUTOR);
    final ServerSelector selector = new ServerSelector(
        segment,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        ServerSelectorTest::cloneOnlyFilter
    );
    selector.addServerAndUpdateSegment(cloneServer, segment);
    selector.addServerAndUpdateSegment(sourceServer, segment);
    selector.addServerAndUpdateSegment(realtimeServer, segment);

    Assert.assertEquals(cloneServer, selector.pick(null, CloneQueryMode.EXCLUDESOURCE));
    Assert.assertEquals(
        List.of(cloneServer.getServer().getMetadata()),
        selector.getCandidates(3, CloneQueryMode.EXCLUDESOURCE)
    );
    Assert.assertEquals(
        List.of(cloneServer.getServer().getMetadata()),
        selector.getAllServers(CloneQueryMode.EXCLUDESOURCE)
    );
  }

  private static Int2ObjectRBTreeMap<Set<QueryableDruidServer>> cloneOnlyFilter(
      final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> historicalServers,
      final CloneQueryMode mode
  )
  {
    if (mode != CloneQueryMode.EXCLUDESOURCE) {
      return historicalServers;
    }

    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> filteredHistoricals = new Int2ObjectRBTreeMap<>();
    for (final int priority : historicalServers.keySet()) {
      filteredHistoricals.put(
          priority,
          historicalServers.get(priority)
                           .stream()
                           .filter(server -> "clone".equals(server.getServer().getHost()))
                           .collect(Collectors.toSet())
      );
    }
    return filteredHistoricals;
  }

  private static QueryableDruidServer queryableServer(final String host, final ServerType serverType)
  {
    return new QueryableDruidServer(
        new DruidServer(host, host, null, 0, null, serverType, DruidServer.DEFAULT_TIER, 1),
        EasyMock.createMock(DirectDruidClient.class)
    );
  }

  private static DataSegment dataSegment()
  {
    return DataSegment.builder()
                      .dataSource("exclude_source_selector_test")
                      .interval(Intervals.of("2014/2015"))
                      .loadSpec(
                          ImmutableMap.of(
                              "type",
                              "local",
                              "path",
                              "somewhere"
                          )
                      )
                      .version("v2")
                      .dimensions(ImmutableList.of())
                      .metrics(ImmutableList.of())
                      .shardSpec(NoneShardSpec.instance())
                      .binaryVersion(9)
                      .size(0)
                      .build();
  }
}
