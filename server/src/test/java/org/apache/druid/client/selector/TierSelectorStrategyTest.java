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
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.query.Query;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TierSelectorStrategyTest
{

  private StubServiceEmitter serviceEmitter;

  private static final Query SAMPLE_GROUPBY_QUERY = GroupByQuery.builder()
                                                                .setDataSource("foo3")
                                                                .setInterval(new MultipleIntervalSegmentSpec(List.of(Intervals.of("2000/3000"))))
                                                                .setGranularity(Granularities.ALL)
                                                                .setDimensions(new DefaultDimensionSpec("dim2", "d0"))
                                                                .build();

  @Before
  public void testSetup()
  {
    serviceEmitter = StubServiceEmitter.createStarted();
    EmittingLogger.registerEmitter(serviceEmitter);
  }

  @Test
  public void testHighestPriorityTierSelectorStrategyRealtime()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer lowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.REALTIME, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer highPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.REALTIME, DruidServer.DEFAULT_TIER, 1),
        client
    );

    testTierSelectorStrategy(
        new HighestPriorityTierSelectorStrategy(new ConnectionCountServerSelectorStrategy()),
        highPriority, lowPriority
    );
  }

  @Test
  public void testHighestPriorityTierSelectorStrategy()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer lowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer highPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );

    testTierSelectorStrategy(
        new HighestPriorityTierSelectorStrategy(new ConnectionCountServerSelectorStrategy()),
        highPriority, lowPriority
    );
  }

  @Test
  public void testLowestPriorityTierSelectorStrategy()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer lowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer highPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );

    testTierSelectorStrategy(
        new LowestPriorityTierSelectorStrategy(new ConnectionCountServerSelectorStrategy()),
        lowPriority, highPriority
    );
  }

  @Test
  public void testCustomPriorityTierSelectorStrategy()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer lowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    QueryableDruidServer mediumPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer highPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );

    testTierSelectorStrategy(
        new CustomTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new CustomTierSelectorStrategyConfig()
            {
              @Override
              public List<Integer> getPriorities()
              {
                return Arrays.asList(2, 0, -1, 1);
              }
            }
        ),
        mediumPriority, lowPriority, highPriority
    );
  }

  @Test
  public void testEmptyCustomPriorityTierSelectorStrategy()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer lowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    QueryableDruidServer mediumPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer highPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );

    testTierSelectorStrategy(
        new CustomTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new CustomTierSelectorStrategyConfig()
            {
              @Override
              public List<Integer> getPriorities()
              {
                return new ArrayList<>();
              }
            }
        ),
        highPriority, mediumPriority, lowPriority
    );
  }

  @Test
  public void testIncompleteCustomPriorityTierSelectorStrategy()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    QueryableDruidServer p1 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer p2 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );
    QueryableDruidServer p3 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 2),
        client
    );
    QueryableDruidServer p4 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 3),
        client
    );
    TierSelectorStrategy tierSelectorStrategy = new CustomTierSelectorStrategy(
        new ConnectionCountServerSelectorStrategy(),
        new CustomTierSelectorStrategyConfig()
        {
          @Override
          public List<Integer> getPriorities()
          {
            return Arrays.asList(2, 0, -1);
          }
        }
    );
    testTierSelectorStrategy(
        tierSelectorStrategy,
        p3, p1, p0, p4, p2
    );
  }

  private void testTierSelectorStrategy(
      TierSelectorStrategy tierSelectorStrategy,
      QueryableDruidServer... expectedSelection
  )
  {
    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2013-01-01/2013-01-02"),
            DateTimes.of("2013-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        tierSelectorStrategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableDruidServer> servers = new ArrayList<>(Arrays.asList(expectedSelection));

    List<DruidServerMetadata> expectedCandidates = new ArrayList<>();
    for (QueryableDruidServer server : servers) {
      expectedCandidates.add(server.getServer().getMetadata());
    }
    Collections.shuffle(servers);
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    Assert.assertEquals(expectedSelection[0], serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(expectedSelection[0], serverSelector.pick(EasyMock.createMock(Query.class), CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(expectedCandidates, serverSelector.getCandidates(-1, CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(expectedCandidates.subList(0, 2), serverSelector.getCandidates(2, CloneQueryMode.EXCLUDECLONES));
  }

  @Test
  public void testServerSelectorStrategyDefaults()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    Set<QueryableDruidServer> servers = new HashSet<>();
    servers.add(p0);
    RandomServerSelectorStrategy strategy = new RandomServerSelectorStrategy();
    Assert.assertEquals(strategy.pick(servers, EasyMock.createMock(DataSegment.class)), p0);
    Assert.assertEquals(
        strategy.pick(
            EasyMock.createMock(Query.class),
            servers,
            EasyMock.createMock(DataSegment.class)
        ), p0
    );
    ServerSelectorStrategy defaultDeprecatedServerSelectorStrategy = new ServerSelectorStrategy()
    {
      @Override
      public <T> List<QueryableDruidServer> pick(
          @Nullable Query<T> query, Set<QueryableDruidServer> servers, DataSegment segment,
          int numServersToPick
      )
      {
        return strategy.pick(servers, segment, numServersToPick);
      }
    };
    Assert.assertEquals(
        defaultDeprecatedServerSelectorStrategy.pick(servers, EasyMock.createMock(DataSegment.class)),
        p0
    );
    Assert.assertEquals(
        defaultDeprecatedServerSelectorStrategy.pick(servers, EasyMock.createMock(DataSegment.class), 1)
                                               .get(0), p0
    );
  }

  /**
   * Tests the PreferredTierSelectorStrategy with various configurations and expected selections.
   * It verifies
   * 1. The preferred tier is respected when picking a server.
   * 2. When getting all servers, the preferred tier is ignored, and the returned list is sorted by priority.
   * 3. When getting a limited number of candidates, it returns the top N servers with the preferred tier first.
   */
  private void testPreferredTierSelectorStrategy(
      PreferredTierSelectorStrategy tierSelectorStrategy,
      QueryableDruidServer... expectedSelection
  )
  {
    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2013-01-01/2013-01-02"),
            DateTimes.of("2013-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        tierSelectorStrategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableDruidServer> servers = new ArrayList<>(Arrays.asList(expectedSelection));

    List<DruidServerMetadata> expectedCandidates = new ArrayList<>();
    for (QueryableDruidServer server : servers) {
      expectedCandidates.add(server.getServer().getMetadata());
    }
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    // Verify that the preferred tier is respected when picking a server
    Assert.assertEquals(expectedSelection[0], serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(expectedSelection[0], serverSelector.pick(EasyMock.createMock(Query.class), CloneQueryMode.EXCLUDECLONES));

    // Verify that when getting all severs, the preferred tier is ignored, the returned list is sorted by priority
    List<DruidServerMetadata> allServers = new ArrayList<>(expectedCandidates);
    allServers.sort((o1, o2) -> tierSelectorStrategy.getComparator().compare(o1.getPriority(), o2.getPriority()));
    // verify the priority only because values with same priority may return in different order
    Assert.assertEquals(
        allServers.stream().map(DruidServerMetadata::getPriority).collect(Collectors.toList()),
        serverSelector.getCandidates(-1, CloneQueryMode.EXCLUDECLONES).stream().map(DruidServerMetadata::getPriority).collect(Collectors.toList())
    );

    // Verify that when getting a limited number of candidates, returns the top N servers with preferred tier first
    Assert.assertEquals(expectedCandidates.subList(0, 2), serverSelector.getCandidates(2, CloneQueryMode.EXCLUDECLONES));
  }

  @Test
  public void testPreferredTierSelectorStrategyHighestPriority()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);

    // Two servers that have same tier and priority
    QueryableDruidServer preferredTierLowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 0),
        client
    );
    QueryableDruidServer preferredTierHighPriority = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 1),
        client
    );

    QueryableDruidServer preferredTierHighPriority2 = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 1),
        client
    );

    QueryableDruidServer nonPreferredTierHighestPriority = new QueryableDruidServer(
        new DruidServer("test4", "localhost", null, 0, null, ServerType.HISTORICAL, "non-preferred", 2),
        client
    );

    PreferredTierSelectorStrategy tierSelectorStrategy = new PreferredTierSelectorStrategy(
        // Use a customized strategy that return the 2nd server
        new ServerSelectorStrategy()
        {
          @Override
          public List<QueryableDruidServer> pick(Set<QueryableDruidServer> servers, DataSegment segment, int numServersToPick)
          {
            if (servers.size() <= numServersToPick) {
              return ImmutableList.copyOf(servers);
            }
            List<QueryableDruidServer> list = new ArrayList<>(servers);
            if (numServersToPick == 1) {
              // return the server whose name is greater
              return list.stream()
                         .sorted((o1, o2) -> o1.getServer().getName().compareTo(o2.getServer().getName()))
                         .skip(1)
                         .limit(1)
                         .collect(Collectors.toList());
            } else {
              return list.stream().limit(numServersToPick).collect(Collectors.toList());
            }
          }
        },
        new PreferredTierSelectorStrategyConfig("preferred", "highest")
    );

    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2013-01-01/2013-01-02"),
            DateTimes.of("2013-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        tierSelectorStrategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableDruidServer> servers = new ArrayList<>(Arrays.asList(
        preferredTierLowPriority,
        preferredTierHighPriority,
        preferredTierHighPriority2,
        nonPreferredTierHighestPriority
    ));

    List<DruidServerMetadata> expectedCandidates = new ArrayList<>();
    for (QueryableDruidServer server : servers) {
      expectedCandidates.add(server.getServer().getMetadata());
    }
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    // Verify that the 2nd server is selected
    Assert.assertEquals(preferredTierHighPriority2, serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(preferredTierHighPriority2, serverSelector.pick(EasyMock.createMock(Query.class), CloneQueryMode.EXCLUDECLONES));

    // Verify that when getting all severs, the preferred tier is ignored, the returned list is sorted by priority
    List<DruidServerMetadata> allServers = new ArrayList<>(expectedCandidates);
    allServers.sort((o1, o2) -> tierSelectorStrategy.getComparator().compare(o1.getPriority(), o2.getPriority()));
    // verify the priority only because values with same priority may return in different order
    Assert.assertEquals(
        allServers.stream().map(DruidServerMetadata::getPriority).collect(Collectors.toList()),
        serverSelector.getCandidates(-1, CloneQueryMode.EXCLUDECLONES).stream().map(DruidServerMetadata::getPriority).collect(Collectors.toList())
    );

    // Verify that when getting 2 candidates, returns the top N servers with preferred tier first
    Assert.assertEquals(
        Arrays.asList(
            preferredTierHighPriority.getServer().getMetadata(),
            preferredTierHighPriority2.getServer().getMetadata()
        ),

        serverSelector.getCandidates(2, CloneQueryMode.EXCLUDECLONES)
                      .stream()
                      // sort the name to make sure the test is stable
                      .sorted((o1, o2) -> o1.getName().compareTo(o2.getName()))
                      .collect(Collectors.toList())
    );
  }

  @Test
  public void testPreferredTierSelectorStrategyLowestPriority()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer preferredTierLowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 0),
        client
    );
    QueryableDruidServer preferredTierHighPriority = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 1),
        client
    );
    QueryableDruidServer nonPreferredTierLowestPriority = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, "non-preferred", -1),
        client
    );

    testPreferredTierSelectorStrategy(
        new PreferredTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new PreferredTierSelectorStrategyConfig("preferred", "lowest")
        ),
        preferredTierLowPriority, preferredTierHighPriority, nonPreferredTierLowestPriority
    );
  }

  @Test
  public void testPreferredTierSelectorStrategyWithFallback()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    // Create only non-preferred tier servers with different priorities
    QueryableDruidServer nonPreferredTierLowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, "non-preferred", 0),
        client
    );
    QueryableDruidServer nonPreferredTierMediumPriority = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, "non-preferred", 1),
        client
    );
    QueryableDruidServer nonPreferredTierHighPriority = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, "non-preferred", 2),
        client
    );

    // Since no preferred tier servers are available, it should fall back to other servers
    // based on highest priority
    testPreferredTierSelectorStrategy(
        new PreferredTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new PreferredTierSelectorStrategyConfig("preferred", "highest")
        ),
        nonPreferredTierHighPriority, nonPreferredTierMediumPriority, nonPreferredTierLowPriority
    );
  }

  @Test
  public void testPreferredTierSelectorStrategyMixedServers()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer preferredTierLowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 0),
        client
    );
    QueryableDruidServer preferredTierHighPriority = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 1),
        client
    );
    QueryableDruidServer anotherTierHighPriority = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, "tier1", 3),
        client
    );
    QueryableDruidServer yetAnotherTierMediumPriority = new QueryableDruidServer(
        new DruidServer("test4", "localhost", null, 0, null, ServerType.HISTORICAL, "tier2", 2),
        client
    );

    // Should return preferred tier servers first, sorted by priority
    testPreferredTierSelectorStrategy(
        new PreferredTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new PreferredTierSelectorStrategyConfig("preferred", "highest")
        ),
        preferredTierHighPriority, preferredTierLowPriority, anotherTierHighPriority, yetAnotherTierMediumPriority
    );
  }

  @Test
  public void testPreferredTierSelectorStrategyDefaultPriority()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);

    QueryableDruidServer preferredTierLowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 0),
        client
    );
    QueryableDruidServer preferredTierHighPriority = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 1),
        client
    );
    QueryableDruidServer nonPreferredTierHighestPriority = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, "non-preferred", 2),
        client
    );

    testPreferredTierSelectorStrategy(
        new PreferredTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            // Using null for priority should default to highest priority
            new PreferredTierSelectorStrategyConfig("preferred", null)
        ),
        preferredTierHighPriority, preferredTierLowPriority, nonPreferredTierHighestPriority
    );
  }

  @Test
  public void testStrictTierSelectorStrategyAllConfigured()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer pNeg1 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer p2 = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 2),
        client
    );

    testTierSelectorStrategy(
        new StrictTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new StrictTierSelectorStrategyConfig(List.of(2, 0, -1)),
            serviceEmitter
        ),
        p2, p0, pNeg1
    );
  }

  @Test
  public void testStrictTierSelectorStrategyNoMatchingPriorities()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.INDEXER_EXECUTOR, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer p1 = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.INDEXER_EXECUTOR, DruidServer.DEFAULT_TIER, 1),
        client
    );
    QueryableDruidServer p2 = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.INDEXER_EXECUTOR, DruidServer.DEFAULT_TIER, 2),
        client
    );

    final ServerSelector serverSelector = new ServerSelector(
        DataSegment.builder(SegmentId.dummy("foo")).shardSpec(NoneShardSpec.instance()).build(),
        new StrictTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new StrictTierSelectorStrategyConfig(List.of(5, 6)),
            serviceEmitter
        ),
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableDruidServer> servers = List.of(p0, p1, p2);
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    // Should return null when no matching priorities
    Assert.assertNull(serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assert.assertNull(serverSelector.pick(SAMPLE_GROUPBY_QUERY, CloneQueryMode.EXCLUDECLONES));

    serviceEmitter.verifyEmitted("tierSelector/noServer", 1);

    // Should return empty list for getCandidates
    Assert.assertEquals(Collections.emptyList(), serverSelector.getCandidates(2, CloneQueryMode.EXCLUDECLONES));
  }

  @Test
  public void testEmptyStrictTierPrioritiesThrowsException()
  {
    DruidException ex = Assert.assertThrows(
        DruidException.class,
        () -> new StrictTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new StrictTierSelectorStrategyConfig(List.of()),
            serviceEmitter
        )
    );
    Assert.assertEquals(
        "priorities must be non-empty when using strict tier selector on the Broker. Found priorities[[]].",
        ex.getMessage()
    );
  }

  @Test
  public void testPooledTierSelectorWithRandomServerStrategy()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer pNeg1 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer p2 = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 2),
        client
    );

    TierSelectorStrategy strategy = new PooledTierSelectorStrategy(
        new RandomServerSelectorStrategy(),
        new PooledTierSelectorStrategyConfig(Set.of(2, 0, -1)),
        serviceEmitter
    );

    final ServerSelector serverSelector = new ServerSelector(
        DataSegment.builder(SegmentId.dummy("foo")).shardSpec(NoneShardSpec.instance()).build(),
        strategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableDruidServer> servers = List.of(pNeg1, p0, p2);
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    // All 3 servers should be configured
    List<DruidServerMetadata> allServers = serverSelector.getAllServers(CloneQueryMode.EXCLUDECLONES);
    Assert.assertEquals(3, allServers.size());
    Set<Integer> priorities = allServers.stream()
                                        .map(DruidServerMetadata::getPriority)
                                        .collect(Collectors.toSet());
    Assert.assertEquals(Set.of(-1, 0, 2), priorities);

    // Test getCandidates with different sizes - verify correct count
    List<DruidServerMetadata> candidates1 = serverSelector.getCandidates(1, CloneQueryMode.EXCLUDECLONES);
    Assert.assertEquals(1, candidates1.size());
    Assert.assertTrue(Set.of(-1, 0, 2).contains(candidates1.get(0).getPriority()));

    List<DruidServerMetadata> candidates2 = serverSelector.getCandidates(2, CloneQueryMode.EXCLUDECLONES);
    Assert.assertEquals(2, candidates2.size());

    List<DruidServerMetadata> candidates3 = serverSelector.getCandidates(3, CloneQueryMode.EXCLUDECLONES);
    Assert.assertEquals(3, candidates3.size());
    Set<Integer> candidates3Priorities = candidates3.stream()
                                                     .map(DruidServerMetadata::getPriority)
                                                     .collect(Collectors.toSet());
    Assert.assertEquals(Set.of(-1, 0, 2), candidates3Priorities);

    // Pick should return one of the three servers (any priority - demonstrates flattening)
    QueryableDruidServer picked = serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES);
    Assert.assertNotNull(picked);
    Assert.assertTrue(
        "Picked server should have one of the configured priorities",
        picked.getServer().getPriority() == -1 ||
        picked.getServer().getPriority() == 0 ||
        picked.getServer().getPriority() == 2
    );

    // Pick multiple times to verify servers from flattened pool are accessible
    Set<Integer> pickedPriorities = new HashSet<>();
    for (int i = 0; i < 20; i++) {
      QueryableDruidServer server = serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES);
      Assert.assertNotNull(server);
      pickedPriorities.add(server.getServer().getPriority());
    }
    // With RandomServerSelectorStrategy and 20 picks from 3 servers, we should see multiple priorities
    Assert.assertTrue(
        "Expected to see servers from multiple priorities, but only saw: " + pickedPriorities,
        pickedPriorities.size() >= 2
    );
  }

  @Test
  public void testPooledTierSelectorWithConnectionCountServerStrategy()
  {
    AtomicInteger c0 = new AtomicInteger(20);
    AtomicInteger c1 = new AtomicInteger(25);
    AtomicInteger c2a = new AtomicInteger(1);
    AtomicInteger c2b = new AtomicInteger(2);

    DirectDruidClient client0 = EasyMock.createMock(DirectDruidClient.class);
    EasyMock.expect(client0.getNumOpenConnections()).andAnswer(c0::get).anyTimes();

    DirectDruidClient client1 = EasyMock.createMock(DirectDruidClient.class);
    EasyMock.expect(client1.getNumOpenConnections()).andAnswer(c1::get).anyTimes();

    DirectDruidClient client2a = EasyMock.createMock(DirectDruidClient.class);
    EasyMock.expect(client2a.getNumOpenConnections()).andAnswer(c2a::get).anyTimes();

    DirectDruidClient client2b = EasyMock.createMock(DirectDruidClient.class);
    EasyMock.expect(client2b.getNumOpenConnections()).andAnswer(c2b::get).anyTimes();

    EasyMock.replay(client0, client1, client2a, client2b);

    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("p0", "localhost:8001", null, 0, null, ServerType.INDEXER_EXECUTOR, DruidServer.DEFAULT_TIER, 0),
        client0
    );
    QueryableDruidServer p1 = new QueryableDruidServer(
        new DruidServer("p1", "localhost:8002", null, 0, null, ServerType.INDEXER_EXECUTOR, DruidServer.DEFAULT_TIER, 1),
        client1
    );
    QueryableDruidServer p2a = new QueryableDruidServer(
        new DruidServer("p2a", "localhost:8003", null, 0, null, ServerType.INDEXER_EXECUTOR, DruidServer.DEFAULT_TIER, 2),
        client2a
    );
    QueryableDruidServer p2b = new QueryableDruidServer(
        new DruidServer("p2b", "localhost:8004", null, 0, null, ServerType.INDEXER_EXECUTOR, DruidServer.DEFAULT_TIER, 2),
        client2b
    );

    TierSelectorStrategy strategy = new PooledTierSelectorStrategy(
        new ConnectionCountServerSelectorStrategy(),
        new PooledTierSelectorStrategyConfig(Set.of(0, 1, 2)),
        serviceEmitter
    );

    ServerSelector selector = new ServerSelector(
        DataSegment.builder(SegmentId.dummy("foo")).shardSpec(NoneShardSpec.instance()).build(),
        strategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    for (QueryableDruidServer s : List.of(p0, p1, p2a, p2b)) {
      selector.addServerAndUpdateSegment(s, selector.getSegment());
    }

    for (int i = 0; i < 20; i++) {
      QueryableDruidServer picked = selector.pick(null, CloneQueryMode.EXCLUDECLONES);
      Assert.assertNotNull(picked);

      // Should always be one of the priority-2 replicas
      Assert.assertEquals(2, picked.getServer().getPriority());

      if (picked.getServer().equals(p2a.getServer())) {
        c2a.incrementAndGet();
      } else if (picked.getServer().equals(p2b.getServer())) {
        c2b.incrementAndGet();
      } else {
        Assert.fail("Expected pick to be either p2a or p2b but got: " + picked);
      }
    }

    // Now we just verify that all the servers are picked at least once
    final Set<DruidServer> pickedServers = new HashSet<>();
    for (int i = 0; i < 50; i++) {
      QueryableDruidServer picked = selector.pick(null, CloneQueryMode.EXCLUDECLONES);
      Assert.assertNotNull(picked);

      DruidServer pickedServer = picked.getServer();
      pickedServers.add(pickedServer);

      if (pickedServer.equals(p0.getServer())) {
        c0.incrementAndGet();
      } else if (pickedServer.equals(p1.getServer())) {
        c1.incrementAndGet();
      } else if (pickedServer.equals(p2a.getServer())) {
        c2a.incrementAndGet();
      } else {
        c2b.incrementAndGet();
      }
    }

    Assert.assertEquals(4, pickedServers.size());

    EasyMock.verify(client0, client1, client2a, client2b);
  }

  @Test
  public void testPooledTierSelectorStrategyWithNoServerCandidatesInPool()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer p1 = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );
    QueryableDruidServer p2 = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 2),
        client
    );

    TierSelectorStrategy strategy = new PooledTierSelectorStrategy(
        new ConnectionCountServerSelectorStrategy(),
        new PooledTierSelectorStrategyConfig(Set.of(5, 6)),
        serviceEmitter
    );

    final ServerSelector serverSelector = new ServerSelector(
        DataSegment.builder(SegmentId.dummy("foo")).shardSpec(NoneShardSpec.instance()).build(),
        strategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableDruidServer> servers = List.of(p0, p1, p2);
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    // Should return null since there are no matching priorities
    Assert.assertNull(serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assert.assertNull(serverSelector.pick(SAMPLE_GROUPBY_QUERY, CloneQueryMode.EXCLUDECLONES));

    Assert.assertEquals(List.of(), serverSelector.getCandidates(1, CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(List.of(), serverSelector.getCandidates(2, CloneQueryMode.EXCLUDECLONES));
  }

  @Test
  public void testEmptyPooledTierPrioritiesThrowsException()
  {
    DruidException ex = Assert.assertThrows(
        DruidException.class,
        () -> new PooledTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new PooledTierSelectorStrategyConfig(Set.of()),
            serviceEmitter
        )
    );
    Assert.assertEquals(
        "priorities must be non-empty when using pooled tier selector on the Broker. Found priorities[[]].",
        ex.getMessage()
    );
  }
}
