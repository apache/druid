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

import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.Query;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TierSelectorStrategyTest
{
  
  @Test
  public void testHighestPriorityTierSelectorStrategyRealtime()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer lowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.REALTIME, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer highPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.REALTIME, DruidServer.DEFAULT_TIER, 1),
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
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer highPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
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
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer highPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
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
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    QueryableDruidServer mediumPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer highPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
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
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    QueryableDruidServer mediumPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer highPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
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
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    QueryableDruidServer p1 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer p2 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );
    QueryableDruidServer p3 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 2),
        client
    );
    QueryableDruidServer p4 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 3),
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
        tierSelectorStrategy
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

    Assert.assertEquals(expectedSelection[0], serverSelector.pick(null));
    Assert.assertEquals(expectedSelection[0], serverSelector.pick(EasyMock.createMock(Query.class)));
    Assert.assertEquals(expectedCandidates, serverSelector.getCandidates(-1));
    Assert.assertEquals(expectedCandidates.subList(0, 2), serverSelector.getCandidates(2));
  }
  
  @Test
  public void testServerSelectorStrategyDefaults()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    Set<QueryableDruidServer> servers = new HashSet<>();
    servers.add(p0);
    RandomServerSelectorStrategy strategy = new RandomServerSelectorStrategy();
    Assert.assertEquals(strategy.pick(servers, EasyMock.createMock(DataSegment.class)), p0);
    Assert.assertEquals(strategy.pick(EasyMock.createMock(Query.class), servers, EasyMock.createMock(DataSegment.class)), p0);
    ServerSelectorStrategy defaultDeprecatedServerSelectorStrategy = new ServerSelectorStrategy() {
      @Override
      public <T> List<QueryableDruidServer> pick(@Nullable Query<T> query, Set<QueryableDruidServer> servers, DataSegment segment,
          int numServersToPick)
      {
        return strategy.pick(servers, segment, numServersToPick);
      }
    };
    Assert.assertEquals(defaultDeprecatedServerSelectorStrategy.pick(servers, EasyMock.createMock(DataSegment.class)), p0);
    Assert.assertEquals(defaultDeprecatedServerSelectorStrategy.pick(servers, EasyMock.createMock(DataSegment.class), 1).get(0), p0);
  }

}
