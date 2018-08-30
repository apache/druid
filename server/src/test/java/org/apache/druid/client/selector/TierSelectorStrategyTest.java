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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TierSelectorStrategyTest
{

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
            Maps.newHashMap(),
            Lists.newArrayList(),
            Lists.newArrayList(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        tierSelectorStrategy
    );

    List<QueryableDruidServer> servers = new ArrayList<>(Arrays.asList(expectedSelection));

    List<DruidServerMetadata> expectedCandidates = Lists.newArrayList();
    for (QueryableDruidServer server : servers) {
      expectedCandidates.add(server.getServer().getMetadata());
    }
    Collections.shuffle(servers);
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    Assert.assertEquals(expectedSelection[0], serverSelector.pick());
    Assert.assertEquals(expectedCandidates, serverSelector.getCandidates(-1));
    Assert.assertEquals(expectedCandidates.subList(0, 2), serverSelector.getCandidates(2));
  }

}
