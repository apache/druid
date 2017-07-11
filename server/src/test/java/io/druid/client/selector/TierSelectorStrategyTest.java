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

package io.druid.client.selector;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.client.DirectDruidClient;
import io.druid.client.DruidServer;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.ServerType;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

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
            new ConnectionCountServerSelectorStrategy(), new CustomTierSelectorStrategyConfig()
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
            new Interval("2013-01-01/2013-01-02"),
            new DateTime("2013-01-01").toString(),
            Maps.<String, Object>newHashMap(),
            Lists.<String>newArrayList(),
            Lists.<String>newArrayList(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        tierSelectorStrategy
    );

    List<QueryableDruidServer> servers = Lists.newArrayList(expectedSelection);

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
