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

package org.apache.druid.server.coordinator.balancer;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class EnumeratedDistributionBalancerStrategyTest
{
  private ServerHolder holder1;
  private ServerHolder holder2;
  private ServerHolder holder3;
  private ServerHolder holder4;

  private DataSegment segment1;
  private DataSegment segment2;
  private DataSegment segment3;
  private DataSegment segment4;

  @Before
  public void setUp()
  {
    segment1 = new DataSegment(
        "datasource1",
        Intervals.utc(0, 1),
        "",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        10L
    );
    segment2 = new DataSegment(
        "datasource2",
        Intervals.utc(0, 1),
        "",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        50L
    );
    segment3 = new DataSegment(
        "datasource3",
        Intervals.utc(0, 1),
        "",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        90L
    );
    segment4 = new DataSegment(
        "datasource4",
        Intervals.utc(0, 1),
        "",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        5L
    );
    // 10% utilized
    holder1 = new ServerHolder(
        new DruidServer("server1", "host1", null, 100L, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0).addDataSegment(segment1).toImmutableDruidServer(),
        new TestLoadQueuePeon());
    // 90% utilized
    holder2 = new ServerHolder(
        new DruidServer("server2", "host2", null, 100L, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0).addDataSegment(segment3).toImmutableDruidServer(),
        new TestLoadQueuePeon());
    // 50% utilized
    holder3 = new ServerHolder(
        new DruidServer("server3", "host3", null, 100L, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0).addDataSegment(segment2).toImmutableDruidServer(),
        new TestLoadQueuePeon());
    // 5% utilized
    holder4 = new ServerHolder(
        new DruidServer("server4", "host4", null, 100L, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0).addDataSegment(segment4).toImmutableDruidServer(),
        new TestLoadQueuePeon());
  }

  @Test
  public void testpickServersToDrop()
  {
    List<ServerHolder> serverHolders = new ArrayList<>();
    serverHolders.add(holder1);
    serverHolders.add(holder2);
    serverHolders.add(holder3);

    BalancerStrategy balancerStrategy = new EnumeratedDistributionBalancerStrategy();
    Iterator<ServerHolder> iterator = balancerStrategy.findServersToDropSegment(segment1, serverHolders);
    Assert.assertEquals(holder2, iterator.next());
    Assert.assertEquals(holder3, iterator.next());
    Assert.assertEquals(holder1, iterator.next());
  }
  @Test
  public void testFindServersToLoadSegment()
  {
    List<ServerHolder> serverHolders = new ArrayList<>();
    serverHolders.add(holder1);
    serverHolders.add(holder2);
    serverHolders.add(holder3);
    serverHolders.add(holder4);

    BalancerStrategy balancerStrategy = new EnumeratedDistributionBalancerStrategy();
    Iterator<ServerHolder> pickList = balancerStrategy.findServersToLoadSegment(segment4, serverHolders);
    Assert.assertNotNull(pickList);

    HashMap<String, Integer> pickMap = new HashMap<>();
    pickMap.put(holder1.getServer().getName(), 0);
    pickMap.put(holder2.getServer().getName(), 0);
    pickMap.put(holder3.getServer().getName(), 0);
    pickMap.put(holder4.getServer().getName(), 0);

    for (int i = 0; i < 10000; i++) {
      pickList = balancerStrategy.findServersToLoadSegment(segment4, serverHolders);
      ServerHolder myPick = pickList.next();
      Integer curNum = pickMap.get(myPick.getServer().getName());
      pickMap.put(myPick.getServer().getName(), curNum + 1);
    }
    Assert.assertEquals(0L, (long) pickMap.get(holder4.getServer().getName()));

    Assert.assertTrue(pickMap.get(holder3.getServer().getName()) > pickMap.get(holder2.getServer().getName()));
    // Transitive property makes 3 greater than 2 as well
    Assert.assertTrue(pickMap.get(holder1.getServer().getName()) > pickMap.get(holder3.getServer().getName()));
  }
  @Test
  public void testFindDestinationServerToMoveSegment()
  {
    List<ServerHolder> serverHolders = new ArrayList<>();
    serverHolders.add(holder1);
    serverHolders.add(holder2);
    serverHolders.add(holder3);

    BalancerStrategy balancerStrategy = new EnumeratedDistributionBalancerStrategy();
    ServerHolder pick = balancerStrategy.findDestinationServerToMoveSegment(segment4, holder4, serverHolders);
    Assert.assertNotNull(pick);

    HashMap<String, Integer> pickMap = new HashMap<>();
    pickMap.put(holder1.getServer().getName(), 0);
    pickMap.put(holder2.getServer().getName(), 0);
    pickMap.put(holder3.getServer().getName(), 0);
    pickMap.put(holder4.getServer().getName(), 0);

    for (int i = 0; i < 10000; i++) {
      ServerHolder mypick = balancerStrategy.findDestinationServerToMoveSegment(segment4, holder4, serverHolders);
      Integer curNum = pickMap.get(mypick.getServer().getName());
      pickMap.put(mypick.getServer().getName(), curNum + 1);
    }
    Assert.assertTrue(pickMap.get(holder3.getServer().getName()) > pickMap.get(holder2.getServer().getName()));
    // Transitive property makes 1 greater than 2 as well
    Assert.assertTrue(pickMap.get(holder1.getServer().getName()) > pickMap.get(holder3.getServer().getName()));
  }
}
