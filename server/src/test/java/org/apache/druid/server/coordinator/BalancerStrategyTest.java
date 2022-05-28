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

package org.apache.druid.server.coordinator;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@RunWith(Parameterized.class)
public class BalancerStrategyTest
{
  private final BalancerStrategy balancerStrategy;
  private DataSegment proposedDataSegment;
  private List<ServerHolder> serverHolders;

  @Parameterized.Parameters(name = "{index}: BalancerStrategy:{0}")
  public static Iterable<Object[]> data()
  {
    return Arrays.asList(
        new Object[][]{
            {new CostBalancerStrategy(Execs.directExecutor())},
            {new RandomBalancerStrategy()}
        }
    );
  }

  public BalancerStrategyTest(BalancerStrategy balancerStrategy)
  {
    this.balancerStrategy = balancerStrategy;
  }

  @Before
  public void setUp()
  {
    this.proposedDataSegment = new DataSegment(
        "datasource1",
        Intervals.utc(0, 1),
        "",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        11L
    );
  }


  @Test
  public void findNewSegmentHomeReplicatorNotEnoughSpace()
  {
    final ServerHolder serverHolder = new ServerHolder(
        new DruidServer("server1", "host1", null, 10L, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0).addDataSegment(proposedDataSegment).toImmutableDruidServer(),
        new LoadQueuePeonTester());
    serverHolders = new ArrayList<>();
    serverHolders.add(serverHolder);
    final ServerHolder foundServerHolder = balancerStrategy.findNewSegmentHomeReplicator(proposedDataSegment, serverHolders);
    // since there is not enough space on server having available size 10L to host a segment of size 11L, it should be null
    Assert.assertNull(foundServerHolder);
  }

  @Test(timeout = 5000L)
  public void findNewSegmentHomeReplicatorNotEnoughNodesForReplication()
  {
    final ServerHolder serverHolder1 = new ServerHolder(
        new DruidServer("server1", "host1", null, 1000L, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0).addDataSegment(proposedDataSegment).toImmutableDruidServer(),
        new LoadQueuePeonTester());

    final ServerHolder serverHolder2 = new ServerHolder(
        new DruidServer("server2", "host2", null, 1000L, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0).addDataSegment(proposedDataSegment).toImmutableDruidServer(),
        new LoadQueuePeonTester());

    serverHolders = new ArrayList<>();
    serverHolders.add(serverHolder1);
    serverHolders.add(serverHolder2);

    final ServerHolder foundServerHolder = balancerStrategy.findNewSegmentHomeReplicator(proposedDataSegment, serverHolders);
    // since there is not enough nodes to load 3 replicas of segment
    Assert.assertNull(foundServerHolder);
  }

  @Test
  public void findNewSegmentHomeReplicatorEnoughSpace()
  {
    final ServerHolder serverHolder = new ServerHolder(
        new DruidServer("server1", "host1", null, 1000L, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0).toImmutableDruidServer(),
        new LoadQueuePeonTester());
    serverHolders = new ArrayList<>();
    serverHolders.add(serverHolder);
    final ServerHolder foundServerHolder = balancerStrategy.findNewSegmentHomeReplicator(proposedDataSegment, serverHolders);
    // since there is enough space on server it should be selected
    Assert.assertEquals(serverHolder, foundServerHolder);
  }
}
