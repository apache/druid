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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
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
            {new CostBalancerStrategy(MoreExecutors.sameThreadExecutor())},
            {new RandomBalancerStrategy()}
        }
    );
  }

  public BalancerStrategyTest(BalancerStrategy balancerStrategy)
  {
    this.balancerStrategy = balancerStrategy;
  }

  @Before
  public void setUp() throws Exception
  {
    this.proposedDataSegment = new DataSegment(
        "datasource1",
        new Interval(0, 1),
        "",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        11L
    );
    final ServerHolder serverHolder = new ServerHolder(new ImmutableDruidServer(
        new DruidServerMetadata(
            "server1",
            "localhost:8081",
            null,
            10L,
            ServerType.HISTORICAL,
            "_default_tier",
            0
        ), 0L, ImmutableMap.of(), 0), new LoadQueuePeonTester());
    serverHolders = new ArrayList<>();
    serverHolders.add(serverHolder);
  }

  @Test
  public void findNewSegmentHomeReplicatorNotEnoughSpace()
  {
    final ServerHolder serverHolder = balancerStrategy.findNewSegmentHomeReplicator(proposedDataSegment, serverHolders);
    // since there is not enough space on server having avaialable size 10L to host a segment of size 11L, it should be null
    Assert.assertNull(serverHolder);
  }
}
