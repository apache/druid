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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.RandomBalancerStrategy;
import org.apache.druid.server.coordinator.loading.LoadQueuePeonTester;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CollectSegmentAndServerStatsTest
{

  @Test
  public void testCollectedSegmentStats()
  {
    DruidServer server = new DruidServer("s1", "localhost", null, 100, ServerType.HISTORICAL, "tier1", 1);
    DruidCluster cluster = DruidCluster.builder().add(
        new ServerHolder(server.toImmutableDruidServer(), new LoadQueuePeonTester())
    ).build();

    DruidCoordinatorRuntimeParams runtimeParams =
        DruidCoordinatorRuntimeParams.newBuilder(DateTimes.nowUtc())
                                     .withDruidCluster(cluster)
                                     .withUsedSegmentsInTest()
                                     .withBalancerStrategy(new RandomBalancerStrategy())
                                     .withSegmentAssignerUsing(new SegmentLoadQueueManager(null, null))
                                     .build();

    CoordinatorDuty duty = new CollectSegmentAndServerStats();
    DruidCoordinatorRuntimeParams params = duty.run(runtimeParams);

    CoordinatorRunStats stats = params.getCoordinatorStats();
    Assert.assertTrue(stats.hasStat(Stats.SegmentQueue.NUM_TO_LOAD));
    Assert.assertTrue(stats.hasStat(Stats.SegmentQueue.NUM_TO_DROP));
    Assert.assertTrue(stats.hasStat(Stats.SegmentQueue.BYTES_TO_LOAD));
  }

}
