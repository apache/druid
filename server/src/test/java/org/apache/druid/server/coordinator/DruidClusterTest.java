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
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.Collectors;

public class DruidClusterTest
{
  private static final List<DataSegment> SEGMENTS = CreateDataSegments
      .ofDatasource("test")
      .forIntervals(2, Granularities.DAY)
      .startingAt("2015-04-12")
      .withNumPartitions(1)
      .eachOfSizeInMb(100);

  private static final ServerHolder NEW_REALTIME = new ServerHolder(
      new DruidServer("name1", "host2", null, 100L, ServerType.REALTIME, "tier1", 0)
          .addDataSegment(SEGMENTS.get(0)).toImmutableDruidServer(),
      new TestLoadQueuePeon()
  );

  private static final ServerHolder NEW_HISTORICAL = new ServerHolder(
      new DruidServer("name1", "host2", null, 100L, ServerType.HISTORICAL, "tier1", 0)
          .addDataSegment(SEGMENTS.get(0)).toImmutableDruidServer(),
      new TestLoadQueuePeon()
  );

  private DruidCluster.Builder clusterBuilder;

  @Before
  public void setup()
  {
    clusterBuilder = DruidCluster
        .builder()
        .add(
            new ServerHolder(
                new DruidServer("name1", "host1", null, 100L, ServerType.REALTIME, "tier1", 0)
                    .addDataSegment(SEGMENTS.get(0)).toImmutableDruidServer(),
                new TestLoadQueuePeon()
            )
        )
        .add(
            new ServerHolder(
                new DruidServer("name1", "host1", null, 100L, ServerType.HISTORICAL, "tier1", 0)
                    .addDataSegment(SEGMENTS.get(0)).toImmutableDruidServer(),
                new TestLoadQueuePeon()
            )
        );
  }

  @Test
  public void testAdd()
  {
    DruidCluster cluster = clusterBuilder.build();
    Assert.assertEquals(1, cluster.getHistoricals().values().stream().mapToInt(Collection::size).sum());
    Assert.assertEquals(1, cluster.getRealtimes().size());

    clusterBuilder.add(NEW_REALTIME);
    cluster = clusterBuilder.build();
    Assert.assertEquals(1, cluster.getHistoricals().values().stream().mapToInt(Collection::size).sum());
    Assert.assertEquals(2, cluster.getRealtimes().size());

    clusterBuilder.add(NEW_HISTORICAL);
    cluster = clusterBuilder.build();
    Assert.assertEquals(2, cluster.getHistoricals().values().stream().mapToInt(Collection::size).sum());
    Assert.assertEquals(2, cluster.getRealtimes().size());
  }

  @Test
  public void testGetAllServers()
  {
    clusterBuilder.add(NEW_REALTIME);
    clusterBuilder.add(NEW_HISTORICAL);

    DruidCluster cluster = clusterBuilder.build();
    final Set<ServerHolder> expectedRealtimes = cluster.getRealtimes();
    final Map<String, NavigableSet<ServerHolder>> expectedHistoricals = cluster.getHistoricals();

    final Collection<ServerHolder> allServers = cluster.getAllServers();
    Assert.assertEquals(4, allServers.size());
    Assert.assertTrue(allServers.containsAll(cluster.getRealtimes()));
    Assert.assertTrue(
        allServers.containsAll(
            cluster.getHistoricals().values().stream()
                   .flatMap(Collection::stream)
                   .collect(Collectors.toList())
        )
    );

    Assert.assertEquals(expectedHistoricals, cluster.getHistoricals());
    Assert.assertEquals(expectedRealtimes, cluster.getRealtimes());
  }

  @Test
  public void testIsEmpty()
  {
    final DruidCluster emptyCluster = DruidCluster.EMPTY;
    Assert.assertFalse(clusterBuilder.build().isEmpty());
    Assert.assertTrue(emptyCluster.isEmpty());
  }
}
