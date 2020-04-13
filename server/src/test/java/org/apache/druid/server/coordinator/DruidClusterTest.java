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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.Collectors;

public class DruidClusterTest
{
  private static final List<DataSegment> SEGMENTS = ImmutableList.of(
      new DataSegment(
          "test",
          Intervals.of("2015-04-12/2015-04-13"),
          "1",
          ImmutableMap.of("containerName", "container1", "blobPath", "blobPath1"),
          null,
          null,
          NoneShardSpec.instance(),
          0,
          1
      ),
      new DataSegment(
          "test",
          Intervals.of("2015-04-12/2015-04-13"),
          "1",
          ImmutableMap.of("containerName", "container2", "blobPath", "blobPath2"),
          null,
          null,
          NoneShardSpec.instance(),
          0,
          1
      )
  );

  private static final Map<String, ImmutableDruidDataSource> DATA_SOURCES = ImmutableMap.of(
      "src1", new ImmutableDruidDataSource("src1", Collections.emptyMap(), Collections.singletonList(SEGMENTS.get(0))),
      "src2", new ImmutableDruidDataSource("src2", Collections.emptyMap(), Collections.singletonList(SEGMENTS.get(0)))
  );

  private static final ServerHolder NEW_REALTIME = new ServerHolder(
      new ImmutableDruidServer(
          new DruidServerMetadata("name1", "host2", null, 100L, ServerType.REALTIME, "tier1", 0),
          0L,
          ImmutableMap.of("src1", DATA_SOURCES.get("src1")),
          1
      ),
      new LoadQueuePeonTester()
  );

  private static final ServerHolder NEW_HISTORICAL = new ServerHolder(
      new ImmutableDruidServer(
          new DruidServerMetadata("name1", "host2", null, 100L, ServerType.HISTORICAL, "tier1", 0),
          0L,
          ImmutableMap.of("src1", DATA_SOURCES.get("src1")),
          1
      ),
      new LoadQueuePeonTester()
  );

  private DruidCluster cluster;

  @Before
  public void setup()
  {
    cluster = DruidClusterBuilder
        .newBuilder()
        .withRealtimes(
            new ServerHolder(
                new ImmutableDruidServer(
                    new DruidServerMetadata("name1", "host1", null, 100L, ServerType.REALTIME, "tier1", 0),
                    0L,
                    ImmutableMap.of("src1", DATA_SOURCES.get("src1")),
                    1
                ),
                new LoadQueuePeonTester()
            )
        )
        .addTier(
            "tier1",
            new ServerHolder(
                new ImmutableDruidServer(
                    new DruidServerMetadata("name1", "host1", null, 100L, ServerType.HISTORICAL, "tier1", 0),
                    0L,
                    ImmutableMap.of("src1", DATA_SOURCES.get("src1")),
                    1
                ),
                new LoadQueuePeonTester()
            )
        )
        .build();
  }

  @Test
  public void testAdd()
  {
    Assert.assertEquals(1, cluster.getHistoricals().values().stream().mapToInt(Collection::size).sum());
    Assert.assertEquals(1, cluster.getRealtimes().size());

    cluster.add(NEW_REALTIME);
    Assert.assertEquals(1, cluster.getHistoricals().values().stream().mapToInt(Collection::size).sum());
    Assert.assertEquals(2, cluster.getRealtimes().size());

    cluster.add(NEW_HISTORICAL);
    Assert.assertEquals(2, cluster.getHistoricals().values().stream().mapToInt(Collection::size).sum());
    Assert.assertEquals(2, cluster.getRealtimes().size());
  }

  @Test
  public void testGetAllServers()
  {
    cluster.add(NEW_REALTIME);
    cluster.add(NEW_HISTORICAL);
    final Set<ServerHolder> expectedRealtimes = cluster.getRealtimes();
    final Map<String, NavigableSet<ServerHolder>> expectedHistoricals = cluster.getHistoricals();

    final Collection<ServerHolder> allServers = cluster.getAllServers();
    Assert.assertEquals(4, allServers.size());
    Assert.assertTrue(allServers.containsAll(cluster.getRealtimes()));
    Assert.assertTrue(
        allServers.containsAll(
            cluster.getHistoricals().values().stream().flatMap(Collection::stream).collect(Collectors.toList())
        )
    );

    Assert.assertEquals(expectedHistoricals, cluster.getHistoricals());
    Assert.assertEquals(expectedRealtimes, cluster.getRealtimes());
  }

  @Test
  public void testIsEmpty()
  {
    final DruidCluster emptyCluster = new DruidCluster();
    Assert.assertFalse(cluster.isEmpty());
    Assert.assertTrue(emptyCluster.isEmpty());
  }
}
