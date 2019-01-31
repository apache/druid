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
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ServerHolderTest
{
  private static final List<DataSegment> segments = ImmutableList.of(
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

  private static final Map<String, ImmutableDruidDataSource> dataSources = ImmutableMap.of(
      "src1", new ImmutableDruidDataSource("src1", Collections.emptyMap(), Collections.singletonList(segments.get(0))),
      "src2", new ImmutableDruidDataSource("src2", Collections.emptyMap(), Collections.singletonList(segments.get(1)))
  );

  @Test
  public void testCompareTo()
  {
    // available size of 100
    final ServerHolder h1 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host1", null, 100L, ServerType.HISTORICAL, "tier1", 0),
            0L,
            ImmutableMap.of("src1", dataSources.get("src1")),
            1
        ),
        new LoadQueuePeonTester()
    );

    // available size of 100
    final ServerHolder h2 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host1", null, 200L, ServerType.HISTORICAL, "tier1", 0),
            100L,
            ImmutableMap.of("src1", dataSources.get("src1")),
            1
        ),
        new LoadQueuePeonTester()
    );

    // available size of 10
    final ServerHolder h3 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host1", null, 1000L, ServerType.HISTORICAL, "tier1", 0),
            990L,
            ImmutableMap.of("src1", dataSources.get("src1")),
            1
        ),
        new LoadQueuePeonTester()
    );

    // available size of 50
    final ServerHolder h4 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host1", null, 50L, ServerType.HISTORICAL, "tier1", 0),
            0L,
            ImmutableMap.of("src1", dataSources.get("src1")),
            1
        ),
        new LoadQueuePeonTester()
    );

    Assert.assertEquals(0, h1.compareTo(h2));
    Assert.assertEquals(-1, h3.compareTo(h1));
    Assert.assertEquals(-1, h3.compareTo(h4));
  }

  @Test
  public void testEquals()
  {
    final ServerHolder h1 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host1", null, 100L, ServerType.HISTORICAL, "tier1", 0),
            0L,
            ImmutableMap.of("src1", dataSources.get("src1")),
            1
        ),
        new LoadQueuePeonTester()
    );

    final ServerHolder h2 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name2", "host1", null, 200L, ServerType.HISTORICAL, "tier1", 0),
            100L,
            ImmutableMap.of("src1", dataSources.get("src1")),
            1
        ),
        new LoadQueuePeonTester()
    );

    final ServerHolder h3 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host2", null, 200L, ServerType.HISTORICAL, "tier1", 0),
            100L,
            ImmutableMap.of("src1", dataSources.get("src1")),
            1
        ),
        new LoadQueuePeonTester()
    );

    final ServerHolder h4 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host1", null, 200L, ServerType.HISTORICAL, "tier2", 0),
            100L,
            ImmutableMap.of("src1", dataSources.get("src1")),
            1
        ),
        new LoadQueuePeonTester()
    );

    final ServerHolder h5 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host1", null, 100L, ServerType.REALTIME, "tier1", 0),
            0L,
            ImmutableMap.of("src1", dataSources.get("src1")),
            1
        ),
        new LoadQueuePeonTester()
    );

    Assert.assertEquals(h1, h2);
    Assert.assertNotEquals(h1, h3);
    Assert.assertNotEquals(h1, h4);
    Assert.assertNotEquals(h1, h5);
  }
}
