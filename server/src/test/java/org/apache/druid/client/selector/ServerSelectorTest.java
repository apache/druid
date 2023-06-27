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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerSelectorTest
{
  @BeforeEach
  void setUp()
  {
    TierSelectorStrategy tierSelectorStrategy = EasyMock.createMock(TierSelectorStrategy.class);
    EasyMock.expect(tierSelectorStrategy.getComparator()).andReturn(Integer::compare).anyTimes();
  }

  @Test
  void testSegmentUpdate()
  {
    final ServerSelector selector = new ServerSelector(
        DataSegment.builder()
                   .dataSource("test_broker_server_view")
                   .interval(Intervals.of("2012/2013"))
                   .loadSpec(
                       ImmutableMap.of(
                           "type",
                           "local",
                           "path",
                           "somewhere"
                       )
                   )
                   .version("v1")
                   .dimensions(ImmutableList.of())
                   .metrics(ImmutableList.of())
                   .shardSpec(NoneShardSpec.instance())
                   .binaryVersion(9)
                   .size(0)
                   .build(),
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy())
    );

    selector.addServerAndUpdateSegment(
        new QueryableDruidServer(
            new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
            EasyMock.createMock(DirectDruidClient.class)
        ),
        DataSegment.builder()
                   .dataSource(
                       "test_broker_server_view")
                   .interval(Intervals.of("2012/2013"))
                   .loadSpec(
                       ImmutableMap.of(
                           "type",
                           "local",
                           "path",
                           "somewhere"
                       )
                   )
                   .version("v1")
                   .dimensions(
                       ImmutableList.of(
                           "a",
                           "b",
                           "c"
                       ))
                   .metrics(
                       ImmutableList.of())
                   .shardSpec(NoneShardSpec.instance())
                   .binaryVersion(9)
                   .size(0)
                   .build()
    );

    assertEquals(ImmutableList.of("a", "b", "c"), selector.getSegment().getDimensions());
  }

  @Test
  void testSegmentCannotBeNull()
  {
    assertThrows(NullPointerException.class, () -> new ServerSelector(
        null,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy())
    ));
  }

  @Test
  void testSegmentWithNoData()
  {
    final ServerSelector selector = new ServerSelector(
        DataSegment.builder()
                   .dataSource("test_broker_server_view")
                   .interval(Intervals.of("2012/2013"))
                   .loadSpec(
                       ImmutableMap.of(
                           "type",
                           "tombstone"
                       )
                   )
                   .version("v1")
                   .dimensions(ImmutableList.of())
                   .metrics(ImmutableList.of())
                   .shardSpec(new TombstoneShardSpec())
                   .binaryVersion(9)
                   .size(0)
                   .build(),
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy())
    );
    assertFalse(selector.hasData());
  }

  @Test
  public void testSegmentWithData()
  {
    final ServerSelector selector = new ServerSelector(
        DataSegment.builder()
                   .dataSource("another segment") // fool the interner inside the selector
                   .interval(Intervals.of("2012/2013"))
                   .loadSpec(
                       ImmutableMap.of(
                           "type",
                           "local",
                           "path",
                           "somewhere"
                       )
                   )
                   .version("v1")
                   .dimensions(ImmutableList.of())
                   .metrics(ImmutableList.of())
                   .shardSpec(NoneShardSpec.instance())
                   .binaryVersion(9)
                   .size(0)
                   .build(),
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy())
    );
    assertTrue(selector.hasData());
  }

}
