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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.client.DirectDruidClient;
import io.druid.client.DruidServer;
import io.druid.server.coordination.ServerType;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class ServerSelectorTest
{
  TierSelectorStrategy tierSelectorStrategy;

  @Before
  public void setUp() throws Exception
  {
    tierSelectorStrategy = EasyMock.createMock(TierSelectorStrategy.class);
    EasyMock.expect(tierSelectorStrategy.getComparator()).andReturn(Integer::compare).anyTimes();
  }

  @Test
  public void testSegmentUpdate() throws Exception
  {
    final ServerSelector selector = new ServerSelector(
        DataSegment.builder()
                   .dataSource("test_broker_server_view")
                   .interval(new Interval("2012/2013"))
                   .loadSpec(
                       ImmutableMap.<String, Object>of(
                           "type",
                           "local",
                           "path",
                           "somewhere"
                       )
                   )
                   .version("v1")
                   .dimensions(ImmutableList.<String>of())
                   .metrics(ImmutableList.<String>of())
                   .shardSpec(NoneShardSpec.instance())
                   .binaryVersion(9)
                   .size(0)
                   .build(),
        EasyMock.createMock(TierSelectorStrategy.class)
    );

    selector.addServerAndUpdateSegment(
        new QueryableDruidServer(
            new DruidServer("test1", "localhost", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
            EasyMock.createMock(DirectDruidClient.class)
        ),
        DataSegment.builder()
                   .dataSource(
                       "test_broker_server_view")
                   .interval(new Interval(
                       "2012/2013"))
                   .loadSpec(
                       ImmutableMap.<String, Object>of(
                           "type",
                           "local",
                           "path",
                           "somewhere"
                       )
                   )
                   .version("v1")
                   .dimensions(
                       ImmutableList.<String>of(
                           "a",
                           "b",
                           "c"
                       ))
                   .metrics(
                       ImmutableList.<String>of())
                   .shardSpec(NoneShardSpec.instance())
                   .binaryVersion(9)
                   .size(0)
                   .build()
    );

    Assert.assertEquals(ImmutableList.of("a", "b", "c"), selector.getSegment().getDimensions());
  }
}
