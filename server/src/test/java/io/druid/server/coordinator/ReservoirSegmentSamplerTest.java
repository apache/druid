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

package io.druid.server.coordinator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.client.ImmutableDruidServer;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ReservoirSegmentSamplerTest
{
  private ImmutableDruidServer druidServer1;
  private ImmutableDruidServer druidServer2;
  private ImmutableDruidServer druidServer3;
  private ImmutableDruidServer druidServer4;

  private ServerHolder holder1;
  private ServerHolder holder2;
  private ServerHolder holder3;
  private ServerHolder holder4;

  private DataSegment segment1;
  private DataSegment segment2;
  private DataSegment segment3;
  private DataSegment segment4;
  Map<String, DataSegment> segmentsMap1;
  Map<String, DataSegment> segmentsMap2;
  Map<String, DataSegment> segmentsMap3;
  Map<String, DataSegment> segmentsMap4;
  List<DataSegment> segments;

  @Before
  public void setUp() throws Exception
  {
    druidServer1 = EasyMock.createMock(ImmutableDruidServer.class);
    druidServer2 = EasyMock.createMock(ImmutableDruidServer.class);
    druidServer3 = EasyMock.createMock(ImmutableDruidServer.class);
    druidServer4 = EasyMock.createMock(ImmutableDruidServer.class);
    holder1 = EasyMock.createMock(ServerHolder.class);
    holder2 = EasyMock.createMock(ServerHolder.class);
    holder3 = EasyMock.createMock(ServerHolder.class);
    holder4 = EasyMock.createMock(ServerHolder.class);
    segment1 = EasyMock.createMock(DataSegment.class);
    segment2 = EasyMock.createMock(DataSegment.class);
    segment3 = EasyMock.createMock(DataSegment.class);
    segment4 = EasyMock.createMock(DataSegment.class);

    DateTime start1 = new DateTime("2012-01-01");
    DateTime start2 = new DateTime("2012-02-01");
    DateTime version = new DateTime("2012-03-01");
    segment1 = new DataSegment(
        "datasource1",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        11L
    );
    segment2 = new DataSegment(
        "datasource1",
        new Interval(start2, start2.plusHours(1)),
        version.toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        7L
    );
    segment3 = new DataSegment(
        "datasource2",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        4L
    );
    segment4 = new DataSegment(
        "datasource2",
        new Interval(start2, start2.plusHours(1)),
        version.toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        8L
    );

    segments = Lists.newArrayList(segment1, segment2, segment3, segment4);

    segmentsMap1 = ImmutableMap.<String, DataSegment>of(
        "datasource1_2012-01-01T00:00:00.000Z_2012-01-01T01:00:00.000Z_2012-03-01T00:00:00.000Z",
        segment1
    );
    segmentsMap2 = ImmutableMap.<String, DataSegment>of(
        "datasource1_2012-02-01T00:00:00.000Z_2012-02-01T01:00:00.000Z_2012-03-01T00:00:00.000Z",
        segment2
    );
    segmentsMap3 = ImmutableMap.<String, DataSegment>of(
        "datasource2_2012-01-01T00:00:00.000Z_2012-01-01T01:00:00.000Z_2012-03-01T00:00:00.000Z",
        segment3
    );
    segmentsMap4 = ImmutableMap.<String, DataSegment>of(
        "datasource2_2012-02-01T00:00:00.000Z_2012-02-01T01:00:00.000Z_2012-03-01T00:00:00.000Z",
        segment4
    );
  }

  //checks if every segment is selected at least once out of 5000 trials
  @Test
  public void getRandomBalancerSegmentHolderTest()
  {
    EasyMock.expect(druidServer1.getName()).andReturn("1").atLeastOnce();
    EasyMock.expect(druidServer1.getCurrSize()).andReturn(30L).atLeastOnce();
    EasyMock.expect(druidServer1.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServer1.getSegments()).andReturn(segmentsMap1).anyTimes();
    EasyMock.expect(druidServer1.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer1);

    EasyMock.expect(druidServer2.getName()).andReturn("2").atLeastOnce();
    EasyMock.expect(druidServer2.getTier()).andReturn("normal").anyTimes();
    EasyMock.expect(druidServer2.getCurrSize()).andReturn(30L).atLeastOnce();
    EasyMock.expect(druidServer2.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServer2.getSegments()).andReturn(segmentsMap2).anyTimes();
    EasyMock.expect(druidServer2.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer2);

    EasyMock.expect(druidServer3.getName()).andReturn("3").atLeastOnce();
    EasyMock.expect(druidServer3.getTier()).andReturn("normal").anyTimes();
    EasyMock.expect(druidServer3.getCurrSize()).andReturn(30L).atLeastOnce();
    EasyMock.expect(druidServer3.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServer3.getSegments()).andReturn(segmentsMap3).anyTimes();
    EasyMock.expect(druidServer3.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer3);

    EasyMock.expect(druidServer4.getName()).andReturn("4").atLeastOnce();
    EasyMock.expect(druidServer4.getTier()).andReturn("normal").anyTimes();
    EasyMock.expect(druidServer4.getCurrSize()).andReturn(30L).atLeastOnce();
    EasyMock.expect(druidServer4.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServer4.getSegments()).andReturn(segmentsMap4).anyTimes();
    EasyMock.expect(druidServer4.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer4);

    EasyMock.expect(holder1.getServer()).andReturn(druidServer1).anyTimes();
    EasyMock.replay(holder1);
    EasyMock.expect(holder2.getServer()).andReturn(druidServer2).anyTimes();
    EasyMock.replay(holder2);

    EasyMock.expect(holder3.getServer()).andReturn(druidServer3).anyTimes();
    EasyMock.replay(holder3);

    EasyMock.expect(holder4.getServer()).andReturn(druidServer4).anyTimes();
    EasyMock.replay(holder4);

    List<ServerHolder> holderList = Lists.newArrayList();
    holderList.add(holder1);
    holderList.add(holder2);
    holderList.add(holder3);
    holderList.add(holder4);

    Map<DataSegment, Integer> segmentCountMap = Maps.newHashMap();
    ReservoirSegmentSampler sampler = new ReservoirSegmentSampler();
    for (int i = 0; i < 5000; i++) {
      segmentCountMap.put(sampler.getRandomBalancerSegmentHolder(holderList).getSegment(), 1);
    }

    for (DataSegment segment : segments) {
      Assert.assertEquals(segmentCountMap.get(segment), new Integer(1));
    }


  }
}
