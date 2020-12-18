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

import com.google.common.collect.Lists;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.ImmutableDruidServerTests;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
  List<DataSegment> segments1;
  List<DataSegment> segments2;
  List<DataSegment> segments3;
  List<DataSegment> segments4;
  List<DataSegment> segments;

  @Before
  public void setUp()
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

    DateTime start1 = DateTimes.of("2012-01-01");
    DateTime start2 = DateTimes.of("2012-02-01");
    DateTime version = DateTimes.of("2012-03-01");
    segment1 = new DataSegment(
        "datasource1",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        11L
    );
    segment2 = new DataSegment(
        "datasource1",
        new Interval(start2, start2.plusHours(1)),
        version.toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        7L
    );
    segment3 = new DataSegment(
        "datasource2",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        4L
    );
    segment4 = new DataSegment(
        "datasource2",
        new Interval(start2, start2.plusHours(1)),
        version.toString(),
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        8L
    );

    segments = Lists.newArrayList(segment1, segment2, segment3, segment4);

    segments1 = Collections.singletonList(segment1);
    segments2 = Collections.singletonList(segment2);
    segments3 = Collections.singletonList(segment3);
    segments4 = Collections.singletonList(segment4);
  }

  //checks if every segment is selected at least once out of 5000 trials
  @Test
  public void getRandomBalancerSegmentHolderTest()
  {
    int iterations = 5000;

    EasyMock.expect(druidServer1.getType()).andReturn(ServerType.HISTORICAL).times(iterations);
    ImmutableDruidServerTests.expectSegments(druidServer1, segments1);
    EasyMock.replay(druidServer1);

    EasyMock.expect(druidServer2.getType()).andReturn(ServerType.HISTORICAL).times(iterations);
    ImmutableDruidServerTests.expectSegments(druidServer2, segments2);
    EasyMock.replay(druidServer2);

    EasyMock.expect(druidServer3.getType()).andReturn(ServerType.HISTORICAL).times(iterations);
    ImmutableDruidServerTests.expectSegments(druidServer3, segments3);
    EasyMock.replay(druidServer3);

    EasyMock.expect(druidServer4.getType()).andReturn(ServerType.HISTORICAL).times(iterations);
    ImmutableDruidServerTests.expectSegments(druidServer4, segments4);
    EasyMock.replay(druidServer4);

    // Have to use anyTimes() because the number of times a segment on a given server is chosen is indetermistic.
    EasyMock.expect(holder1.getServer()).andReturn(druidServer1).anyTimes();
    EasyMock.replay(holder1);
    EasyMock.expect(holder2.getServer()).andReturn(druidServer2).anyTimes();
    EasyMock.replay(holder2);
    EasyMock.expect(holder3.getServer()).andReturn(druidServer3).anyTimes();
    EasyMock.replay(holder3);
    EasyMock.expect(holder4.getServer()).andReturn(druidServer4).anyTimes();
    EasyMock.replay(holder4);

    List<ServerHolder> holderList = new ArrayList<>();
    holderList.add(holder1);
    holderList.add(holder2);
    holderList.add(holder3);
    holderList.add(holder4);

    Map<DataSegment, Integer> segmentCountMap = new HashMap<>();
    for (int i = 0; i < iterations; i++) {
      // due to the pseudo-randomness of this method, we may not select a segment every single time no matter what.
      BalancerSegmentHolder balancerSegmentHolder = ReservoirSegmentSampler.getRandomBalancerSegmentHolder(holderList, Collections.emptySet(), 100);
      if (balancerSegmentHolder != null) {
        segmentCountMap.put(balancerSegmentHolder.getSegment(), 1);
      }
    }

    for (DataSegment segment : segments) {
      Assert.assertEquals(new Integer(1), segmentCountMap.get(segment));
    }

    EasyMock.verify(druidServer1, druidServer2, druidServer3, druidServer4);
    EasyMock.verify(holder1, holder2, holder3, holder4);
  }

  /**
   * Makes sure that the segment on server4 is never chosen in 5k iterations because it should never have its segment
   * checked due to the limit on segment candidates
   */
  @Test
  public void getRandomBalancerSegmentHolderTestSegmentsToConsiderLimit()
  {
    int iterations = 5000;

    EasyMock.expect(druidServer1.getType()).andReturn(ServerType.HISTORICAL).times(iterations);
    ImmutableDruidServerTests.expectSegments(druidServer1, segments1);
    EasyMock.replay(druidServer1);

    EasyMock.expect(druidServer2.getType()).andReturn(ServerType.HISTORICAL).times(iterations);
    ImmutableDruidServerTests.expectSegments(druidServer2, segments2);
    EasyMock.replay(druidServer2);

    EasyMock.expect(druidServer3.getType()).andReturn(ServerType.HISTORICAL).times(iterations);
    ImmutableDruidServerTests.expectSegments(druidServer3, segments3);
    EasyMock.replay(druidServer3);

    ImmutableDruidServerTests.expectSegments(druidServer4, segments4);
    EasyMock.replay(druidServer4);

    // Have to use anyTimes() because the number of times a segment on a given server is chosen is indetermistic.
    EasyMock.expect(holder1.getServer()).andReturn(druidServer1).anyTimes();
    EasyMock.replay(holder1);
    EasyMock.expect(holder2.getServer()).andReturn(druidServer2).anyTimes();
    EasyMock.replay(holder2);
    EasyMock.expect(holder3.getServer()).andReturn(druidServer3).anyTimes();
    EasyMock.replay(holder3);
    // We only run getServer() each time we calculate the limit on segments to consider. Always 5k
    EasyMock.expect(holder4.getServer()).andReturn(druidServer4).times(5000);
    EasyMock.replay(holder4);

    List<ServerHolder> holderList = new ArrayList<>();
    holderList.add(holder1);
    holderList.add(holder2);
    holderList.add(holder3);
    holderList.add(holder4);

    Map<DataSegment, Integer> segmentCountMap = new HashMap<>();
    for (int i = 0; i < iterations; i++) {
      segmentCountMap.put(
          ReservoirSegmentSampler.getRandomBalancerSegmentHolder(holderList, Collections.emptySet(), 75).getSegment(), 1
      );
    }

    for (DataSegment segment : segments) {
      if (!segment.equals(segment4)) {
        Assert.assertEquals(new Integer(1), segmentCountMap.get(segment));
      } else {
        Assert.assertNull(segmentCountMap.get(segment));
      }
    }

    EasyMock.verify(druidServer1, druidServer2, druidServer3, druidServer4);
    EasyMock.verify(holder1, holder2, holder3, holder4);
  }
}
