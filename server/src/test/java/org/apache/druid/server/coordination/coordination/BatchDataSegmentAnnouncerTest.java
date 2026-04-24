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

package org.apache.druid.server.coordination.coordination;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.server.coordination.BatchDataSegmentAnnouncer;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentChangeRequestDrop;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class BatchDataSegmentAnnouncerTest
{
  private BatchDataSegmentAnnouncer announcer;

  @Before
  public void setUp()
  {
    announcer = new BatchDataSegmentAnnouncer();
  }

  @Test
  public void testAnnounceAndUnannounceProduceChangeHistory() throws Exception
  {
    DataSegment segmentA = makeSegment(0);
    DataSegment segmentB = makeSegment(1);

    announcer.announceSegment(segmentA);
    announcer.announceSegment(segmentB);
    announcer.unannounceSegment(segmentA);

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot =
        announcer.getSegmentChangesSince(new ChangeRequestHistory.Counter(-1, -1)).get();
    // When counter is negative, getSegmentChangesSince returns the current snapshot of announced segments, which
    // at this point is just segmentB.
    Assert.assertEquals(1, snapshot.getRequests().size());
    Assert.assertTrue(snapshot.getRequests().get(0) instanceof SegmentChangeRequestLoad);
  }

  @Test
  public void testAnnounceSegmentsBatchesChangeRequests() throws Exception
  {
    List<DataSegment> segments = ImmutableList.of(makeSegment(0), makeSegment(1), makeSegment(2));

    announcer.announceSegments(segments);

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot =
        announcer.getSegmentChangesSince(ChangeRequestHistory.Counter.ZERO).get();
    Assert.assertEquals(segments.size(), snapshot.getRequests().size());
    for (DataSegmentChangeRequest request : snapshot.getRequests()) {
      Assert.assertTrue(request instanceof SegmentChangeRequestLoad);
    }
  }

  @Test
  public void testUnannounceAfterAnnounceProducesDropRequest() throws Exception
  {
    DataSegment segment = makeSegment(0);

    announcer.announceSegment(segment);
    announcer.unannounceSegment(segment);

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot =
        announcer.getSegmentChangesSince(ChangeRequestHistory.Counter.ZERO).get();
    Assert.assertEquals(2, snapshot.getRequests().size());
    Assert.assertTrue(snapshot.getRequests().get(0) instanceof SegmentChangeRequestLoad);
    Assert.assertTrue(snapshot.getRequests().get(1) instanceof SegmentChangeRequestDrop);
  }

  @Test
  public void testDuplicateAnnouncementIsIgnored() throws Exception
  {
    DataSegment segment = makeSegment(0);

    announcer.announceSegment(segment);
    announcer.announceSegment(segment);

    ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshot =
        announcer.getSegmentChangesSince(ChangeRequestHistory.Counter.ZERO).get();
    Assert.assertEquals(1, snapshot.getRequests().size());
  }

  private DataSegment makeSegment(int offset)
  {
    return new DataSegment(
        "test",
        new Interval(DateTimes.utc(0), DateTimes.utc(offset + 1)),
        "v1",
        null,
        ImmutableList.of("dim"),
        ImmutableList.of("met"),
        NoneShardSpec.instance(),
        0,
        1
    );
  }
}
