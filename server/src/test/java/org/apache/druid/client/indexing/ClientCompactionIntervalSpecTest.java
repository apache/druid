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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ClientCompactionIntervalSpecTest
{
  private final DataSegment dataSegment1 = new DataSegment(
      "test",
      Intervals.of("2015-04-11/2015-04-12"),
      DateTimes.nowUtc().toString(),
      new HashMap<>(),
      new ArrayList<>(),
      new ArrayList<>(),
      NoneShardSpec.instance(),
      IndexIO.CURRENT_VERSION_ID,
      11
  );
  private final DataSegment dataSegment2 = new DataSegment(
      "test",
      Intervals.of("2015-04-12/2015-04-14"),
      DateTimes.nowUtc().toString(),
      new HashMap<>(),
      new ArrayList<>(),
      new ArrayList<>(),
      NoneShardSpec.instance(),
      IndexIO.CURRENT_VERSION_ID,
      11
  );
  private final DataSegment dataSegment3 = new DataSegment(
      "test",
      Intervals.of("2015-02-12/2015-03-13"),
      DateTimes.nowUtc().toString(),
      new HashMap<>(),
      new ArrayList<>(),
      new ArrayList<>(),
      NoneShardSpec.instance(),
      IndexIO.CURRENT_VERSION_ID,
      11
  );

  @Test
  public void testFromSegmentWithNoSegmentGranularity()
  {
    // The umbrella interval of segments is 2015-02-12/2015-04-14
    CompactionCandidate actual = CompactionCandidate.from(ImmutableList.of(dataSegment1, dataSegment2, dataSegment3), null);
    Assert.assertEquals(Intervals.of("2015-02-12/2015-04-14"), actual.getCompactionInterval());
  }

  @Test
  public void testFromSegmentWitSegmentGranularitySameAsSegment()
  {
    // The umbrella interval of segments is 2015-04-11/2015-04-12
    CompactionCandidate actual = CompactionCandidate.from(ImmutableList.of(dataSegment1), Granularities.DAY);
    Assert.assertEquals(Intervals.of("2015-04-11/2015-04-12"), actual.getCompactionInterval());
  }

  @Test
  public void testFromSegmentWithCoarserSegmentGranularity()
  {
    // The umbrella interval of segments is 2015-02-12/2015-04-14
    CompactionCandidate actual = CompactionCandidate.from(ImmutableList.of(dataSegment1, dataSegment2, dataSegment3), Granularities.YEAR);
    // The compaction interval should be expanded to start of the year and end of the year to cover the segmentGranularity
    Assert.assertEquals(Intervals.of("2015-01-01/2016-01-01"), actual.getCompactionInterval());
  }

  @Test
  public void testFromSegmentWithFinerSegmentGranularityAndUmbrellaIntervalAlign()
  {
    // The umbrella interval of segments is 2015-02-12/2015-04-14
    CompactionCandidate actual = CompactionCandidate.from(ImmutableList.of(dataSegment1, dataSegment2, dataSegment3), Granularities.DAY);
    // The segmentGranularity of DAY align with the umbrella interval (umbrella interval can be evenly divide into the segmentGranularity)
    Assert.assertEquals(Intervals.of("2015-02-12/2015-04-14"), actual.getCompactionInterval());
  }

  @Test
  public void testFromSegmentWithFinerSegmentGranularityAndUmbrellaIntervalNotAlign()
  {
    // The umbrella interval of segments is 2015-02-12/2015-04-14
    CompactionCandidate actual = CompactionCandidate.from(ImmutableList.of(dataSegment1, dataSegment2, dataSegment3), Granularities.WEEK);
    // The segmentGranularity of WEEK does not align with the umbrella interval (umbrella interval cannot be evenly divide into the segmentGranularity)
    // Hence the compaction interval is modified to aling with the segmentGranularity
    Assert.assertEquals(Intervals.of("2015-02-09/2015-04-20"), actual.getCompactionInterval());
  }

  @Test
  public void testClientCompactionIntervalSpec_throwsException_whenEmptySegmentsList()
  {
    Interval interval = Intervals.of("2015-04-11/2015-04-12");
    List<SegmentDescriptor> emptySegments = List.of();

    Assert.assertThrows(
        IAE.class,
        () -> new ClientCompactionIntervalSpec(interval, emptySegments, null)
    );
  }

  @Test
  public void testClientCompactionIntervalSpec_serde() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    Interval interval = Intervals.of("2015-04-11/2015-04-12");
    List<SegmentDescriptor> segments = List.of(
        new SegmentDescriptor(Intervals.of("2015-04-11/2015-04-12"), "v1", 0)
    );

    // Test with uncompactedSegments (incremental compaction)
    ClientCompactionIntervalSpec withSegments = new ClientCompactionIntervalSpec(interval, segments, "sha256hash");
    String json1 = mapper.writeValueAsString(withSegments);
    ClientCompactionIntervalSpec deserialized1 = mapper.readValue(json1, ClientCompactionIntervalSpec.class);
    Assert.assertEquals(withSegments, deserialized1);
    Assert.assertEquals(segments, deserialized1.getUncompactedSegments());

    // Test without uncompactedSegments (full compaction)
    ClientCompactionIntervalSpec withoutSegments = new ClientCompactionIntervalSpec(interval, null, null);
    String json2 = mapper.writeValueAsString(withoutSegments);
    ClientCompactionIntervalSpec deserialized2 = mapper.readValue(json2, ClientCompactionIntervalSpec.class);
    Assert.assertEquals(withoutSegments, deserialized2);
    Assert.assertNull(deserialized2.getUncompactedSegments());
  }
}
