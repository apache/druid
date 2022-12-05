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

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

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
    ClientCompactionIntervalSpec actual = ClientCompactionIntervalSpec.fromSegments(ImmutableList.of(dataSegment1, dataSegment2, dataSegment3), null);
    Assert.assertEquals(Intervals.of("2015-02-12/2015-04-14"), actual.getInterval());
  }

  @Test
  public void testFromSegmentWitSegmentGranularitySameAsSegment()
  {
    // The umbrella interval of segments is 2015-04-11/2015-04-12
    ClientCompactionIntervalSpec actual = ClientCompactionIntervalSpec.fromSegments(ImmutableList.of(dataSegment1), Granularities.DAY);
    Assert.assertEquals(Intervals.of("2015-04-11/2015-04-12"), actual.getInterval());
  }

  @Test
  public void testFromSegmentWithCoarserSegmentGranularity()
  {
    // The umbrella interval of segments is 2015-02-12/2015-04-14
    ClientCompactionIntervalSpec actual = ClientCompactionIntervalSpec.fromSegments(ImmutableList.of(dataSegment1, dataSegment2, dataSegment3), Granularities.YEAR);
    // The compaction interval should be expanded to start of the year and end of the year to cover the segmentGranularity
    Assert.assertEquals(Intervals.of("2015-01-01/2016-01-01"), actual.getInterval());
  }

  @Test
  public void testFromSegmentWithFinerSegmentGranularityAndUmbrellaIntervalAlign()
  {
    // The umbrella interval of segments is 2015-02-12/2015-04-14
    ClientCompactionIntervalSpec actual = ClientCompactionIntervalSpec.fromSegments(ImmutableList.of(dataSegment1, dataSegment2, dataSegment3), Granularities.DAY);
    // The segmentGranularity of DAY align with the umbrella interval (umbrella interval can be evenly divide into the segmentGranularity)
    Assert.assertEquals(Intervals.of("2015-02-12/2015-04-14"), actual.getInterval());
  }

  @Test
  public void testFromSegmentWithFinerSegmentGranularityAndUmbrellaIntervalNotAlign()
  {
    // The umbrella interval of segments is 2015-02-12/2015-04-14
    ClientCompactionIntervalSpec actual = ClientCompactionIntervalSpec.fromSegments(ImmutableList.of(dataSegment1, dataSegment2, dataSegment3), Granularities.WEEK);
    // The segmentGranularity of WEEK does not align with the umbrella interval (umbrella interval cannot be evenly divide into the segmentGranularity)
    // Hence the compaction interval is modified to aling with the segmentGranularity
    Assert.assertEquals(Intervals.of("2015-02-09/2015-04-20"), actual.getInterval());
  }
}
