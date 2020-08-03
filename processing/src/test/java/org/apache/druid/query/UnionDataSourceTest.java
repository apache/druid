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

package org.apache.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NumberedPartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class UnionDataSourceTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  public final String FOO = "foo";
  public final String BAR = "bar";

  private final UnionDataSource unionDataSource = new UnionDataSource(
      ImmutableList.of(
          new TableDataSource(FOO),
          new TableDataSource(BAR)
      )
  );

  private final UnionDataSource unionDataSourceWithDuplicates = new UnionDataSource(
      ImmutableList.of(
          new TableDataSource(BAR),
          new TableDataSource(FOO),
          new TableDataSource(BAR)
      )
  );

  @Test
  public void test_getTableNames()
  {
    Assert.assertEquals(ImmutableSet.of(FOO, BAR), unionDataSource.getTableNames());
  }

  @Test
  public void test_getTableNames_withDuplicates()
  {
    Assert.assertEquals(ImmutableSet.of(FOO, BAR), unionDataSourceWithDuplicates.getTableNames());
  }

  @Test
  public void test_getChildren()
  {
    Assert.assertEquals(
        ImmutableList.of(new TableDataSource(FOO), new TableDataSource(BAR)),
        unionDataSource.getChildren()
    );
  }

  @Test
  public void test_getChildren_withDuplicates()
  {
    Assert.assertEquals(
        ImmutableList.of(new TableDataSource(BAR), new TableDataSource(FOO), new TableDataSource(BAR)),
        unionDataSourceWithDuplicates.getChildren()
    );
  }

  @Test
  public void test_isCacheable()
  {
    Assert.assertFalse(unionDataSource.isCacheable());
  }

  @Test
  public void test_isGlobal()
  {
    Assert.assertFalse(unionDataSource.isGlobal());
  }

  @Test
  public void test_isConcrete()
  {
    Assert.assertTrue(unionDataSource.isConcrete());
  }

  @Test
  public void test_withChildren_empty()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Expected [2] children, got [0]");

    unionDataSource.withChildren(Collections.emptyList());
  }

  @Test
  public void test_withChildren_sameNumber()
  {
    final List<TableDataSource> newDataSources = ImmutableList.of(
        new TableDataSource("baz"),
        new TableDataSource("qux")
    );

    //noinspection unchecked
    Assert.assertEquals(
        new UnionDataSource(newDataSources),
        unionDataSource.withChildren((List) newDataSources)
    );
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(UnionDataSource.class).usingGetClass().withNonnullFields("dataSources").verify();
  }

  @Test
  public void test_serde() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final UnionDataSource deserialized = (UnionDataSource) jsonMapper.readValue(
        jsonMapper.writeValueAsString(unionDataSource),
        DataSource.class
    );

    Assert.assertEquals(unionDataSource, deserialized);
  }

  @Test
  public void test_retrieveSegmentsForIntervals()
  {
    final DataSegment fooSegment1 = new DataSegment(
        FOO,
        Intervals.of("2020-07-01T00:00:00.000Z/2020-07-01T01:00:00.000Z"),
        "v1",
        null,
        null,
        null,
        null,
        0,
        0
    );

    final DataSegment fooSegment2 = new DataSegment(
        FOO,
        Intervals.of("2020-07-01T01:00:00.000Z/2020-07-01T02:00:00.000Z"),
        "v1",
        null,
        null,
        null,
        null,
        0,
        0
    );

    final DataSegment fooSegment3 = new DataSegment(
        FOO,
        Intervals.of("2020-07-01T02:00:00.000Z/2020-07-01T03:00:00.000Z"),
        "v1",
        null,
        null,
        null,
        null,
        0,
        0
    );

    final DataSegment barSegment1 = new DataSegment(
        BAR,
        Intervals.of("2020-07-01T20:00:00.000Z/2020-07-01T21:00:00.000Z"),
        "v1",
        null,
        null,
        null,
        null,
        0,
        0
    );

    final DataSegment barSegment2 = new DataSegment(
        BAR,
        Intervals.of("2020-07-01T22:00:00.000Z/2020-07-01T23:00:00.000Z"),
        "v1",
        null,
        null,
        null,
        null,
        0,
        0
    );
    TimelineLookup<String, DataSegment> fooTimeLine = VersionedIntervalTimeline.forSegments(ImmutableList.of(
        fooSegment1,
        fooSegment2,
        fooSegment3
    ));
    TimelineLookup<String, DataSegment> barTimeLine = VersionedIntervalTimeline.forSegments(ImmutableList.of(
        barSegment1,
        barSegment2
    ));
    Map<String, TimelineLookup<String, DataSegment>> timelineMap = ImmutableMap.of(
        FOO,
        fooTimeLine,
        BAR,
        barTimeLine
    );
    List<Interval> intervals = ImmutableList.of(
        Intervals.of("2020-07-01/2020-07-02"),
        Intervals.of("2020-07-02/2020-07-03")
    );

    List<List<TimelineObjectHolder<String, DataSegment>>> multiDataSourceServerLookup = unionDataSource.retrieveSegmentsForIntervals(
        intervals,
        timelineMap,
        (interval, timeline) ->
            timeline.lookup(interval)
    );
    List<TimelineObjectHolder<String, DataSegment>> fooTimelineObjects = new ArrayList<>();
    fooTimelineObjects.add(new TimelineObjectHolder<>(
        Intervals.of("2020-07-01T00:00:00.000Z/2020-07-01T01:00:00.000Z"),
        "v1",
        new PartitionHolder<>(new NumberedPartitionChunk<>(
            0,
            1,
            fooSegment1
        ))
    ));
    fooTimelineObjects.add(new TimelineObjectHolder<>(
        Intervals.of("2020-07-01T01:00:00.000Z/2020-07-01T02:00:00.000Z"),
        "v1",
        new PartitionHolder<>(new NumberedPartitionChunk<>(
            0,
            1,
            fooSegment2
        ))
    ));
    fooTimelineObjects.add(new TimelineObjectHolder<>(
        Intervals.of("2020-07-01T02:00:00.000Z/2020-07-01T03:00:00.000Z"),
        "v1",
        new PartitionHolder<>(new NumberedPartitionChunk<>(
            0,
            1,
            fooSegment3
        ))
    ));

    List<TimelineObjectHolder<String, DataSegment>> barTimelineObjects = new ArrayList<>();
    barTimelineObjects.add(new TimelineObjectHolder<>(
        Intervals.of("2020-07-01T20:00:00.000Z/2020-07-01T21:00:00.000Z"),
        "v1",
        new PartitionHolder<>(new NumberedPartitionChunk<>(
            0,
            1,
            barSegment1
        ))
    ));
    barTimelineObjects.add(new TimelineObjectHolder<>(
        Intervals.of("2020-07-01T22:00:00.000Z/2020-07-01T23:00:00.000Z"),
        "v1",
        new PartitionHolder<>(new NumberedPartitionChunk<>(
            0,
            1,
            barSegment2
        ))
    ));
    Assert.assertEquals(ImmutableList.of(fooTimelineObjects, barTimelineObjects), multiDataSourceServerLookup);
  }
}
