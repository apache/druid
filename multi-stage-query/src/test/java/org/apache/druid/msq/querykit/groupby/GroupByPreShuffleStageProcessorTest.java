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

package org.apache.druid.msq.querykit.groupby;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.Interval;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GroupByPreShuffleStageProcessorTest
{
  private static final String DATASOURCE = "test";

  @Test
  public void testFilterBaseInput_notTimeBoundaryQuery()
  {
    final GroupByQuery query = makeNonTimeBoundaryQuery();
    final GroupByPreShuffleStageProcessor processor = new GroupByPreShuffleStageProcessor(query);

    final List<PhysicalInputSlice> slices = ImmutableList.of(
        makeSlice(
            makeSegment("2000/2001"),
            makeSegment("2001/2002"),
            makeSegment("2002/2003")
        )
    );

    final List<PhysicalInputSlice> result = processor.filterBaseInput(slices);
    assertSame(slices, result);
  }

  @Test
  public void testFilterBaseInput_emptySlices()
  {
    final GroupByPreShuffleStageProcessor processor =
        new GroupByPreShuffleStageProcessor(makeTimeBoundaryQuery(true, true));

    final List<PhysicalInputSlice> result = processor.filterBaseInput(Collections.emptyList());
    assertEquals(0, result.size());
  }

  @Test
  public void testFilterBaseInput_singleSegment()
  {
    final GroupByPreShuffleStageProcessor processor =
        new GroupByPreShuffleStageProcessor(makeTimeBoundaryQuery(true, true));

    final PhysicalInputSlice slice = makeSlice(makeSegment("2000/2001"));
    final List<PhysicalInputSlice> slices = ImmutableList.of(slice);

    final List<PhysicalInputSlice> result = processor.filterBaseInput(slices);
    assertEquals(1, result.size());
    assertSame(slice, result.get(0));
  }

  @Test
  public void testFilterBaseInput_emptySegmentList()
  {
    final GroupByPreShuffleStageProcessor processor =
        new GroupByPreShuffleStageProcessor(makeTimeBoundaryQuery(true, true));

    final PhysicalInputSlice slice = makeSlice();
    final List<PhysicalInputSlice> slices = ImmutableList.of(slice);

    final List<PhysicalInputSlice> result = processor.filterBaseInput(slices);
    assertEquals(1, result.size());
    assertSame(slice, result.get(0));
  }

  @Test
  public void testFilterBaseInput_minOnly()
  {
    final GroupByPreShuffleStageProcessor processor =
        new GroupByPreShuffleStageProcessor(makeTimeBoundaryQuery(true, false));

    final LoadableSegment earliest = makeSegment("2000/2001");
    final LoadableSegment middle = makeSegment("2001/2002");
    final LoadableSegment latest = makeSegment("2002/2003");

    final List<PhysicalInputSlice> result = processor.filterBaseInput(
        ImmutableList.of(makeSlice(earliest, middle, latest))
    );

    assertEquals(1, result.size());
    assertEquals(ImmutableList.of(earliest), result.get(0).getLoadableSegments());
  }

  @Test
  public void testFilterBaseInput_maxOnly()
  {
    final GroupByPreShuffleStageProcessor processor =
        new GroupByPreShuffleStageProcessor(makeTimeBoundaryQuery(false, true));

    final LoadableSegment earliest = makeSegment("2000/2001");
    final LoadableSegment middle = makeSegment("2001/2002");
    final LoadableSegment latest = makeSegment("2002/2003");

    final List<PhysicalInputSlice> result = processor.filterBaseInput(
        ImmutableList.of(makeSlice(earliest, middle, latest))
    );

    assertEquals(1, result.size());
    assertEquals(ImmutableList.of(latest), result.get(0).getLoadableSegments());
  }

  @Test
  public void testFilterBaseInput_minAndMax()
  {
    final GroupByPreShuffleStageProcessor processor =
        new GroupByPreShuffleStageProcessor(makeTimeBoundaryQuery(true, true));

    final LoadableSegment earliest = makeSegment("2000/2001");
    final LoadableSegment middle = makeSegment("2001/2002");
    final LoadableSegment latest = makeSegment("2002/2003");

    final List<PhysicalInputSlice> result = processor.filterBaseInput(
        ImmutableList.of(makeSlice(earliest, middle, latest))
    );

    assertEquals(1, result.size());
    assertEquals(ImmutableList.of(earliest, latest), result.get(0).getLoadableSegments());
  }

  @Test
  public void testFilterBaseInput_minAndMaxSameInterval()
  {
    final GroupByPreShuffleStageProcessor processor =
        new GroupByPreShuffleStageProcessor(makeTimeBoundaryQuery(true, true));

    final LoadableSegment seg1 = makeSegment("2000/2001");
    final LoadableSegment seg2 = makeSegment("2000/2001");

    final List<PhysicalInputSlice> result = processor.filterBaseInput(
        ImmutableList.of(makeSlice(seg1, seg2))
    );

    assertEquals(1, result.size());
    assertEquals(ImmutableList.of(seg1, seg2), result.get(0).getLoadableSegments());
  }

  @Test
  public void testFilterBaseInput_multipleSegmentsWithSameEarliestStart()
  {
    final GroupByPreShuffleStageProcessor processor =
        new GroupByPreShuffleStageProcessor(makeTimeBoundaryQuery(true, false));

    final LoadableSegment early1 = makeSegment("2000/2001");
    final LoadableSegment early2 = makeSegment("2000/2002");
    final LoadableSegment latest = makeSegment("2003/2004");

    final List<PhysicalInputSlice> result = processor.filterBaseInput(
        ImmutableList.of(makeSlice(early1, early2, latest))
    );

    // minInterval is 2000/2001 (earliest start). Both early1 (2000/2001) and early2 (2000/2002) overlap with it.
    assertEquals(1, result.size());
    assertEquals(ImmutableList.of(early1, early2), result.get(0).getLoadableSegments());
  }

  @Test
  public void testFilterBaseInput_multipleSegmentsWithSameLatestEnd()
  {
    final GroupByPreShuffleStageProcessor processor =
        new GroupByPreShuffleStageProcessor(makeTimeBoundaryQuery(false, true));

    final LoadableSegment earliest = makeSegment("2000/2001");
    final LoadableSegment late1 = makeSegment("2002/2004");
    final LoadableSegment late2 = makeSegment("2003/2004");

    final List<PhysicalInputSlice> result = processor.filterBaseInput(
        ImmutableList.of(makeSlice(earliest, late1, late2))
    );

    // maxInterval is 2002/2004 (latest end). Both late1 (2002/2004) and late2 (2003/2004) overlap with it.
    assertEquals(1, result.size());
    assertEquals(ImmutableList.of(late1, late2), result.get(0).getLoadableSegments());
  }

  @Test
  public void testFilterBaseInput_multipleSlices()
  {
    final GroupByPreShuffleStageProcessor processor =
        new GroupByPreShuffleStageProcessor(makeTimeBoundaryQuery(true, true));

    final LoadableSegment s1a = makeSegment("2000/2001");
    final LoadableSegment s1b = makeSegment("2001/2002");
    final LoadableSegment s1c = makeSegment("2002/2003");

    final LoadableSegment s2a = makeSegment("2010/2011");
    final LoadableSegment s2b = makeSegment("2011/2012");
    final LoadableSegment s2c = makeSegment("2012/2013");

    final List<PhysicalInputSlice> result = processor.filterBaseInput(
        ImmutableList.of(
            makeSlice(s1a, s1b, s1c),
            makeSlice(s2a, s2b, s2c)
        )
    );

    assertEquals(2, result.size());
    assertEquals(ImmutableList.of(s1a, s1c), result.get(0).getLoadableSegments());
    assertEquals(ImmutableList.of(s2a, s2c), result.get(1).getLoadableSegments());
  }

  @Test
  public void testFilterBaseInput_overlappingIntervals()
  {
    final GroupByPreShuffleStageProcessor processor =
        new GroupByPreShuffleStageProcessor(makeTimeBoundaryQuery(true, true));

    final LoadableSegment seg1 = makeSegment("2000/2003");
    final LoadableSegment seg2 = makeSegment("2001/2004");
    final LoadableSegment seg3 = makeSegment("2005/2006");

    final List<PhysicalInputSlice> result = processor.filterBaseInput(
        ImmutableList.of(makeSlice(seg1, seg2, seg3))
    );

    // min interval is 2000/2003 (earliest start), max interval is 2005/2006 (latest end).
    // seg1 overlaps with minInterval, seg2 overlaps with minInterval, seg3 overlaps with maxInterval.
    assertEquals(1, result.size());
    assertEquals(ImmutableList.of(seg1, seg2, seg3), result.get(0).getLoadableSegments());
  }

  @Test
  public void testFilterBaseInput_twoSegments()
  {
    final GroupByPreShuffleStageProcessor processor =
        new GroupByPreShuffleStageProcessor(makeTimeBoundaryQuery(true, true));

    final LoadableSegment earliest = makeSegment("2000/2001");
    final LoadableSegment latest = makeSegment("2002/2003");

    final List<PhysicalInputSlice> result = processor.filterBaseInput(
        ImmutableList.of(makeSlice(earliest, latest))
    );

    assertEquals(1, result.size());
    assertEquals(ImmutableList.of(earliest, latest), result.get(0).getLoadableSegments());
  }

  private static GroupByQuery makeTimeBoundaryQuery(final boolean includeMin, final boolean includeMax)
  {
    final GroupByQuery.Builder builder =
        GroupByQuery.builder()
                    .setDataSource(DATASOURCE)
                    .setQuerySegmentSpec(
                        new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.ETERNITY))
                    )
                    .setGranularity(Granularities.ALL);

    final ImmutableList.Builder<org.apache.druid.query.aggregation.AggregatorFactory> aggs =
        ImmutableList.builder();

    if (includeMin) {
      aggs.add(new LongMinAggregatorFactory("a0", ColumnHolder.TIME_COLUMN_NAME));
    }
    if (includeMax) {
      aggs.add(new LongMaxAggregatorFactory("a1", ColumnHolder.TIME_COLUMN_NAME));
    }

    builder.setAggregatorSpecs(aggs.build());
    return builder.build();
  }

  private static GroupByQuery makeNonTimeBoundaryQuery()
  {
    return GroupByQuery.builder()
                       .setDataSource(DATASOURCE)
                       .setQuerySegmentSpec(
                           new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.ETERNITY))
                       )
                       .setGranularity(Granularities.ALL)
                       .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                       .build();
  }

  private static LoadableSegment makeSegment(final String interval)
  {
    final Interval parsedInterval = Intervals.of(interval);
    final LoadableSegment segment = mock(LoadableSegment.class);
    when(segment.descriptor()).thenReturn(new SegmentDescriptor(parsedInterval, "1", 0));
    return segment;
  }

  private static PhysicalInputSlice makeSlice(final LoadableSegment... segments)
  {
    return new PhysicalInputSlice(
        ReadablePartitions.empty(),
        ImmutableList.copyOf(segments),
        Collections.emptyList()
    );
  }
}
