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

package org.apache.druid.msq.input.table;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.querykit.DataSegmentTimelineView;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

public class TableInputSpecSlicerTest extends InitializedNullHandlingTest
{
  private static final String DATASOURCE = "test-ds";
  private static final long BYTES_PER_SEGMENT = 1000;

  private static final DataSegment SEGMENT1 = new DataSegment(
      DATASOURCE,
      Intervals.of("2000/2001"),
      "1",
      Collections.emptyMap(),
      Collections.emptyList(),
      Collections.emptyList(),
      new DimensionRangeShardSpec(
          ImmutableList.of("dim"),
          null,
          new StringTuple(new String[]{"foo"}),
          0,
          2
      ),
      null,
      null,
      BYTES_PER_SEGMENT
  );

  private static final DataSegment SEGMENT2 = new DataSegment(
      DATASOURCE,
      Intervals.of("2000/2001"),
      "1",
      Collections.emptyMap(),
      Collections.emptyList(),
      Collections.emptyList(),
      new DimensionRangeShardSpec(
          ImmutableList.of("dim"),
          new StringTuple(new String[]{"foo"}),
          null,
          1,
          2
      ),
      null,
      null,
      BYTES_PER_SEGMENT
  );

  private VersionedIntervalTimeline<String, DataSegment> timeline;
  private DataSegmentTimelineView timelineView;
  private TableInputSpecSlicer slicer;

  @Before
  public void setUp()
  {
    timeline = VersionedIntervalTimeline.forSegments(ImmutableList.of(SEGMENT1, SEGMENT2));
    timelineView = (dataSource, intervals) -> {
      if (DATASOURCE.equals(dataSource)) {
        return Optional.of(timeline);
      } else {
        return Optional.empty();
      }
    };
    slicer = new TableInputSpecSlicer(timelineView);
  }

  @Test
  public void test_canSliceDynamic()
  {
    Assert.assertTrue(slicer.canSliceDynamic(new TableInputSpec(DATASOURCE, null, null)));
  }

  @Test
  public void test_sliceStatic_noDataSource()
  {
    final TableInputSpec spec = new TableInputSpec("no such datasource", null, null);
    Assert.assertEquals(Collections.emptyList(), slicer.sliceStatic(spec, 2));
  }

  @Test
  public void test_sliceStatic_intervalFilter()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        ImmutableList.of(
            Intervals.of("2000/P1M"),
            Intervals.of("2000-06-01/P1M")
        ),
        null
    );

    Assert.assertEquals(
        Collections.singletonList(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        Intervals.of("2000-06-01/P1M"),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        Intervals.of("2000-06-01/P1M"),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                )
            )
        ),
        slicer.sliceStatic(spec, 1)
    );
  }

  @Test
  public void test_sliceStatic_intervalFilterMatchesNothing()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        Collections.singletonList(Intervals.of("2002/P1M")),
        null
    );

    Assert.assertEquals(Collections.emptyList(), slicer.sliceStatic(spec, 2));
  }

  @Test
  public void test_sliceStatic_dimFilter()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        null,
        new SelectorDimFilter("dim", "bar", null)
    );

    Assert.assertEquals(
        ImmutableList.of(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        SEGMENT1.getInterval(),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    )
                )
            ),
            NilInputSlice.INSTANCE
        ),
        slicer.sliceStatic(spec, 2)
    );
  }

  @Test
  public void test_sliceStatic_intervalAndDimFilter()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        ImmutableList.of(
            Intervals.of("2000/P1M"),
            Intervals.of("2000-06-01/P1M")
        ),
        new SelectorDimFilter("dim", "bar", null)
    );

    Assert.assertEquals(
        ImmutableList.of(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    )
                )
            ),
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        Intervals.of("2000-06-01/P1M"),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    )
                )
            )
        ),
        slicer.sliceStatic(spec, 2)
    );
  }

  @Test
  public void test_sliceStatic_oneSlice()
  {
    final TableInputSpec spec = new TableInputSpec(DATASOURCE, null, null);
    Assert.assertEquals(
        Collections.singletonList(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        SEGMENT1.getInterval(),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        SEGMENT2.getInterval(),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                )
            )
        ),
        slicer.sliceStatic(spec, 1)
    );
  }

  @Test
  public void test_sliceStatic_needTwoSlices()
  {
    final TableInputSpec spec = new TableInputSpec(DATASOURCE, null, null);
    Assert.assertEquals(
        ImmutableList.of(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        SEGMENT1.getInterval(),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    )
                )
            ),
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        SEGMENT2.getInterval(),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                )
            )
        ),
        slicer.sliceStatic(spec, 2)
    );
  }

  @Test
  public void test_sliceStatic_threeSlices()
  {
    final TableInputSpec spec = new TableInputSpec(DATASOURCE, null, null);
    Assert.assertEquals(
        ImmutableList.of(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        SEGMENT1.getInterval(),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    )
                )
            ),
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        SEGMENT2.getInterval(),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                )
            ),
            NilInputSlice.INSTANCE
        ),
        slicer.sliceStatic(spec, 3)
    );
  }

  @Test
  public void test_sliceDynamic_none()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        ImmutableList.of(Intervals.of("2002/P1M")),
        null
    );

    Assert.assertEquals(
        Collections.emptyList(),
        slicer.sliceDynamic(spec, 1, 1, 1)
    );
  }

  @Test
  public void test_sliceDynamic_maxOneSlice()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        ImmutableList.of(Intervals.of("2000/P1M")),
        null
    );

    Assert.assertEquals(
        Collections.singletonList(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                )
            )
        ),
        slicer.sliceDynamic(spec, 1, 1, 1)
    );
  }

  @Test
  public void test_sliceDynamic_needOne()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        ImmutableList.of(Intervals.of("2000/P1M")),
        null
    );

    Assert.assertEquals(
        Collections.singletonList(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                )
            )
        ),
        slicer.sliceDynamic(spec, 100, 5, BYTES_PER_SEGMENT * 5)
    );
  }

  @Test
  public void test_sliceDynamic_needTwoDueToFiles()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        ImmutableList.of(Intervals.of("2000/P1M")),
        null
    );

    Assert.assertEquals(
        ImmutableList.of(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    )
                )
            ),
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                )
            )
        ),
        slicer.sliceDynamic(spec, 100, 1, BYTES_PER_SEGMENT * 5)
    );
  }

  @Test
  public void test_sliceDynamic_needTwoDueToBytes()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        ImmutableList.of(Intervals.of("2000/P1M")),
        null
    );

    Assert.assertEquals(
        ImmutableList.of(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    )
                )
            ),
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                )
            )
        ),
        slicer.sliceDynamic(spec, 100, 5, BYTES_PER_SEGMENT)
    );
  }
}
