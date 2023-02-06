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

import com.google.common.base.Preconditions;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.SlicerUtils;
import org.apache.druid.msq.querykit.DataSegmentTimelineView;
import org.apache.druid.query.filter.DimFilterUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.joda.time.Interval;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;

/**
 * Slices {@link TableInputSpec} into {@link SegmentsInputSlice}.
 */
public class TableInputSpecSlicer implements InputSpecSlicer
{
  private final DataSegmentTimelineView timelineView;

  public TableInputSpecSlicer(DataSegmentTimelineView timelineView)
  {
    this.timelineView = timelineView;
  }

  @Override
  public boolean canSliceDynamic(InputSpec inputSpec)
  {
    return true;
  }

  @Override
  public List<InputSlice> sliceStatic(InputSpec inputSpec, int maxNumSlices)
  {
    final TableInputSpec tableInputSpec = (TableInputSpec) inputSpec;
    final Set<DataSegmentWithInterval> prunedSegmentSet = getPrunedSegmentSet(tableInputSpec);
    return makeSlices(tableInputSpec, prunedSegmentSet, maxNumSlices);
  }

  @Override
  public List<InputSlice> sliceDynamic(
      InputSpec inputSpec,
      int maxNumSlices,
      int maxFilesPerSlice,
      long maxBytesPerSlice
  )
  {
    final TableInputSpec tableInputSpec = (TableInputSpec) inputSpec;
    final Set<DataSegmentWithInterval> prunedSegmentSet = getPrunedSegmentSet(tableInputSpec);

    if (prunedSegmentSet.isEmpty()) {
      return Collections.emptyList();
    }

    int totalFiles = 0;
    long totalBytes = 0;

    for (DataSegmentWithInterval segmentWithInterval : prunedSegmentSet) {
      totalFiles++;
      totalBytes += segmentWithInterval.getSegment().getSize();
    }

    final int numSlices =
        Math.min(
            maxNumSlices,
            Math.max(
                Ints.checkedCast(LongMath.divide(totalFiles, maxFilesPerSlice, RoundingMode.CEILING)),
                Ints.checkedCast(LongMath.divide(totalBytes, maxBytesPerSlice, RoundingMode.CEILING))
            )
        );

    return makeSlices(tableInputSpec, prunedSegmentSet, numSlices);
  }

  private Set<DataSegmentWithInterval> getPrunedSegmentSet(final TableInputSpec tableInputSpec)
  {
    final TimelineLookup<String, DataSegment> timeline =
        timelineView.getTimeline(tableInputSpec.getDataSource(), tableInputSpec.getIntervals()).orElse(null);

    if (timeline == null) {
      return Collections.emptySet();
    } else {
      final Iterator<DataSegmentWithInterval> dataSegmentIterator =
          tableInputSpec.getIntervals().stream()
                        .flatMap(interval -> timeline.lookup(interval).stream())
                        .flatMap(
                            holder ->
                                StreamSupport.stream(holder.getObject().spliterator(), false)
                                             .map(
                                                 chunk ->
                                                     new DataSegmentWithInterval(
                                                         chunk.getObject(),
                                                         holder.getInterval()
                                                     )
                                             )
                        ).iterator();

      return DimFilterUtils.filterShards(
          tableInputSpec.getFilter(),
          () -> dataSegmentIterator,
          segment -> segment.getSegment().getShardSpec()
      );
    }
  }

  private List<InputSlice> makeSlices(
      final TableInputSpec tableInputSpec,
      final Set<DataSegmentWithInterval> prunedSegmentSet,
      final int maxNumSlices
  )
  {
    if (prunedSegmentSet.isEmpty()) {
      return Collections.emptyList();
    }

    final List<List<DataSegmentWithInterval>> assignments = SlicerUtils.makeSlices(
        prunedSegmentSet.iterator(),
        segment -> segment.getSegment().getSize(),
        maxNumSlices
    );

    final List<InputSlice> retVal = new ArrayList<>();

    for (final List<DataSegmentWithInterval> dataSegmentWithIntervals : assignments) {
      final List<RichSegmentDescriptor> descriptors = new ArrayList<>();
      for (final DataSegmentWithInterval dataSegmentWithInterval : dataSegmentWithIntervals) {
        descriptors.add(dataSegmentWithInterval.toRichSegmentDescriptor());
      }

      if (descriptors.isEmpty()) {
        retVal.add(NilInputSlice.INSTANCE);
      } else {
        retVal.add(new SegmentsInputSlice(tableInputSpec.getDataSource(), descriptors));
      }
    }

    return retVal;
  }

  private static class DataSegmentWithInterval
  {
    private final DataSegment segment;
    private final Interval interval;

    public DataSegmentWithInterval(DataSegment segment, Interval interval)
    {
      this.segment = Preconditions.checkNotNull(segment, "segment");
      this.interval = Preconditions.checkNotNull(interval, "interval");
    }

    public DataSegment getSegment()
    {
      return segment;
    }

    public RichSegmentDescriptor toRichSegmentDescriptor()
    {
      return new RichSegmentDescriptor(
          segment.getInterval(),
          interval,
          segment.getVersion(),
          segment.getShardSpec().getPartitionNum()
      );
    }
  }
}
