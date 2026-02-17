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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.msq.exec.FrameContext;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.querykit.BaseLeafStageProcessor;
import org.apache.druid.msq.querykit.ReadableInput;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.SegmentMapFunction;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;

@JsonTypeName("groupByPreShuffle")
public class GroupByPreShuffleStageProcessor extends BaseLeafStageProcessor
{
  private final GroupByQuery query;

  @JsonCreator
  public GroupByPreShuffleStageProcessor(@JsonProperty("query") GroupByQuery query)
  {
    super(query);
    this.query = Preconditions.checkNotNull(query, "query");
  }

  @JsonProperty
  public GroupByQuery getQuery()
  {
    return query;
  }

  @Override
  protected FrameProcessor<Object> makeProcessor(
      final ReadableInput baseInput,
      final SegmentMapFunction segmentMapFn,
      final ResourceHolder<WritableFrameChannel> outputChannelHolder,
      final ResourceHolder<FrameWriterFactory> frameWriterFactoryHolder,
      final FrameContext frameContext
  )
  {
    return new GroupByPreShuffleFrameProcessor(
        query,
        frameContext.groupingEngine(),
        frameContext.processingBuffers().getBufferPool(),
        baseInput,
        segmentMapFn,
        outputChannelHolder,
        frameWriterFactoryHolder
    );
  }

  @Override
  protected List<PhysicalInputSlice> filterBaseInput(final List<PhysicalInputSlice> slices)
  {
    if (!GroupByTimeBoundaryUtils.isTimeBoundaryQuery(query)) {
      return slices;
    }

    // This is a time-boundary style query (see GroupByTimeBoundaryUtils.isTimeBoundaryQuery).
    // This means we can look at just the earliest (for min) and latest (for max) segments,
    // ignoring the ones in the middle.
    final boolean needsMin = GroupByTimeBoundaryUtils.needsMinTime(query);
    final boolean needsMax = GroupByTimeBoundaryUtils.needsMaxTime(query);

    final List<PhysicalInputSlice> filteredSlices = new ArrayList<>(slices.size());

    for (final PhysicalInputSlice slice : slices) {
      final List<LoadableSegment> segments = slice.getLoadableSegments();

      if (segments.size() <= 1) {
        filteredSlices.add(slice);
        continue;
      }

      // Find the earliest and latest intervals by start time.
      Interval minInterval = null;
      Interval maxInterval = null;

      for (final LoadableSegment segment : segments) {
        final Interval interval = segment.descriptor().getInterval();
        if (needsMin) {
          if (minInterval == null || interval.getStart().isBefore(minInterval.getStart())) {
            minInterval = interval;
          }
        }
        if (needsMax) {
          if (maxInterval == null || interval.getEnd().isAfter(maxInterval.getEnd())) {
            maxInterval = interval;
          }
        }
      }

      // Keep only segments whose interval overlaps with the earliest or latest interval.
      final List<LoadableSegment> kept = new ArrayList<>();
      for (final LoadableSegment segment : segments) {
        final Interval segmentInterval = segment.descriptor().getInterval();
        if ((minInterval != null && segmentInterval.overlaps(minInterval))
            || (maxInterval != null && segmentInterval.overlaps(maxInterval))) {
          kept.add(segment);
        }
      }

      filteredSlices.add(new PhysicalInputSlice(slice.getReadablePartitions(), kept, slice.getQueryableServers()));
    }

    return filteredSlices;
  }

  @Override
  public boolean usesProcessingBuffers()
  {
    return true;
  }
}
