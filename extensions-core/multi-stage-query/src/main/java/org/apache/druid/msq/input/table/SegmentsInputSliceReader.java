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

import com.google.common.collect.Iterators;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.CounterNames;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.ReadableInputs;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.timeline.SegmentId;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * Reads slices of type {@link SegmentsInputSlice}.
 */
public class SegmentsInputSliceReader implements InputSliceReader
{
  private final DataSegmentProvider dataSegmentProvider;

  public SegmentsInputSliceReader(final DataSegmentProvider dataSegmentProvider)
  {
    this.dataSegmentProvider = dataSegmentProvider;
  }

  @Override
  public int numReadableInputs(InputSlice slice)
  {
    final SegmentsInputSlice segmentsInputSlice = (SegmentsInputSlice) slice;
    return segmentsInputSlice.getDescriptors().size();
  }

  @Override
  public ReadableInputs attach(
      final int inputNumber,
      final InputSlice slice,
      final CounterTracker counters,
      final Consumer<Throwable> warningPublisher
  )
  {
    final SegmentsInputSlice segmentsInputSlice = (SegmentsInputSlice) slice;

    return ReadableInputs.segments(
        () -> Iterators.transform(
            dataSegmentIterator(
                segmentsInputSlice.getDataSource(),
                segmentsInputSlice.getDescriptors(),
                counters.channel(CounterNames.inputChannel(inputNumber)).setTotalFiles(slice.fileCount())
            ),
            ReadableInput::segment
        )
    );
  }

  private Iterator<SegmentWithDescriptor> dataSegmentIterator(
      final String dataSource,
      final List<RichSegmentDescriptor> descriptors,
      final ChannelCounters channelCounters
  )
  {
    return descriptors.stream().map(
        descriptor -> {
          final SegmentId segmentId = SegmentId.of(
              dataSource,
              descriptor.getFullInterval(),
              descriptor.getVersion(),
              descriptor.getPartitionNumber()
          );

          final ResourceHolder<Segment> segmentHolder = dataSegmentProvider.fetchSegment(segmentId, channelCounters);
          return new SegmentWithDescriptor(segmentHolder, descriptor);
        }
    ).iterator();
  }
}
