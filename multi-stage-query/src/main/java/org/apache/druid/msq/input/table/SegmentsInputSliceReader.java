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

import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.CounterNames;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.exec.DataServerQueryHandler;
import org.apache.druid.msq.exec.DataServerQueryHandlerFactory;
import org.apache.druid.msq.exec.FrameContext;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.timeline.SegmentId;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Reads slices of type {@link SegmentsInputSlice}.
 */
public class SegmentsInputSliceReader implements InputSliceReader
{
  private final DataSegmentProvider dataSegmentProvider;
  private final DataServerQueryHandlerFactory dataServerQueryHandlerFactory;
  private final boolean isReindex;

  public SegmentsInputSliceReader(final FrameContext frameContext, final boolean isReindex)
  {
    this.dataSegmentProvider = frameContext.dataSegmentProvider();
    this.dataServerQueryHandlerFactory = frameContext.dataServerQueryHandlerFactory();
    this.isReindex = isReindex;
  }

  @Override
  public PhysicalInputSlice attach(
      final int inputNumber,
      final InputSlice slice,
      final CounterTracker counters,
      final Consumer<Throwable> warningPublisher
  )
  {
    final SegmentsInputSlice segmentsInputSlice = (SegmentsInputSlice) slice;
    final ChannelCounters inputCounters = counters.channel(CounterNames.inputChannel(inputNumber))
                                                  .setTotalFiles(slice.fileCount());
    final List<LoadableSegment> loadableSegments = new ArrayList<>();
    final List<DataServerQueryHandler> queryableServers = new ArrayList<>();

    for (final RichSegmentDescriptor descriptor : segmentsInputSlice.getDescriptors()) {
      final SegmentId segmentId = SegmentId.of(
          segmentsInputSlice.getDataSource(),
          descriptor.getFullInterval(),
          descriptor.getVersion(),
          descriptor.getPartitionNumber()
      );
      loadableSegments.add(dataSegmentProvider.fetchSegment(segmentId, descriptor, inputCounters, isReindex));
    }

    if (segmentsInputSlice.getQueryableServers() != null) {
      for (final DataServerRequestDescriptor queryableServer : segmentsInputSlice.getQueryableServers()) {
        queryableServers.add(
            dataServerQueryHandlerFactory.createDataServerQueryHandler(
                inputNumber,
                segmentsInputSlice.getDataSource(),
                inputCounters,
                queryableServer
            )
        );
      }
    }

    return new PhysicalInputSlice(ReadablePartitions.empty(), loadableSegments, queryableServers);
  }
}
