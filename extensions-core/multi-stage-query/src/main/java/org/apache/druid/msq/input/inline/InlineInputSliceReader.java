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

package org.apache.druid.msq.input.inline;

import com.google.common.collect.Iterables;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.ReadableInputs;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.msq.input.table.SegmentWithDescriptor;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.segment.InlineSegmentWrangler;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.timeline.SegmentId;

import java.util.function.Consumer;

/**
 * Reads {@link InlineInputSlice} using {@link SegmentWrangler} (which is expected to contain an
 * {@link InlineSegmentWrangler}).
 */
public class InlineInputSliceReader implements InputSliceReader
{
  public static final String SEGMENT_ID = "__inline";
  private static final RichSegmentDescriptor DUMMY_SEGMENT_DESCRIPTOR
      = new RichSegmentDescriptor(SegmentId.dummy(SEGMENT_ID).toDescriptor(), null);

  private final SegmentWrangler segmentWrangler;

  public InlineInputSliceReader(SegmentWrangler segmentWrangler)
  {
    this.segmentWrangler = segmentWrangler;
  }

  @Override
  public int numReadableInputs(InputSlice slice)
  {
    return 1;
  }

  @Override
  public ReadableInputs attach(
      final int inputNumber,
      final InputSlice slice,
      final CounterTracker counters,
      final Consumer<Throwable> warningPublisher
  )
  {
    final InlineDataSource dataSource = ((InlineInputSlice) slice).getDataSource();

    return ReadableInputs.segments(
        Iterables.transform(
            segmentWrangler.getSegmentsForIntervals(dataSource, Intervals.ONLY_ETERNITY),
            segment -> ReadableInput.segment(
                new SegmentWithDescriptor(
                    () -> ResourceHolder.fromCloseable(segment),
                    DUMMY_SEGMENT_DESCRIPTOR
                )
            )
        )
    );
  }
}
