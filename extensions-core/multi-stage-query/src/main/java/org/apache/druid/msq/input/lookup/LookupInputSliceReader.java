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

package org.apache.druid.msq.input.lookup;

import com.google.common.collect.Iterators;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.ReadableInputs;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.msq.input.table.SegmentWithDescriptor;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Reads {@link LookupInputSlice} using a {@link SegmentWrangler} (which is expected to contain a
 * {@link org.apache.druid.segment.LookupSegmentWrangler}).
 */
public class LookupInputSliceReader implements InputSliceReader
{
  private static final Logger log = new Logger(LookupInputSliceReader.class);

  private final SegmentWrangler segmentWrangler;

  public LookupInputSliceReader(SegmentWrangler segmentWrangler)
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
    final String lookupName = ((LookupInputSlice) slice).getLookupName();

    return ReadableInputs.segments(
        () -> Iterators.singletonIterator(
            ReadableInput.segment(
                new SegmentWithDescriptor(
                    () -> {
                      final Iterable<Segment> segments =
                          segmentWrangler.getSegmentsForIntervals(
                              new LookupDataSource(lookupName),
                              Intervals.ONLY_ETERNITY
                          );

                      final Iterator<Segment> segmentIterator = segments.iterator();
                      if (!segmentIterator.hasNext()) {
                        throw new ISE("Lookup[%s] is not loaded", lookupName);
                      }

                      final Segment segment = segmentIterator.next();
                      if (segmentIterator.hasNext()) {
                        // LookupSegmentWrangler always returns zero or one segments, so this code block can't
                        // happen. That being said: we'll program defensively anyway.
                        CloseableUtils.closeAndSuppressExceptions(
                            segment,
                            e -> log.warn(e, "Failed to close segment for lookup[%s]", lookupName)
                        );
                        throw new ISE("Lookup[%s] has multiple segments; cannot read", lookupName);
                      }

                      return ResourceHolder.fromCloseable(segment);
                    },
                    new RichSegmentDescriptor(SegmentId.dummy(lookupName).toDescriptor(), null)
                )
            )
        )
    );
  }
}
