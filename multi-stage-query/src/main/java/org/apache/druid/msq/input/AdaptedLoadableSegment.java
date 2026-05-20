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

package org.apache.druid.msq.input;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.error.DruidException;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link LoadableSegment} for segments adapted from non-regular sources such as inline data,
 * external data, or lookups. These segments may reference files on disk or in cloud storage, not just in-memory data.
 *
 * @see RegularLoadableSegment for segments loaded via SegmentManager
 */
public class AdaptedLoadableSegment implements LoadableSegment
{
  private final AtomicBoolean acquired = new AtomicBoolean(false);
  private final SegmentDescriptor descriptor;
  @Nullable
  private final ChannelCounters inputCounters;
  @Nullable
  private final String description;
  private final ListenableFuture<DataSegment> dataSegmentFuture;
  private final AcquireSegmentAction acquireSegmentAction;

  private AdaptedLoadableSegment(
      final Segment segment,
      final SegmentDescriptor descriptor,
      @Nullable final String description,
      @Nullable final ChannelCounters inputCounters
  )
  {
    this.descriptor = descriptor;
    this.description = description;
    this.inputCounters = inputCounters;

    // These segments don't have an associated DataSegment
    this.dataSegmentFuture = Futures.immediateFailedFuture(
        DruidException.defensive("DataSegment not available for adapted segments")
    );

    // Pre-create the acquire action since the segment is already available
    final ListenableFuture<AcquireSegmentResult> segmentFuture =
        Futures.immediateFuture(AcquireSegmentResult.cached(ReferenceCountedSegmentProvider.of(segment)));
    this.acquireSegmentAction = new AcquireSegmentAction(() -> segmentFuture, null);
  }

  /**
   * Creates an AdaptedLoadableSegment wrapper around a Segment object which is not a regular Druid segment,
   * has no associated {@link DataSegment}, and whose lifecycle is not managed by the LoadableSegment instance.
   *
   * @param segment         the segment to wrap
   * @param queryInterval   the query interval to use for filtering
   * @param description     user-oriented description for error messages
   * @param channelCounters counters for tracking input
   */
  public static AdaptedLoadableSegment create(
      final Segment segment,
      final Interval queryInterval,
      @Nullable final String description,
      @Nullable final ChannelCounters channelCounters
  )
  {
    return new AdaptedLoadableSegment(
        segment,
        new SegmentDescriptor(queryInterval, "0", 0),
        description,
        channelCounters
    );
  }

  @Override
  public ListenableFuture<DataSegment> dataSegmentFuture()
  {
    return dataSegmentFuture;
  }

  @Override
  public SegmentDescriptor descriptor()
  {
    return descriptor;
  }

  @Override
  @Nullable
  public ChannelCounters inputCounters()
  {
    return inputCounters;
  }

  @Override
  @Nullable
  public String description()
  {
    return description;
  }

  /**
   * Adapted segments are not managed by SegmentManager, so they are never cached.
   */
  @Override
  public Optional<Segment> acquireIfCached()
  {
    return Optional.empty();
  }

  @Override
  public AcquireSegmentAction acquire()
  {
    if (!acquired.compareAndSet(false, true)) {
      throw DruidException.defensive("Segment with descriptor[%s] is already acquired", descriptor);
    }
    return acquireSegmentAction;
  }
}
