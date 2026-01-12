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

package org.apache.druid.segment;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DataSegmentAndDescriptor;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wrapper for a {@link DataSegment} (Nullable), a {@link SegmentDescriptor} and an optional {@link Segment}.
 * {@link Segment} is created by a {@link SegmentMapFunction} being applied to a {@link ReferenceCountedSegmentProvider}.
 * <p>
 * If the {@link SegmentMapFunction} you want to apply is not available at the time the {@link SegmentReference}
 * is created, use {@link #map(SegmentMapFunction)} to apply it.
 * <p>
 * Closing this object closes both the {@link #getSegmentReference()} and any closeables attached from the process of
 * creating this object, such as from {@link AcquireSegmentAction}. The object from {@link #getSegmentReference()}
 * should not be closed directly by callers.
 */
public class SegmentReference implements Closeable
{
  public static SegmentReference missing(SegmentDescriptor segmentDescriptor)
  {
    return new SegmentReference(segmentDescriptor, Optional.empty(), null);
  }

  @Nullable
  private final DataSegment dataSegment;
  private final SegmentDescriptor segmentDescriptor;
  private final Optional<Segment> segmentReference;
  @Nullable
  private final Closeable cleanupHold;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public SegmentReference(
      DataSegmentAndDescriptor segment,
      Optional<Segment> segmentReference,
      @Nullable Closeable cleanupHold
  )
  {
    this(segment.getDataSegment(), segment.getDescriptor(), segmentReference, cleanupHold);
  }

  public SegmentReference(
      SegmentDescriptor segmentDescriptor,
      Optional<Segment> segmentReference,
      @Nullable Closeable cleanupHold
  )
  {
    this(null, segmentDescriptor, segmentReference, cleanupHold);
  }

  private SegmentReference(
      @Nullable DataSegment segment,
      SegmentDescriptor segmentDescriptor,
      Optional<Segment> segmentReference,
      @Nullable Closeable cleanupHold
  )
  {
    this.dataSegment = segment;
    this.segmentDescriptor = segmentDescriptor;
    this.segmentReference = segmentReference;
    this.cleanupHold = cleanupHold;
  }

  @Nullable
  public DataSegment getDataSegment()
  {
    return dataSegment;
  }

  public SegmentDescriptor getSegmentDescriptor()
  {
    return segmentDescriptor;
  }

  /**
   * Returns the actual segment. Do not close the Segment when you are done with it, only close the SegmentReference.
   */
  public Optional<Segment> getSegmentReference()
  {
    return segmentReference;
  }

  /**
   * Maps the wrapped segment and returns a reference to it. Closes the reference if the {@link SegmentMapFunction}
   * throws an exception. Regardless of success or failure of this method, the old reference should be discarded.
   * Do not call {@link #close()} on the old reference, only call it on the new one.
   */
  public SegmentReference map(final SegmentMapFunction segmentMapFn)
  {
    try {
      final Optional<Segment> mappedSegment = segmentMapFn.apply(segmentReference);

      // Resources are handed off to the new reference.
      return new SegmentReference(
          segmentDescriptor,
          mappedSegment,
          cleanupHold
      );
    }
    catch (Throwable e) {
      // segmentMapFn threw an error
      throw CloseableUtils.closeAndWrapInCatch(e, this);
    }
    finally {
      // After this method is done, prevent this instance from being closed again.
      closed.set(true);
    }
  }

  @Override
  public void close() throws IOException
  {
    if (!closed.compareAndSet(false, true)) {
      throw DruidException.defensive("Reference is closed, cannot close again");
    }

    final Closer closer = Closer.create();
    closer.register(cleanupHold);
    segmentReference.ifPresent(closer::register);
    closer.close();
  }
}
