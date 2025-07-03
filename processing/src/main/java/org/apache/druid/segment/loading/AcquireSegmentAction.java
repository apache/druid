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

package org.apache.druid.segment.loading;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.utils.CloseableUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class represents an intent to acquire a reference to a {@link Segment} and then use it to do stuff, finally
 * closing when done. When an {@link AcquireSegmentAction} is created, segment cache implementations will create a
 * 'hold' to ensure it cannot be removed from the cache until this action has been closed. {@link #getSegmentFuture()}
 * will return the segment if already cached, or attempt to download from deep storage to load into the cache if not.
 * The {@link Segment} returned by the future places a separate hold on the cache until the segment itself is closed,
 * and MUST be closed when the caller is finished doing segment things with it. The caller must also call
 * {@link #close()} to clean up the hold that exists while possibly loading the segment, and may do so as soon as
 * the {@link Segment} is acquired (or can do so earlier to abort the load and release the hold).
 */
public class AcquireSegmentAction implements Closeable
{
  private static final Closeable NOOP = () -> {};

  public static List<SegmentReference> mapAllSegments(
      List<AcquireSegmentAction> acquireSegmentActions,
      SegmentMapFunction segmentMapFunction,
      Closer closer,
      long timeoutAt
  )
  {
    final List<ListenableFuture<Optional<Segment>>> futures =
        acquireSegmentActions.stream().map(AcquireSegmentAction::getSegmentFuture).collect(Collectors.toList());
    final List<SegmentReference> segmentReferences = new ArrayList<>(acquireSegmentActions.size());
    Throwable failure = null;
    for (int i = 0; i < acquireSegmentActions.size(); i++) {
      try {
        final ListenableFuture<Optional<Segment>> future = futures.get(i);
        final Optional<Segment> segment = future.get(timeoutAt - System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                                .map(closer::register);
        segmentReferences.add(
            new SegmentReference(
                acquireSegmentActions.get(i).getDescriptor(),
                segmentMapFunction.apply(segment)
            )
        );
      }
      catch (Throwable t) {
        if (failure == null) {
          failure = t;
        } else {
          failure.addSuppressed(t);
        }
      }
    }
    if (failure != null) {
      throw CloseableUtils.closeAndWrapInCatch(failure, closer);
    }
    return segmentReferences;
  }

  public static AcquireSegmentAction alreadyLoaded(
      final SegmentDescriptor descriptor,
      final Optional<Segment> segment
  )
  {
    return new AcquireSegmentAction(descriptor, () -> Futures.immediateFuture(segment), NOOP);
  }

  public static AcquireSegmentAction missingSegment(final SegmentDescriptor descriptor)
  {
    return new AcquireSegmentAction(descriptor, () -> Futures.immediateFuture(Optional.empty()), NOOP);
  }

  private final SegmentDescriptor segmentDescriptor;
  private final Supplier<ListenableFuture<Optional<Segment>>> segmentFutureSupplier;
  private final Closeable loadCleanup;

  public AcquireSegmentAction(
      SegmentDescriptor segmentDescriptor,
      Supplier<ListenableFuture<Optional<Segment>>> segmentFutureSupplier,
      Closeable loadCleanup
  )
  {
    this.segmentDescriptor = segmentDescriptor;
    this.segmentFutureSupplier = segmentFutureSupplier;
    this.loadCleanup = loadCleanup;
  }

  public SegmentDescriptor getDescriptor()
  {
    return segmentDescriptor;
  }

  public ListenableFuture<Optional<Segment>> getSegmentFuture()
  {
    return segmentFutureSupplier.get();
  }

  @Override
  public void close() throws IOException
  {
    loadCleanup.close();
  }
}
