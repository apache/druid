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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * This class represents an intent to acquire a reference to a {@link Segment} and then use it to do stuff, finally
 * closing when done. When an {@link AcquireSegmentAction} is created, segment cache implementations will create a
 * 'hold' to ensure it cannot be removed from the cache until this action has been closed. {@link #getSegmentFuture()}
 * actually initiates the 'action', and will return the segment if already cached, or attempt to download from deep
 * storage to load into the cache if not. The {@link Segment} returned by the future places a separate hold on the
 * cache until the segment itself is closed, and MUST be closed when the caller is finished doing segment things with
 * it. The caller must also call {@link #close()} on this object to clean up the hold that exists while possibly
 * loading the segment, and may do so as soon as the {@link Segment} is acquired (or can do so earlier to abort the
 * load and release the hold).
 */
public class AcquireSegmentAction implements Closeable
{
  public static final Closeable NOOP_CLEANUP = () -> {};

  public static List<SegmentReference> mapAllSegments(
      List<AcquireSegmentAction> acquireSegmentActions,
      SegmentMapFunction segmentMapFunction,
      long timeoutAt
  )
  {
    final Closer safetyNet = Closer.create();
    Throwable failure = null;

    // getting the future kicks off any background action, so materialize them all to a list to get things started
    final List<ListenableFuture<Optional<Segment>>> futures = new ArrayList<>(acquireSegmentActions.size());
    for (AcquireSegmentAction acquireSegmentAction : acquireSegmentActions) {
      safetyNet.register(acquireSegmentAction);
      // if we haven't failed yet, keep collecing futures (we always want to collect the actions themselves though
      // to close
      if (failure == null) {
        try {
          futures.add(acquireSegmentAction.getSegmentFuture());
        }
        catch (Throwable t) {
          failure = t;
        }
      } else {
        futures.add(Futures.immediateFuture(Optional.empty()));
      }
    }

    boolean isTimeout = false;

    final List<SegmentReference> segmentReferences = new ArrayList<>(acquireSegmentActions.size());
    int failCount = 0;
    for (int i = 0; i < acquireSegmentActions.size(); i++) {
      // if anything fails, want to ignore it initially so we can collect all additional futures to properly clean up
      // all references before rethrowing the error
      try {
        final AcquireSegmentAction action = acquireSegmentActions.get(i);
        final ListenableFuture<Optional<Segment>> future = futures.get(i);
        final Optional<Segment> segment = future.get(timeoutAt - System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                                .map(safetyNet::register);
        segmentReferences.add(
            new SegmentReference(
                action.getDescriptor(),
                segmentMapFunction.apply(segment),
                action
            )
        );
      }
      catch (Throwable t) {
        // add callback to make sure everything gets cleaned up, just in case
        Futures.addCallback(futures.get(i), releaseCallback(acquireSegmentActions.get(i)), Execs.directExecutor());
        if (failure == null) {
          failure = t;
        } else {
          failCount++;
          // no need to get carried away, if a bunch fail this ceases to be useful
          if (failCount <= 10) {
            failure.addSuppressed(t);
          }
        }
      }
    }
    if (failure != null) {
      throw CloseableUtils.closeAndWrapInCatch(failure, safetyNet);
    }
    return segmentReferences;
  }

  public static AcquireSegmentAction missingSegment(final SegmentDescriptor descriptor)
  {
    return new AcquireSegmentAction(descriptor, () -> Futures.immediateFuture(Optional.empty()), NOOP_CLEANUP);
  }

  private final SegmentDescriptor segmentDescriptor;
  private final Supplier<ListenableFuture<Optional<Segment>>> segmentFutureSupplier;
  private final Closeable loadCleanup;
  private AtomicBoolean closed = new AtomicBoolean(false);

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

  /**
   * Get a {@link Segment} reference to an item that exists in the cache, or if necessary/possible, fetching it from
   * deep storage. Ultimately, the reference is provided with {@link ReferenceCountedSegmentProvider#acquireReference()}
   * either as an immediate future if the segment already exists in cache. The
   * 'action' to fetch the segment and acquire the reference is not initiated until this method is called.
   */
  public ListenableFuture<Optional<Segment>> getSegmentFuture()
  {
    return segmentFutureSupplier.get();
  }

  @Override
  public void close() throws IOException
  {
    if (closed.compareAndSet(false, true)) {
      loadCleanup.close();
    }
  }

  public static FutureCallback<Optional<Segment>> releaseCallback(final AcquireSegmentAction action)
  {
    return new FutureCallback<>()
    {
      @Override
      public void onSuccess(Optional<Segment> result)
      {
        if (result.isPresent()) {
          try {
            result.get().close();
            action.close();
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }

      @Override
      public void onFailure(Throwable t)
      {
        try {
          action.close();
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}
