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
import org.apache.druid.segment.ReferenceCountedObjectProvider;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * This class represents an intent to acquire a reference to a {@link Segment} and then use it to do stuff, and finally
 * closing when done.
 * <p>
 * When an {@link AcquireSegmentAction} is created, segment cache implementations will create a
 * 'hold' to ensure it cannot be removed from the cache until this action has been closed.
 * <p>
 * Calling {@link #getSegmentFuture()} is what actually initiates the 'action', and will return a segment reference
 * provider if already cached, or attempt to download from deep storage to load into the cache if not. The
 * {@link Segment} returned by acquiring a reference from the {@link ReferenceCountedObjectProvider} returned by the
 * future places a separate hold on the cache until the segment itself is closed, and MUST be closed when the caller is
 * finished doing segment things with it.
 * <p>
 * The caller must also call {@link #close()} on this object to clean up the hold that exists while possibly loading
 * the segment, and may do so as soon as the {@link Segment} is acquired (or can do so earlier to abort the load and
 * release the hold).
 */
public class AcquireSegmentAction implements Closeable
{
  public static AcquireSegmentAction missingSegment()
  {
    return new AcquireSegmentAction(() -> Futures.immediateFuture(AcquireSegmentResult.empty()), null);
  }

  private final Supplier<ListenableFuture<AcquireSegmentResult>> segmentFutureSupplier;
  @Nullable
  private final Closeable loadCleanup;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public AcquireSegmentAction(
      Supplier<ListenableFuture<AcquireSegmentResult>> segmentFutureSupplier,
      @Nullable Closeable loadCleanup
  )
  {
    this.segmentFutureSupplier = segmentFutureSupplier;
    this.loadCleanup = loadCleanup;
  }

  /**
   * Get a {@link ReferenceCountedObjectProvider<Segment>} to acquire a reference to an item that exists in the cache,
   * or if necessary/possible, fetching it from deep storage. This is typically {@link ReferenceCountedSegmentProvider},
   * either as an immediate future if the segment already exists in cache. The 'action' to fetch the segment and return
   * the reference provider is not initiated until this method is called.
   */
  public ListenableFuture<AcquireSegmentResult> getSegmentFuture()
  {
    return segmentFutureSupplier.get();
  }

  @Override
  public void close() throws IOException
  {
    if (loadCleanup != null && closed.compareAndSet(false, true)) {
      loadCleanup.close();
    }
  }
}
