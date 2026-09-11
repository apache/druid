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
import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.error.DruidException;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.loading.AcquireMode;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Implementation of {@link LoadableSegment} for segments adapted from non-regular sources such as inline data,
 * external data, or lookups. These segments may reference files on disk or in cloud storage, not just in-memory data.
 *
 * @see RegularLoadableSegment for segments loaded via SegmentManager
 */
public class AdaptedLoadableSegment implements LoadableSegment
{
  private final AtomicBoolean acquired = new AtomicBoolean(false);
  private final Supplier<AsyncResource<AcquireSegmentResult>> asyncSegmentSupplier;
  private final SegmentDescriptor descriptor;
  @Nullable
  private final String description;
  @Nullable
  private final ChannelCounters inputCounters;

  /**
   * Creates a wrapper around a supplier of an {@link AcquireSegmentResult}. The lifecycle of the supplied
   * {@link AsyncResource} is folded into the {@link AcquireSegmentAction} returned from {@link #acquire}: closing
   * the delivered segment (or the un-released action) closes the resource, releasing whatever it owns.
   *
   * @param asyncSegmentSupplier the supplier to wrap. The supplied resource's result must carry its segment as a
   *                             {@link ReferenceCountedSegmentProvider.LeafReference} so the resource's close can be
   *                             folded into the segment's close.
   * @param descriptor           descriptor containing the interval to use for filtering
   * @param description          user-oriented description for error messages
   * @param inputCounters        counters for tracking input, updated at delivery via {@link #countDelivered}.
   */
  public AdaptedLoadableSegment(
      final Supplier<AsyncResource<AcquireSegmentResult>> asyncSegmentSupplier,
      final SegmentDescriptor descriptor,
      @Nullable final String description,
      @Nullable final ChannelCounters inputCounters
  )
  {
    this.asyncSegmentSupplier = asyncSegmentSupplier;
    this.descriptor = descriptor;
    this.description = description;
    this.inputCounters = inputCounters;
  }

  /**
   * Creates an AdaptedLoadableSegment wrapper around a Segment object which is not a regular Druid segment,
   * has no associated {@link DataSegment}, and whose lifecycle is not managed by the LoadableSegment instance.
   *
   * @param segment       the segment to wrap
   * @param descriptor    descriptor containing the interval to use for filtering
   * @param description   user-oriented description for error messages
   * @param inputCounters counters for tracking input
   */
  public static AdaptedLoadableSegment fromUnmanagedSegment(
      final Segment segment,
      final SegmentDescriptor descriptor,
      @Nullable final String description,
      @Nullable final ChannelCounters inputCounters
  )
  {
    // Pre-create the acquire result since the segment is already available. The segment's lifecycle is unmanaged:
    // the delivered UnmanagedReference's close is a no-op on the wrapped segment.
    final AcquireSegmentResult acquireSegmentResult =
        AcquireSegmentResult.of(ReferenceCountedSegmentProvider.unmanaged(segment));

    final AsyncResource<AcquireSegmentResult> resource = AsyncResources.unmanaged(acquireSegmentResult);
    return new AdaptedLoadableSegment(
        () -> resource,
        descriptor,
        description,
        inputCounters
    );
  }

  @Override
  public ListenableFuture<DataSegment> dataSegmentFuture()
  {
    return Futures.immediateFailedFuture(
        DruidException.defensive("DataSegment not available for adapted segments")
    );
  }

  @Override
  public SegmentDescriptor descriptor()
  {
    return descriptor;
  }

  @Override
  @Nullable
  public String description()
  {
    return description;
  }

  /**
   * Adapted segments are not managed by SegmentManager, so they are never cached. The {@code acquireMode} is ignored:
   * an adapted segment is produced by its own async supplier, not the cache manager's full-vs-partial machinery.
   */
  @Override
  public Optional<Segment> acquireIfCached(AcquireMode acquireMode)
  {
    return Optional.empty();
  }

  /**
   * The {@code acquireMode} is ignored: an adapted segment is produced by its own async supplier, not the cache
   * manager's full-vs-partial machinery.
   */
  @Override
  public AcquireSegmentAction acquire(AcquireMode acquireMode)
  {
    if (!acquired.compareAndSet(false, true)) {
      throw DruidException.defensive("Segment with descriptor[%s] is already acquired", descriptor);
    }

    // The supplier starts the underlying work (e.g. VSF file fetches); fromResource folds the resource's lifecycle
    // into the delivered segment's close (and cancels it if the action is closed before readiness).
    return AcquireSegmentHandles.fromResource(asyncSegmentSupplier.get());
  }

  @Override
  public void countDelivered(AcquireSegmentResult result)
  {
    if (inputCounters == null) {
      return;
    }
    inputCounters.addLoad(result);
    final int rowCount = result.getSegment().map(LoadableSegmentUtils::getSegmentRowCount).orElse(0);
    // Use byteCount = 0 for adapted segments; we can't really tell what it is from the AcquireSegmentResult
    // (the "load size" may not be the entire size if the segment was fully or partially cached). Implementations
    // call ChannelCounters#incrementBytes if they have something useful to put there.
    inputCounters.addFile(rowCount, 0);
  }
}
