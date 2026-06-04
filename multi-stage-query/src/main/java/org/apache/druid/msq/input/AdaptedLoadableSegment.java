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
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
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
   * {@link AsyncResource} is tied to the {@link AcquireSegmentAction} returned from {@link #acquire()}.
   *
   * @param asyncSegmentSupplier the supplier to wrap
   * @param descriptor           descriptor containing the interval to use for filtering
   * @param description          user-oriented description for error messages
   * @param inputCounters        counters for tracking input via {@link LoadableSegmentUtils#countedLoad}.
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
    // Pre-create the acquire result since the segment is already available
    final AcquireSegmentResult acquireSegmentResult =
        AcquireSegmentResult.cached(ReferenceCountedSegmentProvider.of(segment));

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

    // Synchronized by itself; Closer is not thread-safe.
    final Closer closer = Closer.create();
    final AtomicBoolean closed = new AtomicBoolean(false);

    return new AcquireSegmentAction(
        () -> {
          final AsyncResource<AcquireSegmentResult> asyncSegment = asyncSegmentSupplier.get();
          synchronized (closer) {
            if (closed.get()) {
              asyncSegment.close();
              return Futures.immediateFailedFuture(new ISE("Already closed"));
            } else {
              closer.register(asyncSegment);
            }
          }

          final SettableFuture<AcquireSegmentResult> retVal = SettableFuture.create();

          asyncSegment.addReadyCallback(() -> {
            try {
              retVal.set(asyncSegment.get());
            }
            catch (Throwable e) {
              retVal.setException(e);
            }
          });

          // Use byteCount = 0 for adapted segments; we can't really tell what it is from the AcquireSegmentResult
          // (the "load size" may not be the entire size if the segment was fully or partially cached). Implementations
          // call ChannelCounters#incrementBytes if they have something useful to put there.
          return LoadableSegmentUtils.countedLoad(retVal, 0, inputCounters);
        },
        () -> {
          synchronized (closer) {
            closed.set(true);
            closer.close();
          }
        }
    );
  }
}
