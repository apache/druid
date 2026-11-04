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

import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.utils.CloseableUtils;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Shared machinery for bridging {@link AsyncResource}-producing segment sources into the releasable
 * {@link AcquireSegmentAction} consumer handle.
 */
final class AcquireSegmentHandles
{
  private static final Logger log = new Logger(AcquireSegmentHandles.class);

  private AcquireSegmentHandles()
  {
    // No instantiation.
  }

  /**
   * Wraps {@code target} with a CAS guard so it is closed at most once. {@link AcquireSegmentAction#close()} is not
   * idempotent, and a producer's error path can race its canceler.
   */
  static Runnable closeOnce(Closeable target)
  {
    final AtomicBoolean closed = new AtomicBoolean(false);
    return () -> {
      if (closed.compareAndSet(false, true)) {
        CloseableUtils.closeAndSuppressExceptions(
            target,
            e -> log.warn(e, "Failed to close acquire resource of class[%s]", target.getClass().getName())
        );
      }
    };
  }

  /**
   * Bridges a NON-releasable {@code inner} resource (e.g. one produced by {@code AsyncResources.collect/transform/
   * recover} combinators) into a releasable {@link AcquireSegmentAction}. The lifecycle of {@code inner} is folded
   * into the delivered result: when the delivered result carries a segment, that segment's close also closes
   * {@code inner} (releasing whatever the combinator chain owns, e.g. cached source files, which therefore strictly
   * outlive the segment); when it is empty, {@code inner} is closed at delivery. Closing the returned action before
   * readiness closes {@code inner} (cancelling in-flight work).
   * <p>
   * Uses {@code inner.get()}, not {@code release()}: combinator-produced resources are not releasable; inner keeps
   * nominal ownership and the fold transfers the close responsibility onto the delivered segment.
   */
  static AcquireSegmentAction fromResource(AsyncResource<AcquireSegmentResult> inner)
  {
    final Runnable closeInnerOnce = closeOnce(inner);
    final AcquireSegmentAction outer = new AcquireSegmentAction(closeInnerOnce);
    deliverOnReady(inner, outer, closeInnerOnce, () -> foldInnerClose(inner.get(), closeInnerOnce));
    return outer;
  }

  /**
   * Wires a releasable {@code inner} handle into {@code outer}: on readiness, ownership of the result transfers
   * inner to outer.
   *
   * @param inner          the source handle
   * @param outer          the handle handed to the consumer
   * @param closeInnerOnce close-once guard for {@code inner}, shared with the canceler that may race this transfer
   */
  static void transferOnReady(AcquireSegmentAction inner, AcquireSegmentAction outer, Runnable closeInnerOnce)
  {
    deliverOnReady(inner, outer, closeInnerOnce, inner::release);
  }

  /**
   * Shared delivery skeleton for {@link #fromResource} and {@link #transferOnReady}. On {@code inner}'s readiness,
   * obtain the result and deliver it to {@code outer}; on failure report to {@code outer} (silently absorbed if outer
   * was closed first) and release {@code inner} exactly once; if delivery loses the race with {@code outer}'s close,
   * close the orphaned result.
   * <p>
   * The callback may fire inline on this thread when {@code inner} is already ready; its body routes every failure
   * (including throws from {@code obtainResult}) through {@code outer.setException} itself, so the registration catch
   * below normally only sees {@code addReadyCallback} rejecting a concurrently-closed {@code inner}.
   */
  private static void deliverOnReady(
      AsyncResource<AcquireSegmentResult> inner,
      AcquireSegmentAction outer,
      Runnable closeInnerOnce,
      Supplier<AcquireSegmentResult> obtainResult
  )
  {
    try {
      inner.addReadyCallback(() -> {
        final AcquireSegmentResult delivered;
        try {
          delivered = obtainResult.get();
        }
        catch (Throwable t) {
          outer.setException(t);
          closeInnerOnce.run();
          return;
        }
        if (!outer.set(delivered)) {
          // the action was closed while delivering; we own the orphaned result (closing it releases inner too)
          CloseableUtils.closeAndSuppressExceptions(
              delivered,
              e -> log.warn(e, "Failed to close orphaned acquire result after losing the delivery race")
          );
        }
      });
    }
    catch (Throwable t) {
      // just in case, close and setException to be sure we tidy everything up and leave no chance a consumer is waiting
      closeInnerOnce.run();
      try {
        outer.setException(t);
      }
      catch (Throwable t2) {
        // outer already completed, so no consumer is left hanging, log rather than mask the original failure
        t2.addSuppressed(t);
        log.warn(t2, "Failed to report acquire delivery failure to its handle");
      }
    }
  }

  /**
   * Rebuilds {@code result} so the contained segment's close also runs {@code closeInnerOnce}; an empty result
   * closes {@code inner} immediately. The segment must be a {@link ReferenceCountedSegmentProvider.LeafReference}.
   */
  private static AcquireSegmentResult foldInnerClose(AcquireSegmentResult result, Runnable closeInnerOnce)
  {
    final Optional<Segment> segment = result.getSegment();
    if (segment.isEmpty()) {
      closeInnerOnce.run();
      return result;
    }
    if (!(segment.get() instanceof ReferenceCountedSegmentProvider.LeafReference leaf)) {
      throw DruidException.defensive(
          "Segment[%s] of type[%s] is not a LeafReference; cannot fold resource close into it",
          segment.get().getDebugString(),
          segment.get().getClass().getSimpleName()
      );
    }
    return new AcquireSegmentResult(
        ReferenceCountedSegmentProvider.wrapCloseable(leaf, closeInnerOnce::run),
        result.getLoadSizeBytes(),
        result.getWaitTimeNanos(),
        result.getLoadTimeNanos()
    );
  }
}
