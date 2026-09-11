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
 * {@link AcquireSegmentAction} consumer handle. Used by {@link AdaptedLoadableSegment} (combinator-produced
 * resources) and {@link RegularLoadableSegment} (the deferred two-stage Coordinator-fetch chain).
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
   * idempotent, and a bridge's error path can race its canceler — every dual-owner close goes through one of these.
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
   * {@code inner} (releasing whatever the combinator chain owns, e.g. cached source files — which therefore strictly
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
   * inner → outer. Used for the second stage of {@link RegularLoadableSegment}'s deferred chain, where the inner
   * handle comes from the cache manager and its result is already self-contained (holds folded into the segment).
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
   * Shared delivery skeleton for the two bridges: on {@code inner}'s readiness, obtain the result and deliver it to
   * {@code outer}; on failure report to {@code outer} (silently absorbed if outer was closed first) and release
   * {@code inner} exactly once; if delivery loses the race with {@code outer}'s close, close the orphaned result.
   * <p>
   * The callback body handles every throwable internally (the orphan close is suppressed-and-logged), so the
   * registration catch below only ever sees {@code addReadyCallback} itself rejecting a concurrently-closed
   * {@code inner} — never errors escaping an immediately-fired callback.
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
          // a real load failure, or "Closed" when the canceler won the race (in which case outer absorbs silently)
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
    catch (DruidException e) {
      // inner was concurrently closed by the canceler before the callback could be registered; nothing to do
    }
  }

  /**
   * Rebuilds {@code result} so the contained segment's close also runs {@code closeInnerOnce}; an empty result
   * closes {@code inner} immediately (nothing can carry the close). The segment must be a
   * {@link ReferenceCountedSegmentProvider.LeafReference} — an MSQ-internal contract of all
   * {@link AdaptedLoadableSegment} suppliers.
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
