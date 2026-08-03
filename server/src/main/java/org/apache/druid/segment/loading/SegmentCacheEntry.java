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

import org.apache.druid.error.DruidException;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Optional;

/**
 * A {@link CacheEntry} that manages a {@link Segment} on local disk.
 */
public interface SegmentCacheEntry extends CacheEntry
{
  /**
   * Identity of the segment this entry manages. Distinct from {@link #getId()}, which returns the
   * {@link CacheEntryIdentifier} used by {@link StorageLocation} as a map key.
   */
  SegmentId getSegmentId();

  /**
   * Acquire a closeable {@link Segment} reference, if one is available. Returns {@link Optional#empty()} when the
   * entry is not mounted.
   * <p>
   * The returned {@link Segment} must be closed by the caller. The entry tracks its own outstanding references and
   * defers any cleanup work until they're all released.
   */
  Optional<Segment> acquireReference();

  /**
   * Acquire a segment reference and compose {@code extraOnClose} into the segment's close lifecycle. The
   * implementation takes ownership of {@code extraOnClose}: on a successful acquire it becomes part of the returned
   * segment's close; on a miss (entry not mounted, or a race lost to a concurrent drop) it is closed immediately
   * before returning {@link Optional#empty()}.
   */
  default Optional<Segment> acquireReference(Closeable extraOnClose)
  {
    final Optional<Segment> segment;
    try {
      segment = acquireReference();
    }
    catch (Throwable t) {
      CloseableUtils.closeAndSuppressExceptions(extraOnClose, t::addSuppressed);
      throw t;
    }
    if (segment.isEmpty()) {
      CloseableUtils.closeAndSuppressExceptions(extraOnClose, ignored -> {});
      return Optional.empty();
    }
    if (segment.get() instanceof ReferenceCountedSegmentProvider.LeafReference leaf) {
      return ReferenceCountedSegmentProvider.wrapCloseable(leaf, extraOnClose);
    }
    // Defensive: the returned segment isn't composable via wrapCloseable. Close both to avoid leaks and throw.
    CloseableUtils.closeAndSuppressExceptions(segment.get(), ignored -> {});
    CloseableUtils.closeAndSuppressExceptions(extraOnClose, ignored -> {});
    throw DruidException.defensive(
        "Segment[%s] of type[%s] from acquireReference() is not a LeafReference; cannot compose extra close",
        segment.get().getDebugString(),
        segment.get().getClass().getSimpleName()
    );
  }

  /**
   * Attach (or clear) a hook that runs when this entry is finally purged from the cache. Replaces any previously-set
   * hook; pass {@code null} to clear. Implementations must invoke the hook exactly once, after all on-disk and
   * in-memory state owned by the entry has been released, never while a reference acquired via
   * {@link #acquireReference} is outstanding.
   */
  void setOnUnmount(@Nullable Runnable hook);

  /**
   * Whether every byte the segment needs is already on local disk. Complete entries always return {@code true} once
   * mounted; partial entries return {@code true} only once every internal file has been pulled from deep storage.
   * Callers can use this to gate synchronous segment operations on this so they never block a processing thread to
   * load data on-demand.
   */
  boolean isFullyDownloaded();
}
