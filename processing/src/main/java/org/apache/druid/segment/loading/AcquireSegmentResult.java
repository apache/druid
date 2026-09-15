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

import org.apache.druid.segment.Segment;
import org.apache.druid.utils.CloseableUtils;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The deliverable of {@link AcquireSegmentAction}: a pre-acquired {@link Segment} reference, along with measurements
 * about segment loading if it was required.
 * <p>
 * The segment, when present, is a single already-acquired reference whose {@link Segment#close()} releases everything
 * associated with the acquisition: the reference itself plus any eviction-protective cache holds the loader folded
 * into it. An empty segment means the segment could not be acquired (no longer in the cache, and could not be or was
 * not fetched from deep storage) — a first-class outcome, not an error.
 * <p>
 * Whoever owns this result must {@link #close()} it (closing the contained segment, if any). Close is idempotent:
 * a result may be closed by {@link AcquireSegmentAction#close()} (when the action was never released), by the
 * producer (when delivery lost a race with close/cancel), or by the consumer that
 * {@link AcquireSegmentAction#release()}d it — exactly one of these wins.
 */
public class AcquireSegmentResult implements Closeable
{
  /**
   * Result with no segment (missing from cache and deep storage) and zero metrics. Returns a fresh instance since
   * results are stateful {@link Closeable}s.
   */
  public static AcquireSegmentResult empty()
  {
    return new AcquireSegmentResult(Optional.empty(), 0L, 0L, 0L);
  }

  /**
   * Result for a segment that was already available, with zero load metrics.
   */
  public static AcquireSegmentResult of(Optional<Segment> segment)
  {
    return new AcquireSegmentResult(segment, 0L, 0L, 0L);
  }

  private final Optional<Segment> segment;
  private final long loadSizeBytes;
  private final long waitTimeNanos;
  private final long loadTimeNanos;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public AcquireSegmentResult(
      Optional<Segment> segment,
      long loadSizeBytes,
      long waitTimeNanos,
      long loadTimeNanos
  )
  {
    this.segment = segment;
    this.loadSizeBytes = loadSizeBytes;
    this.waitTimeNanos = waitTimeNanos;
    this.loadTimeNanos = loadTimeNanos;
  }

  /**
   * The acquired segment reference, or empty if the segment is not available. Unlike the reference providers this
   * type once wrapped, this is a single pre-acquired reference: callers must not attempt to mint additional
   * references from it, and must arrange for it to be closed exactly once (directly, or via {@link #close()}).
   */
  public Optional<Segment> getSegment()
  {
    return segment;
  }

  /**
   * Amount of data loaded into the cache, or 0 if it was already available in the cache
   */
  public long getLoadSizeBytes()
  {
    return loadSizeBytes;
  }

  /**
   * Amount of time spent waiting before actually loading the segment (e.g. if loads are done on a shared thread pool)
   */
  public long getWaitTimeNanos()
  {
    return waitTimeNanos;
  }

  /**
   * Amount of time spent loading a segment, or 0 if the segment was already available in the cache
   */
  public long getLoadTimeNanos()
  {
    return loadTimeNanos;
  }

  @Override
  public void close()
  {
    if (closed.compareAndSet(false, true) && segment.isPresent()) {
      CloseableUtils.closeAndWrapExceptions(segment.get());
    }
  }
}
