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

package org.apache.druid.segment.realtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 */
public class FireHydrant
{
  private final int count;
  private final AtomicReference<ReferenceCountingSegment> adapter;
  private volatile IncrementalIndex index;

  public FireHydrant(IncrementalIndex index, int count, SegmentId segmentId)
  {
    this.index = index;
    this.adapter = new AtomicReference<>(
        ReferenceCountingSegment.wrapRootGenerationSegment(new IncrementalIndexSegment(index, segmentId))
    );
    this.count = count;
  }

  public FireHydrant(Segment adapter, int count)
  {
    this.index = null;
    this.adapter = new AtomicReference<>(ReferenceCountingSegment.wrapRootGenerationSegment(adapter));
    this.count = count;
  }

  public IncrementalIndex getIndex()
  {
    return index;
  }

  public SegmentId getSegmentId()
  {
    return adapter.get().getId();
  }

  public int getSegmentNumDimensionColumns()
  {
    final Segment segment = adapter.get().getBaseSegment();
    if (segment != null) {
      final StorageAdapter storageAdapter = segment.asStorageAdapter();
      return storageAdapter.getAvailableDimensions().size();
    }
    return 0;
  }

  public int getSegmentNumMetricColumns()
  {
    final Segment segment = adapter.get().getBaseSegment();
    if (segment != null) {
      final StorageAdapter storageAdapter = segment.asStorageAdapter();
      return Iterables.size(storageAdapter.getAvailableMetrics());
    }
    return 0;
  }

  public Interval getSegmentDataInterval()
  {
    return adapter.get().getDataInterval();
  }

  public int getCount()
  {
    return count;
  }

  public boolean hasSwapped()
  {
    return index == null;
  }

  public void swapSegment(@Nullable Segment newSegment)
  {
    while (true) {
      ReferenceCountingSegment currentSegment = adapter.get();
      if (currentSegment == null && newSegment == null) {
        return;
      }
      if (currentSegment != null && newSegment != null &&
          !newSegment.getId().equals(currentSegment.getId())) {
        // Sanity check: identifier should not change
        throw new ISE(
            "Cannot swap identifier[%s] -> [%s]",
            currentSegment.getId(),
            newSegment.getId()
        );
      }
      if (currentSegment == newSegment) {
        throw new ISE("Cannot swap to the same segment");
      }
      ReferenceCountingSegment newReferenceCountingSegment =
          newSegment != null ? ReferenceCountingSegment.wrapRootGenerationSegment(newSegment) : null;
      if (adapter.compareAndSet(currentSegment, newReferenceCountingSegment)) {
        if (currentSegment != null) {
          currentSegment.close();
        }
        index = null;
        return;
      }
    }
  }

  public ReferenceCountingSegment getIncrementedSegment()
  {
    ReferenceCountingSegment segment = adapter.get();
    while (true) {
      if (segment.increment()) {
        return segment;
      }
      // segment.increment() returned false, means it is closed. Since close() in swapSegment() happens after segment
      // swap, the new segment should already be visible.
      ReferenceCountingSegment newSegment = adapter.get();
      if (segment == newSegment) {
        throw new ISE("segment.close() is called somewhere outside FireHydrant.swapSegment()");
      }
      if (newSegment == null) {
        throw new ISE("FireHydrant was 'closed' by swapping segment to null while acquiring a segment");
      }
      segment = newSegment;
      // Spin loop.
    }
  }

  public Pair<ReferenceCountingSegment, Closeable> getAndIncrementSegment()
  {
    ReferenceCountingSegment segment = getIncrementedSegment();
    return new Pair<>(segment, segment.decrementOnceCloseable());
  }

  /**
   * This method is like a combined form of {@link #getIncrementedSegment} and {@link #getAndIncrementSegment} that
   * deals in {@link SegmentReference} instead of directly with {@link ReferenceCountingSegment} in order to acquire
   * reference count for both hydrant's segment and any tracked joinables taking part in the query.
   */
  public Optional<Pair<SegmentReference, Closeable>> getSegmentForQuery(
      Function<SegmentReference, SegmentReference> segmentMapFn
  )
  {
    ReferenceCountingSegment sinkSegment = adapter.get();

    if (sinkSegment == null) {
      // adapter can be null if this segment is removed (swapped to null) while being queried.
      return Optional.empty();
    }

    SegmentReference segment = segmentMapFn.apply(sinkSegment);
    while (true) {
      Optional<Closeable> reference = segment.acquireReferences();
      if (reference.isPresent()) {

        return Optional.of(new Pair<>(segment, reference.get()));
      }
      // segment.acquireReferences() returned false, means it is closed. Since close() in swapSegment() happens after
      // segment swap, the new segment should already be visible.
      ReferenceCountingSegment newSinkSegment = adapter.get();
      if (newSinkSegment == null) {
        // adapter can be null if this segment is removed (swapped to null) while being queried.
        return Optional.empty();
      }
      if (sinkSegment == newSinkSegment) {
        if (newSinkSegment.isClosed()) {
          throw new ISE("segment.close() is called somewhere outside FireHydrant.swapSegment()");
        }
        // if segment is not closed, but is same segment it means we are having trouble getting references for joinables
        // of a HashJoinSegment created by segmentMapFn
        return Optional.empty();
      }
      segment = segmentMapFn.apply(newSinkSegment);
      // Spin loop.
    }
  }

  @VisibleForTesting
  public ReferenceCountingSegment getHydrantSegment()
  {
    return adapter.get();
  }

  @Override
  public String toString()
  {
    // Do not include IncrementalIndex in toString as AbstractIndex.toString() actually prints
    // all the rows in the index
    return "FireHydrant{" +
           "queryable=" + (adapter.get() == null ? "null" : adapter.get().getId()) +
           ", count=" + count +
           '}';
  }
}
