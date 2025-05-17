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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.PhysicalSegmentInspector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class FireHydrant
{
  private final int count;
  private final AtomicReference<ReferenceCountedSegmentProvider> segmentReferenceProvider;
  @Nullable
  private volatile IncrementalIndex index;

  public FireHydrant(IncrementalIndex index, int count, SegmentId segmentId)
  {
    this.index = index;
    this.segmentReferenceProvider = new AtomicReference<>(
        ReferenceCountedSegmentProvider.wrapRootGenerationSegment(new IncrementalIndexSegment(index, segmentId))
    );
    this.count = count;
  }

  public FireHydrant(Segment segmentReferenceProvider, int count)
  {
    this.index = null;
    this.segmentReferenceProvider = new AtomicReference<>(ReferenceCountedSegmentProvider.wrapRootGenerationSegment(
        segmentReferenceProvider));
    this.count = count;
  }

  @Nullable
  public IncrementalIndex getIndex()
  {
    return index;
  }

  public SegmentId getSegmentId()
  {
    return segmentReferenceProvider.get().getBaseSegment().getId();
  }

  public int getSegmentNumDimensionColumns()
  {
    if (hasSwapped()) {
      final Segment segment = segmentReferenceProvider.get().getBaseSegment();
      if (segment != null) {
        QueryableIndex queryableIndex = segment.as(QueryableIndex.class);
        if (queryableIndex != null) {
          return queryableIndex.getAvailableDimensions().size();
        }
      }
    } else {
      return index.getDimensions().size();
    }
    return 0;
  }

  public int getSegmentNumMetricColumns()
  {
    final Segment segment = segmentReferenceProvider.get().getBaseSegment();
    if (segment != null) {
      final PhysicalSegmentInspector segmentInspector = segment.as(PhysicalSegmentInspector.class);
      final Metadata metadata = segmentInspector == null ? null : segmentInspector.getMetadata();
      return metadata != null && metadata.getAggregators() != null ? metadata.getAggregators().length : 0;
    }
    return 0;
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
      ReferenceCountedSegmentProvider currentSegment = segmentReferenceProvider.get();
      if (currentSegment == null && newSegment == null) {
        return;
      }
      if (currentSegment != null && newSegment != null &&
          !newSegment.getId().equals(currentSegment.getBaseSegment().getId())) {
        // Sanity check: identifier should not change
        throw new ISE(
            "Cannot swap identifier[%s] -> [%s]",
            currentSegment.getBaseSegment().getId(),
            newSegment.getId()
        );
      }
      if (currentSegment != null && currentSegment.getBaseSegment() == newSegment) {
        throw new ISE("Cannot swap to the same segment");
      }
      ReferenceCountedSegmentProvider newReferenceCountingSegment =
          newSegment != null ? ReferenceCountedSegmentProvider.wrapRootGenerationSegment(newSegment) : null;
      if (segmentReferenceProvider.compareAndSet(currentSegment, newReferenceCountingSegment)) {
        if (currentSegment != null) {
          currentSegment.close();
        }
        index = null;
        return;
      }
    }
  }

  /**
   * Get a segment {@link Segment} from {@link #segmentReferenceProvider}
   */
  public Segment acquireSegment()
  {
    ReferenceCountedSegmentProvider segment = segmentReferenceProvider.get();
    while (true) {
      final Optional<Segment> maybeSegment = segment.acquireReference();
      if (maybeSegment.isPresent()) {
        return maybeSegment.get();
      }
      // segment.acquireReferences() returned false, means it is closed. Since close() in swapSegment() happens after
      // segment swap, the new segment should already be visible.
      ReferenceCountedSegmentProvider newSegment = segmentReferenceProvider.get();
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

  /**
   * Get a {@link Segment}, applying a {@link SegmentMapFunction} to {@link #segmentReferenceProvider}. Similar to
   * {@link #acquireSegment()}, but applies the {@link SegmentMapFunction} inside of the loop.
   */
  public Optional<Segment> getSegmentForQuery(SegmentMapFunction segmentMapFn)
  {
    ReferenceCountedSegmentProvider sinkSegment = segmentReferenceProvider.get();

    if (sinkSegment == null) {
      // adapter can be null if this segment is removed (swapped to null) while being queried.
      return Optional.empty();
    }

    Optional<Segment> segment = segmentMapFn.apply(sinkSegment);
    while (true) {
      if (segment.isPresent()) {
        return segment;
      }
      // segment.acquireReferences() returned false, means it is closed. Since close() in swapSegment() happens after
      // segment swap, the new segment should already be visible.
      ReferenceCountedSegmentProvider newSinkSegment = segmentReferenceProvider.get();
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
  public ReferenceCountedSegmentProvider getHydrantSegment()
  {
    return segmentReferenceProvider.get();
  }

  @Override
  public String toString()
  {
    // Do not include IncrementalIndex in toString as AbstractIndex.toString() actually prints
    // all the rows in the index
    return "FireHydrant{" +
           "queryable=" + (segmentReferenceProvider.get() == null ? "null" : segmentReferenceProvider.get().getBaseSegment().getId()) +
           ", count=" + count +
           '}';
  }
}
