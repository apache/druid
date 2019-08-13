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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;

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

  public Interval getSegmentDataInterval()
  {
    return adapter.get().getDataInterval();
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
            "WTF?! Cannot swap identifier[%s] -> [%s]!",
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

  public Pair<Segment, Closeable> getAndIncrementSegment()
  {
    ReferenceCountingSegment segment = getIncrementedSegment();
    return new Pair<>(segment, segment.decrementOnceCloseable());
  }

  @Override
  public String toString()
  {
    // Do not include IncrementalIndex in toString as AbstractIndex.toString() actually prints
    // all the rows in the index
    return "FireHydrant{" +
           ", queryable=" + adapter.get().getId() +
           ", count=" + count +
           '}';
  }
}
