/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IncrementalIndex;
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

  public FireHydrant(IncrementalIndex index, int count, String segmentIdentifier)
  {
    this.index = index;
    this.adapter = new AtomicReference<>(
        new ReferenceCountingSegment(new IncrementalIndexSegment(index, segmentIdentifier))
    );
    this.count = count;
  }

  public FireHydrant(Segment adapter, int count)
  {
    this.index = null;
    this.adapter = new AtomicReference<>(new ReferenceCountingSegment(adapter));
    this.count = count;
  }

  public IncrementalIndex getIndex()
  {
    return index;
  }

  public String getSegmentIdentifier()
  {
    return adapter.get().getIdentifier();
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
          !newSegment.getIdentifier().equals(currentSegment.getIdentifier())) {
        // Sanity check: identifier should not change
        throw new ISE(
            "WTF?! Cannot swap identifier[%s] -> [%s]!",
            currentSegment.getIdentifier(),
            newSegment.getIdentifier()
        );
      }
      if (currentSegment == newSegment) {
        throw new ISE("Cannot swap to the same segment");
      }
      ReferenceCountingSegment newReferenceCountingSegment =
          newSegment != null ? new ReferenceCountingSegment(newSegment) : null;
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
    return "FireHydrant{" +
           "index=" + index +
           ", queryable=" + adapter.get().getIdentifier() +
           ", count=" + count +
           '}';
  }
}
