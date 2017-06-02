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

import com.google.common.base.Throwables;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IncrementalIndex;

import java.io.Closeable;
import java.io.IOException;

/**
 */
public class FireHydrant
{
  private final int count;
  private final Object swapLock = new Object();

  private volatile IncrementalIndex index;
  private volatile ReferenceCountingSegment adapter;

  public FireHydrant(
      IncrementalIndex index,
      int count,
      String segmentIdentifier
  )
  {
    this.index = index;
    this.adapter = new ReferenceCountingSegment(new IncrementalIndexSegment(index, segmentIdentifier));
    this.count = count;
  }

  public FireHydrant(
      Segment adapter,
      int count
  )
  {
    this.index = null;
    this.adapter = new ReferenceCountingSegment(adapter);
    this.count = count;
  }

  public IncrementalIndex getIndex()
  {
    return index;
  }

  public Segment getSegment()
  {
    return adapter;
  }

  public int getCount()
  {
    return count;
  }

  public boolean hasSwapped()
  {
    return index == null;
  }

  public void swapSegment(Segment newAdapter)
  {
    synchronized (swapLock) {
      if (adapter != null && newAdapter != null && !newAdapter.getIdentifier().equals(adapter.getIdentifier())) {
        // Sanity check: identifier should not change
        throw new ISE(
            "WTF?! Cannot swap identifier[%s] -> [%s]!",
            adapter.getIdentifier(),
            newAdapter.getIdentifier()
        );
      }
      if (this.adapter != null) {
        try {
          this.adapter.close();
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
      this.adapter = new ReferenceCountingSegment(newAdapter);
      this.index = null;
    }
  }

  public Pair<Segment, Closeable> getAndIncrementSegment()
  {
    // Prevent swapping of index before increment is called
    synchronized (swapLock) {
      Closeable closeable = adapter.increment();
      return new Pair<Segment, Closeable>(adapter, closeable);
    }
  }

  @Override
  public String toString()
  {
    return "FireHydrant{" +
           "index=" + index +
           ", queryable=" + adapter.getIdentifier() +
           ", count=" + count +
           '}';
  }
}
