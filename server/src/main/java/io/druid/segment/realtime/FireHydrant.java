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
import com.metamx.common.Pair;
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
  private volatile IncrementalIndex index;
  private volatile ReferenceCountingSegment adapter;
  private final Object swapLock = new Object();

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

  public void swapSegment(Segment adapter)
  {
    synchronized (swapLock) {
      ReferenceCountingSegment oldAdapter = this.adapter;
      this.adapter = new ReferenceCountingSegment(adapter);
      this.index = null;
      if (oldAdapter != null) {
        try {
          oldAdapter.close();
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
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
           ", queryable=" + adapter +
           ", count=" + count +
           '}';
  }
}
