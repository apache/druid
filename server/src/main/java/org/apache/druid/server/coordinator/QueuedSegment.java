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

package org.apache.druid.server.coordinator;

import com.google.common.collect.ImmutableList;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentChangeRequestDrop;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a segment queued for a load or drop operation in a LoadQueuePeon.
 */
public class QueuedSegment
{
  private final DataSegment segment;
  private final DataSegmentChangeRequest changeRequest;
  private final SegmentAction action;
  private final boolean isLoad;

  // Guaranteed to store only non-null elements
  private final List<LoadPeonCallback> callbacks = new ArrayList<>();
  private final AtomicLong firstRequestMillis = new AtomicLong(0);

  QueuedSegment(
      DataSegment segment,
      SegmentAction action,
      @Nullable LoadPeonCallback callback
  )
  {
    this.segment = segment;
    this.action = action;
    this.changeRequest = (action == SegmentAction.DROP)
                         ? new SegmentChangeRequestDrop(segment)
                         : new SegmentChangeRequestLoad(segment);
    this.isLoad = action != SegmentAction.DROP;
    if (callback != null) {
      callbacks.add(callback);
    }
  }

  public DataSegment getSegment()
  {
    return segment;
  }

  public SegmentAction getAction()
  {
    return action;
  }

  public boolean isLoad()
  {
    return isLoad;
  }

  public DataSegmentChangeRequest getChangeRequest()
  {
    return changeRequest;
  }

  public String getSegmentIdentifier()
  {
    return segment.getId().toString();
  }

  public void addCallback(@Nullable LoadPeonCallback callback)
  {
    if (callback != null) {
      synchronized (callbacks) {
        callbacks.add(callback);
      }
    }
  }

  /**
   * Returns an immutable copy of all non-null callbacks for this queued segment.
   */
  public List<LoadPeonCallback> getCallbacks()
  {
    synchronized (callbacks) {
      return ImmutableList.copyOf(callbacks);
    }
  }

  public long getFirstRequestTimeMillis()
  {
    firstRequestMillis.compareAndSet(0L, System.currentTimeMillis());
    return firstRequestMillis.get();
  }

  @Override
  public String toString()
  {
    return changeRequest.toString();
  }
}
