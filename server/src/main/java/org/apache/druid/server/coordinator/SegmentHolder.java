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
import com.google.common.collect.Ordering;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentChangeRequestDrop;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a segment queued for a load or drop operation in a LoadQueuePeon.
 * <p>
 * Requests are naturally ordered using the {@link #COMPARE_ACTION_THEN_INTERVAL}.
 */
public class SegmentHolder implements Comparable<SegmentHolder>
{
  /**
   * Orders segment requests:
   * <ul>
   *   <li>first by action: all drops, then all loads, then all moves</li>
   *   <li>then by interval: newest segments first</li>
   * </ul>
   */
  public static final Comparator<SegmentHolder> COMPARE_ACTION_THEN_INTERVAL =
      Ordering.explicit(SegmentAction.DROP, SegmentAction.LOAD, SegmentAction.REPLICATE, SegmentAction.MOVE_TO)
              .onResultOf(SegmentHolder::getAction)
              .compound(DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST.onResultOf(SegmentHolder::getSegment));

  private final DataSegment segment;
  private final DataSegmentChangeRequest changeRequest;
  private final SegmentAction action;

  // Guaranteed to store only non-null elements
  private final List<LoadPeonCallback> callbacks = new ArrayList<>();
  private final AtomicLong firstRequestMillis = new AtomicLong(0);

  SegmentHolder(
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
    return action != SegmentAction.DROP;
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

  public void markRequestSentToServer()
  {
    firstRequestMillis.compareAndSet(0L, System.currentTimeMillis());
  }

  public boolean isRequestSentToServer()
  {
    return firstRequestMillis.get() > 0;
  }

  public long getFirstRequestMillis()
  {
    return firstRequestMillis.get();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentHolder that = (SegmentHolder) o;
    return getSegment().equals(that.getSegment()) && getAction() == that.getAction();
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getSegment(), getAction());
  }

  @Override
  public int compareTo(SegmentHolder that)
  {
    return Objects.compare(this, that, COMPARE_ACTION_THEN_INTERVAL);
  }

  @Override
  public String toString()
  {
    return changeRequest.toString();
  }
}
