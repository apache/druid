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

package org.apache.druid.server.coordinator.loading;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentChangeRequestDrop;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.server.coordinator.config.HttpLoadQueuePeonConfig;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Represents a segment queued for a load or drop operation in a LoadQueuePeon.
 * <p>
 * Requests are naturally ordered using the {@link #COMPARE_ACTION_THEN_INTERVAL}.
 */
public class SegmentHolder implements Comparable<SegmentHolder>
{
  /**
   * Orders newest segments first (i.e. segments with most recent intervals).
   * <p>
   * The order is needed to ensure that:
   * <ul>
   * <li>Round-robin assignment distributes segments belonging to same or adjacent
   * intervals uniformly across all servers.</li>
   * <li>Load queue prioritizes load of most recent segments, as
   * they are presumed to contain more important data which is queried more often.</li>
   * <li>Replication throttler has a smaller impact on replicas of newer segments.</li>
   * </ul>
   */
  public static final Ordering<DataSegment> NEWEST_SEGMENT_FIRST = Ordering
      .from(Comparators.intervalsByEndThenStart())
      .onResultOf(DataSegment::getInterval)
      .compound(Ordering.<DataSegment>natural())
      .reverse();

  /**
   * Orders segment requests:
   * <ul>
   *   <li>first by action: all drops, then all loads, then all moves</li>
   *   <li>then by interval: newest segments first</li>
   * </ul>
   */
  private static final Comparator<SegmentHolder> COMPARE_ACTION_THEN_INTERVAL =
      Ordering.explicit(SegmentAction.DROP, SegmentAction.LOAD, SegmentAction.REPLICATE, SegmentAction.MOVE_TO)
              .onResultOf(SegmentHolder::getAction)
              .compound(NEWEST_SEGMENT_FIRST.onResultOf(SegmentHolder::getSegment));

  private final DataSegment segment;
  private final DataSegmentChangeRequest changeRequest;
  private final SegmentAction action;
  @Nullable
  private final PartialLoadProfile profile;

  private final Duration requestTimeout;

  // Guaranteed to store only non-null elements
  private final List<LoadPeonCallback> callbacks = new ArrayList<>();
  private final Stopwatch sinceRequestSentToServer = Stopwatch.createUnstarted();
  private int runsInQueue = 0;

  public SegmentHolder(
      DataSegment segment,
      SegmentAction action,
      Duration requestTimeout,
      @Nullable LoadPeonCallback callback
  )
  {
    this(segment, action, null, requestTimeout, callback);
  }

  /**
   * Creates a holder for a load (or move) request that may carry a partial-load profile. When {@code profile} is
   * non-null and the action is a load, the outbound {@link SegmentChangeRequestLoad}'s segment has its load spec
   * replaced with {@link PartialLoadProfile#wrappedLoadSpec()} so the historical sees the partial-load wrapper. The
   * holder's own {@link #segment} field stays the original so identity / equals / hashCode / callback resolution
   * remain unchanged from the regular full-load path.
   */
  public SegmentHolder(
      DataSegment segment,
      SegmentAction action,
      @Nullable PartialLoadProfile profile,
      Duration requestTimeout,
      @Nullable LoadPeonCallback callback
  )
  {
    this.segment = segment;
    this.action = action;
    this.profile = profile;
    if (action == SegmentAction.DROP) {
      this.changeRequest = new SegmentChangeRequestDrop(segment);
    } else if (profile != null && profile.wrappedLoadSpec() != null) {
      // Stamp the wrapped load-spec map onto the outbound segment so the historical receives the partial-load
      // wrapper; identity (segment field) stays original so dedup in the load queue is unaffected.
      this.changeRequest = new SegmentChangeRequestLoad(segment.withLoadSpec(profile.wrappedLoadSpec()));
    } else {
      this.changeRequest = new SegmentChangeRequestLoad(segment);
    }
    if (callback != null) {
      callbacks.add(callback);
    }
    this.requestTimeout = requestTimeout;
  }

  /**
   * Returns the {@link PartialLoadProfile} that this holder was created with, or {@code null} for regular full-load
   * (or drop) requests. The fingerprint inside the profile is what an in-flight reconciler reads to decide whether
   * an in-flight load satisfies the current rule.
   */
  @Nullable
  public PartialLoadProfile getProfile()
  {
    return profile;
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
    if (!sinceRequestSentToServer.isRunning()) {
      sinceRequestSentToServer.start();
    }
  }

  /**
   * A request is considered to have timed out if the time elapsed since it was
   * first sent to the server is greater than the configured load timeout.
   *
   * @see HttpLoadQueuePeonConfig#getLoadTimeout()
   */
  public boolean hasRequestTimedOut()
  {
    return sinceRequestSentToServer.millisElapsed() > requestTimeout.getMillis();
  }

  public int incrementAndGetRunsInQueue()
  {
    return ++runsInQueue;
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
    return action + "{" +
           "segment=" + segment.getId() +
           ", runsInQueue=" + runsInQueue +
           '}';
  }
}
