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

package org.apache.druid.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.http.MetadataResource;
import org.apache.druid.timeline.SegmentId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Tracks the segments a Broker has been left with no server for, and asks the Coordinator whether each of them ought
 * to be available.
 * <p>
 * A Broker cannot answer that question itself. When the last server for a segment goes away, the segment may have
 * been legitimately killed or marked unused -- in which case its disappearance is correct -- or it may have gone
 * missing, in which case every query touching it silently returns incomplete results. Distinguishing the two is what
 * lets the Broker report the second case. See apache/druid#18716.
 * <p>
 * The tracked set is normally empty and is expected to stay small, so checks are batched point queries rather than
 * any kind of bulk feed; the Coordinator's used-segment set can run to tens of millions and must never be shipped
 * wholesale to every Broker.
 */
@ManageLifecycle
public class SegmentAvailabilityTracker
{
  private static final EmittingLogger log = new EmittingLogger(SegmentAvailabilityTracker.class);

  /**
   * Upper bound on one round of Coordinator checks, so that an unresponsive Coordinator cannot wedge the tracker.
   */
  private static final long FETCH_TIMEOUT_MILLIS = 30_000;

  /**
   * How many check periods to wait before asking the Coordinator about a segment whose status is already resolved.
   * Re-checking is only needed to notice a segment being marked unused after the fact, which is rare, whereas a
   * large-scale outage can leave a million segments tracked -- asking about all of them every period would be a
   * standing load on the Coordinator for no benefit.
   */
  private static final int RESOLVED_RECHECK_PERIODS = 12;

  /**
   * Upper bound on how many segments one round asks about, so that a very large tracked set is spread over several
   * rounds rather than issuing hundreds of requests at once.
   */
  private static final int MAX_SEGMENTS_PER_ROUND = 50_000;

  private final CoordinatorClient coordinatorClient;
  private final BrokerSegmentWatcherConfig config;

  /**
   * Segments with no server, and the selector left in the timeline for each. Also the set that gets re-checked
   * periodically, so that a segment later marked unused stops being reported.
   */
  private final Map<SegmentId, TrackedSegment> trackedSegments = new ConcurrentHashMap<>();

  /**
   * Segments whose status has not been resolved yet. Drained by each check.
   */
  private final Set<SegmentId> pendingChecks = ConcurrentHashMap.newKeySet();

  /**
   * Called with segments the Coordinator reports as not expected to be available, so that the Broker can drop them
   * from its timeline. Registered by {@link BrokerServerView}, which owns the timeline.
   */
  private volatile Consumer<Set<SegmentId>> evictionHandler = segmentIds -> {};

  private volatile ScheduledExecutorService exec;

  @Inject
  public SegmentAvailabilityTracker(
      CoordinatorClient coordinatorClient,
      BrokerSegmentWatcherConfig config
  )
  {
    this.coordinatorClient = coordinatorClient;
    this.config = config;
  }

  @LifecycleStart
  public void start()
  {
    if (config.getUnavailableSegmentPolicy() == UnavailableSegmentPolicy.IGNORE) {
      return;
    }

    exec = Execs.scheduledSingleThreaded("SegmentAvailabilityTracker-%d");
    final long checkPeriodMillis = config.getUnavailableCheckPeriod().getMillis();
    exec.scheduleWithFixedDelay(
        this::runChecks,
        checkPeriodMillis,
        checkPeriodMillis,
        TimeUnit.MILLISECONDS
    );
  }

  @LifecycleStop
  public void stop()
  {
    if (exec != null) {
      exec.shutdownNow();
      exec = null;
    }
    trackedSegments.clear();
    pendingChecks.clear();
  }

  public void registerEvictionHandler(Consumer<Set<SegmentId>> evictionHandler)
  {
    this.evictionHandler = evictionHandler;
  }

  /**
   * Starts tracking a segment that has just lost its last server.
   *
   * @return false if the segment could not be tracked because the cap was already reached, in which case the caller
   *         should fall back to dropping the segment from the timeline as it did before this feature existed.
   */
  public boolean track(SegmentId segmentId, ServerSelector selector)
  {
    if (config.getUnavailableSegmentPolicy() == UnavailableSegmentPolicy.IGNORE) {
      return false;
    }

    // A whole tier restarting can push a very large number of segments to zero replicas at once. Retaining their
    // selectors costs about what holding them loaded costs, so the ceiling is already provisioned, but the cap keeps
    // a stalled or broken Coordinator from growing the set without bound.
    if (trackedSegments.size() >= config.getMaxUnavailableSegments()) {
      log.noStackTrace().warn(
          "Tracking [%d] unavailable segments, at the limit of [%d]. Segment[%s] will be dropped from the timeline"
          + " instead of being reported. Queries touching it may silently return partial results.",
          trackedSegments.size(), config.getMaxUnavailableSegments(), segmentId
      );
      return false;
    }

    selector.setAvailability(SegmentAvailability.UNKNOWN);
    trackedSegments.put(segmentId, new TrackedSegment(selector, System.currentTimeMillis()));
    pendingChecks.add(segmentId);
    return true;
  }

  /**
   * Stops tracking a segment, because it has a server again or has been dropped from the timeline.
   */
  public void untrack(SegmentId segmentId)
  {
    final TrackedSegment tracked = trackedSegments.remove(segmentId);
    if (tracked != null) {
      tracked.selector.setAvailability(SegmentAvailability.UNKNOWN);
    }
    pendingChecks.remove(segmentId);
  }

  /**
   * Number of segments currently believed to have no server. Emitted as {@code segment/noServer/count}.
   */
  public int getNumUnavailableSegments()
  {
    int count = 0;
    for (TrackedSegment tracked : trackedSegments.values()) {
      if (tracked.selector.getAvailability() == SegmentAvailability.EXPECTED_AVAILABLE) {
        ++count;
      }
    }
    return count;
  }

  public int getNumTrackedSegments()
  {
    return trackedSegments.size();
  }

  @VisibleForTesting
  void runChecks()
  {
    try {
      expireStaleTracking();

      final Set<SegmentId> toCheck = selectSegmentsToCheck();
      if (toCheck.isEmpty()) {
        return;
      }

      final Set<SegmentId> notExpected = new HashSet<>();
      for (Set<SegmentId> batch : partition(toCheck, MetadataResource.MAX_SEGMENTS_PER_AVAILABILITY_REQUEST)) {
        // Bounded so that an unresponsive Coordinator cannot wedge this thread, which also drives retention expiry.
        applyStatuses(
            coordinatorClient.fetchSegmentAvailability(batch).get(FETCH_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
            notExpected
        );
      }

      if (!notExpected.isEmpty()) {
        notExpected.forEach(this::untrack);
        evictionHandler.accept(notExpected);
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    catch (Exception e) {
      // Leave everything tracked as UNKNOWN, which never fails a query. An unreachable Coordinator must not turn
      // into query errors.
      log.noStackTrace().warn(
          e,
          "Could not check segment availability with the Coordinator. [%d] segments remain unresolved.",
          trackedSegments.size()
      );
    }
  }

  /**
   * Picks the segments to ask about this round: everything not yet resolved, then any resolved segment that has not
   * been re-checked recently, up to a per-round cap.
   */
  private Set<SegmentId> selectSegmentsToCheck()
  {
    final Set<SegmentId> toCheck = new HashSet<>(pendingChecks);

    final long recheckCutoff = System.currentTimeMillis()
                               - RESOLVED_RECHECK_PERIODS * config.getUnavailableCheckPeriod().getMillis();
    for (Map.Entry<SegmentId, TrackedSegment> entry : trackedSegments.entrySet()) {
      if (toCheck.size() >= MAX_SEGMENTS_PER_ROUND) {
        break;
      }
      if (entry.getValue().lastCheckedMillis < recheckCutoff) {
        toCheck.add(entry.getKey());
      }
    }

    // Drop any pending entry for a segment that has since stopped being tracked.
    toCheck.retainAll(trackedSegments.keySet());
    return toCheck;
  }

  private void applyStatuses(Map<SegmentId, SegmentAvailabilityStatus> statuses, Set<SegmentId> notExpectedOut)
  {
    statuses.forEach((segmentId, status) -> {
      final TrackedSegment tracked = trackedSegments.get(segmentId);
      if (tracked == null) {
        // Got a server back while the check was in flight.
        return;
      }
      tracked.lastCheckedMillis = System.currentTimeMillis();
      pendingChecks.remove(segmentId);

      if (status.isExpectedToBeAvailable()) {
        if (tracked.selector.getAvailability() != SegmentAvailability.EXPECTED_AVAILABLE) {
          log.warn(
              "Segment[%s] is used and requires [%s] replicas but no server is serving it."
              + " Queries touching it will return incomplete results.",
              segmentId, status.getReplicationFactor()
          );
        }
        tracked.selector.setAvailability(SegmentAvailability.EXPECTED_AVAILABLE);
      } else {
        tracked.selector.setAvailability(SegmentAvailability.NOT_EXPECTED);
        notExpectedOut.add(segmentId);
      }
    });
  }

  /**
   * Drops segments that have been tracked longer than the retention period. Without this, a segment that the
   * Coordinator can never be asked about -- because it is permanently unreachable, say -- would be tracked forever.
   */
  private void expireStaleTracking()
  {
    final long cutoff = System.currentTimeMillis() - config.getUnavailableRetentionPeriod().getMillis();
    final Set<SegmentId> expired = new HashSet<>();
    trackedSegments.forEach((segmentId, tracked) -> {
      if (tracked.trackedSinceMillis < cutoff) {
        expired.add(segmentId);
      }
    });

    if (!expired.isEmpty()) {
      log.warn(
          "Dropping [%d] segments that have had no server for over [%s].",
          expired.size(), config.getUnavailableRetentionPeriod()
      );
      expired.forEach(this::untrack);
      evictionHandler.accept(expired);
    }
  }

  private static List<Set<SegmentId>> partition(Set<SegmentId> segmentIds, int batchSize)
  {
    if (segmentIds.size() <= batchSize) {
      return Collections.singletonList(segmentIds);
    }

    final List<Set<SegmentId>> batches = new ArrayList<>();
    Set<SegmentId> current = new HashSet<>();
    for (SegmentId segmentId : segmentIds) {
      current.add(segmentId);
      if (current.size() == batchSize) {
        batches.add(current);
        current = new HashSet<>();
      }
    }
    if (!current.isEmpty()) {
      batches.add(current);
    }
    return batches;
  }

  private static class TrackedSegment
  {
    private final ServerSelector selector;
    private final long trackedSinceMillis;

    /**
     * When the Coordinator was last asked about this segment. Zero until the first answer, which is what keeps a
     * newly tracked segment in the next round regardless of the re-check cadence.
     */
    private volatile long lastCheckedMillis;

    private TrackedSegment(ServerSelector selector, long trackedSinceMillis)
    {
      this.selector = selector;
      this.trackedSinceMillis = trackedSinceMillis;
    }
  }
}
