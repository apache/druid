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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.realtime.appenderator.SegmentAllocator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Allocates and tracks actively used segments.
 */
public class SegmentTracker
{
  private static final Logger log = new Logger(SegmentTracker.class);

  // All access to "activeSegments" and "lastSegmentId" must be synchronized on "activeSegments".

  // sequenceName -> start of segment interval -> segment we're currently adding data to
  private final Map<String, NavigableMap<Long, SegmentIdentifier>> activeSegments = new TreeMap<>();

  // sequenceName -> list of identifiers of segments waiting for being published
  // publishPendingSegments is always a super set of activeSegments because there can be some segments to which data
  // are not added anymore, but not published yet.
  private final Map<String, List<SegmentIdentifier>> publishPendingSegments = new HashMap<>();

  // sequenceName -> most recently allocated segment
  private final Map<String, String> lastSegmentIds = Maps.newHashMap();

  private final SegmentAllocator segmentAllocator;

  public SegmentTracker(SegmentAllocator segmentAllocator)
  {
    this.segmentAllocator = segmentAllocator;
  }

  @VisibleForTesting
  public Map<String, NavigableMap<Long, SegmentIdentifier>> getActiveSegments()
  {
    return activeSegments;
  }

  @VisibleForTesting
  public Map<String, List<SegmentIdentifier>> getPublishPendingSegments()
  {
    return publishPendingSegments;
  }

  public void clear()
  {
    synchronized (activeSegments) {
      activeSegments.clear();
    }
  }

  public List<SegmentIdentifier> removePublished()
  {
    synchronized (activeSegments) {
      return removePublished(
          ImmutableList.copyOf(publishPendingSegments.keySet())
      );
    }
  }

  public List<SegmentIdentifier> removePublished(Collection<String> sequenceNames)
  {
    synchronized (activeSegments) {
      final List<SegmentIdentifier> segments = sequenceNames.stream()
                                                            .map(publishPendingSegments::remove)
                                                            .filter(Objects::nonNull)
                                                            .flatMap(Collection::stream)
                                                            .collect(Collectors.toList());
      sequenceNames.forEach(activeSegments::remove);

      return segments;
    }
  }

  public SegmentTrackerMetadata wrapMetadata(Object callerMetadata)
  {
    synchronized (activeSegments) {
      return new SegmentTrackerMetadata(
          ImmutableMap.copyOf(
              Maps.transformValues(
                  activeSegments,
                  new Function<NavigableMap<Long, SegmentIdentifier>, List<SegmentIdentifier>>()
                  {
                    @Override
                    public List<SegmentIdentifier> apply(NavigableMap<Long, SegmentIdentifier> input)
                    {
                      return ImmutableList.copyOf(input.values());
                    }
                  }
              )
          ),
          ImmutableMap.copyOf(publishPendingSegments),
          ImmutableMap.copyOf(lastSegmentIds),
          callerMetadata
      );
    }
  }

  public void restoreFromMetadata(SegmentTrackerMetadata metadata)
  {
    synchronized (activeSegments) {
      for (Map.Entry<String, List<SegmentIdentifier>> entry : metadata.getActiveSegments().entrySet()) {
        final String sequenceName = entry.getKey();
        final TreeMap<Long, SegmentIdentifier> segmentMap = Maps.newTreeMap();

        activeSegments.put(sequenceName, segmentMap);

        for (SegmentIdentifier identifier : entry.getValue()) {
          segmentMap.put(identifier.getInterval().getStartMillis(), identifier);
        }
      }
      publishPendingSegments.putAll(metadata.getPublishPendingSegments());
      lastSegmentIds.putAll(metadata.getLastSegmentIds());
    }
  }

  /**
   * Return a segment usable for "timestamp". May return null if no segment can be allocated.
   *
   * @param row    Input row
   * @param sequenceName sequenceName for potential segment allocation
   *
   * @return identifier, or null
   *
   * @throws IOException if an exception occurs while allocating a segment
   */
  public SegmentIdentifier getSegment(final InputRow row, final String sequenceName) throws IOException
  {
    synchronized (activeSegments) {
      final DateTime timestamp = row.getTimestamp();
      final SegmentIdentifier existing = getActiveSegment(timestamp, sequenceName);
      if (existing != null) {
        return existing;
      } else {
        // Allocate new segment.
        final SegmentIdentifier newSegment = segmentAllocator.allocate(
            row,
            sequenceName,
            lastSegmentIds.get(sequenceName)
        );


        if (newSegment != null) {
          log.info("New segment[%s] for sequenceName[%s].", newSegment, sequenceName);
          addSegment(sequenceName, newSegment);
        } else {
          // Well, we tried.
          log.warn("Cannot allocate segment for timestamp[%s], sequenceName[%s]. ", timestamp, sequenceName);
        }

        return newSegment;
      }
    }
  }

  /**
   * Move a set of identifiers out from "active", making way for newer segments.
   */
  public void moveSegmentOut(final String sequenceName, final List<SegmentIdentifier> identifiers)
  {
    synchronized (activeSegments) {
      final NavigableMap<Long, SegmentIdentifier> activeSegmentsForSequence = activeSegments.get(sequenceName);
      if (activeSegmentsForSequence == null) {
        throw new ISE("WTF?! Asked to remove segments for sequenceName[%s] which doesn't exist...", sequenceName);
      }

      for (final SegmentIdentifier identifier : identifiers) {
        log.info("Moving segment[%s] out of active list.", identifier);
        final long key = identifier.getInterval().getStartMillis();
        if (!activeSegmentsForSequence.remove(key).equals(identifier)) {
          throw new ISE("WTF?! Asked to remove segment[%s] that didn't exist...", identifier);
        }
      }
    }
  }

  private SegmentIdentifier getActiveSegment(final DateTime timestamp, final String sequenceName)
  {
    synchronized (activeSegments) {
      final NavigableMap<Long, SegmentIdentifier> activeSegmentsForSequence = activeSegments.get(sequenceName);

      if (activeSegmentsForSequence == null) {
        return null;
      }

      final Map.Entry<Long, SegmentIdentifier> candidateEntry = activeSegmentsForSequence.floorEntry(timestamp.getMillis());
      if (candidateEntry != null && candidateEntry.getValue().getInterval().contains(timestamp)) {
        return candidateEntry.getValue();
      } else {
        return null;
      }
    }
  }

  private void addSegment(String sequenceName, SegmentIdentifier identifier)
  {
    synchronized (activeSegments) {
      activeSegments.computeIfAbsent(sequenceName, k -> new TreeMap<>())
                    .putIfAbsent(identifier.getInterval().getStartMillis(), identifier);

      publishPendingSegments.computeIfAbsent(sequenceName, k -> new ArrayList<>())
                            .add(identifier);
      lastSegmentIds.put(sequenceName, identifier.getIdentifierAsString());
    }
  }

}
