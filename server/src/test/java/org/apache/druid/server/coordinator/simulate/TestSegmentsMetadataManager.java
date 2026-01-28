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

package org.apache.druid.server.coordinator.simulate;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TestSegmentsMetadataManager implements SegmentsMetadataManager
{
  private final ConcurrentMap<String, DataSegment> allSegments = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, DataSegment> usedSegments = new ConcurrentHashMap<>();

  private volatile DataSourcesSnapshot snapshot;
  private volatile boolean pollingStarted;

  @Override
  public void start()
  {

  }

  @Override
  public void stop()
  {

  }

  public boolean addSegment(DataSegment segment)
  {
    allSegments.put(segment.getId().toString(), segment);
    boolean added = usedSegments.put(segment.getId().toString(), segment) != null;

    invalidateDatasourcesSnapshot();

    return added;
  }

  public void removeSegment(DataSegment segment)
  {
    allSegments.remove(segment.getId().toString());
    usedSegments.remove(segment.getId().toString());
    invalidateDatasourcesSnapshot();
  }

  public Set<DataSegment> getAllUnusedSegments()
  {
    return Set.copyOf(Maps.difference(allSegments, usedSegments).entriesOnlyOnLeft().values());
  }

  public Set<DataSegment> getAllSegments()
  {
    return Set.copyOf(allSegments.values());
  }

  @Override
  public void startPollingDatabasePeriodically()
  {
    this.pollingStarted = true;
  }

  @Override
  public void stopPollingDatabasePeriodically()
  {
    this.pollingStarted = false;
  }

  @Override
  public boolean isPollingDatabasePeriodically()
  {
    return pollingStarted;
  }

  public int markAsUsedAllNonOvershadowedSegmentsInDataSource(String dataSource)
  {
    return markNonOvershadowedSegmentsAsUsed(dataSource, Predicates.alwaysTrue());
  }

  public int markAsUsedNonOvershadowedSegmentsInInterval(String dataSource, Interval interval, @Nullable List<String> versions)
  {
    final Set<String> allowedVersions = versions == null ? null : new HashSet<>(versions);
    return markNonOvershadowedSegmentsAsUsed(
        dataSource,
        segment -> segment.getInterval().overlaps(interval)
                   && (versions == null || allowedVersions.contains(segment.getVersion()))
    );
  }

  public int markAsUsedNonOvershadowedSegments(String dataSource, Set<SegmentId> segmentIds)
  {
    return markNonOvershadowedSegmentsAsUsed(dataSource, segment -> segmentIds.contains(segment.getId()));
  }

  public boolean markSegmentAsUsed(String segmentId)
  {
    if (!allSegments.containsKey(segmentId)
        || usedSegments.containsKey(segmentId)) {
      return false;
    }

    usedSegments.put(segmentId, allSegments.get(segmentId));
    return true;
  }

  public int markAsUnusedAllSegmentsInDataSource(String dataSource)
  {
    return markSegmentsAsUnused(segment -> segment.getDataSource().equals(dataSource));
  }

  public int markAsUnusedSegmentsInInterval(String dataSource, Interval interval, @Nullable List<String> versions)
  {
    final Set<String> eligibleVersions = versions == null ? null : new HashSet<>(versions);
    return markSegmentsAsUnused(
        segment -> segment.getDataSource().equals(dataSource)
                   && segment.getInterval().overlaps(interval)
                   && (eligibleVersions == null || eligibleVersions.contains(segment.getVersion()))
    );
  }

  public int markSegmentsAsUnused(Set<SegmentId> segmentIds)
  {
    return markSegmentsAsUnused(
        segment -> segmentIds.contains(segment.getId())
    );
  }

  public boolean markSegmentAsUnused(SegmentId segmentId)
  {
    boolean updated = usedSegments.remove(segmentId.toString()) != null;
    if (updated) {
      invalidateDatasourcesSnapshot();
    }

    return updated;
  }

  @Override
  public DataSourcesSnapshot getRecentDataSourcesSnapshot()
  {
    if (snapshot == null) {
      snapshot = DataSourcesSnapshot.fromUsedSegments(usedSegments.values());
    }
    return snapshot;
  }

  @Override
  public DataSourcesSnapshot forceUpdateDataSourcesSnapshot()
  {
    snapshot = DataSourcesSnapshot.fromUsedSegments(usedSegments.values());
    return snapshot;
  }

  public Set<String> retrieveAllDataSourceNames()
  {
    return allSegments.values().stream().map(DataSegment::getDataSource).collect(Collectors.toSet());
  }

  @Override
  public void populateUsedFlagLastUpdatedAsync()
  {
  }

  @Override
  public void stopAsyncUsedFlagLastUpdatedUpdate()
  {
  }

  private int markNonOvershadowedSegmentsAsUsed(String dataSource, Predicate<DataSegment> isSegmentEligible)
  {
    // Build a timeline of all datasource segments
    final Set<DataSegment> datasourceSegments = allSegments.values().stream().filter(
        segment -> segment.getDataSource().equals(dataSource)
    ).collect(Collectors.toSet());
    final SegmentTimeline timeline = SegmentTimeline.forSegments(datasourceSegments);

    // Find all unused segments which are not overshadowed
    final Map<String, DataSegment> segmentsToUpdate = new HashMap<>();
    datasourceSegments.forEach(segment -> {
      final String segmentId = segment.getId().toString();
      if (isSegmentEligible.test(segment)
          && !usedSegments.containsKey(segmentId)
          && !timeline.isOvershadowed(segment)) {
        segmentsToUpdate.put(segmentId, segment);
      }
    });

    if (segmentsToUpdate.isEmpty()) {
      return 0;
    } else {
      usedSegments.putAll(segmentsToUpdate);
      invalidateDatasourcesSnapshot();
      return segmentsToUpdate.size();
    }
  }

  private int markSegmentsAsUnused(Predicate<DataSegment> isSegmentEligible)
  {
    final Set<String> segmentIdsToMarkUnused
        = usedSegments.values()
                      .stream()
                      .filter(isSegmentEligible::test)
                      .map(segment -> segment.getId().toString())
                      .collect(Collectors.toSet());

    if (segmentIdsToMarkUnused.isEmpty()) {
      return 0;
    } else {
      segmentIdsToMarkUnused.forEach(usedSegments::remove);
      invalidateDatasourcesSnapshot();
      return segmentIdsToMarkUnused.size();
    }
  }

  private void invalidateDatasourcesSnapshot()
  {
    snapshot = null;
  }
}
