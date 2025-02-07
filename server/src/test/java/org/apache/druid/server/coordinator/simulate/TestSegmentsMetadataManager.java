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

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SortOrder;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collection;
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

  public void addSegment(DataSegment segment)
  {
    allSegments.put(segment.getId().toString(), segment);
    usedSegments.put(segment.getId().toString(), segment);
    invalidateDatasourcesSnapshot();
  }

  public void removeSegment(DataSegment segment)
  {
    allSegments.remove(segment.getId().toString());
    usedSegments.remove(segment.getId().toString());
    invalidateDatasourcesSnapshot();
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

  @Override
  public int markAsUsedAllNonOvershadowedSegmentsInDataSource(String dataSource)
  {
    return markNonOvershadowedSegmentsAsUsed(dataSource, Predicates.alwaysTrue());
  }

  @Override
  public int markAsUsedNonOvershadowedSegmentsInInterval(String dataSource, Interval interval, @Nullable List<String> versions)
  {
    final Set<String> allowedVersions = versions == null ? null : new HashSet<>(versions);
    return markNonOvershadowedSegmentsAsUsed(
        dataSource,
        segment -> segment.getInterval().overlaps(interval)
                   && (versions == null || allowedVersions.contains(segment.getVersion()))
    );
  }

  @Override
  public int markAsUsedNonOvershadowedSegments(String dataSource, Set<String> segmentIds)
  {
    return markNonOvershadowedSegmentsAsUsed(dataSource, segment -> segmentIds.contains(segment.getId().toString()));
  }

  @Override
  public boolean markSegmentAsUsed(String segmentId)
  {
    if (!allSegments.containsKey(segmentId)
        || usedSegments.containsKey(segmentId)) {
      return false;
    }

    usedSegments.put(segmentId, allSegments.get(segmentId));
    return true;
  }

  @Override
  public int markAsUnusedAllSegmentsInDataSource(String dataSource)
  {
    return markSegmentsAsUnused(segment -> segment.getDataSource().equals(dataSource));
  }

  @Override
  public int markAsUnusedSegmentsInInterval(String dataSource, Interval interval, @Nullable List<String> versions)
  {
    final Set<String> eligibleVersions = versions == null ? null : new HashSet<>(versions);
    return markSegmentsAsUnused(
        segment -> segment.getDataSource().equals(dataSource)
                   && segment.getInterval().overlaps(interval)
                   && (eligibleVersions == null || eligibleVersions.contains(segment.getVersion()))
    );
  }

  @Override
  public int markSegmentsAsUnused(Set<SegmentId> segmentIds)
  {
    return markSegmentsAsUnused(
        segment -> segmentIds.contains(segment.getId())
    );
  }

  @Override
  public boolean markSegmentAsUnused(SegmentId segmentId)
  {
    boolean updated = usedSegments.remove(segmentId.toString()) != null;
    if (updated) {
      invalidateDatasourcesSnapshot();
    }

    return updated;
  }

  @Nullable
  @Override
  public ImmutableDruidDataSource getImmutableDataSourceWithUsedSegments(String dataSource)
  {
    if (snapshot == null) {
      getSnapshotOfDataSourcesWithAllUsedSegments();
    }
    return snapshot.getDataSource(dataSource);
  }

  @Override
  public Collection<ImmutableDruidDataSource> getImmutableDataSourcesWithAllUsedSegments()
  {
    return getSnapshotOfDataSourcesWithAllUsedSegments().getDataSourcesWithAllUsedSegments();
  }

  @Override
  public DataSourcesSnapshot getSnapshotOfDataSourcesWithAllUsedSegments()
  {
    if (snapshot == null) {
      snapshot = DataSourcesSnapshot.fromUsedSegments(usedSegments.values());
    }
    return snapshot;
  }

  @Override
  public Iterable<DataSegment> iterateAllUsedSegments()
  {
    return usedSegments.values();
  }

  @Override
  public Optional<Iterable<DataSegment>> iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(
      String datasource,
      Interval interval,
      boolean requiresLatest
  )
  {
    VersionedIntervalTimeline<String, DataSegment> usedSegmentsTimeline
        = getSnapshotOfDataSourcesWithAllUsedSegments().getUsedSegmentsTimelinesPerDataSource().get(datasource);
    return Optional.fromNullable(usedSegmentsTimeline)
                   .transform(timeline -> timeline.findNonOvershadowedObjectsInInterval(
                       interval,
                       Partitions.ONLY_COMPLETE
                   ));
  }

  @Override
  public Iterable<DataSegmentPlus> iterateAllUnusedSegmentsForDatasource(
      String datasource,
      @Nullable Interval interval,
      @Nullable Integer limit,
      @Nullable String lastSegmentId,
      @Nullable SortOrder sortOrder
  )
  {
    return null;
  }

  @Override
  public Set<String> retrieveAllDataSourceNames()
  {
    return allSegments.values().stream().map(DataSegment::getDataSource).collect(Collectors.toSet());
  }

  @Override
  public List<Interval> getUnusedSegmentIntervals(
      final String dataSource,
      @Nullable final DateTime minStartTime,
      final DateTime maxEndTime,
      final int limit,
      final DateTime maxUsedStatusLastUpdatedTime
  )
  {
    return null;
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
