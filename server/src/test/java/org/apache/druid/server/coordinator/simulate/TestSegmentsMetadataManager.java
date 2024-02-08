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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SortOrder;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TestSegmentsMetadataManager implements SegmentsMetadataManager
{
  private final ConcurrentMap<String, DataSegment> segments = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, DataSegment> usedSegments = new ConcurrentHashMap<>();

  private volatile DataSourcesSnapshot snapshot;

  public void addSegment(DataSegment segment)
  {
    segments.put(segment.getId().toString(), segment);
    usedSegments.put(segment.getId().toString(), segment);
    snapshot = null;
  }

  public void removeSegment(DataSegment segment)
  {
    segments.remove(segment.getId().toString());
    usedSegments.remove(segment.getId().toString());
    snapshot = null;
  }

  @Override
  public void startPollingDatabasePeriodically()
  {

  }

  @Override
  public void stopPollingDatabasePeriodically()
  {

  }

  @Override
  public boolean isPollingDatabasePeriodically()
  {
    return true;
  }

  @Override
  public int markAsUsedAllNonOvershadowedSegmentsInDataSource(String dataSource)
  {
    return 0;
  }

  @Override
  public int markAsUsedNonOvershadowedSegmentsInInterval(String dataSource, Interval interval)
  {
    return 0;
  }

  @Override
  public int markAsUsedNonOvershadowedSegments(String dataSource, Set<String> segmentIds)
  {
    return 0;
  }

  @Override
  public boolean markSegmentAsUsed(String segmentId)
  {
    if (!segments.containsKey(segmentId)) {
      return false;
    }

    usedSegments.put(segmentId, segments.get(segmentId));
    return true;
  }

  @Override
  public int markAsUnusedAllSegmentsInDataSource(String dataSource)
  {
    return 0;
  }

  @Override
  public int markAsUnusedSegmentsInInterval(String dataSource, Interval interval)
  {
    return 0;
  }

  @Override
  public int markSegmentsAsUnused(Set<SegmentId> segmentIds)
  {
    int numModifiedSegments = 0;
    for (SegmentId segmentId : segmentIds) {
      if (usedSegments.remove(segmentId.toString()) != null) {
        ++numModifiedSegments;
      }
    }

    if (numModifiedSegments > 0) {
      snapshot = null;
    }
    return numModifiedSegments;
  }

  @Override
  public boolean markSegmentAsUnused(SegmentId segmentId)
  {
    boolean updated = usedSegments.remove(segmentId.toString()) != null;
    if (updated) {
      snapshot = null;
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
      snapshot = DataSourcesSnapshot.fromUsedSegments(usedSegments.values(), ImmutableMap.of());
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
    return null;
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
  public void poll()
  {

  }

  @Override
  public void populateUsedFlagLastUpdatedAsync()
  {
  }

  @Override
  public void stopAsyncUsedFlagLastUpdatedUpdate()
  {
  }
}
