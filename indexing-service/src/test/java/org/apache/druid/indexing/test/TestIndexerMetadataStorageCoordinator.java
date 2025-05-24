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

package org.apache.druid.indexing.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.SegmentCreateRequest;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.metadata.SortOrder;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.coordinator.simulate.TestSegmentsMetadataManager;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TestIndexerMetadataStorageCoordinator implements IndexerMetadataStorageCoordinator
{
  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  private final Set<DataSegment> nuked = Sets.newConcurrentHashSet();
  private final TestSegmentsMetadataManager segmentsMetadataManager = new TestSegmentsMetadataManager();

  private int deleteSegmentsCount = 0;

  @Override
  public Set<String> retrieveAllDatasourceNames()
  {
    return Set.of();
  }

  @Override
  public List<Interval> retrieveUnusedSegmentIntervals(String dataSource, int limit)
  {
    return List.of();
  }

  @Override
  public List<DataSegment> retrieveUnusedSegmentsWithExactInterval(
      String dataSource,
      Interval interval,
      DateTime maxUpdatedTime,
      int limit
  )
  {
    return List.of();
  }

  @Override
  public DataSourceMetadata retrieveDataSourceMetadata(String dataSource)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean deleteDataSourceMetadata(String dataSource)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean resetDataSourceMetadata(String dataSource, DataSourceMetadata dataSourceMetadata)
  {
    return false;
  }

  @Override
  public boolean insertDataSourceMetadata(String dataSource, DataSourceMetadata dataSourceMetadata)
  {
    return false;
  }

  @Override
  public Set<DataSegment> retrieveAllUsedSegments(String dataSource, Segments visibility)
  {
    return Set.copyOf(
        segmentsMetadataManager.getRecentDataSourcesSnapshot().getDataSource(dataSource).getSegments()
    );
  }

  @Override
  public List<Pair<DataSegment, String>> retrieveUsedSegmentsAndCreatedDates(String dataSource, List<Interval> intervals)
  {
    return List.of();
  }

  @Override
  public Set<DataSegment> retrieveUsedSegmentsForIntervals(
      String dataSource,
      List<Interval> intervals,
      Segments visibility
  )
  {
    return Set.of();
  }

  @Override
  public List<DataSegment> retrieveUnusedSegmentsForInterval(
      String dataSource,
      Interval interval,
      @Nullable Integer limit,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  )
  {
    return retrieveUnusedSegmentsForInterval(dataSource, interval, null, limit, maxUsedStatusLastUpdatedTime);
  }

  @Override
  public List<DataSegment> retrieveUnusedSegmentsForInterval(
      String dataSource,
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  )
  {
    return segmentsMetadataManager.getAllUnusedSegments()
                                  .stream()
                                  .filter(ds -> !nuked.contains(ds))
                                  .limit(limit != null ? limit : Long.MAX_VALUE)
                                  .collect(Collectors.toList());
  }

  @Override
  public Set<DataSegment> retrieveSegmentsById(String dataSource, Set<String> segmentIds)
  {
    return Set.of();
  }

  @Override
  public boolean markSegmentAsUnused(SegmentId segmentId)
  {
    return segmentsMetadataManager.markSegmentAsUnused(segmentId);
  }

  @Override
  public int markSegmentsAsUnused(String dataSource, Set<SegmentId> segmentIds)
  {
    return segmentsMetadataManager.markSegmentsAsUnused(segmentIds);
  }

  @Override
  public int markAllSegmentsAsUnused(String dataSource)
  {
    return segmentsMetadataManager.markAsUnusedAllSegmentsInDataSource(dataSource);
  }

  @Override
  public int markSegmentsWithinIntervalAsUnused(String dataSource, Interval interval, @Nullable List<String> versions)
  {
    return segmentsMetadataManager.markAsUnusedSegmentsInInterval(dataSource, interval, versions);
  }

  @Override
  public Set<DataSegment> commitSegments(
      Set<DataSegment> segments,
      final SegmentSchemaMapping segmentSchemaMapping
  )
  {
    Set<DataSegment> added = new HashSet<>();
    for (final DataSegment segment : segments) {
      if (segmentsMetadataManager.addSegment(segment)) {
        added.add(segment);
      }
    }
    return Set.copyOf(added);
  }

  @Override
  public Map<SegmentCreateRequest, SegmentIdWithShardSpec> allocatePendingSegments(
      String dataSource,
      Interval interval,
      boolean skipSegmentLineageCheck,
      List<SegmentCreateRequest> requests,
      boolean isTimeChunk
  )
  {
    return Map.of();
  }

  @Override
  public SegmentPublishResult commitReplaceSegments(
      Set<DataSegment> replaceSegments,
      Set<ReplaceTaskLock> locksHeldByReplaceTask,
      SegmentSchemaMapping segmentSchemaMapping
  )
  {
    return SegmentPublishResult.ok(commitSegments(replaceSegments, segmentSchemaMapping));
  }

  @Override
  public SegmentPublishResult commitAppendSegments(
      Set<DataSegment> appendSegments,
      Map<DataSegment, ReplaceTaskLock> appendSegmentToReplaceLock,
      String taskGroup,
      SegmentSchemaMapping segmentSchemaMapping
  )
  {
    return SegmentPublishResult.ok(commitSegments(appendSegments, segmentSchemaMapping));
  }

  @Override
  public SegmentPublishResult commitAppendSegmentsAndMetadata(
      Set<DataSegment> appendSegments,
      Map<DataSegment, ReplaceTaskLock> appendSegmentToReplaceLock,
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata,
      String taskAllocatorId,
      SegmentSchemaMapping segmentSchemaMapping
  )
  {
    return SegmentPublishResult.ok(commitSegments(appendSegments, segmentSchemaMapping));
  }

  @Override
  public SegmentPublishResult commitSegmentsAndMetadata(
      Set<DataSegment> segments,
      @Nullable DataSourceMetadata startMetadata,
      @Nullable DataSourceMetadata endMetadata,
      SegmentSchemaMapping segmentSchemaMapping
  )
  {
    // Don't actually compare metadata, just do it!
    return SegmentPublishResult.ok(commitSegments(segments, segmentSchemaMapping));
  }

  @Override
  public SegmentPublishResult commitMetadataOnly(
      String dataSource,
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata
  )
  {
    throw new UnsupportedOperationException("Not implemented, no test uses this currently.");
  }

  @Override
  public int removeDataSourceMetadataOlderThan(long timestamp, @Nullable Set<String> excludeDatasources)
  {
    throw new UnsupportedOperationException("Not implemented, no test uses this currently.");
  }


  @Override
  public SegmentIdWithShardSpec allocatePendingSegment(
      String dataSource,
      Interval interval,
      boolean skipSegmentLineageCheck,
      SegmentCreateRequest createRequest
  )
  {
    return new SegmentIdWithShardSpec(
        dataSource,
        interval,
        createRequest.getVersion(),
        createRequest.getPartialShardSpec().complete(objectMapper, 0, 0)
    );
  }

  @Override
  public int deletePendingSegmentsCreatedInInterval(String dataSource, Interval deleteInterval)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int deletePendingSegments(String dataSource)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int deleteSegments(Set<DataSegment> segments)
  {
    deleteSegmentsCount++;
    nuked.addAll(segments);
    return segments.size();
  }

  @Override
  public void updateSegmentMetadata(Set<DataSegment> segments)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataSegment retrieveSegmentForId(SegmentId segmentId)
  {
    return null;
  }

  @Override
  public DataSegment retrieveUsedSegmentForId(SegmentId segmentId)
  {
    return null;
  }

  @Override
  public int deleteUpgradeSegmentsForTask(final String taskId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int deletePendingSegmentsForTaskAllocatorId(final String datasource, final String taskAllocatorId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<PendingSegmentRecord> getPendingSegments(String datasource, Interval interval)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, String> retrieveUpgradedFromSegmentIds(
      final String dataSource,
      final Set<String> segmentIds
  )
  {
    return Map.of();
  }

  @Override
  public Map<String, Set<String>> retrieveUpgradedToSegmentIds(
      final String dataSource,
      final Set<String> segmentIds
  )
  {
    return Map.of();
  }

  @Override
  public List<DataSegmentPlus> iterateAllUnusedSegmentsForDatasource(
      String datasource,
      @Nullable Interval interval,
      @Nullable Integer limit,
      @Nullable String lastSegmentId,
      @Nullable SortOrder sortOrder
  )
  {
    return List.of();
  }

  @Override
  public List<Interval> getUnusedSegmentIntervals(
      String dataSource,
      @Nullable DateTime minStartTime,
      DateTime maxEndTime,
      int limit,
      DateTime maxUsedStatusLastUpdatedTime
  )
  {
    return List.of();
  }

  @Override
  public int markAllNonOvershadowedSegmentsAsUsed(String dataSource)
  {
    return segmentsMetadataManager.markAsUsedAllNonOvershadowedSegmentsInDataSource(dataSource);
  }

  @Override
  public int markNonOvershadowedSegmentsAsUsed(
      String dataSource,
      Interval interval,
      @Nullable List<String> versions
  )
  {
    return segmentsMetadataManager.markAsUsedNonOvershadowedSegmentsInInterval(dataSource, interval, versions);
  }

  @Override
  public int markNonOvershadowedSegmentsAsUsed(String dataSource, Set<SegmentId> segmentIds)
  {
    return segmentsMetadataManager.markAsUsedNonOvershadowedSegments(dataSource, segmentIds);
  }

  @Override
  public boolean markSegmentAsUsed(SegmentId segmentId)
  {
    return segmentsMetadataManager.markSegmentAsUsed(segmentId.toString());
  }

  @Override
  public SegmentTimeline getSegmentTimelineForAllocation(
      String dataSource,
      Interval interval,
      boolean skipSegmentPayloadFetchForAllocation
  )
  {
    return SegmentTimeline.forSegments(retrieveUsedSegmentsForIntervals(
        dataSource,
        List.of(interval),
        Segments.INCLUDING_OVERSHADOWED
    ));
  }

  public Set<DataSegment> getPublished()
  {
    return segmentsMetadataManager.getAllSegments();
  }

  public Set<DataSegment> getNuked()
  {
    return Set.copyOf(nuked);
  }

  public int getDeleteSegmentsCount()
  {
    return deleteSegmentsCount;
  }
}
