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

package org.apache.druid.metadata.segment.cache;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Datasource-level cache for segments and pending segments.
 */
class DatasourceSegmentCache extends BaseCache
{
  private static final DatasourceSegmentCache EMPTY_INSTANCE = new DatasourceSegmentCache();

  /**
   * Used to obtain the segment for a given ID so that it can be updated in the
   * timeline.
   */
  private final Map<String, DataSegmentPlus> idToUsedSegment = new HashMap<>();

  /**
   * Current state of segments as seen by the cache.
   */
  private final Map<String, SegmentState> idToSegmentState = new HashMap<>();

  /**
   * Allows lookup of visible segments for a given interval.
   */
  private final SegmentTimeline usedSegmentTimeline = SegmentTimeline.forSegments(Set.of());

  private final Map<Interval, Map<String, PendingSegmentRecord>>
      intervalToPendingSegments = new HashMap<>();

  private final Set<String> unusedSegmentIds = new HashSet<>();

  static DatasourceSegmentCache empty()
  {
    return EMPTY_INSTANCE;
  }

  DatasourceSegmentCache()
  {
    super(true);
  }

  void clear()
  {
    withWriteLock(() -> {
      idToSegmentState.clear();
      idToUsedSegment.clear();
      unusedSegmentIds.clear();
      idToUsedSegment.values().forEach(s -> usedSegmentTimeline.remove(s.getDataSegment()));
    });
  }

  boolean isEmpty()
  {
    return withReadLock(() -> idToSegmentState.isEmpty() && intervalToPendingSegments.isEmpty());
  }

  /**
   * Checks if a segment needs to be refreshed. A refresh is required if the
   * cache has no known state for the given segment or if the metadata store
   * has a more recent last_updated_time than the cache.
   */
  boolean shouldRefreshSegment(String segmentId, SegmentState metadataState)
  {
    return withReadLock(() -> {
      final SegmentState cachedState = idToSegmentState.get(segmentId);
      return cachedState == null
             || cachedState.getLastUpdatedTime().isBefore(metadataState.getLastUpdatedTime());
    });
  }

  /**
   * Checks if a pending segment needs to be refreshed in the cache.
   */
  boolean shouldRefreshPendingSegment(PendingSegmentRecord record)
  {
    final SegmentIdWithShardSpec segmentId = record.getId();
    return withReadLock(
        () -> intervalToPendingSegments.getOrDefault(segmentId.getInterval(), Map.of())
                                       .containsKey(segmentId.toString())
    );
  }

  boolean refreshUnusedSegment(String segmentId, SegmentState newState)
  {
    if (newState.isUsed()) {
      return false;
    }

    return withWriteLock(() -> {
      if (!shouldRefreshSegment(segmentId, newState)) {
        return false;
      }

      final SegmentState oldState = idToSegmentState.put(segmentId, newState);

      if (oldState != null && oldState.isUsed()) {
        // Segment has transitioned from used to unused
        DataSegmentPlus segment = idToUsedSegment.remove(segmentId);
        if (segment != null) {
          usedSegmentTimeline.remove(segment.getDataSegment());
        }
      }

      unusedSegmentIds.add(segmentId);
      return true;
    });
  }

  boolean refreshUsedSegment(DataSegmentPlus segmentPlus)
  {
    final DataSegment segment = segmentPlus.getDataSegment();
    final String segmentId = getId(segment);

    final SegmentState newState = new SegmentState(
        Boolean.TRUE.equals(segmentPlus.getUsed()),
        segmentPlus.getUsedStatusLastUpdatedDate()
    );
    if (!newState.isUsed()) {
      return refreshUnusedSegment(segmentId, newState);
    }

    return withWriteLock(() -> {
      if (!shouldRefreshSegment(segmentId, newState)) {
        return false;
      }

      final SegmentState oldState = idToSegmentState.put(segmentId, newState);
      final DataSegmentPlus oldSegmentPlus = idToUsedSegment.put(segmentId, segmentPlus);

      if (oldState == null) {
        // This is a new segment
      } else if (oldState.isUsed()) {
        // Segment payload may have changed
        if (oldSegmentPlus != null) {
          usedSegmentTimeline.remove(oldSegmentPlus.getDataSegment());
        }
      } else {
        // Segment has transitioned from unused to used
        unusedSegmentIds.remove(segmentId);
      }

      usedSegmentTimeline.add(segment);
      return true;
    });
  }

  int removeSegmentIds(Set<String> segmentIds)
  {
    return withWriteLock(() -> {
      int removedCount = 0;
      for (String segmentId : segmentIds) {
        SegmentState state = idToSegmentState.remove(segmentId);
        if (state != null) {
          ++removedCount;
        }

        unusedSegmentIds.remove(segmentId);

        final DataSegmentPlus segment = idToUsedSegment.remove(segmentId);
        if (segment != null) {
          usedSegmentTimeline.remove(segment.getDataSegment());
        }
      }

      return removedCount;
    });
  }

  /**
   * Returns the set of segment IDs present in the cache but not present in the
   * given set of known segment IDs.
   */
  Set<String> getSegmentIdsNotIn(Set<String> knownSegmentIds)
  {
    return withReadLock(
        () -> knownSegmentIds.stream()
                             .filter(id -> !idToSegmentState.containsKey(id))
                             .collect(Collectors.toSet())
    );
  }

  // READ METHODS

  @Override
  public Set<String> findExistingSegmentIds(Set<DataSegment> segments)
  {
    return withReadLock(
        () -> segments.stream()
                      .map(DatasourceSegmentCache::getId)
                      .filter(idToSegmentState::containsKey)
                      .collect(Collectors.toSet())
    );
  }

  @Override
  public Set<SegmentId> findUsedSegmentIdsOverlapping(Interval interval)
  {
    return findUsedSegmentsPlusOverlappingAnyOf(List.of(interval))
        .stream()
        .map(s -> s.getDataSegment().getId())
        .collect(Collectors.toSet());
  }

  @Override
  public Set<String> findUnusedSegmentIdsWithExactIntervalAndVersion(Interval interval, String version)
  {
    // TODO: implement this or may be add a variant of this method to find the
    //  max unused segment ID for an exact interval and version
    throw DruidException.defensive("Unsupported: Unused segments are not cached");
  }

  @Override
  public CloseableIterator<DataSegment> findUsedSegmentsOverlappingAnyOf(List<Interval> intervals)
  {
    return CloseableIterators.withEmptyBaggage(
        findUsedSegmentsPlusOverlappingAnyOf(intervals)
            .stream()
            .map(DataSegmentPlus::getDataSegment)
            .iterator()
    );
  }

  @Override
  public List<DataSegment> findUsedSegments(Set<String> segmentIds)
  {
    return withReadLock(
        () -> segmentIds.stream()
                        .map(idToUsedSegment::get)
                        .filter(Objects::nonNull)
                        .map(DataSegmentPlus::getDataSegment)
                        .collect(Collectors.toList())
    );
  }

  @Override
  public Set<DataSegmentPlus> findUsedSegmentsPlusOverlappingAnyOf(List<Interval> intervals)
  {
    return withReadLock(
        () -> idToUsedSegment.values()
                             .stream()
                             .filter(s -> anyIntervalOverlaps(intervals, s.getDataSegment().getInterval()))
                             .collect(Collectors.toSet())
    );
  }

  @Override
  public DataSegment findSegment(String segmentId)
  {
    throw DruidException.defensive("Unsupported: Unused segments are not cached");
  }

  @Override
  public DataSegment findUsedSegment(String segmentId)
  {
    return withReadLock(() -> {
      final DataSegmentPlus segmentPlus = idToUsedSegment.get(segmentId);
      return segmentPlus == null ? null : segmentPlus.getDataSegment();
    });
  }

  @Override
  public List<DataSegmentPlus> findSegments(Set<String> segmentIds)
  {
    throw DruidException.defensive("Unsupported: Unused segments are not cached");
  }

  @Override
  public List<DataSegmentPlus> findSegmentsWithSchema(Set<String> segmentIds)
  {
    throw DruidException.defensive("Unsupported: Unused segments are not cached");
  }

  @Override
  public List<DataSegment> findUnusedSegments(
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  )
  {
    throw DruidException.defensive("Unsupported: Unused segments are not cached");
  }

  @Override
  public List<SegmentIdWithShardSpec> findPendingSegmentIds(String sequenceName, String sequencePreviousId)
  {
    return findPendingSegmentsMatching(
        record -> sequenceName.equals(record.getSequenceName())
                  && sequencePreviousId.equals(record.getSequencePrevId())
    )
        .stream()
        .map(PendingSegmentRecord::getId)
        .collect(Collectors.toList());
  }

  @Override
  public List<SegmentIdWithShardSpec> findPendingSegmentIdsWithExactInterval(String sequenceName, Interval interval)
  {
    return withReadLock(
        () -> intervalToPendingSegments
            .getOrDefault(interval, Map.of())
            .values()
            .stream()
            .filter(record -> record.getSequenceName().equals(sequenceName))
            .map(PendingSegmentRecord::getId)
            .collect(Collectors.toList())
    );
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegmentsOverlapping(Interval interval)
  {
    return withReadLock(
        () -> intervalToPendingSegments.entrySet()
                                       .stream()
                                       .filter(entry -> entry.getKey().overlaps(interval))
                                       .flatMap(entry -> entry.getValue().values().stream())
                                       .collect(Collectors.toList())
    );
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegmentsWithExactInterval(Interval interval)
  {
    return withReadLock(
        () -> List.copyOf(
            intervalToPendingSegments.getOrDefault(interval, Map.of()).values()
        )
    );
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegments(String taskAllocatorId)
  {
    return findPendingSegmentsMatching(record -> taskAllocatorId.equals(record.getTaskAllocatorId()));
  }

  // WRITE METHODS

  @Override
  public int insertSegments(Set<DataSegmentPlus> segments)
  {
    return withWriteLock(() -> {
      int numInsertedSegments = 0;
      for (DataSegmentPlus segmentPlus : segments) {
        final DataSegment segment = segmentPlus.getDataSegment();
        final String segmentId = getId(segment);
        final SegmentState state = new SegmentState(
            Boolean.TRUE.equals(segmentPlus.getUsed()),
            segmentPlus.getUsedStatusLastUpdatedDate()
        );

        final boolean updated = state.isUsed()
                                ? refreshUsedSegment(segmentPlus)
                                : refreshUnusedSegment(segmentId, state);
        if (updated) {
          ++numInsertedSegments;
        }
      }

      return numInsertedSegments;
    });
  }

  @Override
  public int insertSegmentsWithMetadata(Set<DataSegmentPlus> segments)
  {
    return insertSegments(segments);
  }

  @Override
  public int markSegmentsWithinIntervalAsUnused(Interval interval, DateTime updateTime)
  {
    int updatedCount = 0;
    try (CloseableIterator<DataSegment> segmentIterator
             = findUsedSegmentsOverlappingAnyOf(List.of(interval))) {
      while (segmentIterator.hasNext()) {
        boolean updated = refreshUnusedSegment(
            getId(segmentIterator.next()),
            new SegmentState(false, updateTime)
        );
        if (updated) {
          ++updatedCount;
        }
      }
    }
    catch (IOException e) {
      throw DruidException.defensive("Error while updating segments in cache");
    }

    return updatedCount;
  }

  @Override
  public int deleteSegments(Set<DataSegment> segments)
  {
    final Set<String> segmentIdsToDelete =
        segments.stream()
                .map(DatasourceSegmentCache::getId)
                .collect(Collectors.toSet());
    return withWriteLock(() -> removeSegmentIds(segmentIdsToDelete));
  }

  @Override
  public boolean updateSegmentPayload(DataSegment segment)
  {
    // Segment payload updates are not supported since we don't know if the segment is used or unused
    throw DruidException.defensive("Unsupported: Segment payload updates are not supported in the cache");
  }

  @Override
  public boolean insertPendingSegment(PendingSegmentRecord pendingSegment, boolean skipSegmentLineageCheck)
  {
    return insertPendingSegments(List.of(pendingSegment), skipSegmentLineageCheck) > 0;
  }

  @Override
  public int insertPendingSegments(List<PendingSegmentRecord> pendingSegments, boolean skipSegmentLineageCheck)
  {
    return withWriteLock(() -> {
      int insertedCount = 0;
      for (PendingSegmentRecord record : pendingSegments) {
        final SegmentIdWithShardSpec segmentId = record.getId();
        PendingSegmentRecord oldValue =
            intervalToPendingSegments.computeIfAbsent(segmentId.getInterval(), interval -> new HashMap<>())
                                     .putIfAbsent(segmentId.toString(), record);
        if (oldValue == null) {
          ++insertedCount;
        }
      }

      return insertedCount;
    });
  }

  @Override
  public int deleteAllPendingSegments()
  {
    return withWriteLock(() -> {
      int numPendingSegments = intervalToPendingSegments.values().stream().mapToInt(Map::size).sum();
      intervalToPendingSegments.clear();
      return numPendingSegments;
    });
  }

  @Override
  public int deletePendingSegments(List<String> segmentIdsToDelete)
  {
    final Set<String> remainingIdsToDelete = new HashSet<>(segmentIdsToDelete);

    withWriteLock(() -> intervalToPendingSegments.forEach(
        (interval, pendingSegments) -> {
          final Set<String> deletedIds =
              remainingIdsToDelete.stream()
                                  .map(pendingSegments::remove)
                                  .filter(Objects::nonNull)
                                  .map(record -> record.getId().toString())
                                  .collect(Collectors.toSet());

          remainingIdsToDelete.removeAll(deletedIds);
        }
    ));

    return segmentIdsToDelete.size() - remainingIdsToDelete.size();
  }

  @Override
  public int deletePendingSegments(String taskAllocatorId)
  {
    return withWriteLock(() -> {
      List<String> idsToDelete = findPendingSegmentsMatching(
          record -> taskAllocatorId.equals(record.getTaskAllocatorId())
      ).stream().map(record -> record.getId().toString()).collect(Collectors.toList());

      return deletePendingSegments(idsToDelete);
    });
  }

  @Override
  public int deletePendingSegmentsCreatedIn(Interval interval)
  {
    return withWriteLock(() -> {
      List<String> idsToDelete = findPendingSegmentsMatching(
          record -> interval.contains(record.getCreatedDate())
      ).stream().map(record -> record.getId().toString()).collect(Collectors.toList());

      return deletePendingSegments(idsToDelete);
    });
  }

  /**
   * Returns all the pending segments that match the given predicate.
   */
  private List<PendingSegmentRecord> findPendingSegmentsMatching(Predicate<PendingSegmentRecord> predicate)
  {
    return withReadLock(
        () -> intervalToPendingSegments.entrySet()
                                       .stream()
                                       .flatMap(entry -> entry.getValue().values().stream())
                                       .filter(predicate)
                                       .collect(Collectors.toList())
    );
  }

  private static boolean anyIntervalOverlaps(List<Interval> intervals, Interval testInterval)
  {
    return intervals.isEmpty()
           || intervals.stream().anyMatch(interval -> interval.overlaps(testInterval));
  }

  private static String getId(DataSegment segment)
  {
    return segment.getId().toString();
  }
}
