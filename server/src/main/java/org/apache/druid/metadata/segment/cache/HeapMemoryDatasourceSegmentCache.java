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
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * In-memory cache for segments and pending segments of a single datasource.
 */
class HeapMemoryDatasourceSegmentCache extends ReadWriteCache
{
  private final String dataSource;
  private final Map<String, DataSegmentPlus> idToUsedSegment = new HashMap<>();
  private final Map<String, DateTime> unusedSegmentIdToUpdatedTime = new HashMap<>();

  /**
   * Not being used right now. Could allow lookup of visible segments for a given interval.
   */
  private final SegmentTimeline usedSegmentTimeline = SegmentTimeline.forSegments(Set.of());

  /**
   * Map from interval to segment ID to pending segment record.
   */
  private final Map<Interval, Map<String, PendingSegmentRecord>>
      intervalToPendingSegments = new HashMap<>();

  private final Map<Interval, Map<String, Integer>>
      intervalVersionToHighestUnusedPartitionNumber = new HashMap<>();

  HeapMemoryDatasourceSegmentCache(String dataSource)
  {
    super(true);
    this.dataSource = dataSource;
  }

  @Override
  public void stop()
  {
    withWriteLock(() -> {
      unusedSegmentIdToUpdatedTime.clear();
      idToUsedSegment.values().forEach(s -> usedSegmentTimeline.remove(s.getDataSegment()));
      idToUsedSegment.clear();
      intervalVersionToHighestUnusedPartitionNumber.clear();
      intervalToPendingSegments.clear();
      super.stop();
    });
  }

  /**
   * Checks if a segment needs to be refreshed. A refresh is required if the
   * cache has no entry for the given segment or if the metadata store has a
   * more recently updated copy of the segment.
   *
   * @param persistedUpdateTime Last updated time of this segment as persisted
   *                            in the metadata store. This value can be null
   *                            for segments persisted before the column
   *                            used_status_last_updated was added to the table.
   */
  boolean shouldRefreshUsedSegment(String segmentId, @Nullable DateTime persistedUpdateTime)
  {
    return withReadLock(() -> {
      final DataSegmentPlus cachedState = idToUsedSegment.get(segmentId);
      return cachedState == null
             || shouldUpdateCache(cachedState.getUsedStatusLastUpdatedDate(), persistedUpdateTime);
    });
  }

  /**
   * Checks if a pending segment needs to be refreshed in the cache.
   */
  boolean shouldRefreshPendingSegment(PendingSegmentRecord record)
  {
    final SegmentIdWithShardSpec segmentId = record.getId();
    return withReadLock(
        () -> !intervalToPendingSegments.getOrDefault(segmentId.getInterval(), Map.of())
                                        .containsKey(segmentId.toString())
    );
  }

  /**
   * Checks if a record in the cache needs to be updated.
   *
   * @param cachedUpdateTime Updated time of record already present in cache
   * @param newUpdateTime    Updated time of record being considered to replace
   *                         the existing one
   */
  private boolean shouldUpdateCache(
      @Nullable DateTime cachedUpdateTime,
      @Nullable DateTime newUpdateTime
  )
  {
    if (newUpdateTime == null) {
      // Do not update cache as candidate entry is probably from before the
      // used_status_last_updated column was added
      return false;
    } else {
      // Update cache as entry is older than that persisted in metadata store
      return cachedUpdateTime == null || cachedUpdateTime.isBefore(newUpdateTime);
    }
  }

  /**
   * Adds or updates the given segment in the cache.
   *
   * @return true if the segment was updated in the cache, false if the segment
   * was left unchanged in the cache.
   */
  boolean addSegment(DataSegmentPlus segmentPlus)
  {
    if (Boolean.TRUE.equals(segmentPlus.getUsed())) {
      return addUsedSegment(segmentPlus);
    } else {
      return addUnusedSegmentId(
          segmentPlus.getDataSegment().getId(),
          segmentPlus.getUsedStatusLastUpdatedDate()
      );
    }
  }

  /**
   * Adds or updates a used segment in the cache.
   */
  private boolean addUsedSegment(DataSegmentPlus segmentPlus)
  {
    final DataSegment segment = segmentPlus.getDataSegment();
    final String segmentId = getId(segment);

    return withWriteLock(() -> {
      if (!shouldRefreshUsedSegment(segmentId, segmentPlus.getUsedStatusLastUpdatedDate())) {
        return false;
      }

      final DataSegmentPlus oldSegmentPlus = idToUsedSegment.put(segmentId, segmentPlus);
      if (oldSegmentPlus != null) {
        // Segment payload may have changed, remove old value from timeline
        usedSegmentTimeline.remove(oldSegmentPlus.getDataSegment());
      }

      unusedSegmentIdToUpdatedTime.remove(segmentId);
      usedSegmentTimeline.add(segment);
      return true;
    });
  }

  /**
   * Adds or updates an unused segment in the cache.
   *
   * @param updatedTime Last updated time of this segment as persisted in the
   *                    metadata store. This value can be null for segments
   *                    persisted to the metadata store before the column
   *                    used_status_last_updated was added to the segments table.
   */
  boolean addUnusedSegmentId(SegmentId segmentId, @Nullable DateTime updatedTime)
  {
    final int partitionNum = segmentId.getPartitionNum();
    return withWriteLock(() -> {
      final DataSegmentPlus oldSegmentPlus = idToUsedSegment.remove(segmentId.toString());
      if (oldSegmentPlus != null) {
        // Segment has transitioned from used to unused
        usedSegmentTimeline.remove(oldSegmentPlus.getDataSegment());
      }

      final String serializedId = segmentId.toString();
      if (!unusedSegmentIdToUpdatedTime.containsKey(serializedId)
          || shouldUpdateCache(unusedSegmentIdToUpdatedTime.get(serializedId), updatedTime)) {
        unusedSegmentIdToUpdatedTime.put(serializedId, updatedTime);
        intervalVersionToHighestUnusedPartitionNumber
            .computeIfAbsent(segmentId.getInterval(), i -> new HashMap<>())
            .merge(segmentId.getVersion(), partitionNum, Math::max);
        return true;
      } else {
        return false;
      }
    });
  }

  /**
   * Removes all pending segments which are present in the cache but not present
   * in the metadata store.
   */
  int removeUnpersistedPendingSegments(Set<String> persistedPendingSegmentIds, DateTime pollStartTime)
  {
    return withWriteLock(() -> {
      final Set<String> unpersistedSegmentIds =
          findPendingSegmentsMatching(
              record -> !persistedPendingSegmentIds.contains(record.getId().toString())
                        && shouldUpdateCache(record.getCreatedDate(), pollStartTime)
          ).stream().map(record -> record.getId().toString()).collect(Collectors.toSet());
      return deletePendingSegments(unpersistedSegmentIds);
    });
  }

  /**
   * Removes all segments which are not present in the metadata store and were
   * updated before the current sync started.
   *
   * @param persistedSegmentIds Segment IDs present in the metadata store
   * @param syncStartTime       Start time of the current sync
   * @return Number of unpersisted segments removed from cache.
   */
  int removeUnpersistedSegments(Set<String> persistedSegmentIds, DateTime syncStartTime)
  {
    return withWriteLock(() -> {
      final Set<String> unpersistedSegmentIds = new HashSet<>();
      unusedSegmentIdToUpdatedTime.entrySet().stream().filter(
          entry -> !persistedSegmentIds.contains(entry.getKey())
                   && shouldUpdateCache(entry.getValue(), syncStartTime)
      ).map(Map.Entry::getKey).forEach(unpersistedSegmentIds::add);

      idToUsedSegment.entrySet().stream().filter(
          entry -> !persistedSegmentIds.contains(entry.getKey())
                   && shouldUpdateCache(entry.getValue().getUsedStatusLastUpdatedDate(), syncStartTime)
      ).map(Map.Entry::getKey).forEach(unpersistedSegmentIds::add);

      return removeSegmentsForIds(unpersistedSegmentIds);
    });
  }

  int removeSegmentsForIds(Set<String> segmentIds)
  {
    return withWriteLock(() -> {
      int removedCount = 0;
      for (String segmentId : segmentIds) {
        final DataSegmentPlus segment = idToUsedSegment.remove(segmentId);
        if (segment != null) {
          usedSegmentTimeline.remove(segment.getDataSegment());
          ++removedCount;
        } else if (unusedSegmentIdToUpdatedTime.containsKey(segmentId)) {
          unusedSegmentIdToUpdatedTime.remove(segmentId);
          ++removedCount;
        }
      }

      return removedCount;
    });
  }

  /**
   * Resets the {@link #intervalVersionToHighestUnusedPartitionNumber} with the
   * new values.
   */
  void resetMaxUnusedIds(Map<Interval, Map<String, Integer>> intervalVersionToHighestPartitionNumber)
  {
    withWriteLock(() -> {
      this.intervalVersionToHighestUnusedPartitionNumber.clear();
      this.intervalVersionToHighestUnusedPartitionNumber.putAll(intervalVersionToHighestPartitionNumber);
    });
  }

  // READ METHODS

  @Override
  public Set<String> findExistingSegmentIds(Set<DataSegment> segments)
  {
    return withReadLock(
        () -> segments.stream()
                      .map(HeapMemoryDatasourceSegmentCache::getId)
                      .filter(this::isSegmentIdCached)
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
  public SegmentId findHighestUnusedSegmentId(Interval interval, String version)
  {
    final Integer highestPartitionNum = intervalVersionToHighestUnusedPartitionNumber
        .getOrDefault(interval, Map.of())
        .get(version);

    return highestPartitionNum == null
           ? null
           : SegmentId.of(dataSource, interval, version, highestPartitionNum);
  }

  @Override
  public Set<DataSegment> findUsedSegmentsOverlappingAnyOf(List<Interval> intervals)
  {
    return findUsedSegmentsPlusOverlappingAnyOf(intervals)
        .stream()
        .map(DataSegmentPlus::getDataSegment)
        .collect(Collectors.toSet());
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
      @Nullable DateTime maxUpdatedTime
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
        if (addSegment(segmentPlus)) {
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
    for (DataSegment segment : findUsedSegmentsOverlappingAnyOf(List.of(interval))) {
      boolean updated = addUnusedSegmentId(segment.getId(), updateTime);
      if (updated) {
        ++updatedCount;
      }
    }

    return updatedCount;
  }

  @Override
  public int deleteSegments(Set<String> segmentIdsToDelete)
  {
    return removeSegmentsForIds(segmentIdsToDelete);
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
  public int deletePendingSegments(Set<String> segmentIdsToDelete)
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
      Set<String> idsToDelete = findPendingSegmentsMatching(
          record -> taskAllocatorId.equals(record.getTaskAllocatorId())
      ).stream().map(record -> record.getId().toString()).collect(Collectors.toSet());

      return deletePendingSegments(idsToDelete);
    });
  }

  @Override
  public int deletePendingSegmentsCreatedIn(Interval interval)
  {
    return withWriteLock(() -> {
      Set<String> idsToDelete = findPendingSegmentsMatching(
          record -> interval.contains(record.getCreatedDate())
      ).stream().map(record -> record.getId().toString()).collect(Collectors.toSet());

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

  private boolean isSegmentIdCached(String id)
  {
    return idToUsedSegment.containsKey(id)
           || unusedSegmentIdToUpdatedTime.containsKey(id);
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
