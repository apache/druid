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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * In-memory cache for segments and pending segments of a single datasource.
 */
class HeapMemoryDatasourceSegmentCache extends ReadWriteCache
{
  private final String dataSource;

  /**
   * Map from interval to all segments and pending segments that have that exact
   * interval.
   * <p>
   * Keys are sorted by end time to allow easy pruning of all intervals that end
   * strictly before a given search interval, thus benefiting all metadata
   * operations performed on newer intervals.
   */
  private final TreeMap<Interval, SegmentsInInterval> intervalToSegments
      = new TreeMap<>(Comparators.intervalsByEndThenStart());

  HeapMemoryDatasourceSegmentCache(String dataSource)
  {
    super(true);
    this.dataSource = dataSource;
  }

  @Override
  public void stop()
  {
    withWriteLock(() -> {
      intervalToSegments.values().forEach(SegmentsInInterval::clear);
      intervalToSegments.clear();
      super.stop();
    });
  }

  /**
   * @return If the cache has no entries
   */
  boolean isEmpty()
  {
    return withReadLock(intervalToSegments::isEmpty);
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
  boolean shouldRefreshUsedSegment(SegmentId segmentId, @Nullable DateTime persistedUpdateTime)
  {
    return withReadLock(
        () -> readSegmentsFor(segmentId.getInterval())
            .shouldRefreshUsedSegment(segmentId, persistedUpdateTime)
    );
  }

  /**
   * Checks if a record in the cache needs to be updated.
   *
   * @param cachedUpdateTime Updated time of record already present in cache
   * @param newUpdateTime    Updated time of record being considered to replace
   *                         the existing one
   */
  private static boolean shouldUpdateCache(
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
   * Atomically updates segment IDs in the cache based on the segments
   * currently present in the metadata store.
   *
   * @param persistedSegments All segments present in the metadata store.
   * @param syncStartTime     Start time of the current sync
   * @return Summary of updates made to the cache.
   */
  SegmentSyncResult syncSegmentIds(List<SegmentRecord> persistedSegments, DateTime syncStartTime)
  {
    return withWriteLock(() -> {
      // Clear the highest partition numbers for each interval so that
      // they can be updated with the newly polled records
      intervalToSegments.values().forEach(
          interval -> interval.versionToHighestUnusedPartitionNumber.clear()
      );

      final Set<String> usedSegmentIdsToRefresh = new HashSet<>();
      int numUnusedSegmentsUpdated = 0;

      for (SegmentRecord record : persistedSegments) {
        final SegmentId segmentId = record.getSegmentId();
        final SegmentsInInterval intervalSegments = writeSegmentsFor(segmentId.getInterval());

        if (record.isUsed()) {
          // Refresh this used segment if it has been updated in the metadata store
          if (intervalSegments.shouldRefreshUsedSegment(segmentId, record.getLastUpdatedTime())) {
            usedSegmentIdsToRefresh.add(segmentId.toString());
          }
        } else {
          // Add or update the unused segment if needed
          if (intervalSegments.addUnusedSegmentId(segmentId, record.getLastUpdatedTime())) {
            ++numUnusedSegmentsUpdated;
          }
        }
      }

      // Remove unknown segments from cache
      final Set<SegmentId> persistedSegmentIds
          = persistedSegments.stream().map(SegmentRecord::getSegmentId).collect(Collectors.toSet());
      final int numSegmentsRemoved = removeUnpersistedSegments(persistedSegmentIds, syncStartTime);

      return new SegmentSyncResult(numSegmentsRemoved, numUnusedSegmentsUpdated, usedSegmentIdsToRefresh);
    });
  }

  /**
   * Atomically updates pending segments in the cache based on the segments
   * currently present in the metadata store.
   *
   * @param persistedPendingSegments All pending segments present in the metadata store.
   * @param syncStartTime            Start time of the current sync
   * @return Summary of updates made to the cache.
   */
  SegmentSyncResult syncPendingSegments(
      List<PendingSegmentRecord> persistedPendingSegments,
      DateTime syncStartTime
  )
  {
    return withWriteLock(() -> {
      int numSegmentsUpdated = 0;
      for (PendingSegmentRecord record : persistedPendingSegments) {
        if (insertPendingSegment(record, false)) {
          ++numSegmentsUpdated;
        }
      }

      final Set<String> persistedSegmentIds
          = persistedPendingSegments.stream().map(s -> s.getId().toString()).collect(Collectors.toSet());
      final int numSegmentsRemoved = removeUnpersistedPendingSegments(persistedSegmentIds, syncStartTime);
      return new SegmentSyncResult(numSegmentsRemoved, numSegmentsUpdated, Set.of());
    });
  }

  /**
   * Removes all pending segments which are present in the cache but not present
   * in the metadata store.
   */
  private int removeUnpersistedPendingSegments(Set<String> persistedPendingSegmentIds, DateTime pollStartTime)
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
  private int removeUnpersistedSegments(Set<SegmentId> persistedSegmentIds, DateTime syncStartTime)
  {
    return withWriteLock(() -> {
      final Set<SegmentId> unpersistedSegmentIds = new HashSet<>();

      for (SegmentsInInterval segments : intervalToSegments.values()) {
        segments.unusedSegmentIdToUpdatedTime.entrySet().stream().filter(
            entry -> !persistedSegmentIds.contains(entry.getKey())
                     && shouldUpdateCache(entry.getValue(), syncStartTime)
        ).map(Map.Entry::getKey).forEach(unpersistedSegmentIds::add);

        segments.idToUsedSegment.entrySet().stream().filter(
            entry -> !persistedSegmentIds.contains(entry.getKey())
                     && shouldUpdateCache(entry.getValue().getUsedStatusLastUpdatedDate(), syncStartTime)
        ).map(Map.Entry::getKey).forEach(unpersistedSegmentIds::add);
      }

      return removeSegmentsForIds(unpersistedSegmentIds);
    });
  }

  /**
   * Removes the segments for the given IDs (used or unused) from the cache.
   *
   * @return Number of used and unused segments removed
   */
  int removeSegmentsForIds(Set<SegmentId> segmentIds)
  {
    return withWriteLock(() -> {
      int removedCount = 0;
      for (SegmentId segmentId : segmentIds) {
        if (segmentId == null) {
          continue;
        }

        final SegmentsInInterval segmentsInInterval = writeSegmentsFor(segmentId.getInterval());
        final DataSegmentPlus segment = segmentsInInterval.idToUsedSegment.remove(segmentId);
        if (segment != null) {
          ++removedCount;
        } else if (segmentsInInterval.unusedSegmentIdToUpdatedTime.containsKey(segmentId)) {
          segmentsInInterval.unusedSegmentIdToUpdatedTime.remove(segmentId);
          ++removedCount;
        }
      }

      return removedCount;
    });
  }

  /**
   * Indicates to the cache that it has now been synced with the metadata store.
   * Removes empty intervals from the cache.
   */
  void markCacheSynced()
  {
    withWriteLock(() -> {
      final Set<Interval> emptyIntervals =
          intervalToSegments.entrySet()
                            .stream()
                            .filter(entry -> entry.getValue().isEmpty())
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toSet());
      emptyIntervals.forEach(intervalToSegments::remove);
    });
  }

  /**
   * Must be accessed within a {@link #withReadLock} method.
   */
  private SegmentsInInterval readSegmentsFor(Interval interval)
  {
    return intervalToSegments.getOrDefault(interval, SegmentsInInterval.EMPTY);
  }

  /**
   * Must be accessed within a {@link #withWriteLock} method.
   */
  private SegmentsInInterval writeSegmentsFor(Interval interval)
  {
    return intervalToSegments.computeIfAbsent(interval, i -> new SegmentsInInterval());
  }

  // CACHE READ METHODS

  @Override
  public Set<String> findExistingSegmentIds(Set<DataSegment> segments)
  {
    return withReadLock(
        () -> segments.stream()
                      .map(DataSegment::getId)
                      .filter(id -> readSegmentsFor(id.getInterval()).isSegmentIdCached(id))
                      .map(SegmentId::toString)
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
    final Integer highestPartitionNum =
        readSegmentsFor(interval)
            .versionToHighestUnusedPartitionNumber
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
  public List<DataSegment> findUsedSegments(Set<SegmentId> segmentIds)
  {
    return withReadLock(
        () -> segmentIds.stream()
                        .map(id -> readSegmentsFor(id.getInterval()).idToUsedSegment.get(id))
                        .filter(Objects::nonNull)
                        .map(DataSegmentPlus::getDataSegment)
                        .collect(Collectors.toList())
    );
  }

  @Override
  public Set<DataSegmentPlus> findUsedSegmentsPlusOverlappingAnyOf(List<Interval> intervals)
  {
    if (intervals.isEmpty()) {
      return withReadLock(
          () -> intervalToSegments.values()
                                  .stream()
                                  .flatMap(segments -> segments.idToUsedSegment.values().stream())
                                  .collect(Collectors.toSet())
      );
    } else {
      return withReadLock(
          () -> intervals.stream()
                         .flatMap(this::findOverlappingIntervals)
                         .flatMap(segments -> segments.idToUsedSegment.values().stream())
                         .collect(Collectors.toSet())
      );
    }
  }

  @Override
  public DataSegment findSegment(SegmentId segmentId)
  {
    throw DruidException.defensive("Unsupported: Unused segments are not cached");
  }

  @Override
  @Nullable
  public DataSegment findUsedSegment(SegmentId segmentId)
  {
    return withReadLock(() -> {
      final DataSegmentPlus segmentPlus = readSegmentsFor(segmentId.getInterval())
          .idToUsedSegment.get(segmentId);
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
        () -> readSegmentsFor(interval)
            .idToPendingSegment
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
        () -> findOverlappingIntervals(interval)
            .flatMap(segments -> segments.idToPendingSegment.values().stream())
            .collect(Collectors.toList())
    );
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegmentsWithExactInterval(Interval interval)
  {
    return withReadLock(
        () -> List.copyOf(readSegmentsFor(interval).idToPendingSegment.values())
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
        final Interval interval = segmentPlus.getDataSegment().getInterval();
        if (writeSegmentsFor(interval).addSegment(segmentPlus)) {
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
  public boolean markSegmentAsUnused(SegmentId segmentId, DateTime updateTime)
  {
    return writeSegmentsFor(segmentId.getInterval()).addUnusedSegmentId(segmentId, updateTime);
  }

  @Override
  public int markSegmentsAsUnused(Set<SegmentId> segmentIds, DateTime updateTime)
  {
    return withWriteLock(() -> {
      int updatedCount = 0;
      for (SegmentId segmentId : segmentIds) {
        final Interval interval = segmentId.getInterval();
        if (writeSegmentsFor(interval).addUnusedSegmentId(segmentId, updateTime)) {
          ++updatedCount;
        }
      }
      return updatedCount;
    });
  }

  @Override
  public int markSegmentsWithinIntervalAsUnused(
      Interval interval,
      @Nullable List<String> versions,
      DateTime updateTime
  )
  {
    final Set<String> eligibleVersions = versions == null ? null : Set.copyOf(versions);

    return withWriteLock(() -> {
      int updatedCount = 0;
      for (DataSegmentPlus segmentPlus : findUsedSegmentsPlusOverlappingAnyOf(List.of(interval))) {
        // Update segments with eligible versions or all versions (if eligibleVersions is null)
        final DataSegment segment = segmentPlus.getDataSegment();
        final boolean isEligibleVersion = eligibleVersions == null
                                          || eligibleVersions.contains(segment.getVersion());
        if (isEligibleVersion
            && writeSegmentsFor(segment.getInterval()).addUnusedSegmentId(segment.getId(), updateTime)) {
          ++updatedCount;
        }
      }

      return updatedCount;
    });
  }

  @Override
  public int markAllSegmentsAsUnused(DateTime updateTime)
  {
    return withWriteLock(() -> {
      int updatedCount = 0;
      for (DataSegmentPlus segmentPlus : findUsedSegmentsPlusOverlappingAnyOf(List.of())) {
        final DataSegment segment = segmentPlus.getDataSegment();
        if (writeSegmentsFor(segment.getInterval())
            .addUnusedSegmentId(segment.getId(), updateTime)) {
          ++updatedCount;
        }
      }

      return updatedCount;
    });
  }

  @Override
  public int deleteSegments(Set<SegmentId> segmentIdsToDelete)
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
        PendingSegmentRecord oldValue = writeSegmentsFor(segmentId.getInterval())
            .idToPendingSegment.putIfAbsent(segmentId.toString(), record);
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
      int numPendingSegments =
          intervalToSegments.values().stream()
                            .mapToInt(interval -> interval.idToPendingSegment.size())
                            .sum();
      intervalToSegments.values().forEach(interval -> interval.idToPendingSegment.clear());
      return numPendingSegments;
    });
  }

  @Override
  public int deletePendingSegments(Set<String> segmentIdsToDelete)
  {
    final Set<String> remainingIdsToDelete = new HashSet<>(segmentIdsToDelete);

    withWriteLock(() -> intervalToSegments.forEach(
        (interval, segments) -> {
          final Set<String> deletedIds =
              remainingIdsToDelete.stream()
                                  .map(segments.idToPendingSegment::remove)
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
   * Iterates over all the pending segments to find ones that match the given predicate.
   */
  private List<PendingSegmentRecord> findPendingSegmentsMatching(Predicate<PendingSegmentRecord> predicate)
  {
    return withReadLock(
        () -> intervalToSegments.values()
                                .stream()
                                .flatMap(interval -> interval.idToPendingSegment.values().stream())
                                .filter(predicate)
                                .collect(Collectors.toList())
    );
  }

  private Stream<SegmentsInInterval> findOverlappingIntervals(Interval searchInterval)
  {
    // If searchInterval is (-inf, +inf), everything overlaps with it
    if (Intervals.isEternity(searchInterval)) {
      return withReadLock(() -> intervalToSegments.values().stream());
    }

    // If searchInterval is (-inf, end), overlapStart = (-inf, -inf)
    // If searchInterval is (start, +inf), overlapStart = (-inf, start)
    // Filter out intervals which end strictly before the start of the searchInterval
    final Interval overlapStart = Intervals.ETERNITY.withEnd(searchInterval.getStart());

    return withReadLock(
        () -> intervalToSegments.tailMap(overlapStart)
                                .entrySet()
                                .stream()
                                .filter(entry -> entry.getKey().overlaps(searchInterval))
                                .map(Map.Entry::getValue)
    );
  }

  /**
   * Contains segments exactly aligned with an interval.
   */
  private static class SegmentsInInterval
  {
    static final SegmentsInInterval EMPTY = new SegmentsInInterval();

    /**
     * Map from segment ID to used segment.
     */
    final Map<SegmentId, DataSegmentPlus> idToUsedSegment = new HashMap<>();

    /**
     * Map from segment ID to pending segment record.
     */
    final Map<String, PendingSegmentRecord> idToPendingSegment = new HashMap<>();

    /**
     * Map from version to the highest partition number of an unused segment with
     * that version.
     */
    final Map<String, Integer> versionToHighestUnusedPartitionNumber = new HashMap<>();

    /**
     * Map from segment ID to updated time for unused segments only.
     */
    final Map<SegmentId, DateTime> unusedSegmentIdToUpdatedTime = new HashMap<>();

    void clear()
    {
      idToPendingSegment.clear();
      idToUsedSegment.clear();
      versionToHighestUnusedPartitionNumber.clear();
    }

    boolean isEmpty()
    {
      return idToPendingSegment.isEmpty()
             && idToUsedSegment.isEmpty()
             && versionToHighestUnusedPartitionNumber.isEmpty();
    }

    private boolean isSegmentIdCached(SegmentId id)
    {
      return idToUsedSegment.containsKey(id)
             || unusedSegmentIdToUpdatedTime.containsKey(id);
    }

    private void updateMaxUnusedId(SegmentId segmentId)
    {
      versionToHighestUnusedPartitionNumber
          .merge(segmentId.getVersion(), segmentId.getPartitionNum(), Math::max);
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
      final SegmentId segmentId = segment.getId();

      if (!shouldRefreshUsedSegment(segmentId, segmentPlus.getUsedStatusLastUpdatedDate())) {
        return false;
      }

      idToUsedSegment.put(segmentId, segmentPlus);
      unusedSegmentIdToUpdatedTime.remove(segment.getId());
      return true;
    }

    /**
     * Adds or updates an unused segment in the cache.
     *
     * @param updatedTime Last updated time of this segment as persisted in the
     *                    metadata store. This value can be null for segments
     *                    persisted to the metadata store before the column
     *                    used_status_last_updated was added to the segments table.
     */
    private boolean addUnusedSegmentId(SegmentId segmentId, @Nullable DateTime updatedTime)
    {
      idToUsedSegment.remove(segmentId);

      if (!unusedSegmentIdToUpdatedTime.containsKey(segmentId)
          || shouldUpdateCache(unusedSegmentIdToUpdatedTime.get(segmentId), updatedTime)) {
        unusedSegmentIdToUpdatedTime.put(segmentId, updatedTime);
        updateMaxUnusedId(segmentId);
        return true;
      } else {
        return false;
      }
    }

    private boolean shouldRefreshUnusedSegment(SegmentId segmentId, DateTime newUpdateTime)
    {
      return !unusedSegmentIdToUpdatedTime.containsKey(segmentId)
             || shouldUpdateCache(unusedSegmentIdToUpdatedTime.get(segmentId), newUpdateTime);
    }

    private boolean shouldRefreshUsedSegment(SegmentId segmentId, DateTime newUpdateTime)
    {
      final DataSegmentPlus usedSegment = idToUsedSegment.get(segmentId);

      if (usedSegment == null) {
        // Do not refresh the segment if it has recently been marked as unused in the cache
        return shouldRefreshUnusedSegment(segmentId, newUpdateTime);
      } else {
        // Refresh the used segment if the entry in the cache is stale
        return shouldUpdateCache(usedSegment.getUsedStatusLastUpdatedDate(), newUpdateTime);
      }
    }
  }
}
