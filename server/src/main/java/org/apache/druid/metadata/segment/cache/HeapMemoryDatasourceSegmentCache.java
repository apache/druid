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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * In-memory cache for segments and pending segments of a single datasource.
 */
class HeapMemoryDatasourceSegmentCache extends ReadWriteCache implements AutoCloseable
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

  /**
   * Number of transactions currently using this cache. This field is accessed
   * without acquiring an explicit lock on this cache since the operations are
   * always performed within a ConcurrentHashMap.compute() which is atomic.
   */
  private final AtomicInteger references = new AtomicInteger(0);

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
   * Acquires a reference to this cache, which must be closed in {@link #close()}
   * after the transaction holding this reference has completed.
   */
  void acquireReference()
  {
    references.incrementAndGet();
  }

  @Override
  public void close()
  {
    references.decrementAndGet();
  }

  /**
   * @return true if this cache is currently being used by a transaction and
   * the number of {@link #references} is non-zero.
   */
  boolean isBeingUsedByTransaction()
  {
    return references.get() > 0;
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
      final Set<SegmentId> usedSegmentIdsToRefresh = new HashSet<>();

      for (SegmentRecord record : persistedSegments) {
        final SegmentId segmentId = record.getSegmentId();
        final SegmentsInInterval intervalSegments = writeSegmentsFor(segmentId.getInterval());

        if (record.isUsed()) {
          // Refresh this used segment if it has been updated in the metadata store
          if (intervalSegments.shouldRefreshSegment(segmentId, record.getLastUpdatedTime())) {
            usedSegmentIdsToRefresh.add(segmentId);
          }
        } else {
          // Ignore unused segments
        }
      }

      // Remove unknown segments from cache
      final Set<SegmentId> persistedSegmentIds
          = persistedSegments.stream().map(SegmentRecord::getSegmentId).collect(Collectors.toSet());
      final Set<SegmentId> segmentIdsRemoved = removeUnpersistedSegments(persistedSegmentIds, syncStartTime);

      return new SegmentSyncResult(segmentIdsRemoved.size(), 0, usedSegmentIdsToRefresh, segmentIdsRemoved);
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
      return new SegmentSyncResult(numSegmentsRemoved, numSegmentsUpdated, Set.of(), Set.of());
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
   * @return Set of unpersisted segment IDs removed from the cache.
   */
  private Set<SegmentId> removeUnpersistedSegments(Set<SegmentId> persistedSegmentIds, DateTime syncStartTime)
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

      deleteSegments(unpersistedSegmentIds);

      return unpersistedSegmentIds;
    });
  }

  /**
   * Indicates to the cache that it has now been synced with the metadata store.
   * Removes empty intervals from the cache and returns the summary of the current
   * contents of the cache.
   */
  CacheStats markCacheSynced()
  {
    return withWriteLock(() -> {
      // Remove empty intervals
      final Set<Interval> emptyIntervals =
          intervalToSegments.entrySet()
                            .stream()
                            .filter(entry -> entry.getValue().isEmpty())
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toSet());
      emptyIntervals.forEach(intervalToSegments::remove);

      return getCacheStats();
    });
  }

  /**
   * Returns a summary of the current contents of the cache.
   */
  private CacheStats getCacheStats()
  {
    return withWriteLock(() -> {
      int numUsedSegments = 0;
      int numUnusedSegments = 0;
      int numPendingSegments = 0;
      int numIntervals = 0;
      for (SegmentsInInterval segments : intervalToSegments.values()) {
        ++numIntervals;
        numUsedSegments += segments.idToUsedSegment.size();
        numUnusedSegments += segments.unusedSegmentIdToUpdatedTime.size();
        numPendingSegments += segments.idToPendingSegment.size();
      }

      return new CacheStats(numIntervals, numUsedSegments, numUnusedSegments, numPendingSegments);
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
  public Set<String> findExistingSegmentIds(Set<SegmentId> segments)
  {
    throw DruidException.defensive("Unsupported: Unused segments are not cached");
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
    throw DruidException.defensive("Unsupported: Unused segments are not cached");
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
  public List<DataSegmentPlus> findUsedSegments(Set<SegmentId> segmentIds)
  {
    return withReadLock(
        () -> segmentIds.stream()
                        .map(id -> readSegmentsFor(id.getInterval()).idToUsedSegment.get(id))
                        .filter(Objects::nonNull)
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
  public List<DataSegmentPlus> findSegments(Set<SegmentId> segmentIds)
  {
    throw DruidException.defensive("Unsupported: Unused segments are not cached");
  }

  @Override
  public List<DataSegmentPlus> findSegmentsWithSchema(Set<SegmentId> segmentIds)
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
    return writeSegmentsFor(segmentId.getInterval()).markSegmentAsUnused(segmentId, updateTime);
  }

  @Override
  public int markSegmentsAsUnused(Set<SegmentId> segmentIds, DateTime updateTime)
  {
    return withWriteLock(() -> {
      int updatedCount = 0;
      for (SegmentId segmentId : segmentIds) {
        final Interval interval = segmentId.getInterval();
        if (writeSegmentsFor(interval).markSegmentAsUnused(segmentId, updateTime)) {
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
            && writeSegmentsFor(segment.getInterval()).markSegmentAsUnused(segment.getId(), updateTime)) {
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
            .markSegmentAsUnused(segment.getId(), updateTime)) {
          ++updatedCount;
        }
      }

      return updatedCount;
    });
  }

  @Override
  public int deleteSegments(Set<SegmentId> segmentIdsToDelete)
  {
    return withWriteLock(() -> {
      int deletedCount = 0;
      for (SegmentId segmentId : segmentIdsToDelete) {
        if (segmentId != null
            && writeSegmentsFor(segmentId.getInterval()).removeSegment(segmentId)) {
          ++deletedCount;
        }
      }
      return deletedCount;
    });
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
     * Map from segment ID to updated time for segments that have been recently
     * marked as unused. This map is needed to handle race conditions with sync.
     * For example, if a segment has just been marked as unused, the latest poll
     * from metadata store might still show that segment as used. Checking with
     * this map ensures that such a segment is not added back to the cache.
     */
    final Map<SegmentId, DateTime> unusedSegmentIdToUpdatedTime = new HashMap<>();

    void clear()
    {
      idToPendingSegment.clear();
      idToUsedSegment.clear();
      unusedSegmentIdToUpdatedTime.clear();
    }

    boolean isEmpty()
    {
      return idToPendingSegment.isEmpty()
             && idToUsedSegment.isEmpty()
             && unusedSegmentIdToUpdatedTime.isEmpty();
    }

    /**
     * Removes the given segment from the cache.
     *
     * @return true if the segment was removed, false otherwise
     */
    private boolean removeSegment(SegmentId segmentId)
    {
      if (idToUsedSegment.containsKey(segmentId)) {
        idToUsedSegment.remove(segmentId);
        return true;
      } else if (unusedSegmentIdToUpdatedTime.containsKey(segmentId)) {
        unusedSegmentIdToUpdatedTime.remove(segmentId);
        return true;
      } else {
        return false;
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
      final SegmentId segmentId = segmentPlus.getDataSegment().getId();
      if (!shouldRefreshSegment(segmentId, segmentPlus.getUsedStatusLastUpdatedDate())) {
        return false;
      } else if (Boolean.TRUE.equals(segmentPlus.getUsed())) {
        idToUsedSegment.put(segmentId, segmentPlus);
        unusedSegmentIdToUpdatedTime.remove(segmentId);
        return true;
      } else {
        return markSegmentAsUnused(segmentId, segmentPlus.getUsedStatusLastUpdatedDate());
      }
    }

    /**
     * Marks the given segment as unused segment in the cache.
     *
     * @param updatedTime Last updated time of this segment as persisted in the
     *                    metadata store. This value can be null for segments
     *                    persisted to the metadata store before the column
     *                    used_status_last_updated was added to the segments table.
     */
    private boolean markSegmentAsUnused(SegmentId segmentId, @Nullable DateTime updatedTime)
    {
      if (shouldRefreshSegment(segmentId, updatedTime)) {
        idToUsedSegment.remove(segmentId);
        unusedSegmentIdToUpdatedTime.put(segmentId, updatedTime);
        return true;
      } else {
        return false;
      }
    }

    private boolean shouldRefreshSegment(SegmentId segmentId, DateTime newUpdateTime)
    {
      if (unusedSegmentIdToUpdatedTime.containsKey(segmentId)) {
        return shouldUpdateCache(unusedSegmentIdToUpdatedTime.get(segmentId), newUpdateTime);
      } else {
        final DataSegmentPlus usedSegment = idToUsedSegment.get(segmentId);
        return usedSegment == null
            || shouldUpdateCache(usedSegment.getUsedStatusLastUpdatedDate(), newUpdateTime);
      }
    }
  }
}
