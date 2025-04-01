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

package org.apache.druid.metadata.segment;

import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.error.DruidException;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.metadata.segment.cache.DatasourceSegmentCache;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link SegmentMetadataTransaction} that reads only from the cache and sends
 * writes to the metadata store. If the transaction succeeds, all the writes
 * made to the metadata store are also committed to the cache in {@link #close()}.
 * The cache is not updated right away in case the transaction needs to be
 * rolled back.
 * <p>
 * This implies that a transaction CANNOT read back what it has written until it
 * has been committed. e.g. If a transaction inserts a segment and then tries to
 * read back that same segment, it would get a null result since the segment has
 * not been added to the cache yet. This restriction has been imposed to simplify
 * rolling back of a failed transaction.
 */
class CachedSegmentMetadataTransaction implements SegmentMetadataTransaction
{
  private final SegmentMetadataTransaction delegate;
  private final DatasourceSegmentCache metadataCache;
  private final DruidLeaderSelector leaderSelector;

  private final int startTerm;
  private final boolean readFromCache;

  private boolean isRollingBack = false;
  private boolean isClosed = false;

  private final List<Consumer<DatasourceSegmentMetadataWriter>> pendingCacheWrites = new ArrayList<>();

  /**
   * Creates a transaction that may read or write from the cache or delegate to
   * the metadata store if necessary. Writes are always done to the cache unless
   * the update does not affect the cache contents.
   *
   * @param leaderSelector Used to ensure that leadership does not change during
   *                       the course of the transaction.
   * @param readFromCache  If true, reads are done from cache. Otherwise,
   *                       reads are done directly from the metadata store.
   */
  CachedSegmentMetadataTransaction(
      SegmentMetadataTransaction delegate,
      DatasourceSegmentCache metadataCache,
      DruidLeaderSelector leaderSelector,
      boolean readFromCache
  )
  {
    this.delegate = delegate;
    this.metadataCache = metadataCache;
    this.leaderSelector = leaderSelector;
    this.readFromCache = readFromCache;

    if (leaderSelector.isLeader()) {
      this.startTerm = leaderSelector.localTerm();
    } else {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.SERVICE_UNAVAILABLE)
                          .build("This API is currently not available. Please try again after some time.");
    }
  }

  private void verifyStillLeaderWithSameTerm()
  {
    if (!isLeaderWithSameTerm()) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.SERVICE_UNAVAILABLE)
                          .build("This API is currently not available. Please try again after some time.");
    }
  }

  private boolean isLeaderWithSameTerm()
  {
    return leaderSelector.isLeader() && startTerm == leaderSelector.localTerm();
  }

  @Override
  public Handle getHandle()
  {
    return delegate.getHandle();
  }

  @Override
  public void setRollbackOnly()
  {
    isRollingBack = true;
    delegate.setRollbackOnly();
  }

  @Override
  public void close()
  {
    if (isClosed) {
      return;
    } else if (isRollingBack) {
      isClosed = true;
      return;
    }

    // Commit the changes to the cache
    try {
      pendingCacheWrites.forEach(action -> {
        if (isLeaderWithSameTerm()) {
          action.accept(metadataCache);
        } else {
          // Leadership has been lost, do not update cache but allow changes
          // to be committed to metadata store to retain existing code behaviour
        }
      });
    }
    finally {
      delegate.close();
      isClosed = true;
    }
  }

  // READ METHODS

  @Override
  public Set<String> findExistingSegmentIds(Set<SegmentId> segmentIds)
  {
    final Set<SegmentId> remainingIdsToFind = new HashSet<>(segmentIds);

    // Try to find IDs in cache
    final Set<String> foundIds = new HashSet<>();
    if (readFromCache) {
      foundIds.addAll(
          metadataCache.findUsedSegments(remainingIdsToFind)
                       .stream()
                       .map(segment -> segment.getDataSegment().getId().toString())
                       .collect(Collectors.toCollection(HashSet::new))
      );
      remainingIdsToFind.removeIf(id -> foundIds.contains(id.toString()));
    }

    // Find remaining IDs in metadata store
    if (!remainingIdsToFind.isEmpty()) {
      foundIds.addAll(
          delegate.findExistingSegmentIds(remainingIdsToFind)
      );
    }

    return Set.copyOf(foundIds);
  }

  @Override
  public Set<SegmentId> findUsedSegmentIdsOverlapping(Interval interval)
  {
    return performReadAction(reader -> reader.findUsedSegmentIdsOverlapping(interval));
  }

  @Override
  public SegmentId findHighestUnusedSegmentId(Interval interval, String version)
  {
    // Read from metadata store since unused segments are not cached
    return delegate.findHighestUnusedSegmentId(interval, version);
  }

  @Override
  public List<DataSegmentPlus> findSegments(Set<SegmentId> segmentIds)
  {
    final Set<SegmentId> remainingIdsToFind = new HashSet<>(segmentIds);

    // Try to find segments in cache
    final List<DataSegmentPlus> foundSegments = new ArrayList<>();
    if (readFromCache) {
      foundSegments.addAll(
          metadataCache.findUsedSegments(remainingIdsToFind)
      );
      foundSegments.forEach(segment -> remainingIdsToFind.remove(segment.getDataSegment().getId()));
    }

    // Find remaining segments in metadata store
    if (!remainingIdsToFind.isEmpty()) {
      foundSegments.addAll(
          delegate.findSegments(remainingIdsToFind)
      );
    }

    return List.copyOf(foundSegments);
  }

  @Override
  public List<DataSegmentPlus> findSegmentsWithSchema(Set<SegmentId> segmentIds)
  {
    // Read from metadata store since unused segment payloads and schema info is not cached
    return delegate.findSegmentsWithSchema(segmentIds);
  }

  @Override
  public Set<DataSegment> findUsedSegmentsOverlappingAnyOf(List<Interval> intervals)
  {
    return performReadAction(reader -> reader.findUsedSegmentsOverlappingAnyOf(intervals));
  }

  @Override
  public List<DataSegmentPlus> findUsedSegments(Set<SegmentId> segmentIds)
  {
    return performReadAction(reader -> reader.findUsedSegments(segmentIds));
  }

  @Override
  public Set<DataSegmentPlus> findUsedSegmentsPlusOverlappingAnyOf(List<Interval> intervals)
  {
    return performReadAction(reader -> reader.findUsedSegmentsPlusOverlappingAnyOf(intervals));
  }

  @Override
  public List<DataSegment> findUnusedSegments(
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUpdatedTime
  )
  {
    // Read from metadata store since unused segment payloads are not cached
    return delegate.findUnusedSegments(interval, versions, limit, maxUpdatedTime);
  }

  @Override
  public DataSegment findSegment(SegmentId segmentId)
  {
    // Try to find used segment in cache
    final DataSegment usedSegment = metadataCache.findUsedSegment(segmentId);
    if (usedSegment == null) {
      // Read from metadata store since unused segment payloads are not cached
      return delegate.findSegment(segmentId);
    } else {
      return usedSegment;
    }
  }

  @Override
  public DataSegment findUsedSegment(SegmentId segmentId)
  {
    return performReadAction(reader -> reader.findUsedSegment(segmentId));
  }

  @Override
  public List<SegmentIdWithShardSpec> findPendingSegmentIds(
      String sequenceName,
      String sequencePreviousId
  )
  {
    return performReadAction(reader -> reader.findPendingSegmentIds(sequenceName, sequencePreviousId));
  }

  @Override
  public List<SegmentIdWithShardSpec> findPendingSegmentIdsWithExactInterval(
      String sequenceName,
      Interval interval
  )
  {
    return performReadAction(reader -> reader.findPendingSegmentIdsWithExactInterval(sequenceName, interval));
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegmentsOverlapping(Interval interval)
  {
    return performReadAction(reader -> reader.findPendingSegmentsOverlapping(interval));
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegmentsWithExactInterval(Interval interval)
  {
    return performReadAction(reader -> reader.findPendingSegmentsWithExactInterval(interval));
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegments(String taskAllocatorId)
  {
    return performReadAction(reader -> reader.findPendingSegments(taskAllocatorId));
  }

  // WRITE METHODS

  @Override
  public int insertSegments(Set<DataSegmentPlus> segments)
  {
    return performWriteAction(writer -> writer.insertSegments(segments));
  }

  @Override
  public int insertSegmentsWithMetadata(Set<DataSegmentPlus> segments)
  {
    return performWriteAction(writer -> writer.insertSegmentsWithMetadata(segments));
  }

  @Override
  public boolean markSegmentAsUnused(SegmentId segmentId, DateTime updateTime)
  {
    return performWriteAction(
        writer -> writer.markSegmentAsUnused(segmentId, updateTime)
    );
  }

  @Override
  public int markSegmentsAsUnused(Set<SegmentId> segmentIds, DateTime updateTime)
  {
    return performWriteAction(
        writer -> writer.markSegmentsAsUnused(segmentIds, updateTime)
    );
  }

  @Override
  public int markAllSegmentsAsUnused(DateTime updateTime)
  {
    return performWriteAction(
        writer -> writer.markAllSegmentsAsUnused(updateTime)
    );
  }

  @Override
  public int markSegmentsWithinIntervalAsUnused(
      Interval interval,
      @Nullable List<String> versions,
      DateTime updateTime
  )
  {
    return performWriteAction(
        writer -> writer.markSegmentsWithinIntervalAsUnused(interval, versions, updateTime)
    );
  }

  @Override
  public int deleteSegments(Set<SegmentId> segmentsIdsToDelete)
  {
    return performWriteAction(writer -> writer.deleteSegments(segmentsIdsToDelete));
  }

  @Override
  public boolean updateSegmentPayload(DataSegment segment)
  {
    // Write only to metadata store since unused segment payloads are not cached
    return delegate.updateSegmentPayload(segment);
  }

  @Override
  public boolean insertPendingSegment(
      PendingSegmentRecord pendingSegment,
      boolean skipSegmentLineageCheck
  )
  {
    return performWriteAction(
        writer -> writer.insertPendingSegment(pendingSegment, skipSegmentLineageCheck)
    );
  }

  @Override
  public int insertPendingSegments(
      List<PendingSegmentRecord> pendingSegments,
      boolean skipSegmentLineageCheck
  )
  {
    return performWriteAction(
        writer -> writer.insertPendingSegments(pendingSegments, skipSegmentLineageCheck)
    );
  }

  @Override
  public int deleteAllPendingSegments()
  {
    return performWriteAction(
        DatasourceSegmentMetadataWriter::deleteAllPendingSegments
    );
  }

  @Override
  public int deletePendingSegments(Set<String> segmentIdsToDelete)
  {
    return performWriteAction(
        writer -> writer.deletePendingSegments(segmentIdsToDelete)
    );
  }

  @Override
  public int deletePendingSegments(String taskAllocatorId)
  {
    return performWriteAction(
        writer -> writer.deletePendingSegments(taskAllocatorId)
    );
  }

  @Override
  public int deletePendingSegmentsCreatedIn(Interval interval)
  {
    return performWriteAction(
        writer -> writer.deletePendingSegmentsCreatedIn(interval)
    );
  }

  /**
   * Performs a read from cache only if {@link #readFromCache} is true.
   * Otherwise, reads directly from the metadata store.
   */
  private <T> T performReadAction(Function<DatasourceSegmentMetadataReader, T> action)
  {
    if (readFromCache) {
      return action.apply(metadataCache);
    } else {
      return action.apply(delegate);
    }
  }

  private <T> T performWriteAction(Function<DatasourceSegmentMetadataWriter, T> action)
  {
    if (isClosed) {
      throw DruidException.defensive(
          "Transaction has already been committed. No more writes can be performed."
      );
    }

    verifyStillLeaderWithSameTerm();
    final T result = action.apply(delegate);

    // Assume that the metadata write operation succeeded
    // Do not update the cache just yet, add to the list of pending writes
    pendingCacheWrites.add(writer -> {
      T ignored = action.apply(writer);
    });

    return result;
  }
}
