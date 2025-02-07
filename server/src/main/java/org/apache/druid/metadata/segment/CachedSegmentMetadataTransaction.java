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
import org.apache.druid.error.InternalServerError;
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A {@link SegmentMetadataTransaction} that reads only from the cache and sends
 * writes to the metadata store. If the transaction succeeds, all the writes
 * made to the metadata store are also committed to the cache in {@link #close()}.
 * The cache is not updated right away in case the transaction needs to be
 * rolled back. This is okay since we assume that a transaction does not read
 * what it writes.
 */
class CachedSegmentMetadataTransaction implements SegmentMetadataTransaction
{
  private final SegmentMetadataTransaction delegate;
  private final DatasourceSegmentCache metadataCache;
  private final DruidLeaderSelector leaderSelector;

  private final int startTerm;

  private final AtomicBoolean isRollingBack = new AtomicBoolean(false);
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  private final List<Consumer<DatasourceSegmentMetadataWriter>> pendingCacheWrites = new ArrayList<>();

  CachedSegmentMetadataTransaction(
      SegmentMetadataTransaction delegate,
      DatasourceSegmentCache metadataCache,
      DruidLeaderSelector leaderSelector
  )
  {
    this.delegate = delegate;
    this.metadataCache = metadataCache;
    this.leaderSelector = leaderSelector;

    if (leaderSelector.isLeader()) {
      this.startTerm = leaderSelector.localTerm();
    } else {
      throw InternalServerError.exception("Not leader anymore. Cannot start transaction.");
    }
  }

  private void verifyStillLeaderWithSameTerm()
  {
    if (!isLeaderWithSameTerm()) {
      throw InternalServerError.exception("Not leader anymore. Failing transaction.");
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
    isRollingBack.set(true);
    delegate.setRollbackOnly();
  }

  @Override
  public void close()
  {
    if (isClosed.get()) {
      return;
    } else if (isRollingBack.get()) {
      isClosed.set(true);
      return;
    }

    // Commit the changes to the cache
    try {
      pendingCacheWrites.forEach(action -> {
        if (isLeaderWithSameTerm()) {
          action.accept(metadataCache);
        } else {
          // Leadership has been lost, cache would have been stopped and invalidated
        }
      });
    }
    finally {
      delegate.close();
      isClosed.set(true);
    }
  }

  // READ METHODS

  @Override
  public Set<String> findExistingSegmentIds(Set<DataSegment> segments)
  {
    return metadataCache.findExistingSegmentIds(segments);
  }

  @Override
  public Set<SegmentId> findUsedSegmentIdsOverlapping(Interval interval)
  {
    return metadataCache.findUsedSegmentIdsOverlapping(interval);
  }

  @Override
  public SegmentId findHighestUnusedSegmentId(Interval interval, String version)
  {
    return metadataCache.findHighestUnusedSegmentId(interval, version);
  }

  @Override
  public List<DataSegmentPlus> findSegments(Set<String> segmentIds)
  {
    // Read from metadata store since unused segment payloads are not cached
    return delegate.findSegments(segmentIds);
  }

  @Override
  public List<DataSegmentPlus> findSegmentsWithSchema(Set<String> segmentIds)
  {
    // Read from metadata store since unused segment payloads are not cached
    return delegate.findSegmentsWithSchema(segmentIds);
  }

  @Override
  public Set<DataSegment> findUsedSegmentsOverlappingAnyOf(List<Interval> intervals)
  {
    return metadataCache.findUsedSegmentsOverlappingAnyOf(intervals);
  }

  @Override
  public List<DataSegment> findUsedSegments(Set<SegmentId> segmentIds)
  {
    return metadataCache.findUsedSegments(segmentIds);
  }

  @Override
  public Set<DataSegmentPlus> findUsedSegmentsPlusOverlappingAnyOf(List<Interval> intervals)
  {
    return metadataCache.findUsedSegmentsPlusOverlappingAnyOf(intervals);
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
    // Read from metadata store since unused segment payloads are not cached
    return delegate.findSegment(segmentId);
  }

  @Override
  public DataSegment findUsedSegment(SegmentId segmentId)
  {
    return metadataCache.findUsedSegment(segmentId);
  }

  @Override
  public List<SegmentIdWithShardSpec> findPendingSegmentIds(
      String sequenceName,
      String sequencePreviousId
  )
  {
    return metadataCache.findPendingSegmentIds(sequenceName, sequencePreviousId);
  }

  @Override
  public List<SegmentIdWithShardSpec> findPendingSegmentIdsWithExactInterval(
      String sequenceName,
      Interval interval
  )
  {
    return metadataCache.findPendingSegmentIdsWithExactInterval(sequenceName, interval);
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegmentsOverlapping(Interval interval)
  {
    return metadataCache.findPendingSegmentsOverlapping(interval);
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegmentsWithExactInterval(Interval interval)
  {
    return metadataCache.findPendingSegmentsWithExactInterval(interval);
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegments(String taskAllocatorId)
  {
    return metadataCache.findPendingSegments(taskAllocatorId);
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
  public int markSegmentsWithinIntervalAsUnused(Interval interval, DateTime updateTime)
  {
    return performWriteAction(
        writer -> writer.markSegmentsWithinIntervalAsUnused(interval, updateTime)
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

  private <T> T performWriteAction(Function<DatasourceSegmentMetadataWriter, T> action)
  {
    if (isClosed.get()) {
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
