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
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.metadata.segment.cache.SegmentsMetadataCache;
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
 * A {@link SegmentsMetadataTransaction} that performs reads using the cache
 * and sends writes first to the metadata store and then the cache (if the
 * metadata store persist succeeds).
 */
class SqlSegmentsMetadataCachedTransaction implements SegmentsMetadataTransaction
{
  private final SegmentsMetadataTransaction delegate;
  private final SegmentsMetadataCache.DataSource metadataCache;
  private final DruidLeaderSelector leaderSelector;

  private final int startTerm;

  private final AtomicBoolean isRollingBack = new AtomicBoolean(false);
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  private final List<Consumer<DatasourceSegmentMetadataWriter>> pendingWrites = new ArrayList<>();

  SqlSegmentsMetadataCachedTransaction(
      SegmentsMetadataTransaction delegate,
      SegmentsMetadataCache.DataSource metadataCache,
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

  private DatasourceSegmentMetadataReader cacheReader()
  {
    return metadataCache;
  }

  private DatasourceSegmentMetadataWriter cacheWriter()
  {
    return metadataCache;
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
      pendingWrites.forEach(action -> {
        if (isLeaderWithSameTerm()) {
          action.accept(cacheWriter());
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
    return cacheReader().findExistingSegmentIds(segments);
  }

  @Override
  public Set<SegmentId> findUsedSegmentIdsOverlapping(Interval interval)
  {
    return cacheReader().findUsedSegmentIdsOverlapping(interval);
  }

  @Override
  public SegmentId findHighestUnusedSegmentId(Interval interval, String version)
  {
    return cacheReader().findHighestUnusedSegmentId(interval, version);
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
  public CloseableIterator<DataSegment> findUsedSegmentsOverlappingAnyOf(List<Interval> intervals)
  {
    return cacheReader().findUsedSegmentsOverlappingAnyOf(intervals);
  }

  @Override
  public List<DataSegment> findUsedSegments(Set<String> segmentIds)
  {
    return cacheReader().findUsedSegments(segmentIds);
  }

  @Override
  public Set<DataSegmentPlus> findUsedSegmentsPlusOverlappingAnyOf(List<Interval> intervals)
  {
    return cacheReader().findUsedSegmentsPlusOverlappingAnyOf(intervals);
  }

  @Override
  public List<DataSegment> findUnusedSegments(
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  )
  {
    // Read from metadata store since unused segment payloads are not cached
    return delegate.findUnusedSegments(interval, versions, limit, maxUsedStatusLastUpdatedTime);
  }

  @Override
  public DataSegment findSegment(String segmentId)
  {
    // Read from metadata store since unused segment payloads are not cached
    return delegate.findSegment(segmentId);
  }

  @Override
  public DataSegment findUsedSegment(String segmentId)
  {
    return cacheReader().findUsedSegment(segmentId);
  }

  @Override
  public List<SegmentIdWithShardSpec> findPendingSegmentIds(
      String sequenceName,
      String sequencePreviousId
  )
  {
    return cacheReader().findPendingSegmentIds(sequenceName, sequencePreviousId);
  }

  @Override
  public List<SegmentIdWithShardSpec> findPendingSegmentIdsWithExactInterval(
      String sequenceName,
      Interval interval
  )
  {
    return cacheReader().findPendingSegmentIdsWithExactInterval(sequenceName, interval);
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegmentsOverlapping(Interval interval)
  {
    return cacheReader().findPendingSegmentsOverlapping(interval);
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegmentsWithExactInterval(Interval interval)
  {
    return cacheReader().findPendingSegmentsWithExactInterval(interval);
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegments(String taskAllocatorId)
  {
    return cacheReader().findPendingSegments(taskAllocatorId);
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
  public int deleteSegments(Set<DataSegment> segments)
  {
    return performWriteAction(writer -> writer.deleteSegments(segments));
  }

  @Override
  public boolean updateSegmentPayload(DataSegment segment)
  {
    return performWriteAction(writer -> writer.updateSegmentPayload(segment));
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
  public int deletePendingSegments(List<String> segmentIdsToDelete)
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

    // TODO: verify if the write to metadata store was successful
    //  Otherwise, throw an exception
    pendingWrites.add(action::apply);

    return result;
  }
}
