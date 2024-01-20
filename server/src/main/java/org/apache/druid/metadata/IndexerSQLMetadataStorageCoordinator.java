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

package org.apache.druid.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.inject.Inject;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.SegmentCreateRequest;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartialShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionIds;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.ByteArrayMapper;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 */
public class IndexerSQLMetadataStorageCoordinator implements IndexerMetadataStorageCoordinator
{
  private static final Logger log = new Logger(IndexerSQLMetadataStorageCoordinator.class);
  private static final int MAX_NUM_SEGMENTS_TO_ANNOUNCE_AT_ONCE = 100;

  private static final String UPGRADED_PENDING_SEGMENT_PREFIX = "upgraded_to_version__";

  private final ObjectMapper jsonMapper;
  private final MetadataStorageTablesConfig dbTables;
  private final SQLMetadataConnector connector;

  @Inject
  public IndexerSQLMetadataStorageCoordinator(
      ObjectMapper jsonMapper,
      MetadataStorageTablesConfig dbTables,
      SQLMetadataConnector connector
  )
  {
    this.jsonMapper = jsonMapper;
    this.dbTables = dbTables;
    this.connector = connector;
  }

  @LifecycleStart
  public void start()
  {
    connector.createDataSourceTable();
    connector.createPendingSegmentsTable();
    connector.createSegmentTable();
    connector.createUpgradeSegmentsTable();
  }

  @Override
  public Collection<DataSegment> retrieveUsedSegmentsForIntervals(
      final String dataSource,
      final List<Interval> intervals,
      final Segments visibility
  )
  {
    if (intervals == null || intervals.isEmpty()) {
      throw new IAE("null/empty intervals");
    }
    return doRetrieveUsedSegments(dataSource, intervals, visibility);
  }

  @Override
  public Collection<DataSegment> retrieveAllUsedSegments(String dataSource, Segments visibility)
  {
    return doRetrieveUsedSegments(dataSource, Collections.emptyList(), visibility);
  }

  /**
   * @param intervals empty list means unrestricted interval.
   */
  private Collection<DataSegment> doRetrieveUsedSegments(
      final String dataSource,
      final List<Interval> intervals,
      final Segments visibility
  )
  {
    return connector.retryWithHandle(
        handle -> {
          if (visibility == Segments.ONLY_VISIBLE) {
            final SegmentTimeline timeline =
                getTimelineForIntervalsWithHandle(handle, dataSource, intervals);
            return timeline.findNonOvershadowedObjectsInInterval(Intervals.ETERNITY, Partitions.ONLY_COMPLETE);
          } else {
            return retrieveAllUsedSegmentsForIntervalsWithHandle(handle, dataSource, intervals);
          }
        }
    );
  }

  @Override
  public List<Pair<DataSegment, String>> retrieveUsedSegmentsAndCreatedDates(String dataSource, List<Interval> intervals)
  {
    StringBuilder queryBuilder = new StringBuilder(
        "SELECT created_date, payload FROM %1$s WHERE dataSource = :dataSource AND used = true"
    );

    final boolean compareIntervalEndpointsAsString = intervals.stream()
                                                              .allMatch(Intervals::canCompareEndpointsAsStrings);
    final SqlSegmentsMetadataQuery.IntervalMode intervalMode = SqlSegmentsMetadataQuery.IntervalMode.OVERLAPS;

    SqlSegmentsMetadataQuery.appendConditionForIntervalsAndMatchMode(
        queryBuilder,
        compareIntervalEndpointsAsString ? intervals : Collections.emptyList(),
        intervalMode,
        connector
    );

    final String queryString = StringUtils.format(queryBuilder.toString(), dbTables.getSegmentsTable());
    return connector.retryWithHandle(
        handle -> {
          Query<Map<String, Object>> query = handle
              .createQuery(queryString)
              .bind("dataSource", dataSource);

          if (compareIntervalEndpointsAsString) {
            SqlSegmentsMetadataQuery.bindQueryIntervals(query, intervals);
          }

          final List<Pair<DataSegment, String>> segmentsWithCreatedDates = query
              .map((int index, ResultSet r, StatementContext ctx) ->
                       new Pair<>(
                           JacksonUtils.readValue(jsonMapper, r.getBytes("payload"), DataSegment.class),
                           r.getString("created_date")
                       )
              )
              .list();

          if (intervals.isEmpty() || compareIntervalEndpointsAsString) {
            return segmentsWithCreatedDates;
          } else {
            return segmentsWithCreatedDates
                .stream()
                .filter(pair -> {
                  for (Interval interval : intervals) {
                    if (intervalMode.apply(interval, pair.lhs.getInterval())) {
                      return true;
                    }
                  }
                  return false;
                }).collect(Collectors.toList());
          }
        }
    );
  }

  @Override
  public List<DataSegment> retrieveUnusedSegmentsForInterval(
      String dataSource,
      Interval interval,
      @Nullable Integer limit,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  )
  {
    final List<DataSegment> matchingSegments = connector.inReadOnlyTransaction(
        (handle, status) -> {
          try (final CloseableIterator<DataSegment> iterator =
                   SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables, jsonMapper)
                                           .retrieveUnusedSegments(
                                               dataSource,
                                               Collections.singletonList(interval),
                                               limit,
                                               null,
                                               null,
                                               maxUsedStatusLastUpdatedTime
                                           )
          ) {
            return ImmutableList.copyOf(iterator);
          }
        }
    );

    log.info("Found [%,d] unused segments for datasource[%s] in interval[%s] with maxUsedStatusLastUpdatedTime[%s].",
             matchingSegments.size(), dataSource, interval, maxUsedStatusLastUpdatedTime);
    return matchingSegments;
  }

  @Override
  public int markSegmentsAsUnusedWithinInterval(String dataSource, Interval interval)
  {
    final Integer numSegmentsMarkedUnused = connector.retryTransaction(
        (handle, status) ->
            SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables, jsonMapper)
                                    .markSegmentsUnused(dataSource, interval),
        3,
        SQLMetadataConnector.DEFAULT_MAX_TRIES
    );

    log.info("Marked %,d segments unused for %s for interval %s.", numSegmentsMarkedUnused, dataSource, interval);
    return numSegmentsMarkedUnused;
  }

  /**
   * Fetches all the pending segments, whose interval overlaps with the given
   * search interval and has a sequence_name that begins with one of the prefixes in sequenceNamePrefixFilter
   * from the metadata store. Returns a Map from the pending segment ID to the sequence name.
   */
  @VisibleForTesting
  Map<SegmentIdWithShardSpec, String> getPendingSegmentsForIntervalWithHandle(
      final Handle handle,
      final String dataSource,
      final Interval interval,
      final Set<String> sequenceNamePrefixFilter
  ) throws IOException
  {
    if (sequenceNamePrefixFilter.isEmpty()) {
      return Collections.emptyMap();
    }

    final List<String> sequenceNamePrefixes = new ArrayList<>(sequenceNamePrefixFilter);
    final List<String> sequenceNamePrefixConditions = new ArrayList<>();
    for (int i = 0; i < sequenceNamePrefixes.size(); i++) {
      sequenceNamePrefixConditions.add(StringUtils.format("(sequence_name LIKE :prefix%d)", i));
    }

    String sql = "SELECT sequence_name, payload"
                 + " FROM " + dbTables.getPendingSegmentsTable()
                 + " WHERE dataSource = :dataSource"
                 + " AND start < :end"
                 + StringUtils.format(" AND %1$send%1$s > :start", connector.getQuoteString())
                 + " AND ( " + String.join(" OR ", sequenceNamePrefixConditions) + " )";

    Query<Map<String, Object>> query = handle.createQuery(sql)
                                             .bind("dataSource", dataSource)
                                             .bind("start", interval.getStart().toString())
                                             .bind("end", interval.getEnd().toString());

    for (int i = 0; i < sequenceNamePrefixes.size(); i++) {
      query.bind(StringUtils.format("prefix%d", i), sequenceNamePrefixes.get(i) + "%");
    }

    final ResultIterator<PendingSegmentsRecord> dbSegments =
        query.map((index, r, ctx) -> PendingSegmentsRecord.fromResultSet(r))
             .iterator();

    final Map<SegmentIdWithShardSpec, String> pendingSegmentToSequenceName = new HashMap<>();
    while (dbSegments.hasNext()) {
      PendingSegmentsRecord record = dbSegments.next();
      final SegmentIdWithShardSpec identifier = jsonMapper.readValue(record.payload, SegmentIdWithShardSpec.class);

      if (interval.overlaps(identifier.getInterval())) {
        pendingSegmentToSequenceName.put(identifier, record.sequenceName);
      }
    }

    dbSegments.close();

    return pendingSegmentToSequenceName;
  }

  private Map<SegmentIdWithShardSpec, String> getPendingSegmentsForIntervalWithHandle(
      final Handle handle,
      final String dataSource,
      final Interval interval
  ) throws IOException
  {
    final ResultIterator<PendingSegmentsRecord> dbSegments =
        handle.createQuery(
            StringUtils.format(
                // This query might fail if the year has a different number of digits
                // See https://github.com/apache/druid/pull/11582 for a similar issue
                // Using long for these timestamps instead of varchar would give correct time comparisons
                "SELECT sequence_name, payload FROM %1$s"
                + " WHERE dataSource = :dataSource AND start < :end and %2$send%2$s > :start",
                dbTables.getPendingSegmentsTable(), connector.getQuoteString()
            )
        )
              .bind("dataSource", dataSource)
              .bind("start", interval.getStart().toString())
              .bind("end", interval.getEnd().toString())
              .map((index, r, ctx) -> PendingSegmentsRecord.fromResultSet(r))
              .iterator();

    final Map<SegmentIdWithShardSpec, String> pendingSegmentToSequenceName = new HashMap<>();
    while (dbSegments.hasNext()) {
      PendingSegmentsRecord record = dbSegments.next();
      final SegmentIdWithShardSpec identifier = jsonMapper.readValue(record.payload, SegmentIdWithShardSpec.class);

      if (interval.overlaps(identifier.getInterval())) {
        pendingSegmentToSequenceName.put(identifier, record.sequenceName);
      }
    }

    dbSegments.close();

    return pendingSegmentToSequenceName;
  }

  private SegmentTimeline getTimelineForIntervalsWithHandle(
      final Handle handle,
      final String dataSource,
      final List<Interval> intervals
  ) throws IOException
  {
    try (final CloseableIterator<DataSegment> iterator =
             SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables, jsonMapper)
                                     .retrieveUsedSegments(dataSource, intervals)) {
      return SegmentTimeline.forSegments(iterator);
    }
  }

  private Collection<DataSegment> retrieveAllUsedSegmentsForIntervalsWithHandle(
      final Handle handle,
      final String dataSource,
      final List<Interval> intervals
  ) throws IOException
  {
    try (final CloseableIterator<DataSegment> iterator =
             SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables, jsonMapper)
                                     .retrieveUsedSegments(dataSource, intervals)) {
      final List<DataSegment> retVal = new ArrayList<>();
      iterator.forEachRemaining(retVal::add);
      return retVal;
    }
  }

  @Override
  public Set<DataSegment> commitSegments(final Set<DataSegment> segments) throws IOException
  {
    final SegmentPublishResult result = commitSegmentsAndMetadata(segments, null, null);

    // Metadata transaction cannot fail because we are not trying to do one.
    if (!result.isSuccess()) {
      throw new ISE("announceHistoricalSegments failed with null metadata, should not happen.");
    }

    return result.getSegments();
  }

  @Override
  public SegmentPublishResult commitSegmentsAndMetadata(
      final Set<DataSegment> segments,
      @Nullable final DataSourceMetadata startMetadata,
      @Nullable final DataSourceMetadata endMetadata
  ) throws IOException
  {
    if (segments.isEmpty()) {
      throw new IllegalArgumentException("segment set must not be empty");
    }

    final String dataSource = segments.iterator().next().getDataSource();
    for (DataSegment segment : segments) {
      if (!dataSource.equals(segment.getDataSource())) {
        throw new IllegalArgumentException("segments must all be from the same dataSource");
      }
    }

    if ((startMetadata == null && endMetadata != null) || (startMetadata != null && endMetadata == null)) {
      throw new IllegalArgumentException("start/end metadata pair must be either null or non-null");
    }

    // Find which segments are used (i.e. not overshadowed).
    final Set<DataSegment> usedSegments = new HashSet<>();
    List<TimelineObjectHolder<String, DataSegment>> segmentHolders =
        SegmentTimeline.forSegments(segments).lookupWithIncompletePartitions(Intervals.ETERNITY);
    for (TimelineObjectHolder<String, DataSegment> holder : segmentHolders) {
      for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
        usedSegments.add(chunk.getObject());
      }
    }

    final AtomicBoolean definitelyNotUpdated = new AtomicBoolean(false);

    try {
      return connector.retryTransaction(
          new TransactionCallback<SegmentPublishResult>()
          {
            @Override
            public SegmentPublishResult inTransaction(
                final Handle handle,
                final TransactionStatus transactionStatus
            ) throws Exception
            {
              // Set definitelyNotUpdated back to false upon retrying.
              definitelyNotUpdated.set(false);

              if (startMetadata != null) {
                final DataStoreMetadataUpdateResult result = updateDataSourceMetadataWithHandle(
                    handle,
                    dataSource,
                    startMetadata,
                    endMetadata
                );

                if (result.isFailed()) {
                  // Metadata was definitely not updated.
                  transactionStatus.setRollbackOnly();
                  definitelyNotUpdated.set(true);

                  if (result.canRetry()) {
                    throw new RetryTransactionException(result.getErrorMsg());
                  } else {
                    throw new RuntimeException(result.getErrorMsg());
                  }
                }
              }

              final Set<DataSegment> inserted = announceHistoricalSegmentBatch(handle, segments, usedSegments);
              return SegmentPublishResult.ok(ImmutableSet.copyOf(inserted));
            }
          },
          3,
          getSqlMetadataMaxRetry()
      );
    }
    catch (CallbackFailedException e) {
      if (definitelyNotUpdated.get()) {
        return SegmentPublishResult.fail(e.getMessage());
      } else {
        // Must throw exception if we are not sure if we updated or not.
        throw e;
      }
    }
  }

  @Override
  public SegmentPublishResult commitReplaceSegments(
      final Set<DataSegment> replaceSegments,
      final Set<ReplaceTaskLock> locksHeldByReplaceTask
  )
  {
    verifySegmentsToCommit(replaceSegments);

    try {
      return connector.retryTransaction(
          (handle, transactionStatus) -> {
            final Set<DataSegment> segmentsToInsert = new HashSet<>(replaceSegments);
            segmentsToInsert.addAll(
                createNewIdsOfAppendSegmentsAfterReplace(handle, replaceSegments, locksHeldByReplaceTask)
            );
            return SegmentPublishResult.ok(
                insertSegments(handle, segmentsToInsert)
            );
          },
          3,
          getSqlMetadataMaxRetry()
      );
    }
    catch (CallbackFailedException e) {
      return SegmentPublishResult.fail(e.getMessage());
    }
  }

  @Override
  public SegmentPublishResult commitAppendSegments(
      final Set<DataSegment> appendSegments,
      final Map<DataSegment, ReplaceTaskLock> appendSegmentToReplaceLock
  )
  {
    return commitAppendSegmentsAndMetadataInTransaction(
        appendSegments,
        appendSegmentToReplaceLock,
        null,
        null
    );
  }

  @Override
  public SegmentPublishResult commitAppendSegmentsAndMetadata(
      Set<DataSegment> appendSegments,
      Map<DataSegment, ReplaceTaskLock> appendSegmentToReplaceLock,
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata
  )
  {
    return commitAppendSegmentsAndMetadataInTransaction(
        appendSegments,
        appendSegmentToReplaceLock,
        startMetadata,
        endMetadata
    );
  }

  @Override
  public SegmentPublishResult commitMetadataOnly(
      String dataSource,
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata
  )
  {
    if (dataSource == null) {
      throw new IllegalArgumentException("datasource name cannot be null");
    }
    if (startMetadata == null) {
      throw new IllegalArgumentException("start metadata cannot be null");
    }
    if (endMetadata == null) {
      throw new IllegalArgumentException("end metadata cannot be null");
    }

    final AtomicBoolean definitelyNotUpdated = new AtomicBoolean(false);

    try {
      return connector.retryTransaction(
          new TransactionCallback<SegmentPublishResult>()
          {
            @Override
            public SegmentPublishResult inTransaction(
                final Handle handle,
                final TransactionStatus transactionStatus
            ) throws Exception
            {
              // Set definitelyNotUpdated back to false upon retrying.
              definitelyNotUpdated.set(false);

              final DataStoreMetadataUpdateResult result = updateDataSourceMetadataWithHandle(
                  handle,
                  dataSource,
                  startMetadata,
                  endMetadata
              );

              if (result.isFailed()) {
                // Metadata was definitely not updated.
                transactionStatus.setRollbackOnly();
                definitelyNotUpdated.set(true);

                if (result.canRetry()) {
                  throw new RetryTransactionException(result.getErrorMsg());
                } else {
                  throw new RuntimeException(result.getErrorMsg());
                }
              }

              return SegmentPublishResult.ok(ImmutableSet.of());
            }
          },
          3,
          getSqlMetadataMaxRetry()
      );
    }
    catch (CallbackFailedException e) {
      if (definitelyNotUpdated.get()) {
        return SegmentPublishResult.fail(e.getMessage());
      } else {
        // Must throw exception if we are not sure if we updated or not.
        throw e;
      }
    }
  }

  @VisibleForTesting
  public int getSqlMetadataMaxRetry()
  {
    return SQLMetadataConnector.DEFAULT_MAX_TRIES;
  }

  @Override
  public Map<SegmentCreateRequest, SegmentIdWithShardSpec> allocatePendingSegments(
      String dataSource,
      Interval allocateInterval,
      boolean skipSegmentLineageCheck,
      List<SegmentCreateRequest> requests
  )
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(allocateInterval, "interval");

    final Interval interval = allocateInterval.withChronology(ISOChronology.getInstanceUTC());
    return connector.retryWithHandle(
        handle -> allocatePendingSegments(handle, dataSource, interval, skipSegmentLineageCheck, requests)
    );
  }

  @Override
  public SegmentIdWithShardSpec allocatePendingSegment(
      final String dataSource,
      final String sequenceName,
      @Nullable final String previousSegmentId,
      final Interval interval,
      final PartialShardSpec partialShardSpec,
      final String maxVersion,
      final boolean skipSegmentLineageCheck
  )
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(sequenceName, "sequenceName");
    Preconditions.checkNotNull(interval, "interval");
    Preconditions.checkNotNull(maxVersion, "version");
    final Interval allocateInterval = interval.withChronology(ISOChronology.getInstanceUTC());

    return connector.retryWithHandle(
        handle -> {
          // Get the time chunk and associated data segments for the given interval, if any
          final List<TimelineObjectHolder<String, DataSegment>> existingChunks =
              getTimelineForIntervalsWithHandle(handle, dataSource, ImmutableList.of(interval))
                  .lookup(interval);
          if (existingChunks.size() > 1) {
            // Not possible to expand more than one chunk with a single segment.
            log.warn(
                "Cannot allocate new segment for dataSource[%s], interval[%s] as it already has [%,d] versions.",
                dataSource, interval, existingChunks.size()
            );
            return null;
          }

          if (skipSegmentLineageCheck) {
            return allocatePendingSegment(
                handle,
                dataSource,
                sequenceName,
                allocateInterval,
                partialShardSpec,
                maxVersion,
                existingChunks
            );
          } else {
            return allocatePendingSegmentWithSegmentLineageCheck(
                handle,
                dataSource,
                sequenceName,
                previousSegmentId,
                allocateInterval,
                partialShardSpec,
                maxVersion,
                existingChunks
            );
          }
        }
    );
  }

  @Override
  public Map<SegmentIdWithShardSpec, SegmentIdWithShardSpec> upgradePendingSegmentsOverlappingWith(
      Set<DataSegment> replaceSegments,
      Set<String> activeRealtimeSequencePrefixes
  )
  {
    if (replaceSegments.isEmpty()) {
      return Collections.emptyMap();
    }

    // Any replace interval has exactly one version of segments
    final Map<Interval, DataSegment> replaceIntervalToMaxId = new HashMap<>();
    for (DataSegment segment : replaceSegments) {
      DataSegment committedMaxId = replaceIntervalToMaxId.get(segment.getInterval());
      if (committedMaxId == null
          || committedMaxId.getShardSpec().getPartitionNum() < segment.getShardSpec().getPartitionNum()) {
        replaceIntervalToMaxId.put(segment.getInterval(), segment);
      }
    }

    final String datasource = replaceSegments.iterator().next().getDataSource();
    return connector.retryWithHandle(
        handle -> upgradePendingSegments(handle, datasource, replaceIntervalToMaxId, activeRealtimeSequencePrefixes)
    );
  }

  /**
   * Creates and inserts new IDs for the pending segments contained in each replace
   * interval. The newly created pending segment IDs
   * <ul>
   * <li>Have the same interval and version as that of an overlapping segment
   * committed by the REPLACE task.</li>
   * <li>Cannot be committed but are only used to serve realtime queries against
   * those versions.</li>
   * </ul>
   *
   * @return Map from original pending segment to the new upgraded ID.
   */
  private Map<SegmentIdWithShardSpec, SegmentIdWithShardSpec> upgradePendingSegments(
      Handle handle,
      String datasource,
      Map<Interval, DataSegment> replaceIntervalToMaxId,
      Set<String> activeRealtimeSequencePrefixes
  ) throws IOException
  {
    final Map<SegmentCreateRequest, SegmentIdWithShardSpec> newPendingSegmentVersions = new HashMap<>();
    final Map<SegmentIdWithShardSpec, SegmentIdWithShardSpec> pendingSegmentToNewId = new HashMap<>();

    for (Map.Entry<Interval, DataSegment> entry : replaceIntervalToMaxId.entrySet()) {
      final Interval replaceInterval = entry.getKey();
      final DataSegment maxSegmentId = entry.getValue();
      final String replaceVersion = maxSegmentId.getVersion();

      final int numCorePartitions = maxSegmentId.getShardSpec().getNumCorePartitions();
      int currentPartitionNumber = maxSegmentId.getShardSpec().getPartitionNum();

      final Map<SegmentIdWithShardSpec, String> overlappingPendingSegments
          = getPendingSegmentsForIntervalWithHandle(handle, datasource, replaceInterval, activeRealtimeSequencePrefixes);

      for (Map.Entry<SegmentIdWithShardSpec, String> overlappingPendingSegment
          : overlappingPendingSegments.entrySet()) {
        final SegmentIdWithShardSpec pendingSegmentId = overlappingPendingSegment.getKey();
        final String pendingSegmentSequence = overlappingPendingSegment.getValue();

        if (shouldUpgradePendingSegment(pendingSegmentId, pendingSegmentSequence, replaceInterval, replaceVersion)) {
          // Ensure unique sequence_name_prev_id_sha1 by setting
          // sequence_prev_id -> pendingSegmentId
          // sequence_name -> prefix + replaceVersion
          SegmentIdWithShardSpec newId = new SegmentIdWithShardSpec(
              datasource,
              replaceInterval,
              replaceVersion,
              new NumberedShardSpec(++currentPartitionNumber, numCorePartitions)
          );
          newPendingSegmentVersions.put(
              new SegmentCreateRequest(
                  UPGRADED_PENDING_SEGMENT_PREFIX + replaceVersion,
                  pendingSegmentId.toString(),
                  replaceVersion,
                  NumberedPartialShardSpec.instance()
              ),
              newId
          );
          pendingSegmentToNewId.put(pendingSegmentId, newId);
        }
      }
    }

    // Do not skip lineage check so that the sequence_name_prev_id_sha1
    // includes hash of both sequence_name and prev_segment_id
    int numInsertedPendingSegments = insertPendingSegmentsIntoMetastore(
        handle,
        newPendingSegmentVersions,
        datasource,
        false
    );
    log.info(
        "Inserted total [%d] new versions for [%d] pending segments.",
        numInsertedPendingSegments, newPendingSegmentVersions.size()
    );

    return pendingSegmentToNewId;
  }

  private boolean shouldUpgradePendingSegment(
      SegmentIdWithShardSpec pendingSegmentId,
      String pendingSegmentSequenceName,
      Interval replaceInterval,
      String replaceVersion
  )
  {
    if (pendingSegmentId.getVersion().compareTo(replaceVersion) >= 0) {
      return false;
    } else if (!replaceInterval.contains(pendingSegmentId.getInterval())) {
      return false;
    } else {
      // Do not upgrade already upgraded pending segment
      return pendingSegmentSequenceName == null
             || !pendingSegmentSequenceName.startsWith(UPGRADED_PENDING_SEGMENT_PREFIX);
    }
  }

  @Nullable
  private SegmentIdWithShardSpec allocatePendingSegmentWithSegmentLineageCheck(
      final Handle handle,
      final String dataSource,
      final String sequenceName,
      @Nullable final String previousSegmentId,
      final Interval interval,
      final PartialShardSpec partialShardSpec,
      final String maxVersion,
      final List<TimelineObjectHolder<String, DataSegment>> existingChunks
  ) throws IOException
  {
    final String previousSegmentIdNotNull = previousSegmentId == null ? "" : previousSegmentId;

    final String sql = StringUtils.format(
        "SELECT payload FROM %s WHERE "
        + "dataSource = :dataSource AND "
        + "sequence_name = :sequence_name AND "
        + "sequence_prev_id = :sequence_prev_id",
        dbTables.getPendingSegmentsTable()
    );
    final Query<Map<String, Object>> query
        = handle.createQuery(sql)
                .bind("dataSource", dataSource)
                .bind("sequence_name", sequenceName)
                .bind("sequence_prev_id", previousSegmentIdNotNull);

    final String usedSegmentVersion = existingChunks.isEmpty() ? null : existingChunks.get(0).getVersion();
    final CheckExistingSegmentIdResult result = findExistingPendingSegment(
        query,
        interval,
        sequenceName,
        previousSegmentIdNotNull,
        usedSegmentVersion
    );

    if (result.found) {
      // The found existing segment identifier can be null if its interval doesn't match with the given interval
      return result.segmentIdentifier;
    }

    final SegmentIdWithShardSpec newIdentifier = createNewSegment(
        handle,
        dataSource,
        interval,
        partialShardSpec,
        maxVersion,
        existingChunks
    );
    if (newIdentifier == null) {
      return null;
    }

    // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
    // Avoiding ON DUPLICATE KEY since it's not portable.
    // Avoiding try/catch since it may cause inadvertent transaction-splitting.

    // UNIQUE key for the row, ensuring sequences do not fork in two directions.
    // Using a single column instead of (sequence_name, sequence_prev_id) as some MySQL storage engines
    // have difficulty with large unique keys (see https://github.com/apache/druid/issues/2319)
    final String sequenceNamePrevIdSha1 = BaseEncoding.base16().encode(
        Hashing.sha1()
               .newHasher()
               .putBytes(StringUtils.toUtf8(sequenceName))
               .putByte((byte) 0xff)
               .putBytes(StringUtils.toUtf8(previousSegmentIdNotNull))
               .putByte((byte) 0xff)
               .putBytes(StringUtils.toUtf8(newIdentifier.getVersion()))
               .hash()
               .asBytes()
    );

    insertPendingSegmentIntoMetastore(
        handle,
        newIdentifier,
        dataSource,
        interval,
        previousSegmentIdNotNull,
        sequenceName,
        sequenceNamePrevIdSha1
    );
    return newIdentifier;
  }

  private Map<SegmentCreateRequest, SegmentIdWithShardSpec> allocatePendingSegments(
      final Handle handle,
      final String dataSource,
      final Interval interval,
      final boolean skipSegmentLineageCheck,
      final List<SegmentCreateRequest> requests
  ) throws IOException
  {
    // Get the time chunk and associated data segments for the given interval, if any
    final List<TimelineObjectHolder<String, DataSegment>> existingChunks =
        getTimelineForIntervalsWithHandle(handle, dataSource, Collections.singletonList(interval))
            .lookup(interval);
    if (existingChunks.size() > 1) {
      log.warn(
          "Cannot allocate new segments for dataSource[%s], interval[%s] as interval already has [%,d] chunks.",
          dataSource, interval, existingChunks.size()
      );
      return Collections.emptyMap();
    }

    final String existingVersion = existingChunks.isEmpty() ? null : existingChunks.get(0).getVersion();
    final Map<SegmentCreateRequest, CheckExistingSegmentIdResult> existingSegmentIds;
    if (skipSegmentLineageCheck) {
      existingSegmentIds =
          getExistingSegmentIdsSkipLineageCheck(handle, dataSource, interval, existingVersion, requests);
    } else {
      existingSegmentIds =
          getExistingSegmentIdsWithLineageCheck(handle, dataSource, interval, existingVersion, requests);
    }

    // For every request see if a segment id already exists
    final Map<SegmentCreateRequest, SegmentIdWithShardSpec> allocatedSegmentIds = new HashMap<>();
    final List<SegmentCreateRequest> requestsForNewSegments = new ArrayList<>();
    for (SegmentCreateRequest request : requests) {
      CheckExistingSegmentIdResult existingSegmentId = existingSegmentIds.get(request);
      if (existingSegmentId == null || !existingSegmentId.found) {
        requestsForNewSegments.add(request);
      } else if (existingSegmentId.segmentIdentifier != null) {
        log.info("Found valid existing segment [%s] for request.", existingSegmentId.segmentIdentifier);
        allocatedSegmentIds.put(request, existingSegmentId.segmentIdentifier);
      } else {
        log.info("Found clashing existing segment [%s] for request.", existingSegmentId);
      }
    }

    // For each of the remaining requests, create a new segment
    final Map<SegmentCreateRequest, SegmentIdWithShardSpec> createdSegments = createNewSegments(
        handle,
        dataSource,
        interval,
        skipSegmentLineageCheck,
        existingChunks,
        requestsForNewSegments
    );

    // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
    // Avoiding ON DUPLICATE KEY since it's not portable.
    // Avoiding try/catch since it may cause inadvertent transaction-splitting.

    // UNIQUE key for the row, ensuring we don't have more than one segment per sequence per interval.
    // Using a single column instead of (sequence_name, sequence_prev_id) as some MySQL storage engines
    // have difficulty with large unique keys (see https://github.com/apache/druid/issues/2319)
    insertPendingSegmentsIntoMetastore(
        handle,
        createdSegments,
        dataSource,
        skipSegmentLineageCheck
    );

    allocatedSegmentIds.putAll(createdSegments);
    return allocatedSegmentIds;
  }

  @SuppressWarnings("UnstableApiUsage")
  private String getSequenceNameAndPrevIdSha(
      SegmentCreateRequest request,
      SegmentIdWithShardSpec pendingSegmentId,
      boolean skipSegmentLineageCheck
  )
  {
    final Hasher hasher = Hashing.sha1().newHasher()
                                 .putBytes(StringUtils.toUtf8(request.getSequenceName()))
                                 .putByte((byte) 0xff);

    if (skipSegmentLineageCheck) {
      final Interval interval = pendingSegmentId.getInterval();
      hasher
          .putLong(interval.getStartMillis())
          .putLong(interval.getEndMillis());
    } else {
      hasher
          .putBytes(StringUtils.toUtf8(request.getPreviousSegmentId()));
    }

    hasher.putByte((byte) 0xff);
    hasher.putBytes(StringUtils.toUtf8(pendingSegmentId.getVersion()));

    return BaseEncoding.base16().encode(hasher.hash().asBytes());
  }

  @Nullable
  private SegmentIdWithShardSpec allocatePendingSegment(
      final Handle handle,
      final String dataSource,
      final String sequenceName,
      final Interval interval,
      final PartialShardSpec partialShardSpec,
      final String maxVersion,
      final List<TimelineObjectHolder<String, DataSegment>> existingChunks
  ) throws IOException
  {
    final String sql = StringUtils.format(
        "SELECT payload FROM %s WHERE "
        + "dataSource = :dataSource AND "
        + "sequence_name = :sequence_name AND "
        + "start = :start AND "
        + "%2$send%2$s = :end",
        dbTables.getPendingSegmentsTable(),
        connector.getQuoteString()
    );
    final Query<Map<String, Object>> query
        = handle.createQuery(sql)
                .bind("dataSource", dataSource)
                .bind("sequence_name", sequenceName)
                .bind("start", interval.getStart().toString())
                .bind("end", interval.getEnd().toString());

    final CheckExistingSegmentIdResult result = findExistingPendingSegment(
        query,
        interval,
        sequenceName,
        null,
        existingChunks.isEmpty() ? null : existingChunks.get(0).getVersion()
    );

    if (result.found) {
      return result.segmentIdentifier;
    }

    final SegmentIdWithShardSpec newIdentifier = createNewSegment(
        handle,
        dataSource,
        interval,
        partialShardSpec,
        maxVersion,
        existingChunks
    );
    if (newIdentifier == null) {
      return null;
    }

    // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
    // Avoiding ON DUPLICATE KEY since it's not portable.
    // Avoiding try/catch since it may cause inadvertent transaction-splitting.

    // UNIQUE key for the row, ensuring we don't have more than one segment per sequence per interval.
    // Using a single column instead of (sequence_name, sequence_prev_id) as some MySQL storage engines
    // have difficulty with large unique keys (see https://github.com/apache/druid/issues/2319)
    final String sequenceNamePrevIdSha1 = BaseEncoding.base16().encode(
        Hashing.sha1()
               .newHasher()
               .putBytes(StringUtils.toUtf8(sequenceName))
               .putByte((byte) 0xff)
               .putLong(interval.getStartMillis())
               .putLong(interval.getEndMillis())
               .putByte((byte) 0xff)
               .putBytes(StringUtils.toUtf8(newIdentifier.getVersion()))
               .hash()
               .asBytes()
    );

    // always insert empty previous sequence id
    insertPendingSegmentIntoMetastore(handle, newIdentifier, dataSource, interval, "", sequenceName, sequenceNamePrevIdSha1);

    log.info(
        "Created new pending segment[%s] for datasource[%s], sequence[%s], interval[%s].",
        newIdentifier, dataSource, sequenceName, interval
    );

    return newIdentifier;
  }

  /**
   * Returns a map from sequenceName to segment id.
   */
  private Map<SegmentCreateRequest, CheckExistingSegmentIdResult> getExistingSegmentIdsSkipLineageCheck(
      Handle handle,
      String dataSource,
      Interval interval,
      String usedSegmentVersion,
      List<SegmentCreateRequest> requests
  ) throws IOException
  {
    final Query<Map<String, Object>> query = handle
        .createQuery(
            StringUtils.format(
                "SELECT sequence_name, payload "
                + "FROM %s WHERE "
                + "dataSource = :dataSource AND "
                + "start = :start AND "
                + "%2$send%2$s = :end",
                dbTables.getPendingSegmentsTable(),
                connector.getQuoteString()
            )
        )
        .bind("dataSource", dataSource)
        .bind("start", interval.getStart().toString())
        .bind("end", interval.getEnd().toString());

    final ResultIterator<PendingSegmentsRecord> dbSegments = query
        .map((index, r, ctx) -> PendingSegmentsRecord.fromResultSet(r))
        .iterator();

    // Map from sequenceName to segment id
    final Map<String, SegmentIdWithShardSpec> sequenceToSegmentId = new HashMap<>();
    while (dbSegments.hasNext()) {
      final PendingSegmentsRecord record = dbSegments.next();
      final SegmentIdWithShardSpec segmentId =
          jsonMapper.readValue(record.getPayload(), SegmentIdWithShardSpec.class);

      // Consider only the pending segments allocated for the latest used segment version
      if (usedSegmentVersion == null || segmentId.getVersion().equals(usedSegmentVersion)) {
        sequenceToSegmentId.put(record.getSequenceName(), segmentId);
      }
    }

    final Map<SegmentCreateRequest, CheckExistingSegmentIdResult> requestToResult = new HashMap<>();
    for (SegmentCreateRequest request : requests) {
      SegmentIdWithShardSpec segmentId = sequenceToSegmentId.get(request.getSequenceName());
      requestToResult.put(request, new CheckExistingSegmentIdResult(segmentId != null, segmentId));
    }

    return requestToResult;
  }

  /**
   * Returns a map from sequenceName to segment id.
   */
  private Map<SegmentCreateRequest, CheckExistingSegmentIdResult> getExistingSegmentIdsWithLineageCheck(
      Handle handle,
      String dataSource,
      Interval interval,
      String usedSegmentVersion,
      List<SegmentCreateRequest> requests
  ) throws IOException
  {
    // This cannot be batched because there doesn't seem to be a clean option:
    // 1. WHERE must have sequence_name and sequence_prev_id but not start or end.
    //    (sequence columns are used to find the matching segment whereas start and
    //    end are used to determine if the found segment is valid or not)
    // 2. IN filters on sequence_name and sequence_prev_id might perform worse than individual SELECTs?
    // 3. IN filter on sequence_name alone might be a feasible option worth evaluating
    final String sql = StringUtils.format(
        "SELECT payload FROM %s WHERE "
        + "dataSource = :dataSource AND "
        + "sequence_name = :sequence_name AND "
        + "sequence_prev_id = :sequence_prev_id",
        dbTables.getPendingSegmentsTable()
    );

    final Map<SegmentCreateRequest, CheckExistingSegmentIdResult> requestToResult = new HashMap<>();
    for (SegmentCreateRequest request : requests) {
      CheckExistingSegmentIdResult result = findExistingPendingSegment(
          handle.createQuery(sql)
                .bind("dataSource", dataSource)
                .bind("sequence_name", request.getSequenceName())
                .bind("sequence_prev_id", request.getPreviousSegmentId()),
          interval,
          request.getSequenceName(),
          request.getPreviousSegmentId(),
          usedSegmentVersion
      );
      requestToResult.put(request, result);
    }

    return requestToResult;
  }

  private CheckExistingSegmentIdResult findExistingPendingSegment(
      final Query<Map<String, Object>> query,
      final Interval interval,
      final String sequenceName,
      final @Nullable String previousSegmentId,
      final @Nullable String usedSegmentVersion
  ) throws IOException
  {
    final List<byte[]> records = query.map(ByteArrayMapper.FIRST).list();
    if (records.isEmpty()) {
      return new CheckExistingSegmentIdResult(false, null);
    }

    for (byte[] record : records) {
      final SegmentIdWithShardSpec pendingSegment
          = jsonMapper.readValue(record, SegmentIdWithShardSpec.class);

      // Consider only pending segments matching the expected version
      if (usedSegmentVersion == null || pendingSegment.getVersion().equals(usedSegmentVersion)) {
        if (pendingSegment.getInterval().isEqual(interval)) {
          log.info(
              "Found existing pending segment[%s] for sequence[%s], previous segment[%s], version[%s] in DB",
              pendingSegment, sequenceName, previousSegmentId, usedSegmentVersion
          );
          return new CheckExistingSegmentIdResult(true, pendingSegment);
        } else {
          log.warn(
              "Cannot use existing pending segment [%s] for sequence[%s], previous segment[%s] in DB"
              + " as it does not match requested interval[%s], version[%s].",
              pendingSegment, sequenceName, previousSegmentId, interval, usedSegmentVersion
          );
          return new CheckExistingSegmentIdResult(true, null);
        }
      }
    }

    return new CheckExistingSegmentIdResult(false, null);
  }

  private static class CheckExistingSegmentIdResult
  {
    private final boolean found;
    @Nullable
    private final SegmentIdWithShardSpec segmentIdentifier;

    CheckExistingSegmentIdResult(boolean found, @Nullable SegmentIdWithShardSpec segmentIdentifier)
    {
      this.found = found;
      this.segmentIdentifier = segmentIdentifier;
    }
  }

  private static class UniqueAllocateRequest
  {
    private final Interval interval;
    private final String previousSegmentId;
    private final String sequenceName;
    private final boolean skipSegmentLineageCheck;

    private final int hashCode;

    public UniqueAllocateRequest(
        Interval interval,
        SegmentCreateRequest request,
        boolean skipSegmentLineageCheck
    )
    {
      this.interval = interval;
      this.sequenceName = request.getSequenceName();
      this.previousSegmentId = request.getPreviousSegmentId();
      this.skipSegmentLineageCheck = skipSegmentLineageCheck;

      this.hashCode = Objects.hash(interval, sequenceName, previousSegmentId, skipSegmentLineageCheck);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UniqueAllocateRequest that = (UniqueAllocateRequest) o;
      return skipSegmentLineageCheck == that.skipSegmentLineageCheck
             && Objects.equals(interval, that.interval)
             && Objects.equals(sequenceName, that.sequenceName)
             && Objects.equals(previousSegmentId, that.previousSegmentId);
    }

    @Override
    public int hashCode()
    {
      return hashCode;
    }
  }

  private SegmentPublishResult commitAppendSegmentsAndMetadataInTransaction(
      Set<DataSegment> appendSegments,
      Map<DataSegment, ReplaceTaskLock> appendSegmentToReplaceLock,
      @Nullable DataSourceMetadata startMetadata,
      @Nullable DataSourceMetadata endMetadata
  )
  {
    verifySegmentsToCommit(appendSegments);
    if ((startMetadata == null && endMetadata != null)
        || (startMetadata != null && endMetadata == null)) {
      throw new IllegalArgumentException("start/end metadata pair must be either null or non-null");
    }

    final String dataSource = appendSegments.iterator().next().getDataSource();
    final Set<DataSegment> segmentIdsForNewVersions = connector.retryTransaction(
        (handle, transactionStatus)
            -> createNewIdsForAppendSegments(handle, dataSource, appendSegments),
        0,
        SQLMetadataConnector.DEFAULT_MAX_TRIES
    );

    // Create entries for all required versions of the append segments
    final Set<DataSegment> allSegmentsToInsert = new HashSet<>(appendSegments);
    allSegmentsToInsert.addAll(segmentIdsForNewVersions);

    final AtomicBoolean metadataNotUpdated = new AtomicBoolean(false);
    try {
      return connector.retryTransaction(
          (handle, transactionStatus) -> {
            metadataNotUpdated.set(false);

            if (startMetadata != null) {
              final DataStoreMetadataUpdateResult metadataUpdateResult
                  = updateDataSourceMetadataWithHandle(handle, dataSource, startMetadata, endMetadata);

              if (metadataUpdateResult.isFailed()) {
                transactionStatus.setRollbackOnly();
                metadataNotUpdated.set(true);

                if (metadataUpdateResult.canRetry()) {
                  throw new RetryTransactionException(metadataUpdateResult.getErrorMsg());
                } else {
                  throw new RuntimeException(metadataUpdateResult.getErrorMsg());
                }
              }
            }

            insertIntoUpgradeSegmentsTable(handle, appendSegmentToReplaceLock);
            return SegmentPublishResult.ok(insertSegments(handle, allSegmentsToInsert));
          },
          3,
          getSqlMetadataMaxRetry()
      );
    }
    catch (CallbackFailedException e) {
      if (metadataNotUpdated.get()) {
        // Return failed result if metadata was definitely not updated
        return SegmentPublishResult.fail(e.getMessage());
      } else {
        throw e;
      }
    }
  }

  private int insertPendingSegmentsIntoMetastore(
      Handle handle,
      Map<SegmentCreateRequest, SegmentIdWithShardSpec> createdSegments,
      String dataSource,
      boolean skipSegmentLineageCheck
  ) throws JsonProcessingException
  {
    final PreparedBatch insertBatch = handle.prepareBatch(
        StringUtils.format(
        "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, sequence_name, sequence_prev_id, "
        + "sequence_name_prev_id_sha1, payload) "
        + "VALUES (:id, :dataSource, :created_date, :start, :end, :sequence_name, :sequence_prev_id, "
        + ":sequence_name_prev_id_sha1, :payload)",
        dbTables.getPendingSegmentsTable(),
        connector.getQuoteString()
    ));

    // Deduplicate the segment ids by inverting the map
    Map<SegmentIdWithShardSpec, SegmentCreateRequest> segmentIdToRequest = new HashMap<>();
    createdSegments.forEach((request, segmentId) -> segmentIdToRequest.put(segmentId, request));

    for (Map.Entry<SegmentIdWithShardSpec, SegmentCreateRequest> entry : segmentIdToRequest.entrySet()) {
      final SegmentCreateRequest request = entry.getValue();
      final SegmentIdWithShardSpec segmentId = entry.getKey();
      final Interval interval = segmentId.getInterval();

      insertBatch.add()
                 .bind("id", segmentId.toString())
                 .bind("dataSource", dataSource)
                 .bind("created_date", DateTimes.nowUtc().toString())
                 .bind("start", interval.getStart().toString())
                 .bind("end", interval.getEnd().toString())
                 .bind("sequence_name", request.getSequenceName())
                 .bind("sequence_prev_id", request.getPreviousSegmentId())
                 .bind(
                     "sequence_name_prev_id_sha1",
                     getSequenceNameAndPrevIdSha(request, segmentId, skipSegmentLineageCheck)
                 )
                 .bind("payload", jsonMapper.writeValueAsBytes(segmentId));
    }
    int[] updated = insertBatch.execute();
    return Arrays.stream(updated).sum();
  }

  private void insertPendingSegmentIntoMetastore(
      Handle handle,
      SegmentIdWithShardSpec newIdentifier,
      String dataSource,
      Interval interval,
      String previousSegmentId,
      String sequenceName,
      String sequenceNamePrevIdSha1
  ) throws JsonProcessingException
  {
    handle.createStatement(
        StringUtils.format(
            "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, sequence_name, sequence_prev_id, "
            + "sequence_name_prev_id_sha1, payload) "
            + "VALUES (:id, :dataSource, :created_date, :start, :end, :sequence_name, :sequence_prev_id, "
            + ":sequence_name_prev_id_sha1, :payload)",
            dbTables.getPendingSegmentsTable(),
            connector.getQuoteString()
        )
    )
          .bind("id", newIdentifier.toString())
          .bind("dataSource", dataSource)
          .bind("created_date", DateTimes.nowUtc().toString())
          .bind("start", interval.getStart().toString())
          .bind("end", interval.getEnd().toString())
          .bind("sequence_name", sequenceName)
          .bind("sequence_prev_id", previousSegmentId)
          .bind("sequence_name_prev_id_sha1", sequenceNamePrevIdSha1)
          .bind("payload", jsonMapper.writeValueAsBytes(newIdentifier))
          .execute();
  }

  /**
   * Creates new IDs for the given append segments if a REPLACE task started and
   * finished after these append segments had already been allocated. The newly
   * created IDs belong to the same interval and version as the segments committed
   * by the REPLACE task.
   */
  private Set<DataSegment> createNewIdsForAppendSegments(
      Handle handle,
      String dataSource,
      Set<DataSegment> segmentsToAppend
  ) throws IOException
  {
    if (segmentsToAppend.isEmpty()) {
      return Collections.emptySet();
    }

    final Set<Interval> appendIntervals = new HashSet<>();
    final TreeMap<String, Set<DataSegment>> appendVersionToSegments = new TreeMap<>();
    for (DataSegment segment : segmentsToAppend) {
      appendIntervals.add(segment.getInterval());
      appendVersionToSegments.computeIfAbsent(segment.getVersion(), v -> new HashSet<>())
                             .add(segment);
    }

    // Fetch all used segments that overlap with any of the append intervals
    final Collection<DataSegment> overlappingSegments = retrieveUsedSegmentsForIntervals(
        dataSource,
        new ArrayList<>(appendIntervals),
        Segments.INCLUDING_OVERSHADOWED
    );

    final Map<String, Set<Interval>> overlappingVersionToIntervals = new HashMap<>();
    final Map<Interval, Set<DataSegment>> overlappingIntervalToSegments = new HashMap<>();
    for (DataSegment segment : overlappingSegments) {
      overlappingVersionToIntervals.computeIfAbsent(segment.getVersion(), v -> new HashSet<>())
                                   .add(segment.getInterval());
      overlappingIntervalToSegments.computeIfAbsent(segment.getInterval(), i -> new HashSet<>())
                                   .add(segment);
    }

    final Set<DataSegment> upgradedSegments = new HashSet<>();
    for (Map.Entry<String, Set<Interval>> entry : overlappingVersionToIntervals.entrySet()) {
      final String upgradeVersion = entry.getKey();
      Map<Interval, Set<DataSegment>> segmentsToUpgrade = getSegmentsWithVersionLowerThan(
          upgradeVersion,
          entry.getValue(),
          appendVersionToSegments
      );
      for (Map.Entry<Interval, Set<DataSegment>> upgradeEntry : segmentsToUpgrade.entrySet()) {
        final Interval upgradeInterval = upgradeEntry.getKey();
        final Set<DataSegment> segmentsAlreadyOnVersion
            = overlappingIntervalToSegments.getOrDefault(upgradeInterval, Collections.emptySet())
                                           .stream()
                                           .filter(s -> s.getVersion().equals(upgradeVersion))
                                           .collect(Collectors.toSet());
        Set<DataSegment> segmentsUpgradedToVersion = createNewIdsForAppendSegmentsWithVersion(
            handle,
            upgradeVersion,
            upgradeInterval,
            upgradeEntry.getValue(),
            segmentsAlreadyOnVersion
        );
        log.info("Upgraded [%d] segments to version[%s].", segmentsUpgradedToVersion.size(), upgradeVersion);
        upgradedSegments.addAll(segmentsUpgradedToVersion);
      }
    }

    return upgradedSegments;
  }

  /**
   * Creates a Map from eligible interval to Set of segments that are fully
   * contained in that interval and have a version strictly lower than {@code #cutoffVersion}.
   */
  private Map<Interval, Set<DataSegment>> getSegmentsWithVersionLowerThan(
      String cutoffVersion,
      Set<Interval> eligibleIntervals,
      TreeMap<String, Set<DataSegment>> versionToSegments
  )
  {
    final Set<DataSegment> eligibleSegments
        = versionToSegments.headMap(cutoffVersion).values().stream()
                           .flatMap(Collection::stream)
                           .collect(Collectors.toSet());

    final Map<Interval, Set<DataSegment>> eligibleIntervalToSegments = new HashMap<>();

    for (DataSegment segment : eligibleSegments) {
      final Interval segmentInterval = segment.getInterval();
      for (Interval eligibleInterval : eligibleIntervals) {
        if (eligibleInterval.contains(segmentInterval)) {
          eligibleIntervalToSegments.computeIfAbsent(eligibleInterval, itvl -> new HashSet<>())
                                    .add(segment);
          break;
        } else if (eligibleInterval.overlaps(segmentInterval)) {
          // Committed interval overlaps only partially
          throw new ISE(
              "Committed interval[%s] conflicts with interval[%s] of append segment[%s].",
              eligibleInterval, segmentInterval, segment.getId()
          );
        }
      }
    }

    return eligibleIntervalToSegments;
  }

  /**
   * Computes new segment IDs that belong to the upgradeInterval and upgradeVersion.
   *
   * @param committedSegments Segments that already exist in the upgradeInterval
   *                          at upgradeVersion.
   */
  private Set<DataSegment> createNewIdsForAppendSegmentsWithVersion(
      Handle handle,
      String upgradeVersion,
      Interval upgradeInterval,
      Set<DataSegment> segmentsToUpgrade,
      Set<DataSegment> committedSegments
  ) throws IOException
  {
    // Find the committed segments with the higest partition number
    SegmentIdWithShardSpec committedMaxId = null;
    for (DataSegment committedSegment : committedSegments) {
      if (committedMaxId == null
          || committedMaxId.getShardSpec().getPartitionNum() < committedSegment.getShardSpec().getPartitionNum()) {
        committedMaxId = SegmentIdWithShardSpec.fromDataSegment(committedSegment);
      }
    }

    // Get pending segments for the new version to determine the next partition number to allocate
    final String dataSource = segmentsToUpgrade.iterator().next().getDataSource();
    final Set<SegmentIdWithShardSpec> pendingSegmentIds
        = getPendingSegmentsForIntervalWithHandle(handle, dataSource, upgradeInterval).keySet();
    final Set<SegmentIdWithShardSpec> allAllocatedIds = new HashSet<>(pendingSegmentIds);

    // Create new IDs for each append segment
    final Set<DataSegment> newSegmentIds = new HashSet<>();
    for (DataSegment segment : segmentsToUpgrade) {
      SegmentCreateRequest request = new SegmentCreateRequest(
          segment.getId() + "__" + upgradeVersion,
          null,
          upgradeVersion,
          NumberedPartialShardSpec.instance()
      );

      // Create new segment ID based on committed segments, allocated pending segments
      // and new IDs created so far in this method
      final SegmentIdWithShardSpec newId = createNewSegment(
          request,
          dataSource,
          upgradeInterval,
          upgradeVersion,
          committedMaxId,
          allAllocatedIds
      );

      // Update the set so that subsequent segment IDs use a higher partition number
      allAllocatedIds.add(newId);
      newSegmentIds.add(
          DataSegment.builder(segment)
                     .interval(newId.getInterval())
                     .version(newId.getVersion())
                     .shardSpec(newId.getShardSpec())
                     .build()
      );
    }

    return newSegmentIds;
  }

  private Map<SegmentCreateRequest, SegmentIdWithShardSpec> createNewSegments(
      Handle handle,
      String dataSource,
      Interval interval,
      boolean skipSegmentLineageCheck,
      List<TimelineObjectHolder<String, DataSegment>> existingChunks,
      List<SegmentCreateRequest> requests
  ) throws IOException
  {
    if (requests.isEmpty()) {
      return Collections.emptyMap();
    }

    // Shard spec of any of the requests (as they are all compatible) can be used to
    // identify existing shard specs that share partition space with the requested ones.
    final PartialShardSpec partialShardSpec = requests.get(0).getPartialShardSpec();

    // max partitionId of published data segments which share the same partition space.
    SegmentIdWithShardSpec committedMaxId = null;

    @Nullable
    final String versionOfExistingChunk;
    if (existingChunks.isEmpty()) {
      versionOfExistingChunk = null;
    } else {
      TimelineObjectHolder<String, DataSegment> existingHolder = Iterables.getOnlyElement(existingChunks);
      versionOfExistingChunk = existingHolder.getVersion();

      // Don't use the stream API for performance.
      for (DataSegment segment : FluentIterable
          .from(existingHolder.getObject())
          .transform(PartitionChunk::getObject)
          // Here we check only the segments of the shardSpec which shares the same partition space with the given
          // partialShardSpec. Note that OverwriteShardSpec doesn't share the partition space with others.
          // See PartitionIds.
          .filter(segment -> segment.getShardSpec().sharePartitionSpace(partialShardSpec))) {
        if (committedMaxId == null
            || committedMaxId.getShardSpec().getPartitionNum() < segment.getShardSpec().getPartitionNum()) {
          committedMaxId = SegmentIdWithShardSpec.fromDataSegment(segment);
        }
      }
    }


    // Fetch the pending segments for this interval to determine max partitionId
    // across all shard specs (published + pending).
    // A pending segment having a higher partitionId must also be considered
    // to avoid clashes when inserting the pending segment created here.
    final Set<SegmentIdWithShardSpec> pendingSegments =
        new HashSet<>(getPendingSegmentsForIntervalWithHandle(handle, dataSource, interval).keySet());

    final Map<SegmentCreateRequest, SegmentIdWithShardSpec> createdSegments = new HashMap<>();
    final Map<UniqueAllocateRequest, SegmentIdWithShardSpec> uniqueRequestToSegment = new HashMap<>();

    for (SegmentCreateRequest request : requests) {
      // Check if the required segment has already been created in this batch
      final UniqueAllocateRequest uniqueRequest =
          new UniqueAllocateRequest(interval, request, skipSegmentLineageCheck);

      final SegmentIdWithShardSpec createdSegment;
      if (uniqueRequestToSegment.containsKey(uniqueRequest)) {
        createdSegment = uniqueRequestToSegment.get(uniqueRequest);
      } else {
        createdSegment = createNewSegment(
            request,
            dataSource,
            interval,
            versionOfExistingChunk,
            committedMaxId,
            pendingSegments
        );

        // Add to pendingSegments to consider for partitionId
        if (createdSegment != null) {
          pendingSegments.add(createdSegment);
          uniqueRequestToSegment.put(uniqueRequest, createdSegment);
          log.info("Created new segment[%s]", createdSegment);
        }
      }

      if (createdSegment != null) {
        createdSegments.put(request, createdSegment);
      }
    }

    log.info("Created [%d] new segments for [%d] allocate requests.", uniqueRequestToSegment.size(), requests.size());
    return createdSegments;
  }

  private SegmentIdWithShardSpec createNewSegment(
      SegmentCreateRequest request,
      String dataSource,
      Interval interval,
      String versionOfExistingChunk,
      SegmentIdWithShardSpec committedMaxId,
      Set<SegmentIdWithShardSpec> pendingSegments
  )
  {
    final PartialShardSpec partialShardSpec = request.getPartialShardSpec();
    final String existingVersion = request.getVersion();
    final Set<SegmentIdWithShardSpec> mutablePendingSegments = new HashSet<>(pendingSegments);

    // Include the committedMaxId while computing the overallMaxId
    if (committedMaxId != null) {
      mutablePendingSegments.add(committedMaxId);
    }

    // If there is an existing chunk, find the max id with the same version as the existing chunk.
    // There may still be a pending segment with a higher version (but no corresponding used segments)
    // which may generate a clash with an existing segment once the new id is generated
    final SegmentIdWithShardSpec overallMaxId =
        mutablePendingSegments.stream()
                              .filter(id -> id.getShardSpec().sharePartitionSpace(partialShardSpec))
                              .filter(id -> versionOfExistingChunk == null
                                            || id.getVersion().equals(versionOfExistingChunk))
                              .max(Comparator.comparing(SegmentIdWithShardSpec::getVersion)
                                             .thenComparing(id -> id.getShardSpec().getPartitionNum()))
                              .orElse(null);

    // Determine the version of the new segment
    final String newSegmentVersion;
    if (versionOfExistingChunk != null) {
      newSegmentVersion = versionOfExistingChunk;
    } else if (overallMaxId != null) {
      newSegmentVersion = overallMaxId.getVersion();
    } else {
      // this is the first segment for this interval
      newSegmentVersion = null;
    }

    if (overallMaxId == null) {
      // When appending segments, null overallMaxId means that we are allocating the very initial
      // segment for this time chunk.
      // This code is executed when the Overlord coordinates segment allocation, which is either you append segments
      // or you use segment lock. Since the core partitions set is not determined for appended segments, we set
      // it 0. When you use segment lock, the core partitions set doesn't work with it. We simply set it 0 so that the
      // OvershadowableManager handles the atomic segment update.
      final int newPartitionId = partialShardSpec.useNonRootGenerationPartitionSpace()
                                 ? PartitionIds.NON_ROOT_GEN_START_PARTITION_ID
                                 : PartitionIds.ROOT_GEN_START_PARTITION_ID;

      String version = newSegmentVersion == null ? existingVersion : newSegmentVersion;
      return new SegmentIdWithShardSpec(
          dataSource,
          interval,
          version,
          partialShardSpec.complete(jsonMapper, newPartitionId, 0)
      );

    } else if (!overallMaxId.getInterval().equals(interval)) {
      log.warn(
          "Cannot allocate new segment for dataSource[%s], interval[%s], existingVersion[%s]: conflicting segment[%s].",
          dataSource,
          interval,
          existingVersion,
          overallMaxId
      );
      return null;
    } else if (committedMaxId != null
               && committedMaxId.getShardSpec().getNumCorePartitions()
                  == SingleDimensionShardSpec.UNKNOWN_NUM_CORE_PARTITIONS) {
      log.warn(
          "Cannot allocate new segment because of unknown core partition size of segment[%s], shardSpec[%s]",
          committedMaxId,
          committedMaxId.getShardSpec()
      );
      return null;
    } else {
      // The number of core partitions must always be chosen from the set of used segments in the SegmentTimeline.
      // When the core partitions have been dropped, using pending segments may lead to an incorrect state
      // where the chunk is believed to have core partitions and queries results are incorrect.

      return new SegmentIdWithShardSpec(
          dataSource,
          interval,
          Preconditions.checkNotNull(newSegmentVersion, "newSegmentVersion"),
          partialShardSpec.complete(
              jsonMapper,
              overallMaxId.getShardSpec().getPartitionNum() + 1,
              committedMaxId == null ? 0 : committedMaxId.getShardSpec().getNumCorePartitions()
          )
      );
    }
  }

  /**
   * This function creates a new segment for the given datasource/interval/etc. A critical
   * aspect of the creation is to make sure that the new version & new partition number will make
   * sense given the existing segments & pending segments also very important is to avoid
   * clashes with existing pending & used/unused segments.
   * @param handle Database handle
   * @param dataSource datasource for the new segment
   * @param interval interval for the new segment
   * @param partialShardSpec Shard spec info minus segment id stuff
   * @param existingVersion Version of segments in interval, used to compute the version of the very first segment in
   *                        interval
   * @return
   * @throws IOException
   */
  @Nullable
  private SegmentIdWithShardSpec createNewSegment(
      final Handle handle,
      final String dataSource,
      final Interval interval,
      final PartialShardSpec partialShardSpec,
      final String existingVersion,
      final List<TimelineObjectHolder<String, DataSegment>> existingChunks
  ) throws IOException
  {
    // max partitionId of published data segments which share the same partition space.
    SegmentIdWithShardSpec committedMaxId = null;

    @Nullable
    final String versionOfExistingChunk;
    if (existingChunks.isEmpty()) {
      versionOfExistingChunk = null;
    } else {
      TimelineObjectHolder<String, DataSegment> existingHolder = Iterables.getOnlyElement(existingChunks);
      versionOfExistingChunk = existingHolder.getVersion();

      // Don't use the stream API for performance.
      for (DataSegment segment : FluentIterable
          .from(existingHolder.getObject())
          .transform(PartitionChunk::getObject)
          // Here we check only the segments of the shardSpec which shares the same partition space with the given
          // partialShardSpec. Note that OverwriteShardSpec doesn't share the partition space with others.
          // See PartitionIds.
          .filter(segment -> segment.getShardSpec().sharePartitionSpace(partialShardSpec))) {
        if (committedMaxId == null
            || committedMaxId.getShardSpec().getPartitionNum() < segment.getShardSpec().getPartitionNum()) {
          committedMaxId = SegmentIdWithShardSpec.fromDataSegment(segment);
        }
      }
    }


    // Fetch the pending segments for this interval to determine max partitionId
    // across all shard specs (published + pending).
    // A pending segment having a higher partitionId must also be considered
    // to avoid clashes when inserting the pending segment created here.
    final Set<SegmentIdWithShardSpec> pendings = new HashSet<>(
        getPendingSegmentsForIntervalWithHandle(handle, dataSource, interval).keySet()
    );
    if (committedMaxId != null) {
      pendings.add(committedMaxId);
    }

    // If there is an existing chunk, find the max id with the same version as the existing chunk.
    // There may still be a pending segment with a higher version (but no corresponding used segments)
    // which may generate a clash with an existing segment once the new id is generated
    final SegmentIdWithShardSpec overallMaxId;
    overallMaxId = pendings.stream()
                           .filter(id -> id.getShardSpec().sharePartitionSpace(partialShardSpec))
                           .filter(id -> versionOfExistingChunk == null
                                         || id.getVersion().equals(versionOfExistingChunk))
                           .max(Comparator.comparing(SegmentIdWithShardSpec::getVersion)
                                          .thenComparing(id -> id.getShardSpec().getPartitionNum()))
                           .orElse(null);


    // Determine the version of the new segment
    final String newSegmentVersion;
    if (versionOfExistingChunk != null) {
      newSegmentVersion = versionOfExistingChunk;
    } else if (overallMaxId != null) {
      newSegmentVersion = overallMaxId.getVersion();
    } else {
      // this is the first segment for this interval
      newSegmentVersion = null;
    }

    if (overallMaxId == null) {
      // When appending segments, null overallMaxId means that we are allocating the very initial
      // segment for this time chunk.
      // This code is executed when the Overlord coordinates segment allocation, which is either you append segments
      // or you use segment lock. Since the core partitions set is not determined for appended segments, we set
      // it 0. When you use segment lock, the core partitions set doesn't work with it. We simply set it 0 so that the
      // OvershadowableManager handles the atomic segment update.
      final int newPartitionId = partialShardSpec.useNonRootGenerationPartitionSpace()
                                 ? PartitionIds.NON_ROOT_GEN_START_PARTITION_ID
                                 : PartitionIds.ROOT_GEN_START_PARTITION_ID;
      String version = newSegmentVersion == null ? existingVersion : newSegmentVersion;
      return new SegmentIdWithShardSpec(
          dataSource,
          interval,
          version,
          partialShardSpec.complete(jsonMapper, newPartitionId, 0)
      );
    } else if (!overallMaxId.getInterval().equals(interval)) {
      log.warn(
          "Cannot allocate new segment for dataSource[%s], interval[%s], existingVersion[%s]: conflicting segment[%s].",
          dataSource,
          interval,
          existingVersion,
          overallMaxId
      );
      return null;
    } else if (committedMaxId != null
               && committedMaxId.getShardSpec().getNumCorePartitions()
                  == SingleDimensionShardSpec.UNKNOWN_NUM_CORE_PARTITIONS) {
      log.warn(
          "Cannot allocate new segment because of unknown core partition size of segment[%s], shardSpec[%s]",
          committedMaxId,
          committedMaxId.getShardSpec()
      );
      return null;
    } else {
      // The number of core partitions must always be chosen from the set of used segments in the SegmentTimeline.
      // When the core partitions have been dropped, using pending segments may lead to an incorrect state
      // where the chunk is believed to have core partitions and queries results are incorrect.

      return new SegmentIdWithShardSpec(
          dataSource,
          interval,
          Preconditions.checkNotNull(newSegmentVersion, "newSegmentVersion"),
          partialShardSpec.complete(
              jsonMapper,
              overallMaxId.getShardSpec().getPartitionNum() + 1,
              committedMaxId == null ? 0 : committedMaxId.getShardSpec().getNumCorePartitions()
          )
      );
    }
  }

  @Override
  public int deletePendingSegmentsCreatedInInterval(String dataSource, Interval deleteInterval)
  {
    return connector.getDBI().inTransaction(
        (handle, status) -> handle
            .createStatement(
                StringUtils.format(
                    "DELETE FROM %s WHERE datasource = :dataSource AND created_date >= :start AND created_date < :end",
                    dbTables.getPendingSegmentsTable()
                )
            )
            .bind("dataSource", dataSource)
            .bind("start", deleteInterval.getStart().toString())
            .bind("end", deleteInterval.getEnd().toString())
            .execute()
    );
  }

  @Override
  public int deletePendingSegments(String dataSource)
  {
    return connector.getDBI().inTransaction(
        (handle, status) -> handle
            .createStatement(
                StringUtils.format("DELETE FROM %s WHERE datasource = :dataSource", dbTables.getPendingSegmentsTable())
            )
            .bind("dataSource", dataSource)
            .execute()
    );
  }

  private Set<DataSegment> announceHistoricalSegmentBatch(
      final Handle handle,
      final Set<DataSegment> segments,
      final Set<DataSegment> usedSegments
  ) throws IOException
  {
    final Set<DataSegment> toInsertSegments = new HashSet<>();
    try {
      Set<String> existedSegments = segmentExistsBatch(handle, segments);
      log.info("Found these segments already exist in DB: %s", existedSegments);
      for (DataSegment segment : segments) {
        if (!existedSegments.contains(segment.getId().toString())) {
          toInsertSegments.add(segment);
        }
      }

      // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
      // Avoiding ON DUPLICATE KEY since it's not portable.
      // Avoiding try/catch since it may cause inadvertent transaction-splitting.
      final List<List<DataSegment>> partitionedSegments = Lists.partition(
          new ArrayList<>(toInsertSegments),
          MAX_NUM_SEGMENTS_TO_ANNOUNCE_AT_ONCE
      );

      PreparedBatch preparedBatch = handle.prepareBatch(buildSqlToInsertSegments());
      for (List<DataSegment> partition : partitionedSegments) {
        for (DataSegment segment : partition) {
          final String now = DateTimes.nowUtc().toString();
          preparedBatch.add()
              .bind("id", segment.getId().toString())
              .bind("dataSource", segment.getDataSource())
              .bind("created_date", now)
              .bind("start", segment.getInterval().getStart().toString())
              .bind("end", segment.getInterval().getEnd().toString())
              .bind("partitioned", (segment.getShardSpec() instanceof NoneShardSpec) ? false : true)
              .bind("version", segment.getVersion())
              .bind("used", usedSegments.contains(segment))
              .bind("payload", jsonMapper.writeValueAsBytes(segment))
              .bind("used_status_last_updated", now);
        }
        final int[] affectedRows = preparedBatch.execute();
        final boolean succeeded = Arrays.stream(affectedRows).allMatch(eachAffectedRows -> eachAffectedRows == 1);
        if (succeeded) {
          log.infoSegments(partition, "Published segments to DB");
        } else {
          final List<DataSegment> failedToPublish = IntStream.range(0, partition.size())
              .filter(i -> affectedRows[i] != 1)
              .mapToObj(partition::get)
              .collect(Collectors.toList());
          throw new ISE(
              "Failed to publish segments to DB: %s",
              SegmentUtils.commaSeparatedIdentifiers(failedToPublish)
          );
        }
      }
    }
    catch (Exception e) {
      log.errorSegments(segments, "Exception inserting segments");
      throw e;
    }

    return toInsertSegments;
  }

  /**
   * Creates new versions of segments appended while a REPLACE task was in progress.
   */
  private Set<DataSegment> createNewIdsOfAppendSegmentsAfterReplace(
      final Handle handle,
      final Set<DataSegment> replaceSegments,
      final Set<ReplaceTaskLock> locksHeldByReplaceTask
  )
  {
    // If a REPLACE task has locked an interval, it would commit some segments
    // (or at least tombstones) in that interval (except in LEGACY_REPLACE ingestion mode)
    if (replaceSegments.isEmpty() || locksHeldByReplaceTask.isEmpty()) {
      return Collections.emptySet();
    }

    // For each replace interval, find the number of core partitions and total partitions
    final Map<Interval, Integer> intervalToNumCorePartitions = new HashMap<>();
    final Map<Interval, Integer> intervalToCurrentPartitionNum = new HashMap<>();
    for (DataSegment segment : replaceSegments) {
      intervalToNumCorePartitions.put(segment.getInterval(), segment.getShardSpec().getNumCorePartitions());

      int partitionNum = segment.getShardSpec().getPartitionNum();
      intervalToCurrentPartitionNum.compute(
          segment.getInterval(),
          (i, value) -> value == null ? partitionNum : Math.max(value, partitionNum)
      );
    }

    // Find the segments that need to be upgraded
    final String taskId = locksHeldByReplaceTask.stream()
                                                .map(ReplaceTaskLock::getSupervisorTaskId)
                                                .findFirst().orElse(null);
    final Map<String, String> upgradeSegmentToLockVersion
        = getAppendSegmentsCommittedDuringTask(handle, taskId);
    final List<DataSegment> segmentsToUpgrade
        = retrieveSegmentsById(handle, upgradeSegmentToLockVersion.keySet());

    if (segmentsToUpgrade.isEmpty()) {
      return Collections.emptySet();
    }

    final Set<Interval> replaceIntervals = intervalToNumCorePartitions.keySet();

    final Set<DataSegment> upgradedSegments = new HashSet<>();
    for (DataSegment oldSegment : segmentsToUpgrade) {
      // Determine interval of the upgraded segment
      final Interval oldInterval = oldSegment.getInterval();
      Interval newInterval = null;
      for (Interval replaceInterval : replaceIntervals) {
        if (replaceInterval.contains(oldInterval)) {
          newInterval = replaceInterval;
          break;
        } else if (replaceInterval.overlaps(oldInterval)) {
          throw new ISE(
              "Incompatible segment intervals for commit: [%s] and [%s].",
              oldInterval, replaceInterval
          );
        }
      }

      if (newInterval == null) {
        // This can happen only if no replace interval contains this segment
        // but a (revoked) REPLACE lock covers this segment
        newInterval = oldInterval;
      }

      // Compute shard spec of the upgraded segment
      final int partitionNum = intervalToCurrentPartitionNum.compute(
          newInterval,
          (i, value) -> value == null ? 0 : value + 1
      );
      final int numCorePartitions = intervalToNumCorePartitions.get(newInterval);
      ShardSpec shardSpec = new NumberedShardSpec(partitionNum, numCorePartitions);

      // Create upgraded segment with the correct interval, version and shard spec
      String lockVersion = upgradeSegmentToLockVersion.get(oldSegment.getId().toString());
      upgradedSegments.add(
          DataSegment.builder(oldSegment)
                     .interval(newInterval)
                     .version(lockVersion)
                     .shardSpec(shardSpec)
                     .build()
      );
    }

    return upgradedSegments;
  }

  /**
   * Verifies that:
   * <ul>
   * <li>The set of segments being committed is non-empty.</li>
   * <li>All segments belong to the same datasource.</li>
   * </ul>
   */
  private void verifySegmentsToCommit(Collection<DataSegment> segments)
  {
    if (segments.isEmpty()) {
      throw new IllegalArgumentException("No segment to commit");
    }

    final String dataSource = segments.iterator().next().getDataSource();
    for (DataSegment segment : segments) {
      if (!dataSource.equals(segment.getDataSource())) {
        throw new IllegalArgumentException("Segments to commit must all belong to the same datasource");
      }
    }
  }

  /**
   * Inserts the given segments into the DB in batches of size
   * {@link #MAX_NUM_SEGMENTS_TO_ANNOUNCE_AT_ONCE} and returns the set of
   * segments actually inserted.
   * <p>
   * This method avoids inserting segment IDs which already exist in the DB.
   * Callers of this method might need to retry as INSERT followed by SELECT
   * might fail due to race conditions.
   */
  private Set<DataSegment> insertSegments(Handle handle, Set<DataSegment> segments)
      throws IOException
  {
    // Do not insert segment IDs which already exist
    Set<String> existingSegmentIds = segmentExistsBatch(handle, segments);
    final Set<DataSegment> segmentsToInsert = segments.stream().filter(
        s -> !existingSegmentIds.contains(s.getId().toString())
    ).collect(Collectors.toSet());

    // Insert the segments in batches of manageable size
    final List<List<DataSegment>> partitionedSegments = Lists.partition(
        new ArrayList<>(segmentsToInsert),
        MAX_NUM_SEGMENTS_TO_ANNOUNCE_AT_ONCE
    );

    final PreparedBatch batch = handle.prepareBatch(buildSqlToInsertSegments());
    for (List<DataSegment> partition : partitionedSegments) {
      for (DataSegment segment : partition) {
        final String now = DateTimes.nowUtc().toString();
        batch.add()
             .bind("id", segment.getId().toString())
             .bind("dataSource", segment.getDataSource())
             .bind("created_date", now)
             .bind("start", segment.getInterval().getStart().toString())
             .bind("end", segment.getInterval().getEnd().toString())
             .bind("partitioned", (segment.getShardSpec() instanceof NoneShardSpec) ? false : true)
             .bind("version", segment.getVersion())
             .bind("used", true)
             .bind("payload", jsonMapper.writeValueAsBytes(segment))
             .bind("used_status_last_updated", now);
      }

      final int[] affectedRows = batch.execute();

      final List<DataSegment> failedInserts = new ArrayList<>();
      for (int i = 0; i < partition.size(); ++i) {
        if (affectedRows[i] != 1) {
          failedInserts.add(partition.get(i));
        }
      }
      if (failedInserts.isEmpty()) {
        log.infoSegments(partition, "Published segments to DB");
      } else {
        throw new ISE(
            "Failed to publish segments to DB: %s",
            SegmentUtils.commaSeparatedIdentifiers(failedInserts)
        );
      }
    }

    return segmentsToInsert;
  }

  /**
   * Inserts entries into the upgrade_segments table in batches of size
   * {@link #MAX_NUM_SEGMENTS_TO_ANNOUNCE_AT_ONCE}.
   */
  private void insertIntoUpgradeSegmentsTable(
      Handle handle,
      Map<DataSegment, ReplaceTaskLock> segmentToReplaceLock
  )
  {
    if (segmentToReplaceLock.isEmpty()) {
      return;
    }

    final PreparedBatch batch = handle.prepareBatch(
        StringUtils.format(
            "INSERT INTO %1$s (task_id, segment_id, lock_version)"
            + " VALUES (:task_id, :segment_id, :lock_version)",
            dbTables.getUpgradeSegmentsTable()
        )
    );

    final List<List<Map.Entry<DataSegment, ReplaceTaskLock>>> partitions = Lists.partition(
        new ArrayList<>(segmentToReplaceLock.entrySet()),
        MAX_NUM_SEGMENTS_TO_ANNOUNCE_AT_ONCE
    );
    for (List<Map.Entry<DataSegment, ReplaceTaskLock>> partition : partitions) {
      for (Map.Entry<DataSegment, ReplaceTaskLock> entry : partition) {
        DataSegment segment = entry.getKey();
        ReplaceTaskLock lock = entry.getValue();
        batch.add()
             .bind("task_id", lock.getSupervisorTaskId())
             .bind("segment_id", segment.getId().toString())
             .bind("lock_version", lock.getVersion());
      }
      final int[] affectedAppendRows = batch.execute();

      final List<DataSegment> failedInserts = new ArrayList<>();
      for (int i = 0; i < partition.size(); ++i) {
        if (affectedAppendRows[i] != 1) {
          failedInserts.add(partition.get(i).getKey());
        }
      }
      if (failedInserts.size() > 0) {
        throw new ISE(
            "Failed to insert upgrade segments in DB: %s",
            SegmentUtils.commaSeparatedIdentifiers(failedInserts)
        );
      }
    }
  }

  private List<DataSegment> retrieveSegmentsById(Handle handle, Set<String> segmentIds)
  {
    if (segmentIds.isEmpty()) {
      return Collections.emptyList();
    }

    final String segmentIdCsv = segmentIds.stream()
                                          .map(id -> "'" + id + "'")
                                          .collect(Collectors.joining(","));
    ResultIterator<DataSegment> resultIterator = handle
        .createQuery(
            StringUtils.format(
                "SELECT payload FROM %s WHERE id in (%s)",
                dbTables.getSegmentsTable(), segmentIdCsv
            )
        )
        .setFetchSize(connector.getStreamingFetchSize())
        .map(
            (index, r, ctx) ->
                JacksonUtils.readValue(jsonMapper, r.getBytes(1), DataSegment.class)
        )
        .iterator();

    return Lists.newArrayList(resultIterator);
  }

  private String buildSqlToInsertSegments()
  {
    return StringUtils.format(
        "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s,"
        + " partitioned, version, used, payload, used_status_last_updated) "
        + "VALUES (:id, :dataSource, :created_date, :start, :end,"
        + " :partitioned, :version, :used, :payload, :used_status_last_updated)",
        dbTables.getSegmentsTable(),
        connector.getQuoteString()
    );
  }

  /**
   * Finds the append segments that were covered by the given task REPLACE locks.
   * These append segments must now be upgraded to the same version as the segments
   * being committed by this replace task.
   *
   * @return Map from append Segment ID to REPLACE lock version
   */
  private Map<String, String> getAppendSegmentsCommittedDuringTask(
      Handle handle,
      String taskId
  )
  {
    final String sql = StringUtils.format(
        "SELECT segment_id, lock_version FROM %1$s WHERE task_id = :task_id",
        dbTables.getUpgradeSegmentsTable()
    );

    ResultIterator<Pair<String, String>> resultIterator = handle
        .createQuery(sql)
        .bind("task_id", taskId)
        .map(
            (index, r, ctx) -> Pair.of(r.getString("segment_id"), r.getString("lock_version"))
        )
        .iterator();

    final Map<String, String> segmentIdToLockVersion = new HashMap<>();
    while (resultIterator.hasNext()) {
      Pair<String, String> result = resultIterator.next();
      segmentIdToLockVersion.put(result.lhs, result.rhs);
    }
    return segmentIdToLockVersion;
  }

  private Set<String> segmentExistsBatch(final Handle handle, final Set<DataSegment> segments)
  {
    Set<String> existedSegments = new HashSet<>();

    List<List<DataSegment>> segmentsLists = Lists.partition(new ArrayList<>(segments), MAX_NUM_SEGMENTS_TO_ANNOUNCE_AT_ONCE);
    for (List<DataSegment> segmentList : segmentsLists) {
      String segmentIds = segmentList.stream()
          .map(segment -> "'" + StringEscapeUtils.escapeSql(segment.getId().toString()) + "'")
          .collect(Collectors.joining(","));
      List<String> existIds = handle.createQuery(StringUtils.format("SELECT id FROM %s WHERE id in (%s)", dbTables.getSegmentsTable(), segmentIds))
          .mapTo(String.class)
          .list();
      existedSegments.addAll(existIds);
    }
    return existedSegments;
  }

  /**
   * Read dataSource metadata. Returns null if there is no metadata.
   */
  @Override
  public @Nullable DataSourceMetadata retrieveDataSourceMetadata(final String dataSource)
  {
    final byte[] bytes = connector.lookup(
        dbTables.getDataSourceTable(),
        "dataSource",
        "commit_metadata_payload",
        dataSource
    );

    if (bytes == null) {
      return null;
    }

    return JacksonUtils.readValue(jsonMapper, bytes, DataSourceMetadata.class);
  }

  /**
   * Read dataSource metadata as bytes, from a specific handle. Returns null if there is no metadata.
   */
  private @Nullable byte[] retrieveDataSourceMetadataWithHandleAsBytes(
      final Handle handle,
      final String dataSource
  )
  {
    return connector.lookupWithHandle(
        handle,
        dbTables.getDataSourceTable(),
        "dataSource",
        "commit_metadata_payload",
        dataSource
    );
  }

  /**
   * Compare-and-swap dataSource metadata in a transaction. This will only modify dataSource metadata if it equals
   * oldCommitMetadata when this function is called (based on T.equals). This method is idempotent in that if
   * the metadata already equals newCommitMetadata, it will return true.
   *
   * @param handle        database handle
   * @param dataSource    druid dataSource
   * @param startMetadata dataSource metadata pre-insert must match this startMetadata according to
   *                      {@link DataSourceMetadata#matches(DataSourceMetadata)}
   * @param endMetadata   dataSource metadata post-insert will have this endMetadata merged in with
   *                      {@link DataSourceMetadata#plus(DataSourceMetadata)}
   *
   * @return SUCCESS if dataSource metadata was updated from matching startMetadata to matching endMetadata, FAILURE or
   * TRY_AGAIN if it definitely was not updated. This guarantee is meant to help
   * {@link #commitSegmentsAndMetadata} achieve its own guarantee.
   *
   * @throws RuntimeException if state is unknown after this call
   */
  protected DataStoreMetadataUpdateResult updateDataSourceMetadataWithHandle(
      final Handle handle,
      final String dataSource,
      final DataSourceMetadata startMetadata,
      final DataSourceMetadata endMetadata
  ) throws IOException
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(startMetadata, "startMetadata");
    Preconditions.checkNotNull(endMetadata, "endMetadata");

    final byte[] oldCommitMetadataBytesFromDb = retrieveDataSourceMetadataWithHandleAsBytes(handle, dataSource);
    final String oldCommitMetadataSha1FromDb;
    final DataSourceMetadata oldCommitMetadataFromDb;

    if (oldCommitMetadataBytesFromDb == null) {
      oldCommitMetadataSha1FromDb = null;
      oldCommitMetadataFromDb = null;
    } else {
      oldCommitMetadataSha1FromDb = BaseEncoding.base16().encode(
          Hashing.sha1().hashBytes(oldCommitMetadataBytesFromDb).asBytes()
      );
      oldCommitMetadataFromDb = jsonMapper.readValue(oldCommitMetadataBytesFromDb, DataSourceMetadata.class);
    }

    final boolean startMetadataMatchesExisting;
    int startMetadataGreaterThanExisting = 0;

    if (oldCommitMetadataFromDb == null) {
      startMetadataMatchesExisting = startMetadata.isValidStart();
      startMetadataGreaterThanExisting = 1;
    } else {
      // Checking against the last committed metadata.
      // If the new start sequence number is greater than the end sequence number of last commit compareTo() function will return 1,
      // 0 in all other cases. It might be because multiple tasks are publishing the sequence at around same time.
      if (startMetadata instanceof Comparable) {
        startMetadataGreaterThanExisting = ((Comparable) startMetadata.asStartMetadata()).compareTo(oldCommitMetadataFromDb.asStartMetadata());
      }

      // Converting the last one into start metadata for checking since only the same type of metadata can be matched.
      // Even though kafka/kinesis indexing services use different sequenceNumber types for representing
      // start and end sequenceNumbers, the below conversion is fine because the new start sequenceNumbers are supposed
      // to be same with end sequenceNumbers of the last commit.
      startMetadataMatchesExisting = startMetadata.asStartMetadata().matches(oldCommitMetadataFromDb.asStartMetadata());
    }

    if (startMetadataGreaterThanExisting == 1 && !startMetadataMatchesExisting) {
      // Offset stored in StartMetadata is Greater than the last commited metadata,
      // Then retry multiple task might be trying to publish the segment for same partitions.
      log.info("Failed to update the metadata Store. The new start metadata: [%s] is ahead of last commited end state: [%s].",
          startMetadata,
          oldCommitMetadataFromDb);
      return new DataStoreMetadataUpdateResult(true, false,
          "Failed to update the metadata Store. The new start metadata is ahead of last commited end state."
      );
    }

    if (!startMetadataMatchesExisting) {
      // Not in the desired start state.
      return new DataStoreMetadataUpdateResult(true, false, StringUtils.format(
          "Inconsistent metadata state. This can happen if you update input topic in a spec without changing " +
          "the supervisor name. Stored state: [%s], Target state: [%s].",
          oldCommitMetadataFromDb,
          startMetadata
      ));
    }

    // Only endOffsets should be stored in metadata store
    final DataSourceMetadata newCommitMetadata = oldCommitMetadataFromDb == null
                                                 ? endMetadata
                                                 : oldCommitMetadataFromDb.plus(endMetadata);
    final byte[] newCommitMetadataBytes = jsonMapper.writeValueAsBytes(newCommitMetadata);
    final String newCommitMetadataSha1 = BaseEncoding.base16().encode(
        Hashing.sha1().hashBytes(newCommitMetadataBytes).asBytes()
    );

    final DataStoreMetadataUpdateResult retVal;
    if (oldCommitMetadataBytesFromDb == null) {
      // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
      final int numRows = handle.createStatement(
          StringUtils.format(
              "INSERT INTO %s (dataSource, created_date, commit_metadata_payload, commit_metadata_sha1) "
              + "VALUES (:dataSource, :created_date, :commit_metadata_payload, :commit_metadata_sha1)",
              dbTables.getDataSourceTable()
          )
      )
                                .bind("dataSource", dataSource)
                                .bind("created_date", DateTimes.nowUtc().toString())
                                .bind("commit_metadata_payload", newCommitMetadataBytes)
                                .bind("commit_metadata_sha1", newCommitMetadataSha1)
                                .execute();

      retVal = numRows == 1
          ? DataStoreMetadataUpdateResult.SUCCESS
          : new DataStoreMetadataUpdateResult(
              true,
          true,
          "Failed to insert metadata for datasource [%s]",
          dataSource);
    } else {
      // Expecting a particular old metadata; use the SHA1 in a compare-and-swap UPDATE
      final int numRows = handle.createStatement(
          StringUtils.format(
              "UPDATE %s SET "
              + "commit_metadata_payload = :new_commit_metadata_payload, "
              + "commit_metadata_sha1 = :new_commit_metadata_sha1 "
              + "WHERE dataSource = :dataSource AND commit_metadata_sha1 = :old_commit_metadata_sha1",
              dbTables.getDataSourceTable()
          )
      )
                                .bind("dataSource", dataSource)
                                .bind("old_commit_metadata_sha1", oldCommitMetadataSha1FromDb)
                                .bind("new_commit_metadata_payload", newCommitMetadataBytes)
                                .bind("new_commit_metadata_sha1", newCommitMetadataSha1)
                                .execute();

      retVal = numRows == 1
          ? DataStoreMetadataUpdateResult.SUCCESS
          : new DataStoreMetadataUpdateResult(
          true,
          true,
          "Failed to update metadata for datasource [%s]",
          dataSource);
    }

    if (retVal.isSuccess()) {
      log.info("Updated metadata from[%s] to[%s].", oldCommitMetadataFromDb, newCommitMetadata);
    } else {
      log.info("Not updating metadata, compare-and-swap failure.");
    }

    return retVal;
  }

  @Override
  public boolean deleteDataSourceMetadata(final String dataSource)
  {
    return connector.retryWithHandle(
        new HandleCallback<Boolean>()
        {
          @Override
          public Boolean withHandle(Handle handle)
          {
            int rows = handle.createStatement(
                StringUtils.format("DELETE from %s WHERE dataSource = :dataSource", dbTables.getDataSourceTable())
            )
                             .bind("dataSource", dataSource)
                             .execute();

            return rows > 0;
          }
        }
    );
  }

  @Override
  public boolean resetDataSourceMetadata(final String dataSource, final DataSourceMetadata dataSourceMetadata)
      throws IOException
  {
    final byte[] newCommitMetadataBytes = jsonMapper.writeValueAsBytes(dataSourceMetadata);
    final String newCommitMetadataSha1 = BaseEncoding.base16().encode(
        Hashing.sha1().hashBytes(newCommitMetadataBytes).asBytes()
    );

    return connector.retryWithHandle(
        new HandleCallback<Boolean>()
        {
          @Override
          public Boolean withHandle(Handle handle)
          {
            final int numRows = handle.createStatement(
                StringUtils.format(
                    "UPDATE %s SET "
                    + "commit_metadata_payload = :new_commit_metadata_payload, "
                    + "commit_metadata_sha1 = :new_commit_metadata_sha1 "
                    + "WHERE dataSource = :dataSource",
                    dbTables.getDataSourceTable()
                )
            )
                                      .bind("dataSource", dataSource)
                                      .bind("new_commit_metadata_payload", newCommitMetadataBytes)
                                      .bind("new_commit_metadata_sha1", newCommitMetadataSha1)
                                      .execute();
            return numRows == 1;
          }
        }
    );
  }

  @Override
  public void updateSegmentMetadata(final Set<DataSegment> segments)
  {
    connector.getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            for (final DataSegment segment : segments) {
              updatePayload(handle, segment);
            }

            return null;
          }
        }
    );
  }

  @Override
  public void deleteSegments(final Set<DataSegment> segments)
  {
    if (segments.isEmpty()) {
      log.info("No segments to delete.");
      return;
    }

    final String deleteSql = StringUtils.format("DELETE from %s WHERE id = :id", dbTables.getSegmentsTable());
    final String dataSource = segments.stream().findFirst().map(DataSegment::getDataSource).get();

    // generate the IDs outside the transaction block
    final List<String> ids = segments.stream().map(s -> s.getId().toString()).collect(Collectors.toList());

    int numDeletedSegments = connector.getDBI().inTransaction((handle, transactionStatus) -> {
          final PreparedBatch batch = handle.prepareBatch(deleteSql);

          for (final String id : ids) {
            batch.bind("id", id).add();
          }

          int[] deletedRows = batch.execute();
          return Arrays.stream(deletedRows).sum();
        }
    );

    log.debugSegments(segments, "Delete the metadata of segments");
    log.info("Deleted [%d] segments from metadata storage for dataSource [%s].", numDeletedSegments, dataSource);
  }

  private void updatePayload(final Handle handle, final DataSegment segment) throws IOException
  {
    try {
      handle
          .createStatement(
              StringUtils.format("UPDATE %s SET payload = :payload WHERE id = :id", dbTables.getSegmentsTable())
          )
          .bind("id", segment.getId().toString())
          .bind("payload", jsonMapper.writeValueAsBytes(segment))
          .execute();
    }
    catch (IOException e) {
      log.error(e, "Exception inserting into DB");
      throw e;
    }
  }

  @Override
  public boolean insertDataSourceMetadata(String dataSource, DataSourceMetadata metadata)
  {
    return 1 == connector.getDBI().inTransaction(
        (handle, status) -> handle
            .createStatement(
                StringUtils.format(
                    "INSERT INTO %s (dataSource, created_date, commit_metadata_payload, commit_metadata_sha1) VALUES" +
                    " (:dataSource, :created_date, :commit_metadata_payload, :commit_metadata_sha1)",
                    dbTables.getDataSourceTable()
                )
            )
            .bind("dataSource", dataSource)
            .bind("created_date", DateTimes.nowUtc().toString())
            .bind("commit_metadata_payload", jsonMapper.writeValueAsBytes(metadata))
            .bind("commit_metadata_sha1", BaseEncoding.base16().encode(
                Hashing.sha1().hashBytes(jsonMapper.writeValueAsBytes(metadata)).asBytes()))
            .execute()
    );
  }

  @Override
  public int removeDataSourceMetadataOlderThan(long timestamp, @NotNull Set<String> excludeDatasources)
  {
    DateTime dateTime = DateTimes.utc(timestamp);
    List<String> datasourcesToDelete = connector.getDBI().withHandle(
        handle -> handle
            .createQuery(
                StringUtils.format(
                    "SELECT dataSource FROM %1$s WHERE created_date < '%2$s'",
                    dbTables.getDataSourceTable(),
                    dateTime.toString()
                )
            )
            .mapTo(String.class)
            .list()
    );
    datasourcesToDelete.removeAll(excludeDatasources);
    return connector.getDBI().withHandle(
        handle -> {
          final PreparedBatch batch = handle.prepareBatch(
              StringUtils.format(
                  "DELETE FROM %1$s WHERE dataSource = :dataSource AND created_date < '%2$s'",
                  dbTables.getDataSourceTable(),
                  dateTime.toString()
              )
          );
          for (String datasource : datasourcesToDelete) {
            batch.bind("dataSource", datasource).add();
          }
          int[] result = batch.execute();
          return IntStream.of(result).sum();
        }
    );
  }

  @Override
  public DataSegment retrieveSegmentForId(final String id, boolean includeUnused)
  {
    return connector.retryTransaction(
        (handle, status) -> {
          if (includeUnused) {
            return SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables, jsonMapper)
                                           .retrieveSegmentForId(id);
          } else {
            return SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables, jsonMapper)
                                           .retrieveUsedSegmentForId(id);
          }
        },
        3,
        SQLMetadataConnector.DEFAULT_MAX_TRIES
    );
  }

  @Override
  public int deleteUpgradeSegmentsForTask(final String taskId)
  {
    return connector.getDBI().inTransaction(
        (handle, status) -> handle
            .createStatement(
                StringUtils.format(
                    "DELETE FROM %s WHERE task_id = :task_id",
                    dbTables.getUpgradeSegmentsTable()
                )
            )
            .bind("task_id", taskId)
            .execute()
    );
  }

  private static class PendingSegmentsRecord
  {
    private final String sequenceName;
    private final byte[] payload;

    /**
     * The columns expected in the result set are:
     * <ol>
     *   <li>sequence_name</li>
     *   <li>payload</li>
     * </ol>
     */
    static PendingSegmentsRecord fromResultSet(ResultSet resultSet)
    {
      try {
        return new PendingSegmentsRecord(
            resultSet.getString(1),
            resultSet.getBytes(2)
        );
      }
      catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    PendingSegmentsRecord(String sequenceName, byte[] payload)
    {
      this.payload = payload;
      this.sequenceName = sequenceName;
    }

    public byte[] getPayload()
    {
      return payload;
    }

    public String getSequenceName()
    {
      return sequenceName;
    }
  }

  public static class DataStoreMetadataUpdateResult
  {
    private final boolean failed;
    private final boolean canRetry;
    @Nullable private final String errorMsg;

    public static final DataStoreMetadataUpdateResult SUCCESS = new DataStoreMetadataUpdateResult(false, false, null);

    DataStoreMetadataUpdateResult(boolean failed, boolean canRetry, @Nullable String errorMsg, Object... errorFormatArgs)
    {
      this.failed = failed;
      this.canRetry = canRetry;
      this.errorMsg = null == errorMsg ? null : StringUtils.format(errorMsg, errorFormatArgs);

    }

    public boolean isFailed()
    {
      return failed;
    }

    public boolean isSuccess()
    {
      return !failed;
    }

    public boolean canRetry()
    {
      return canRetry;
    }

    @Nullable
    public String getErrorMsg()
    {
      return errorMsg;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DataStoreMetadataUpdateResult that = (DataStoreMetadataUpdateResult) o;
      return failed == that.failed && canRetry == that.canRetry && Objects.equals(errorMsg, that.errorMsg);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(failed, canRetry, errorMsg);
    }

    @Override
    public String toString()
    {
      return "DataStoreMetadataUpdateResult{" +
          "failed=" + failed +
          ", canRetry=" + canRetry +
          ", errorMsg='" + errorMsg + '\'' +
          '}';
    }
  }

}
