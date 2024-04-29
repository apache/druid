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
import org.apache.druid.error.InvalidInput;
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
import org.apache.druid.segment.SegmentMetadata;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.NoneShardSpec;
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
import org.skife.jdbi.v2.PreparedBatchPart;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;
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
  private final SegmentSchemaManager segmentSchemaManager;
  private final CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;
  private final boolean schemaPersistEnabled;

  @Inject
  public IndexerSQLMetadataStorageCoordinator(
      ObjectMapper jsonMapper,
      MetadataStorageTablesConfig dbTables,
      SQLMetadataConnector connector,
      SegmentSchemaManager segmentSchemaManager,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    this.jsonMapper = jsonMapper;
    this.dbTables = dbTables;
    this.connector = connector;
    this.segmentSchemaManager = segmentSchemaManager;
    this.centralizedDatasourceSchemaConfig = centralizedDatasourceSchemaConfig;
    this.schemaPersistEnabled =
        centralizedDatasourceSchemaConfig.isEnabled()
        && !centralizedDatasourceSchemaConfig.isTaskSchemaPublishDisabled();
  }

  @LifecycleStart
  public void start()
  {
    connector.createDataSourceTable();
    connector.createPendingSegmentsTable();
    if (centralizedDatasourceSchemaConfig.isEnabled()) {
      connector.createSegmentSchemasTable();
    }
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

    queryBuilder.append(
        SqlSegmentsMetadataQuery.getConditionForIntervalsAndMatchMode(
            compareIntervalEndpointsAsString ? intervals : Collections.emptyList(),
            intervalMode,
            connector.getQuoteString()
        )
    );

    final String queryString = StringUtils.format(queryBuilder.toString(), dbTables.getSegmentsTable());
    return connector.retryWithHandle(
        handle -> {
          Query<Map<String, Object>> query = handle
              .createQuery(queryString)
              .bind("dataSource", dataSource);

          if (compareIntervalEndpointsAsString) {
            SqlSegmentsMetadataQuery.bindIntervalsToQuery(query, intervals);
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
      @Nullable List<String> versions,
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
                                               versions,
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

    log.info("Found [%,d] unused segments for datasource[%s] in interval[%s] and versions[%s] with maxUsedStatusLastUpdatedTime[%s].",
             matchingSegments.size(), dataSource, interval, versions, maxUsedStatusLastUpdatedTime);
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
   * Fetches all the pending segments, whose interval overlaps with the given search interval, from the metadata store.
   */
  @VisibleForTesting
  List<PendingSegmentRecord> getPendingSegmentsForIntervalWithHandle(
      final Handle handle,
      final String dataSource,
      final Interval interval
  )
  {
    final boolean compareIntervalEndpointsAsStrings = Intervals.canCompareEndpointsAsStrings(interval);

    String sql = "SELECT payload, sequence_name, sequence_prev_id, task_allocator_id, upgraded_from_segment_id"
                 + " FROM " + dbTables.getPendingSegmentsTable()
                 + " WHERE dataSource = :dataSource";
    if (compareIntervalEndpointsAsStrings) {
      sql = sql
            + " AND start < :end"
            + StringUtils.format(" AND %1$send%1$s > :start", connector.getQuoteString());
    }

    Query<Map<String, Object>> query = handle.createQuery(sql)
                                             .bind("dataSource", dataSource);
    if (compareIntervalEndpointsAsStrings) {
      query = query.bind("start", interval.getStart().toString())
                   .bind("end", interval.getEnd().toString());
    }


    final ResultIterator<PendingSegmentRecord> pendingSegmentIterator =
        query.map((index, r, ctx) -> PendingSegmentRecord.fromResultSet(r, jsonMapper))
             .iterator();
    final ImmutableList.Builder<PendingSegmentRecord> pendingSegments = ImmutableList.builder();
    while (pendingSegmentIterator.hasNext()) {
      final PendingSegmentRecord pendingSegment = pendingSegmentIterator.next();
      if (compareIntervalEndpointsAsStrings || pendingSegment.getId().getInterval().overlaps(interval)) {
        pendingSegments.add(pendingSegment);
      }
    }
    pendingSegmentIterator.close();
    return pendingSegments.build();
  }

  List<PendingSegmentRecord> getPendingSegmentsForTaskAllocatorIdWithHandle(
      final Handle handle,
      final String dataSource,
      final String taskAllocatorId
  )
  {
    String sql = "SELECT payload, sequence_name, sequence_prev_id, task_allocator_id, upgraded_from_segment_id"
                 + " FROM " + dbTables.getPendingSegmentsTable()
                 + " WHERE dataSource = :dataSource AND task_allocator_id = :task_allocator_id";

    Query<Map<String, Object>> query = handle.createQuery(sql)
                                             .bind("dataSource", dataSource)
                                             .bind("task_allocator_id", taskAllocatorId);

    final ResultIterator<PendingSegmentRecord> pendingSegmentRecords =
        query.map((index, r, ctx) -> PendingSegmentRecord.fromResultSet(r, jsonMapper))
             .iterator();

    final List<PendingSegmentRecord> pendingSegments = new ArrayList<>();
    while (pendingSegmentRecords.hasNext()) {
      pendingSegments.add(pendingSegmentRecords.next());
    }

    pendingSegmentRecords.close();

    return pendingSegments;
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
  public Set<DataSegment> commitSegments(
      final Set<DataSegment> segments,
      @Nullable final SegmentSchemaMapping segmentSchemaMapping
  ) throws IOException
  {
    final SegmentPublishResult result =
        commitSegmentsAndMetadata(
            segments,
            null,
            null,
            segmentSchemaMapping
        );

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
      @Nullable final DataSourceMetadata endMetadata,
      @Nullable final SegmentSchemaMapping segmentSchemaMapping
  ) throws IOException
  {
    verifySegmentsToCommit(segments);

    if ((startMetadata == null && endMetadata != null) || (startMetadata != null && endMetadata == null)) {
      throw new IllegalArgumentException("start/end metadata pair must be either null or non-null");
    }

    final String dataSource = segments.iterator().next().getDataSource();

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
          (handle, transactionStatus) -> {
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
                  throw InvalidInput.exception(result.getErrorMsg());
                }
              }
            }

            final Set<DataSegment> inserted =
                announceHistoricalSegmentBatch(
                    handle,
                    segments,
                    usedSegments,
                    segmentSchemaMapping
                );
            return SegmentPublishResult.ok(ImmutableSet.copyOf(inserted));
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
      final Set<ReplaceTaskLock> locksHeldByReplaceTask,
      @Nullable final SegmentSchemaMapping segmentSchemaMapping
  )
  {
    verifySegmentsToCommit(replaceSegments);

    try {
      return connector.retryTransaction(
          (handle, transactionStatus) -> {
            final Set<DataSegment> segmentsToInsert = new HashSet<>(replaceSegments);

            Set<DataSegmentPlus> upgradedSegments =
                createNewIdsOfAppendSegmentsAfterReplace(handle, replaceSegments, locksHeldByReplaceTask);

            Map<SegmentId, SegmentMetadata> upgradeSegmentMetadata = new HashMap<>();
            for (DataSegmentPlus dataSegmentPlus : upgradedSegments) {
              segmentsToInsert.add(dataSegmentPlus.getDataSegment());
              if (dataSegmentPlus.getSchemaFingerprint() != null && dataSegmentPlus.getNumRows() != null) {
                upgradeSegmentMetadata.put(
                    dataSegmentPlus.getDataSegment().getId(),
                    new SegmentMetadata(dataSegmentPlus.getNumRows(), dataSegmentPlus.getSchemaFingerprint())
                );
              }
            }
            SegmentPublishResult result = SegmentPublishResult.ok(
                insertSegments(
                    handle,
                    segmentsToInsert,
                    segmentSchemaMapping,
                    upgradeSegmentMetadata,
                    Collections.emptyMap()
                ),
                upgradePendingSegmentsOverlappingWith(segmentsToInsert)
            );
            return result;
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
      final Map<DataSegment, ReplaceTaskLock> appendSegmentToReplaceLock,
      final String taskAllocatorId,
      @Nullable final SegmentSchemaMapping segmentSchemaMapping
  )
  {
    return commitAppendSegmentsAndMetadataInTransaction(
        appendSegments,
        appendSegmentToReplaceLock,
        null,
        null,
        taskAllocatorId,
        segmentSchemaMapping
    );
  }

  @Override
  public SegmentPublishResult commitAppendSegmentsAndMetadata(
      Set<DataSegment> appendSegments,
      Map<DataSegment, ReplaceTaskLock> appendSegmentToReplaceLock,
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata,
      String taskAllocatorId,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    return commitAppendSegmentsAndMetadataInTransaction(
        appendSegments,
        appendSegmentToReplaceLock,
        startMetadata,
        endMetadata,
        taskAllocatorId,
        segmentSchemaMapping
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
      final boolean skipSegmentLineageCheck,
      String taskAllocatorId
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
                existingChunks,
                taskAllocatorId
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
                existingChunks,
                taskAllocatorId
            );
          }
        }
    );
  }

  @Override
  public List<PendingSegmentRecord> upgradePendingSegmentsOverlappingWith(
      Set<DataSegment> replaceSegments
  )
  {
    if (replaceSegments.isEmpty()) {
      return Collections.emptyList();
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
        handle -> upgradePendingSegments(handle, datasource, replaceIntervalToMaxId)
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
   * @return Inserted pending segment records
   */
  private List<PendingSegmentRecord> upgradePendingSegments(
      Handle handle,
      String datasource,
      Map<Interval, DataSegment> replaceIntervalToMaxId
  ) throws JsonProcessingException
  {
    final List<PendingSegmentRecord> upgradedPendingSegments = new ArrayList<>();

    for (Map.Entry<Interval, DataSegment> entry : replaceIntervalToMaxId.entrySet()) {
      final Interval replaceInterval = entry.getKey();
      final DataSegment maxSegmentId = entry.getValue();
      final String replaceVersion = maxSegmentId.getVersion();

      final int numCorePartitions = maxSegmentId.getShardSpec().getNumCorePartitions();
      int currentPartitionNumber = maxSegmentId.getShardSpec().getPartitionNum();

      final List<PendingSegmentRecord> overlappingPendingSegments
          = getPendingSegmentsForIntervalWithHandle(handle, datasource, replaceInterval);

      for (PendingSegmentRecord overlappingPendingSegment : overlappingPendingSegments) {
        final SegmentIdWithShardSpec pendingSegmentId = overlappingPendingSegment.getId();

        if (shouldUpgradePendingSegment(overlappingPendingSegment, replaceInterval, replaceVersion)) {
          // Ensure unique sequence_name_prev_id_sha1 by setting
          // sequence_prev_id -> pendingSegmentId
          // sequence_name -> prefix + replaceVersion
          SegmentIdWithShardSpec newId = new SegmentIdWithShardSpec(
              datasource,
              replaceInterval,
              replaceVersion,
              new NumberedShardSpec(++currentPartitionNumber, numCorePartitions)
          );
          upgradedPendingSegments.add(
              new PendingSegmentRecord(
                  newId,
                  UPGRADED_PENDING_SEGMENT_PREFIX + replaceVersion,
                  pendingSegmentId.toString(),
                  pendingSegmentId.toString(),
                  overlappingPendingSegment.getTaskAllocatorId()
              )
          );
        }
      }
    }

    // Do not skip lineage check so that the sequence_name_prev_id_sha1
    // includes hash of both sequence_name and prev_segment_id
    int numInsertedPendingSegments = insertPendingSegmentsIntoMetastore(
        handle,
        upgradedPendingSegments,
        datasource,
        false
    );
    log.info(
        "Inserted total [%d] new versions for [%d] pending segments.",
        numInsertedPendingSegments, upgradedPendingSegments.size()
    );

    return upgradedPendingSegments;
  }

  private boolean shouldUpgradePendingSegment(
      PendingSegmentRecord pendingSegment,
      Interval replaceInterval,
      String replaceVersion
  )
  {
    if (pendingSegment.getTaskAllocatorId() == null) {
      return false;
    } else if (pendingSegment.getId().getVersion().compareTo(replaceVersion) >= 0) {
      return false;
    } else if (!replaceInterval.contains(pendingSegment.getId().getInterval())) {
      return false;
    } else {
      // Do not upgrade already upgraded pending segment
      return pendingSegment.getSequenceName() == null
             || !pendingSegment.getSequenceName().startsWith(UPGRADED_PENDING_SEGMENT_PREFIX);
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
      final List<TimelineObjectHolder<String, DataSegment>> existingChunks,
      final String taskAllocatorId
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
        sequenceNamePrevIdSha1,
        taskAllocatorId
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
    final Map<SegmentCreateRequest, PendingSegmentRecord> createdSegments = createNewSegments(
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
        ImmutableList.copyOf(createdSegments.values()),
        dataSource,
        skipSegmentLineageCheck
    );

    for (Map.Entry<SegmentCreateRequest, PendingSegmentRecord> entry : createdSegments.entrySet()) {
      allocatedSegmentIds.put(entry.getKey(), entry.getValue().getId());
    }
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
      final List<TimelineObjectHolder<String, DataSegment>> existingChunks,
      final String taskAllocatorId
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
    insertPendingSegmentIntoMetastore(
        handle,
        newIdentifier,
        dataSource,
        interval,
        "",
        sequenceName,
        sequenceNamePrevIdSha1,
        taskAllocatorId
    );

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

  private static void bindColumnValuesToQueryWithInCondition(
      final String columnName,
      final List<String> values,
      final Update query
  )
  {
    if (values == null) {
      return;
    }

    for (int i = 0; i < values.size(); i++) {
      query.bind(StringUtils.format("%s%d", columnName, i), values.get(i));
    }
  }

  private int deletePendingSegmentsById(Handle handle, String datasource, List<String> pendingSegmentIds)
  {
    if (pendingSegmentIds.isEmpty()) {
      return 0;
    }

    Update query = handle.createStatement(
        StringUtils.format(
            "DELETE FROM %s WHERE dataSource = :dataSource %s",
            dbTables.getPendingSegmentsTable(),
            SqlSegmentsMetadataQuery.getParameterizedInConditionForColumn("id", pendingSegmentIds)
        )
    ).bind("dataSource", datasource);
    bindColumnValuesToQueryWithInCondition("id", pendingSegmentIds, query);

    return query.execute();
  }

  private SegmentPublishResult commitAppendSegmentsAndMetadataInTransaction(
      Set<DataSegment> appendSegments,
      Map<DataSegment, ReplaceTaskLock> appendSegmentToReplaceLock,
      @Nullable DataSourceMetadata startMetadata,
      @Nullable DataSourceMetadata endMetadata,
      String taskAllocatorId,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    verifySegmentsToCommit(appendSegments);
    if ((startMetadata == null && endMetadata != null)
        || (startMetadata != null && endMetadata == null)) {
      throw new IllegalArgumentException("start/end metadata pair must be either null or non-null");
    }

    final String dataSource = appendSegments.iterator().next().getDataSource();
    final List<PendingSegmentRecord> segmentIdsForNewVersions = connector.retryTransaction(
        (handle, transactionStatus)
            -> getPendingSegmentsForTaskAllocatorIdWithHandle(handle, dataSource, taskAllocatorId),
        0,
        SQLMetadataConnector.DEFAULT_MAX_TRIES
    );

    // Create entries for all required versions of the append segments
    final Set<DataSegment> allSegmentsToInsert = new HashSet<>(appendSegments);
    final Map<SegmentId, SegmentId> newVersionSegmentToParent = new HashMap<>();
    final Map<String, DataSegment> segmentIdMap = new HashMap<>();
    appendSegments.forEach(segment -> segmentIdMap.put(segment.getId().toString(), segment));
    segmentIdsForNewVersions.forEach(
        pendingSegment -> {
          if (segmentIdMap.containsKey(pendingSegment.getUpgradedFromSegmentId())) {
            final DataSegment oldSegment = segmentIdMap.get(pendingSegment.getUpgradedFromSegmentId());
            final SegmentId newVersionSegmentId = pendingSegment.getId().asSegmentId();
            newVersionSegmentToParent.put(newVersionSegmentId, oldSegment.getId());
            allSegmentsToInsert.add(
                new DataSegment(
                    pendingSegment.getId().asSegmentId(),
                    oldSegment.getLoadSpec(),
                    oldSegment.getDimensions(),
                    oldSegment.getMetrics(),
                    pendingSegment.getId().getShardSpec(),
                    oldSegment.getLastCompactionState(),
                    oldSegment.getBinaryVersion(),
                    oldSegment.getSize()
                )
            );
          }
        }
    );

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

            // Delete the pending segments to be committed in this transaction in batches of at most 100
            final List<List<String>> pendingSegmentIdBatches = Lists.partition(
                allSegmentsToInsert.stream()
                                   .map(pendingSegment -> pendingSegment.getId().toString())
                                   .collect(Collectors.toList()),
                100
            );
            int numDeletedPendingSegments = 0;
            for (List<String> pendingSegmentIdBatch : pendingSegmentIdBatches) {
              numDeletedPendingSegments += deletePendingSegmentsById(handle, dataSource, pendingSegmentIdBatch);
            }
            log.info("Deleted [%d] entries from pending segments table upon commit.", numDeletedPendingSegments);

            return SegmentPublishResult.ok(
                insertSegments(
                    handle,
                    allSegmentsToInsert,
                    segmentSchemaMapping,
                    Collections.emptyMap(),
                    newVersionSegmentToParent
                )
            );
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

  @VisibleForTesting
  int insertPendingSegmentsIntoMetastore(
      Handle handle,
      List<PendingSegmentRecord> pendingSegments,
      String dataSource,
      boolean skipSegmentLineageCheck
  ) throws JsonProcessingException
  {
    final PreparedBatch insertBatch = handle.prepareBatch(
        StringUtils.format(
            "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, sequence_name, sequence_prev_id, "
            + "sequence_name_prev_id_sha1, payload, task_allocator_id, upgraded_from_segment_id) "
            + "VALUES (:id, :dataSource, :created_date, :start, :end, :sequence_name, :sequence_prev_id, "
            + ":sequence_name_prev_id_sha1, :payload, :task_allocator_id, :upgraded_from_segment_id)",
            dbTables.getPendingSegmentsTable(),
            connector.getQuoteString()
        ));

    final String now = DateTimes.nowUtc().toString();
    for (PendingSegmentRecord pendingSegment : pendingSegments) {
      final SegmentIdWithShardSpec segmentId = pendingSegment.getId();
      final Interval interval = segmentId.getInterval();

      insertBatch.add()
                 .bind("id", segmentId.toString())
                 .bind("dataSource", dataSource)
                 .bind("created_date", now)
                 .bind("start", interval.getStart().toString())
                 .bind("end", interval.getEnd().toString())
                 .bind("sequence_name", pendingSegment.getSequenceName())
                 .bind("sequence_prev_id", pendingSegment.getSequencePrevId())
                 .bind(
                     "sequence_name_prev_id_sha1",
                     pendingSegment.computeSequenceNamePrevIdSha1(skipSegmentLineageCheck)
                 )
                 .bind("payload", jsonMapper.writeValueAsBytes(segmentId))
                 .bind("task_allocator_id", pendingSegment.getTaskAllocatorId())
                 .bind("upgraded_from_segment_id", pendingSegment.getUpgradedFromSegmentId());
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
      String sequenceNamePrevIdSha1,
      String taskAllocatorId
  ) throws JsonProcessingException
  {
    handle.createStatement(
        StringUtils.format(
            "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, sequence_name, sequence_prev_id, "
            + "sequence_name_prev_id_sha1, payload, task_allocator_id) "
            + "VALUES (:id, :dataSource, :created_date, :start, :end, :sequence_name, :sequence_prev_id, "
            + ":sequence_name_prev_id_sha1, :payload, :task_allocator_id)",
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
          .bind("task_allocator_id", taskAllocatorId)
          .execute();
  }

  private Map<SegmentCreateRequest, PendingSegmentRecord> createNewSegments(
      Handle handle,
      String dataSource,
      Interval interval,
      boolean skipSegmentLineageCheck,
      List<TimelineObjectHolder<String, DataSegment>> existingChunks,
      List<SegmentCreateRequest> requests
  )
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
    final Set<SegmentIdWithShardSpec> pendingSegments = new HashSet<>(
        getPendingSegmentsForIntervalWithHandle(handle, dataSource, interval).stream()
                                                                             .map(PendingSegmentRecord::getId)
                                                                             .collect(Collectors.toSet())
    );

    final Map<SegmentCreateRequest, PendingSegmentRecord> createdSegments = new HashMap<>();
    final Map<UniqueAllocateRequest, PendingSegmentRecord> uniqueRequestToSegment = new HashMap<>();

    for (SegmentCreateRequest request : requests) {
      // Check if the required segment has already been created in this batch
      final UniqueAllocateRequest uniqueRequest =
          new UniqueAllocateRequest(interval, request, skipSegmentLineageCheck);

      final PendingSegmentRecord createdSegment;
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
          pendingSegments.add(createdSegment.getId());
          uniqueRequestToSegment.put(uniqueRequest, createdSegment);
          log.info("Created new segment[%s]", createdSegment.getId());
        }
      }

      if (createdSegment != null) {
        createdSegments.put(request, createdSegment);
      }
    }

    log.info("Created [%d] new segments for [%d] allocate requests.", uniqueRequestToSegment.size(), requests.size());
    return createdSegments;
  }

  private PendingSegmentRecord createNewSegment(
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
      SegmentIdWithShardSpec pendingSegmentId = new SegmentIdWithShardSpec(
          dataSource,
          interval,
          version,
          partialShardSpec.complete(jsonMapper, newPartitionId, 0)
      );
      return new PendingSegmentRecord(
          pendingSegmentId,
          request.getSequenceName(),
          request.getPreviousSegmentId(),
          request.getUpgradedFromSegmentId(),
          request.getTaskAllocatorId()
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

      SegmentIdWithShardSpec pendingSegmentId = new SegmentIdWithShardSpec(
          dataSource,
          interval,
          Preconditions.checkNotNull(newSegmentVersion, "newSegmentVersion"),
          partialShardSpec.complete(
              jsonMapper,
              overallMaxId.getShardSpec().getPartitionNum() + 1,
              committedMaxId == null ? 0 : committedMaxId.getShardSpec().getNumCorePartitions()
          )
      );
      return new PendingSegmentRecord(
          pendingSegmentId,
          request.getSequenceName(),
          request.getPreviousSegmentId(),
          request.getUpgradedFromSegmentId(),
          request.getTaskAllocatorId()
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
  )
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
        getPendingSegmentsForIntervalWithHandle(handle, dataSource, interval).stream()
                                                                             .map(PendingSegmentRecord::getId)
                                                                             .collect(Collectors.toSet())
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

  private boolean shouldPersistSchema(SegmentSchemaMapping segmentSchemaMapping)
  {
    return schemaPersistEnabled
           && segmentSchemaMapping != null
           && segmentSchemaMapping.isNonEmpty();
  }

  private void persistSchema(
      final Handle handle,
      final Set<DataSegment> segments,
      final SegmentSchemaMapping segmentSchemaMapping
  ) throws JsonProcessingException
  {
    if (segmentSchemaMapping.getSchemaVersion() != CentralizedDatasourceSchemaConfig.SCHEMA_VERSION) {
      log.error(
          "Schema version [%d] doesn't match the current version [%d]. Not persisting this schema [%s]. "
          + "Schema for this segment will be populated by the schema backfill job in Coordinator.",
          segmentSchemaMapping.getSchemaVersion(),
          CentralizedDatasourceSchemaConfig.SCHEMA_VERSION,
          segmentSchemaMapping
      );
      return;
    }
    String dataSource = segments.stream().iterator().next().getDataSource();

    segmentSchemaManager.persistSegmentSchema(
        handle,
        dataSource,
        segmentSchemaMapping.getSchemaVersion(),
        segmentSchemaMapping.getSchemaFingerprintToPayloadMap()
    );
  }

  private Set<DataSegment> announceHistoricalSegmentBatch(
      final Handle handle,
      final Set<DataSegment> segments,
      final Set<DataSegment> usedSegments,
      @Nullable final SegmentSchemaMapping segmentSchemaMapping
  ) throws IOException
  {
    final Set<DataSegment> toInsertSegments = new HashSet<>();
    try {
      boolean shouldPersistSchema = shouldPersistSchema(segmentSchemaMapping);

      if (shouldPersistSchema) {
        persistSchema(handle, segments, segmentSchemaMapping);
      }

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

      final String now = DateTimes.nowUtc().toString();
      PreparedBatch preparedBatch = handle.prepareBatch(buildSqlToInsertSegments());
      for (List<DataSegment> partition : partitionedSegments) {
        for (DataSegment segment : partition) {
          String segmentId = segment.getId().toString();

          PreparedBatchPart preparedBatchPart = preparedBatch.add()
              .bind("id", segmentId)
              .bind("dataSource", segment.getDataSource())
              .bind("created_date", now)
              .bind("start", segment.getInterval().getStart().toString())
              .bind("end", segment.getInterval().getEnd().toString())
              .bind("partitioned", (segment.getShardSpec() instanceof NoneShardSpec) ? false : true)
              .bind("version", segment.getVersion())
              .bind("used", usedSegments.contains(segment))
              .bind("payload", jsonMapper.writeValueAsBytes(segment))
              .bind("used_status_last_updated", now);

          if (schemaPersistEnabled) {
            Long numRows = null;
            String schemaFingerprint = null;
            if (shouldPersistSchema && segmentSchemaMapping.getSegmentIdToMetadataMap().containsKey(segmentId)) {
              SegmentMetadata segmentMetadata = segmentSchemaMapping.getSegmentIdToMetadataMap().get(segmentId);
              numRows = segmentMetadata.getNumRows();
              schemaFingerprint = segmentMetadata.getSchemaFingerprint();
            }
            preparedBatchPart
                .bind("num_rows", numRows)
                .bind("schema_fingerprint", schemaFingerprint);
          }
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
  private Set<DataSegmentPlus> createNewIdsOfAppendSegmentsAfterReplace(
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

    final String datasource = replaceSegments.iterator().next().getDataSource();

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

    final List<DataSegmentPlus> segmentsToUpgrade
        = retrieveSegmentsById(handle, datasource, upgradeSegmentToLockVersion.keySet());

    if (segmentsToUpgrade.isEmpty()) {
      return Collections.emptySet();
    }

    final Set<Interval> replaceIntervals = intervalToNumCorePartitions.keySet();

    final Set<DataSegmentPlus> upgradedSegments = new HashSet<>();
    for (DataSegmentPlus oldSegmentMetadata : segmentsToUpgrade) {
      // Determine interval of the upgraded segment
      DataSegment oldSegment = oldSegmentMetadata.getDataSegment();
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
      DataSegment dataSegment = DataSegment.builder(oldSegment)
                                           .interval(newInterval)
                                           .version(lockVersion)
                                           .shardSpec(shardSpec)
                                           .build();

      upgradedSegments.add(
          new DataSegmentPlus(
              dataSegment,
              null,
              null,
              null,
              oldSegmentMetadata.getSchemaFingerprint(),
              oldSegmentMetadata.getNumRows())
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
  private Set<DataSegment> insertSegments(
      Handle handle,
      Set<DataSegment> segments,
      @Nullable SegmentSchemaMapping segmentSchemaMapping,
      Map<SegmentId, SegmentMetadata> upgradeSegmentMetadata,
      Map<SegmentId, SegmentId> newVersionForAppendToParent
  ) throws IOException
  {
    boolean shouldPersistSchema = shouldPersistSchema(segmentSchemaMapping);

    if (shouldPersistSchema) {
      persistSchema(handle, segments, segmentSchemaMapping);
    }

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

    final String now = DateTimes.nowUtc().toString();
    final PreparedBatch batch = handle.prepareBatch(buildSqlToInsertSegments());
    for (List<DataSegment> partition : partitionedSegments) {
      for (DataSegment segment : partition) {
        PreparedBatchPart preparedBatchPart =
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

        if (schemaPersistEnabled) {
          SegmentMetadata segmentMetadata =
              getSegmentMetadataFromSchemaMappingOrUpgradeMetadata(
                  segment.getId(),
                  segmentSchemaMapping,
                  newVersionForAppendToParent,
                  upgradeSegmentMetadata
              );
          Long numRows = null;
          String schemaFingerprint = null;
          if (segmentMetadata != null) {
            numRows = segmentMetadata.getNumRows();
            schemaFingerprint = segmentMetadata.getSchemaFingerprint();
          }
          preparedBatchPart
              .bind("num_rows", numRows)
              .bind("schema_fingerprint", schemaFingerprint);
        }
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

  private SegmentMetadata getSegmentMetadataFromSchemaMappingOrUpgradeMetadata(
      final SegmentId segmentId,
      final SegmentSchemaMapping segmentSchemaMapping,
      final Map<SegmentId, SegmentId> newVersionForAppendToParent,
      final Map<SegmentId, SegmentMetadata> upgradeSegmentMetadata
  )
  {
    if (!shouldPersistSchema(segmentSchemaMapping)) {
      return null;
    }

    SegmentMetadata segmentMetadata = null;
    boolean presentInSchemaMetadata =
        segmentSchemaMapping.getSegmentIdToMetadataMap().containsKey(segmentId.toString());
    boolean upgradedAppendSegment =
        newVersionForAppendToParent.containsKey(segmentId)
        && segmentSchemaMapping.getSegmentIdToMetadataMap()
                               .containsKey(newVersionForAppendToParent.get(segmentId).toString());

    if (presentInSchemaMetadata || upgradedAppendSegment) {
      String segmentIdToUse;
      if (presentInSchemaMetadata) {
        segmentIdToUse = segmentId.toString();
      } else {
        segmentIdToUse = newVersionForAppendToParent.get(segmentId).toString();
      }
      segmentMetadata = segmentSchemaMapping.getSegmentIdToMetadataMap().get(segmentIdToUse);
    } else if (upgradeSegmentMetadata.containsKey(segmentId)) {
      segmentMetadata = upgradeSegmentMetadata.get(segmentId);
    }

    return segmentMetadata;
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

  private List<DataSegmentPlus> retrieveSegmentsById(Handle handle, String datasource, Set<String> segmentIds)
  {
    if (segmentIds.isEmpty()) {
      return Collections.emptyList();
    }

    if (schemaPersistEnabled) {
      return SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables, jsonMapper)
                                     .retrieveSegmentsWithSchemaById(datasource, segmentIds);
    } else {
      return SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables, jsonMapper)
                                     .retrieveSegmentsById(datasource, segmentIds);
    }
  }

  private String buildSqlToInsertSegments()
  {
    String insertStatement =
        "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s,"
        + " partitioned, version, used, payload, used_status_last_updated %3$s) "
        + "VALUES (:id, :dataSource, :created_date, :start, :end,"
        + " :partitioned, :version, :used, :payload, :used_status_last_updated %4$s)";

    if (schemaPersistEnabled) {
      return StringUtils.format(
          insertStatement,
          dbTables.getSegmentsTable(),
          connector.getQuoteString(),
          ", schema_fingerprint, num_rows",
          ", :schema_fingerprint, :num_rows"
      );
    } else {
      return StringUtils.format(
          insertStatement,
          dbTables.getSegmentsTable(),
          connector.getQuoteString(),
          "",
          ""
      );
    }
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
    boolean startMetadataGreaterThanExisting = false;

    if (oldCommitMetadataFromDb == null) {
      startMetadataMatchesExisting = startMetadata.isValidStart();
      startMetadataGreaterThanExisting = true;
    } else {
      // Checking against the last committed metadata.
      // If the new start sequence number is greater than the end sequence number of the last commit,
      // compareTo() will return 1 and 0 in all other cases. This can happen if multiple tasks are publishing the
      // sequence around the same time.
      if (startMetadata instanceof Comparable) {
        startMetadataGreaterThanExisting = ((Comparable) startMetadata.asStartMetadata())
                                               .compareTo(oldCommitMetadataFromDb.asStartMetadata()) > 0;
      }

      // Converting the last one into start metadata for checking since only the same type of metadata can be matched.
      // Even though kafka/kinesis indexing services use different sequenceNumber types for representing
      // start and end sequenceNumbers, the below conversion is fine because the new start sequenceNumbers are supposed
      // to be same with end sequenceNumbers of the last commit.
      startMetadataMatchesExisting = startMetadata.asStartMetadata().matches(oldCommitMetadataFromDb.asStartMetadata());
    }

    if (startMetadataGreaterThanExisting && !startMetadataMatchesExisting) {
      // Offsets stored in startMetadata is greater than the last commited metadata.
      return new DataStoreMetadataUpdateResult(true, false,
          "The new start metadata state[%s] is ahead of the last commited"
          + " end state[%s]. Try resetting the supervisor.", startMetadata, oldCommitMetadataFromDb
      );
    }

    if (!startMetadataMatchesExisting) {
      // Not in the desired start state.
      return new DataStoreMetadataUpdateResult(true, false,
          "Inconsistency between stored metadata state[%s] and target state[%s]. Try resetting the supervisor.",
          oldCommitMetadataFromDb, startMetadata
      );
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
  public int deletePendingSegmentsForTaskAllocatorId(final String pendingSegmentsGroup)
  {
    return connector.getDBI().inTransaction(
        (handle, status) -> handle
            .createStatement(
                StringUtils.format(
                    "DELETE FROM %s WHERE task_allocator_id = :task_allocator_id",
                    dbTables.getPendingSegmentsTable()
                )
            )
            .bind("task_allocator_id", pendingSegmentsGroup)
            .execute()
    );
  }

  @Override
  public List<PendingSegmentRecord> getPendingSegments(String datasource, Interval interval)
  {
    return connector.retryWithHandle(
        handle -> getPendingSegmentsForIntervalWithHandle(handle, datasource, interval)
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
