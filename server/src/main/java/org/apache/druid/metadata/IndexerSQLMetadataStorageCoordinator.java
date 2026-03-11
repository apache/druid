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
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.inject.Inject;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InternalServerError;
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
import org.apache.druid.metadata.segment.SegmentMetadataReadTransaction;
import org.apache.druid.metadata.segment.SegmentMetadataTransaction;
import org.apache.druid.metadata.segment.SegmentMetadataTransactionFactory;
import org.apache.druid.segment.SegmentMetadata;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.IndexingStateStorage;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartialShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionIds;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
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
  private final IndexingStateStorage indexingStateStorage;

  private final SegmentMetadataTransactionFactory transactionFactory;

  @Inject
  public IndexerSQLMetadataStorageCoordinator(
      SegmentMetadataTransactionFactory transactionFactory,
      ObjectMapper jsonMapper,
      MetadataStorageTablesConfig dbTables,
      SQLMetadataConnector connector,
      SegmentSchemaManager segmentSchemaManager,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig,
      IndexingStateStorage indexingStateStorage
  )
  {
    this.transactionFactory = transactionFactory;
    this.jsonMapper = jsonMapper;
    this.dbTables = dbTables;
    this.connector = connector;
    this.segmentSchemaManager = segmentSchemaManager;
    this.centralizedDatasourceSchemaConfig = centralizedDatasourceSchemaConfig;
    this.schemaPersistEnabled =
        centralizedDatasourceSchemaConfig.isEnabled()
        && !centralizedDatasourceSchemaConfig.isTaskSchemaPublishDisabled();
    this.indexingStateStorage = indexingStateStorage;
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
  public Set<String> retrieveAllDatasourceNames()
  {
    final String sql = StringUtils.format("SELECT DISTINCT(dataSource) FROM %s", dbTables.getSegmentsTable());
    return Set.copyOf(
        connector.inReadOnlyTransaction(
            (handle, status) -> handle.createQuery(sql).mapTo(String.class).list()
        )
    );
  }

  @Override
  public List<Interval> retrieveUnusedSegmentIntervals(String dataSource, int limit)
  {
    return inReadOnlyTransaction(
        sql -> sql.retrieveUnusedSegmentIntervals(dataSource, limit)
    );
  }

  @Override
  public Set<DataSegment> retrieveUsedSegmentsForIntervals(
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
  public Set<DataSegment> retrieveAllUsedSegments(String dataSource, Segments visibility)
  {
    return doRetrieveUsedSegments(dataSource, Collections.emptyList(), visibility);
  }

  /**
   * @param intervals empty list means unrestricted interval.
   */
  private Set<DataSegment> doRetrieveUsedSegments(
      final String dataSource,
      final List<Interval> intervals,
      final Segments visibility
  )
  {
    return inReadOnlyDatasourceTransaction(
        dataSource,
        transaction -> {
          if (visibility == Segments.ONLY_VISIBLE) {
            final SegmentTimeline timeline = getTimelineForIntervals(transaction, intervals);
            return timeline.findNonOvershadowedObjectsInInterval(Intervals.ETERNITY, Partitions.ONLY_COMPLETE);
          } else {
            return transaction.findUsedSegmentsOverlappingAnyOf(intervals);
          }
        }
    );
  }

  @Override
  public List<Pair<DataSegment, String>> retrieveUsedSegmentsAndCreatedDates(String dataSource, List<Interval> intervals)
  {
    return inReadOnlyDatasourceTransaction(
        dataSource,
        transaction -> transaction.findUsedSegmentsPlusOverlappingAnyOf(intervals)
                                  .stream()
                                  .map(s -> Pair.of(
                                      s.getDataSegment(),
                                      s.getCreatedDate() == null ? null : s.getCreatedDate().toString()
                                  ))
                                  .collect(Collectors.toList())
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
    final List<DataSegment> matchingSegments = inReadOnlyDatasourceTransaction(
        dataSource,
        transaction -> transaction.noCacheSql().findUnusedSegments(
            dataSource,
            interval,
            versions,
            limit,
            maxUsedStatusLastUpdatedTime
        )
    );

    log.debug(
        "Found [%,d] unused segments for datasource[%s] in interval[%s] and"
        + " versions[%s] with maxUsedStatusLastUpdatedTime[%s].",
        matchingSegments.size(), dataSource, interval, versions, maxUsedStatusLastUpdatedTime
    );
    return matchingSegments;
  }

  @Override
  public List<DataSegment> retrieveUnusedSegmentsWithExactInterval(
      String dataSource,
      Interval interval,
      DateTime maxUpdatedTime,
      int limit
  )
  {
    return inReadOnlyTransaction(
        sql -> sql.retrieveUnusedSegmentsWithExactInterval(
            dataSource,
            interval,
            maxUpdatedTime,
            limit
        )
    );
  }

  @Override
  public Set<DataSegment> retrieveSegmentsById(String dataSource, Set<String> segmentIds)
  {
    return inReadOnlyDatasourceTransaction(
        dataSource,
        transaction ->
            retrieveSegmentsById(dataSource, transaction, segmentIds)
                .stream()
                .map(DataSegmentPlus::getDataSegment)
                .collect(Collectors.toSet())
    );
  }

  @Override
  public int markSegmentsWithinIntervalAsUnused(
      String dataSource,
      Interval interval,
      @Nullable List<String> versions
  )
  {
    return inReadWriteDatasourceTransaction(
        dataSource,
        transaction -> transaction.markSegmentsWithinIntervalAsUnused(interval, versions, DateTimes.nowUtc())
    );
  }

  @Override
  public boolean markSegmentAsUnused(SegmentId segmentId)
  {
    return inReadWriteDatasourceTransaction(
        segmentId.getDataSource(),
        transaction -> transaction.markSegmentAsUnused(segmentId, DateTimes.nowUtc())
    );
  }

  @Override
  public int markSegmentsAsUnused(String dataSource, Set<SegmentId> segmentIds)
  {
    return inReadWriteDatasourceTransaction(
        dataSource,
        transaction -> transaction.markSegmentsAsUnused(segmentIds, DateTimes.nowUtc())
    );
  }

  @Override
  public int markAllSegmentsAsUnused(String dataSource)
  {
    return inReadWriteDatasourceTransaction(
        dataSource,
        transaction -> transaction.markAllSegmentsAsUnused(DateTimes.nowUtc())
    );
  }

  @Override
  public boolean markSegmentAsUsed(SegmentId segmentId)
  {
    return inWriteTransaction(
        sql -> sql.markSegmentAsUsed(segmentId, DateTimes.nowUtc())
    );
  }

  @Override
  public int markNonOvershadowedSegmentsAsUsed(String dataSource, Set<SegmentId> segmentIds)
  {
    return inWriteTransaction(
        sql -> sql.markNonOvershadowedSegmentsAsUsed(dataSource, segmentIds, DateTimes.nowUtc())
    );
  }

  @Override
  public int markNonOvershadowedSegmentsAsUsed(
      String dataSource,
      Interval interval,
      @Nullable List<String> versions
  )
  {
    return inWriteTransaction(
        sql -> sql.markNonOvershadowedSegmentsAsUsed(dataSource, interval, versions, DateTimes.nowUtc())
    );
  }

  @Override
  public int markAllNonOvershadowedSegmentsAsUsed(String dataSource)
  {
    return inWriteTransaction(
        sql -> sql.markAllNonOvershadowedSegmentsAsUsed(dataSource, DateTimes.nowUtc())
    );
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
    return inReadOnlyTransaction(
        sql -> sql.iterateAllUnusedSegmentsForDatasource(datasource, interval, limit, lastSegmentId, sortOrder)
    );
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
    return inReadOnlyTransaction(
        sql -> sql.retrieveUnusedSegmentIntervals(
            dataSource,
            minStartTime,
            maxEndTime,
            limit,
            maxUsedStatusLastUpdatedTime
        )
    );
  }

  private SegmentTimeline getTimelineForIntervals(
      final SegmentMetadataReadTransaction transaction,
      final List<Interval> intervals
  )
  {
    return SegmentTimeline.forSegments(
        transaction.findUsedSegmentsOverlappingAnyOf(intervals)
    );
  }

  @Override
  public Set<DataSegment> commitSegments(
      final Set<DataSegment> segments,
      @Nullable final SegmentSchemaMapping segmentSchemaMapping
  )
  {
    final SegmentPublishResult result =
        commitSegmentsAndMetadata(
            segments,
            null,
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
      @Nullable final String supervisorId,
      @Nullable final DataSourceMetadata startMetadata,
      @Nullable final DataSourceMetadata endMetadata,
      @Nullable final SegmentSchemaMapping segmentSchemaMapping
  )
  {
    verifySegmentsToCommit(segments);
    IndexerMetadataStorageCoordinator.validateDataSourceMetadata(supervisorId, startMetadata, endMetadata);
    final String dataSource = segments.iterator().next().getDataSource();

    try {
      final SegmentPublishResult result = inReadWriteDatasourceTransaction(
          dataSource,
          transaction -> {
            // Try to update datasource metadata first
            if (startMetadata != null) {
              final SegmentPublishResult metadataResult = updateDataSourceMetadataInTransaction(
                  transaction,
                  supervisorId,
                  dataSource,
                  startMetadata,
                  endMetadata
              );

              // Do not proceed if the datasource metadata update failed
              if (!metadataResult.isSuccess()) {
                return metadataResult;
              }
            }

            return SegmentPublishResult.ok(
                Set.copyOf(insertSegments(transaction, segments, segmentSchemaMapping))
            );
          }
      );

      // Mark compaction state fingerprints as active after successful publish
      if (result.isSuccess()) {
        markIndexingStateFingerprintsAsActive(result.getSegments());
      }

      return result;
    }
    catch (CallbackFailedException e) {
      throw e;
    }
  }

  @Override
  public SegmentPublishResult commitReplaceSegments(
      final Set<DataSegment> replaceSegments,
      final Set<ReplaceTaskLock> locksHeldByReplaceTask,
      @Nullable final SegmentSchemaMapping segmentSchemaMapping
  )
  {
    final String dataSource = verifySegmentsToCommit(replaceSegments);

    try {
      final SegmentPublishResult result = inReadWriteDatasourceTransaction(
          dataSource,
          transaction -> {
            final Set<DataSegment> segmentsToInsert = new HashSet<>(replaceSegments);

            Set<DataSegmentPlus> upgradedSegments = createNewIdsOfAppendSegmentsAfterReplace(
                dataSource,
                transaction,
                replaceSegments,
                locksHeldByReplaceTask
            );

            Map<SegmentId, SegmentMetadata> upgradeSegmentMetadata = new HashMap<>();
            final Map<String, String> upgradedFromSegmentIdMap = new HashMap<>();
            for (DataSegmentPlus dataSegmentPlus : upgradedSegments) {
              segmentsToInsert.add(dataSegmentPlus.getDataSegment());
              if (dataSegmentPlus.getSchemaFingerprint() != null && dataSegmentPlus.getNumRows() != null) {
                upgradeSegmentMetadata.put(
                    dataSegmentPlus.getDataSegment().getId(),
                    new SegmentMetadata(dataSegmentPlus.getNumRows(), dataSegmentPlus.getSchemaFingerprint())
                );
              }
              if (dataSegmentPlus.getUpgradedFromSegmentId() != null) {
                upgradedFromSegmentIdMap.put(
                    dataSegmentPlus.getDataSegment().getId().toString(),
                    dataSegmentPlus.getUpgradedFromSegmentId()
                );
              }
            }
            return SegmentPublishResult.ok(
                insertSegments(
                    transaction,
                    segmentsToInsert,
                    segmentSchemaMapping,
                    upgradeSegmentMetadata,
                    Collections.emptyMap(),
                    upgradedFromSegmentIdMap
                ),
                upgradePendingSegmentsOverlappingWith(transaction, segmentsToInsert)
            );
          }
      );

      // Mark compaction state fingerprints as active after successful publish
      if (result.isSuccess()) {
        markIndexingStateFingerprintsAsActive(result.getSegments());
      }

      return result;
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
        null,
        taskAllocatorId,
        segmentSchemaMapping
    );
  }

  @Override
  public SegmentPublishResult commitAppendSegmentsAndMetadata(
      Set<DataSegment> appendSegments,
      Map<DataSegment, ReplaceTaskLock> appendSegmentToReplaceLock,
      String supervisorId,
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata,
      String taskAllocatorId,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    return commitAppendSegmentsAndMetadataInTransaction(
        appendSegments,
        appendSegmentToReplaceLock,
        supervisorId,
        startMetadata,
        endMetadata,
        taskAllocatorId,
        segmentSchemaMapping
    );
  }

  @Override
  public SegmentPublishResult commitMetadataOnly(
      String supervisorId,
      String dataSource,
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata
  )
  {
    if (supervisorId == null) {
      throw new IllegalArgumentException("supervisorId cannot be null");
    }
    if (dataSource == null) {
      throw new IllegalArgumentException("datasource name cannot be null");
    }
    if (startMetadata == null) {
      throw new IllegalArgumentException("start metadata cannot be null");
    }
    if (endMetadata == null) {
      throw new IllegalArgumentException("end metadata cannot be null");
    }

    try {
      return inReadWriteDatasourceTransaction(
          dataSource,
          transaction -> updateDataSourceMetadataInTransaction(
              transaction,
              supervisorId,
              dataSource,
              startMetadata,
              endMetadata
          )
      );
    }
    catch (CallbackFailedException e) {
      throw e;
    }
  }

  @Override
  public Map<SegmentCreateRequest, SegmentIdWithShardSpec> allocatePendingSegments(
      String dataSource,
      Interval allocateInterval,
      boolean skipSegmentLineageCheck,
      List<SegmentCreateRequest> requests,
      boolean reduceMetadataIO
  )
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(allocateInterval, "interval");

    final Interval interval = allocateInterval.withChronology(ISOChronology.getInstanceUTC());
    return inReadWriteDatasourceTransaction(
        dataSource,
        transaction -> allocatePendingSegments(
            transaction,
            dataSource,
            interval,
            skipSegmentLineageCheck,
            requests,
            reduceMetadataIO
        )
    );
  }

  @Override
  @Nullable
  public SegmentIdWithShardSpec allocatePendingSegment(
      final String dataSource,
      final Interval interval,
      final boolean skipSegmentLineageCheck,
      final SegmentCreateRequest createRequest
  )
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(interval, "interval");
    final Interval allocateInterval = interval.withChronology(ISOChronology.getInstanceUTC());

    return inReadWriteDatasourceTransaction(
        dataSource,
        transaction -> {
          // Get the time chunk and associated data segments for the given interval, if any
          final List<TimelineObjectHolder<String, DataSegment>> existingChunks =
              getTimelineForIntervals(transaction, ImmutableList.of(interval))
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
                transaction,
                dataSource,
                allocateInterval,
                createRequest,
                existingChunks
            );
          } else {
            return allocatePendingSegmentWithSegmentLineageCheck(
                transaction,
                dataSource,
                allocateInterval,
                createRequest,
                existingChunks
            );
          }
        }
    );
  }

  /**
   * Creates and inserts new IDs for the pending segments that overlap with the given
   * replace segments being committed. The newly created pending segment IDs:
   * <ul>
   * <li>Have the same interval and version as that of an overlapping segment
   * committed by the REPLACE task.</li>
   * <li>Cannot be committed but are only used to serve realtime queries against
   * those versions.</li>
   * </ul>
   *
   * @param replaceSegments Segments being committed by a "REPLACE" task
   * @return List of inserted pending segment records
   */
  private List<PendingSegmentRecord> upgradePendingSegmentsOverlappingWith(
      SegmentMetadataTransaction transaction,
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
    return upgradePendingSegments(transaction, datasource, replaceIntervalToMaxId);
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
      SegmentMetadataTransaction transaction,
      String datasource,
      Map<Interval, DataSegment> replaceIntervalToMaxId
  )
  {
    final List<PendingSegmentRecord> upgradedPendingSegments = new ArrayList<>();

    for (Map.Entry<Interval, DataSegment> entry : replaceIntervalToMaxId.entrySet()) {
      final Interval replaceInterval = entry.getKey();
      final DataSegment maxSegmentId = entry.getValue();
      final String replaceVersion = maxSegmentId.getVersion();

      final int numCorePartitions = maxSegmentId.getShardSpec().getNumCorePartitions();
      int currentPartitionNumber = maxSegmentId.getShardSpec().getPartitionNum();

      final List<PendingSegmentRecord> overlappingPendingSegments
          = transaction.findPendingSegmentsOverlapping(replaceInterval);

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
              PendingSegmentRecord.create(
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
    int numInsertedPendingSegments =
        transaction.insertPendingSegments(upgradedPendingSegments, false);
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
      final SegmentId pendingSegmentId = pendingSegment.getId().asSegmentId();
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.UNSUPPORTED)
                          .build(
                              "Replacing with a finer segment granularity than a concurrent append is unsupported."
                              + " Cannot upgrade pendingSegment[%s] to version[%s] as the replace interval[%s]"
                              + " does not fully contain the pendingSegment interval[%s].",
                              pendingSegmentId, replaceVersion, replaceInterval, pendingSegmentId.getInterval()
                          );
    } else {
      // Do not upgrade already upgraded pending segment
      return pendingSegment.getSequenceName() == null
             || !pendingSegment.getSequenceName().startsWith(UPGRADED_PENDING_SEGMENT_PREFIX);
    }
  }

  @Nullable
  private SegmentIdWithShardSpec allocatePendingSegmentWithSegmentLineageCheck(
      final SegmentMetadataTransaction transaction,
      final String dataSource,
      final Interval interval,
      final SegmentCreateRequest createRequest,
      final List<TimelineObjectHolder<String, DataSegment>> existingChunks
  )
  {
    final List<SegmentIdWithShardSpec> existingPendingSegmentIds = transaction.findPendingSegmentIds(
        createRequest.getSequenceName(),
        createRequest.getPreviousSegmentId()
    );
    final String usedSegmentVersion = existingChunks.isEmpty() ? null : existingChunks.get(0).getVersion();
    final CheckExistingSegmentIdResult result = findPendingSegmentMatchingIntervalAndVersion(
        existingPendingSegmentIds,
        interval,
        createRequest.getSequenceName(),
        createRequest.getPreviousSegmentId(),
        usedSegmentVersion
    );

    if (result.found) {
      // The found existing segment identifier can be null if its interval doesn't match with the given interval
      return result.segmentIdentifier;
    }

    final SegmentIdWithShardSpec newIdentifier = createNewPendingSegment(
        transaction,
        dataSource,
        interval,
        createRequest.getPartialShardSpec(),
        createRequest.getVersion(),
        existingChunks
    );
    if (newIdentifier == null) {
      return null;
    }

    final PendingSegmentRecord record = PendingSegmentRecord.create(
        newIdentifier,
        createRequest.getSequenceName(),
        createRequest.getPreviousSegmentId(),
        null,
        createRequest.getTaskAllocatorId()
    );
    transaction.insertPendingSegment(record, false);
    return newIdentifier;
  }

  @Override
  public SegmentTimeline getSegmentTimelineForAllocation(
      String dataSource,
      Interval interval,
      boolean reduceMetadataIO
  )
  {
    return inReadOnlyDatasourceTransaction(
        dataSource,
        transaction -> {
          if (reduceMetadataIO) {
            return SegmentTimeline.forSegments(retrieveUsedSegmentsForAllocation(transaction, dataSource, interval));
          } else {
            return getTimelineForIntervals(transaction, Collections.singletonList(interval));
          }
        }
    );
  }

  private Map<SegmentCreateRequest, SegmentIdWithShardSpec> allocatePendingSegments(
      final SegmentMetadataTransaction transaction,
      final String dataSource,
      final Interval interval,
      final boolean skipSegmentLineageCheck,
      final List<SegmentCreateRequest> requests,
      final boolean reduceMetadataIO
  )
  {
    // Get the time chunk and associated data segments for the given interval, if any
    final List<TimelineObjectHolder<String, DataSegment>> existingChunks
        = getSegmentTimelineForAllocation(dataSource, interval, reduceMetadataIO).lookup(interval);
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
          getExistingSegmentIdsSkipLineageCheck(transaction, interval, existingVersion, requests);
    } else {
      existingSegmentIds =
          getExistingSegmentIdsWithLineageCheck(transaction, interval, existingVersion, requests);
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
        transaction,
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
    transaction.insertPendingSegments(
        ImmutableList.copyOf(createdSegments.values()),
        skipSegmentLineageCheck
    );

    for (Map.Entry<SegmentCreateRequest, PendingSegmentRecord> entry : createdSegments.entrySet()) {
      allocatedSegmentIds.put(entry.getKey(), entry.getValue().getId());
    }
    return allocatedSegmentIds;
  }

  @Nullable
  private SegmentIdWithShardSpec allocatePendingSegment(
      final SegmentMetadataTransaction transaction,
      final String dataSource,
      final Interval interval,
      final SegmentCreateRequest createRequest,
      final List<TimelineObjectHolder<String, DataSegment>> existingChunks
  )
  {
    final List<SegmentIdWithShardSpec> existingPendingSegmentIds = transaction.findPendingSegmentIdsWithExactInterval(
        createRequest.getSequenceName(),
        interval
    );
    final CheckExistingSegmentIdResult result = findPendingSegmentMatchingIntervalAndVersion(
        existingPendingSegmentIds,
        interval,
        createRequest.getSequenceName(),
        null,
        existingChunks.isEmpty() ? null : existingChunks.get(0).getVersion()
    );

    if (result.found) {
      return result.segmentIdentifier;
    }

    final SegmentIdWithShardSpec newIdentifier = createNewPendingSegment(
        transaction,
        dataSource,
        interval,
        createRequest.getPartialShardSpec(),
        createRequest.getVersion(),
        existingChunks
    );
    if (newIdentifier == null) {
      return null;
    }

    // always insert empty previous sequence id
    final PendingSegmentRecord record = PendingSegmentRecord.create(
        newIdentifier,
        createRequest.getSequenceName(),
        "",
        null,
        createRequest.getTaskAllocatorId()
    );
    transaction.insertPendingSegment(record, true);

    log.info(
        "Created new pending segment[%s] for datasource[%s], interval[%s].",
        newIdentifier, dataSource, interval
    );

    return newIdentifier;
  }

  /**
   * Returns a map from sequenceName to segment id.
   */
  private Map<SegmentCreateRequest, CheckExistingSegmentIdResult> getExistingSegmentIdsSkipLineageCheck(
      SegmentMetadataTransaction transaction,
      Interval interval,
      String usedSegmentVersion,
      List<SegmentCreateRequest> requests
  )
  {
    final List<PendingSegmentRecord> existingPendingSegments
        = transaction.findPendingSegmentsWithExactInterval(interval);

    // Map from sequenceName to segment id
    final Map<String, SegmentIdWithShardSpec> sequenceToSegmentId = new HashMap<>();
    for (PendingSegmentRecord record : existingPendingSegments) {
      final SegmentIdWithShardSpec segmentId = record.getId();

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
      SegmentMetadataTransaction transaction,
      Interval interval,
      String usedSegmentVersion,
      List<SegmentCreateRequest> requests
  )
  {
    // This cannot be batched because there doesn't seem to be a clean option:
    // 1. WHERE must have sequence_name and sequence_prev_id but not start or end.
    //    (sequence columns are used to find the matching segment whereas start and
    //    end are used to determine if the found segment is valid or not)
    // 2. IN filters on sequence_name and sequence_prev_id might perform worse than individual SELECTs?
    // 3. IN filter on sequence_name alone might be a feasible option worth evaluating
    final Map<SegmentCreateRequest, CheckExistingSegmentIdResult> requestToResult = new HashMap<>();
    for (SegmentCreateRequest request : requests) {
      final List<SegmentIdWithShardSpec> existingPendingSegmentIds = transaction.findPendingSegmentIds(
          request.getSequenceName(),
          request.getPreviousSegmentId()
      );
      CheckExistingSegmentIdResult result = findPendingSegmentMatchingIntervalAndVersion(
          existingPendingSegmentIds,
          interval,
          request.getSequenceName(),
          request.getPreviousSegmentId(),
          usedSegmentVersion
      );
      requestToResult.put(request, result);
    }

    return requestToResult;
  }

  private CheckExistingSegmentIdResult findPendingSegmentMatchingIntervalAndVersion(
      final List<SegmentIdWithShardSpec> pendingSegments,
      final Interval interval,
      final String sequenceName,
      final @Nullable String previousSegmentId,
      final @Nullable String usedSegmentVersion
  )
  {
    if (pendingSegments.isEmpty()) {
      return new CheckExistingSegmentIdResult(false, null);
    }

    for (SegmentIdWithShardSpec pendingSegment : pendingSegments) {
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
      // Even if the previousSegmentId is set, disregard it when skipping lineage check for streaming ingestion
      this.previousSegmentId = skipSegmentLineageCheck ? null : request.getPreviousSegmentId();
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
      @Nullable String supervisorId,
      @Nullable DataSourceMetadata startMetadata,
      @Nullable DataSourceMetadata endMetadata,
      String taskAllocatorId,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    final String dataSource = verifySegmentsToCommit(appendSegments);
    IndexerMetadataStorageCoordinator.validateDataSourceMetadata(supervisorId, startMetadata, endMetadata);

    final List<PendingSegmentRecord> segmentIdsForNewVersions = inReadOnlyDatasourceTransaction(
        dataSource,
        transaction -> transaction.findPendingSegments(taskAllocatorId)
    );

    // Create entries for all required versions of the append segments
    final Set<DataSegment> allSegmentsToInsert = new HashSet<>(appendSegments);
    final Map<SegmentId, SegmentId> newVersionSegmentToParent = new HashMap<>();
    final Map<String, DataSegment> segmentIdMap = new HashMap<>();
    final Map<String, String> upgradedFromSegmentIdMap = new HashMap<>();
    appendSegments.forEach(segment -> segmentIdMap.put(segment.getId().toString(), segment));
    segmentIdsForNewVersions.forEach(
        pendingSegment -> {
          if (segmentIdMap.containsKey(pendingSegment.getUpgradedFromSegmentId())) {
            final DataSegment oldSegment = segmentIdMap.get(pendingSegment.getUpgradedFromSegmentId());
            final SegmentId newVersionSegmentId = pendingSegment.getId().asSegmentId();
            newVersionSegmentToParent.put(newVersionSegmentId, oldSegment.getId());
            upgradedFromSegmentIdMap.put(newVersionSegmentId.toString(), oldSegment.getId().toString());
            allSegmentsToInsert.add(DataSegment.builder(oldSegment)
                                               .interval(newVersionSegmentId.getInterval())
                                               .version(newVersionSegmentId.getVersion())
                                               .shardSpec(pendingSegment.getId().getShardSpec())
                                               .build());
          }
        }
    );

    try {
      final SegmentPublishResult result = inReadWriteDatasourceTransaction(
          dataSource,
          transaction -> {
            // Try to update datasource metadata first
            if (startMetadata != null) {
              final SegmentPublishResult metadataUpdateResult = updateDataSourceMetadataInTransaction(
                  transaction,
                  supervisorId,
                  dataSource,
                  startMetadata,
                  endMetadata
              );

              // Abort the transaction if datasource metadata update has failed
              if (!metadataUpdateResult.isSuccess()) {
                return metadataUpdateResult;
              }
            }

            insertIntoUpgradeSegmentsTable(transaction, appendSegmentToReplaceLock);

            // Delete the pending segments to be committed in this transaction in batches of at most 100
            int numDeletedPendingSegments = transaction.deletePendingSegments(
                allSegmentsToInsert.stream()
                                   .map(pendingSegment -> pendingSegment.getId().toString())
                                   .collect(Collectors.toSet())
            );
            log.info("Deleted [%d] entries from pending segments table upon commit.", numDeletedPendingSegments);

            return SegmentPublishResult.ok(
                insertSegments(
                    transaction,
                    allSegmentsToInsert,
                    segmentSchemaMapping,
                    Collections.emptyMap(),
                    newVersionSegmentToParent,
                    upgradedFromSegmentIdMap
                )
            );
          }
      );

      // Mark compaction state fingerprints as active after successful publish
      if (result.isSuccess()) {
        markIndexingStateFingerprintsAsActive(result.getSegments());
      }

      return result;
    }
    catch (CallbackFailedException e) {
      throw e;
    }
  }

  private Map<SegmentCreateRequest, PendingSegmentRecord> createNewSegments(
      SegmentMetadataTransaction transaction,
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

    // Shard spec of one of the requests (as they are all compatible) can be used to
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
        transaction.findPendingSegmentsOverlapping(interval)
                   .stream()
                   .map(PendingSegmentRecord::getId)
                   .collect(Collectors.toSet());

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
        createdSegment = createNewPendingSegment(
            transaction,
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
          log.debug("Created new segment[%s]", createdSegment.getId());
        }
      }

      if (createdSegment != null) {
        createdSegments.put(request, createdSegment);
      }
    }

    return createdSegments;
  }

  @Nullable
  private PendingSegmentRecord createNewPendingSegment(
      SegmentMetadataTransaction transaction,
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
      pendingSegmentId = getUniqueIdForPrimaryAllocation(transaction, pendingSegmentId);
      return PendingSegmentRecord.create(
          pendingSegmentId,
          request.getSequenceName(),
          request.getPreviousSegmentId(),
          null,
          request.getTaskAllocatorId()
      );

    } else if (!overallMaxId.getInterval().equals(interval)) {
      log.warn(
          "Cannot allocate new segment for dataSource[%s], interval[%s], existingVersion[%s]: conflicting segment[%s].",
          dataSource, interval, existingVersion, overallMaxId
      );
      return null;
    } else if (committedMaxId != null
               && committedMaxId.getShardSpec().getNumCorePartitions()
                  == SingleDimensionShardSpec.UNKNOWN_NUM_CORE_PARTITIONS) {
      log.warn(
          "Cannot allocate new segment because of unknown core partition size of segment[%s], shardSpec[%s]",
          committedMaxId, committedMaxId.getShardSpec()
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
      return PendingSegmentRecord.create(
          getUniqueIdForSecondaryAllocation(transaction, pendingSegmentId),
          request.getSequenceName(),
          request.getPreviousSegmentId(),
          null,
          request.getTaskAllocatorId()
      );
    }
  }

  /**
   * Creates a new pending segment for the given datasource and interval.
   * @param partialShardSpec Shard spec info minus segment id stuff
   * @param existingVersion Version of segments in interval, used to compute the version of the very first segment in
   *                        interval
   */
  @Nullable
  private SegmentIdWithShardSpec createNewPendingSegment(
      final SegmentMetadataTransaction transaction,
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
    final Set<SegmentIdWithShardSpec> pendings =
        transaction.findPendingSegmentsOverlapping(interval)
                   .stream()
                   .map(PendingSegmentRecord::getId)
                   .collect(Collectors.toSet());

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
      // We are allocating the very first segment for this time chunk.
      // Set numCorePartitions to 0 as core partitions are not determined for append segments
      // When you use segment lock, the core partitions set doesn't work with it. We simply set it 0 so that the
      // OvershadowableManager handles the atomic segment update.
      final int newPartitionId = partialShardSpec.useNonRootGenerationPartitionSpace()
                                 ? PartitionIds.NON_ROOT_GEN_START_PARTITION_ID
                                 : PartitionIds.ROOT_GEN_START_PARTITION_ID;
      String version = newSegmentVersion == null ? existingVersion : newSegmentVersion;
      SegmentIdWithShardSpec allocatedId = new SegmentIdWithShardSpec(
          dataSource,
          interval,
          version,
          partialShardSpec.complete(jsonMapper, newPartitionId, 0)
      );
      return getUniqueIdForPrimaryAllocation(transaction, allocatedId);
    } else if (!overallMaxId.getInterval().equals(interval)) {
      log.warn(
          "Cannot allocate new segment for dataSource[%s], interval[%s], existingVersion[%s]: conflicting segment[%s].",
          dataSource, interval, existingVersion, overallMaxId
      );
      return null;
    } else if (committedMaxId != null
               && committedMaxId.getShardSpec().getNumCorePartitions()
                  == SingleDimensionShardSpec.UNKNOWN_NUM_CORE_PARTITIONS) {
      log.warn(
          "Cannot allocate new segment because of unknown core partition size of segment[%s], shardSpec[%s]",
          committedMaxId, committedMaxId.getShardSpec()
      );
      return null;
    } else {
      // numCorePartitions must always be picked from the committedMaxId and not overallMaxId
      // as overallMaxId may refer to a pending segment which might have stale info of numCorePartitions
      final SegmentIdWithShardSpec allocatedId = new SegmentIdWithShardSpec(
          dataSource,
          interval,
          Preconditions.checkNotNull(newSegmentVersion, "newSegmentVersion"),
          partialShardSpec.complete(
              jsonMapper,
              overallMaxId.getShardSpec().getPartitionNum() + 1,
              committedMaxId == null ? 0 : committedMaxId.getShardSpec().getNumCorePartitions()
          )
      );
      return getUniqueIdForSecondaryAllocation(transaction, allocatedId);
    }
  }

  /**
   * Returns a unique {@link SegmentIdWithShardSpec} which does not clash with
   * any existing unused segment. If an unused segment already exists that matches
   * the interval and version of the given {@code allocatedId}, a fresh version
   * is created by suffixing one or more {@link PendingSegmentRecord#CONCURRENT_APPEND_VERSION_SUFFIX}.
   * Such a conflict can happen only if all the segments in this interval created
   * by a prior APPEND task were marked as unused.
   * <p>
   * This method should be called only when allocating the first segment in an interval.
   */
  private SegmentIdWithShardSpec getUniqueIdForPrimaryAllocation(
      SegmentMetadataTransaction transaction,
      SegmentIdWithShardSpec allocatedId
  )
  {
    // Get all the unused segment versions for this datasource and interval
    final Set<String> unusedSegmentVersions = transaction.noCacheSql().retrieveUnusedSegmentVersionsWithInterval(
        allocatedId.getDataSource(),
        allocatedId.getInterval()
    );

    final String allocatedVersion = allocatedId.getVersion();
    if (!unusedSegmentVersions.contains(allocatedVersion)) {
      // Nothing to do, this version is new
      return allocatedId;
    } else if (!PendingSegmentRecord.DEFAULT_VERSION_FOR_CONCURRENT_APPEND.equals(allocatedVersion)) {
      // Version clash should never happen for non-APPEND locks
      throw DruidException.defensive(
          "Cannot allocate segment[%s] as there are already some unused segments"
          + " for version[%s] in this interval.",
          allocatedId, allocatedVersion
      );
    }

    // Iterate until a new non-clashing version is found
    boolean foundFreshVersion = false;
    StringBuilder candidateVersion = new StringBuilder(allocatedId.getVersion());
    for (int i = 0; i < 10; ++i) {
      if (unusedSegmentVersions.contains(candidateVersion.toString())) {
        candidateVersion.append(PendingSegmentRecord.CONCURRENT_APPEND_VERSION_SUFFIX);
      } else {
        foundFreshVersion = true;
        break;
      }
    }

    if (foundFreshVersion) {
      final SegmentIdWithShardSpec uniqueId = new SegmentIdWithShardSpec(
          allocatedId.getDataSource(),
          allocatedId.getInterval(),
          candidateVersion.toString(),
          allocatedId.getShardSpec()
      );
      log.info(
          "Created new unique pending segment ID[%s] with version[%s] for originally allocated ID[%s].",
          uniqueId, candidateVersion.toString(), allocatedId
      );

      return uniqueId;
    } else {
      throw InternalServerError.exception(
          "Could not allocate segment[%s] as there are too many clashing unused"
          + " versions(upto [%s]) in the interval. Kill the old unused versions to proceed.",
          allocatedId, candidateVersion.toString()
      );
    }
  }

  /**
   * Returns a unique {@link SegmentIdWithShardSpec} which does not clash with
   * any existing unused segment. If an unused segment already exists that matches
   * the interval, version and partition number of the given {@code allocatedId},
   * a higher partition number is used. Such a conflict can happen only if some
   * segments of the underlying version have been marked as unused while others
   * are still used.
   * <p>
   * This method should not be called when allocating the first segment in an
   * interval.
   */
  private SegmentIdWithShardSpec getUniqueIdForSecondaryAllocation(
      SegmentMetadataTransaction transaction,
      SegmentIdWithShardSpec allocatedId
  )
  {
    // Check if there is a conflict with an existing entry in the segments table
    if (transaction.findExistingSegmentIds(Set.of(allocatedId.asSegmentId())).isEmpty()) {
      return allocatedId;
    }

    // If yes, try to compute allocated partition num using the max unused segment shard spec
    SegmentId unusedMaxId = transaction.noCacheSql().retrieveHighestUnusedSegmentId(
        allocatedId.getDataSource(),
        allocatedId.getInterval(),
        allocatedId.getVersion()
    );
    log.info(
        "Allocated SegmentId[%s] is already in use. Using next ID after max[%s].",
        allocatedId.asSegmentId(), unusedMaxId
    );

    // No unused segment. Just return the allocated id
    if (unusedMaxId == null) {
      return allocatedId;
    }

    int maxPartitionNum = Math.max(
        allocatedId.getShardSpec().getPartitionNum(),
        unusedMaxId.getPartitionNum() + 1
    );
    final SegmentIdWithShardSpec uniqueId = new SegmentIdWithShardSpec(
        allocatedId.getDataSource(),
        allocatedId.getInterval(),
        allocatedId.getVersion(),
        new NumberedShardSpec(
            maxPartitionNum,
            allocatedId.getShardSpec().getNumCorePartitions()
        )
    );
    log.info(
        "Created new unique pending segment ID[%s] with partition number[%s] for originally allocated ID[%s].",
        uniqueId, maxPartitionNum, allocatedId
    );

    return uniqueId;
  }

  @Override
  public int deletePendingSegmentsCreatedInInterval(String dataSource, Interval deleteInterval)
  {
    return inReadWriteDatasourceTransaction(
        dataSource,
        transaction -> transaction.deletePendingSegmentsCreatedIn(deleteInterval)
    );
  }

  @Override
  public int deletePendingSegments(String dataSource)
  {
    return inReadWriteDatasourceTransaction(
        dataSource,
        SegmentMetadataTransaction::deleteAllPendingSegments
    );
  }

  private boolean shouldPersistSchema(SegmentSchemaMapping segmentSchemaMapping)
  {
    return schemaPersistEnabled
           && segmentSchemaMapping != null
           && segmentSchemaMapping.isNonEmpty();
  }

  private void persistSchema(
      final SegmentMetadataTransaction transaction,
      final Set<DataSegment> segments,
      final SegmentSchemaMapping segmentSchemaMapping,
      final DateTime updateTime
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
        transaction.getHandle(),
        dataSource,
        segmentSchemaMapping.getSchemaVersion(),
        segmentSchemaMapping.getSchemaFingerprintToPayloadMap(),
        updateTime
    );
  }

  protected Set<DataSegment> insertSegments(
      final SegmentMetadataTransaction transaction,
      final Set<DataSegment> segments,
      @Nullable final SegmentSchemaMapping segmentSchemaMapping
  ) throws Exception
  {
    final Set<DataSegment> toInsertSegments = new HashSet<>();
    try {
      final DateTime createdTime = DateTimes.nowUtc();
      boolean shouldPersistSchema = shouldPersistSchema(segmentSchemaMapping);

      if (shouldPersistSchema) {
        persistSchema(transaction, segments, segmentSchemaMapping, createdTime);
      }

      final Set<SegmentId> segmentIds = segments.stream().map(DataSegment::getId).collect(Collectors.toSet());
      Set<String> existedSegments = transaction.findExistingSegmentIds(segmentIds);
      log.info("Found these segments already exist in DB: %s", existedSegments);

      for (DataSegment segment : segments) {
        if (!existedSegments.contains(segment.getId().toString())) {
          toInsertSegments.add(segment);
        }
      }

      final Set<DataSegment> usedSegments = findNonOvershadowedSegments(segments);

      final Set<DataSegmentPlus> segmentPlusToInsert = toInsertSegments.stream().map(segment -> {
        SegmentMetadata segmentMetadata
            = shouldPersistSchema
              ? segmentSchemaMapping.getSegmentIdToMetadataMap().get(segment.getId().toString())
              : null;

        return new DataSegmentPlus(
            segment,
            createdTime,
            createdTime,
            usedSegments.contains(segment),
            segmentMetadata == null ? null : segmentMetadata.getSchemaFingerprint(),
            segmentMetadata == null ? null : segmentMetadata.getNumRows(),
            null,
            segment.getIndexingStateFingerprint()
        );
      }).collect(Collectors.toSet());

      if (schemaPersistEnabled) {
        transaction.insertSegmentsWithMetadata(segmentPlusToInsert);
      } else {
        transaction.insertSegments(segmentPlusToInsert);
      }
    }
    catch (Exception e) {
      log.errorSegments(e, segments, "Exception inserting segments");
      throw e;
    }

    return toInsertSegments;
  }

  /**
   * Creates new versions of segments appended while a "REPLACE" task was in progress.
   */
  private Set<DataSegmentPlus> createNewIdsOfAppendSegmentsAfterReplace(
      final String dataSource,
      final SegmentMetadataTransaction transaction,
      final Set<DataSegment> replaceSegments,
      final Set<ReplaceTaskLock> locksHeldByReplaceTask
  )
  {
    // If a "REPLACE" task has locked an interval, it would commit some segments
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
        = getAppendSegmentsCommittedDuringTask(transaction, taskId);

    final List<DataSegmentPlus> segmentsToUpgrade
        = retrieveSegmentsById(dataSource, transaction, upgradeSegmentToLockVersion.keySet());

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
          final String conflictingSegmentId = oldSegment.getId().toString();
          final String upgradeVersion = upgradeSegmentToLockVersion.get(conflictingSegmentId);
          throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                              .ofCategory(DruidException.Category.UNSUPPORTED)
                              .build(
                                  "Replacing with a finer segment granularity than a concurrent append is unsupported."
                                  + " Cannot upgrade segment[%s] to version[%s] as the replace interval[%s]"
                                  + " does not fully contain the pending segment interval[%s].",
                                  conflictingSegmentId, upgradeVersion, replaceInterval, oldInterval
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

      // When the segment already has an upgraded_from_segment_id, reuse it for its children
      final String upgradedFromSegmentId = oldSegmentMetadata.getUpgradedFromSegmentId() == null
                                           ? oldSegmentMetadata.getDataSegment().getId().toString()
                                           : oldSegmentMetadata.getUpgradedFromSegmentId();

      upgradedSegments.add(
          new DataSegmentPlus(
              dataSegment,
              null,
              null,
              null,
              oldSegmentMetadata.getSchemaFingerprint(),
              oldSegmentMetadata.getNumRows(),
              upgradedFromSegmentId,
              oldSegmentMetadata.getIndexingStateFingerprint()
          )
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
   * @return Name of the common data source
   */
  private String verifySegmentsToCommit(Collection<DataSegment> segments)
  {
    if (segments.isEmpty()) {
      throw InvalidInput.exception("No segment to commit");
    }

    final String dataSource = segments.iterator().next().getDataSource();
    for (DataSegment segment : segments) {
      if (!dataSource.equals(segment.getDataSource())) {
        throw InvalidInput.exception("Segments to commit must all belong to the same datasource");
      }
    }

    return dataSource;
  }

  private static Set<DataSegment> findNonOvershadowedSegments(Set<DataSegment> segments)
  {
    final Set<DataSegment> nonOvershadowedSegments = new HashSet<>();

    List<TimelineObjectHolder<String, DataSegment>> segmentHolders =
        SegmentTimeline.forSegments(segments).lookupWithIncompletePartitions(Intervals.ETERNITY);
    for (TimelineObjectHolder<String, DataSegment> holder : segmentHolders) {
      for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
        nonOvershadowedSegments.add(chunk.getObject());
      }
    }

    return nonOvershadowedSegments;
  }

  /**
   * Inserts the given segments into the metadata store.
   * <p>
   * This method avoids inserting segment IDs which already exist in the DB.
   * Callers of this method might need to retry as INSERT followed by SELECT
   * might fail due to race conditions.
   *
   * @return Set of segments inserted
   */
  private Set<DataSegment> insertSegments(
      SegmentMetadataTransaction transaction,
      Set<DataSegment> segments,
      @Nullable SegmentSchemaMapping segmentSchemaMapping,
      Map<SegmentId, SegmentMetadata> upgradeSegmentMetadata,
      Map<SegmentId, SegmentId> newVersionForAppendToParent,
      Map<String, String> upgradedFromSegmentIdMap
  ) throws Exception
  {
    final DateTime createdTime = DateTimes.nowUtc();
    if (shouldPersistSchema(segmentSchemaMapping)) {
      persistSchema(transaction, segments, segmentSchemaMapping, createdTime);
    }

    // Do not insert segment IDs which already exist
    final Set<SegmentId> segmentIds = segments.stream().map(DataSegment::getId).collect(Collectors.toSet());
    Set<String> existingSegmentIds = transaction.findExistingSegmentIds(segmentIds);
    final Set<DataSegment> segmentsToInsert = segments.stream().filter(
        s -> !existingSegmentIds.contains(s.getId().toString())
    ).collect(Collectors.toSet());

    final Set<DataSegmentPlus> segmentPlusToInsert = segmentsToInsert.stream().map(segment -> {
      SegmentMetadata segmentMetadata = getSegmentMetadataFromSchemaMappingOrUpgradeMetadata(
          segment.getId(),
          segmentSchemaMapping,
          newVersionForAppendToParent,
          upgradeSegmentMetadata
      );

      return new DataSegmentPlus(
          segment,
          createdTime,
          createdTime,
          true,
          segmentMetadata == null ? null : segmentMetadata.getSchemaFingerprint(),
          segmentMetadata == null ? null : segmentMetadata.getNumRows(),
          upgradedFromSegmentIdMap.get(segment.getId().toString()),
          segment.getIndexingStateFingerprint()
      );
    }).collect(Collectors.toSet());

    if (schemaPersistEnabled) {
      transaction.insertSegmentsWithMetadata(segmentPlusToInsert);
    } else {
      transaction.insertSegments(segmentPlusToInsert);
    }

    return segmentsToInsert;
  }

  @Nullable
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
      SegmentMetadataTransaction transaction,
      Map<DataSegment, ReplaceTaskLock> segmentToReplaceLock
  )
  {
    if (segmentToReplaceLock.isEmpty()) {
      return;
    }

    final PreparedBatch batch = transaction.getHandle().prepareBatch(
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
      if (!failedInserts.isEmpty()) {
        throw new ISE(
            "Failed to insert upgrade segments in DB: %s",
            SegmentUtils.commaSeparatedIdentifiers(failedInserts)
        );
      }
    }
  }

  private List<DataSegmentPlus> retrieveSegmentsById(
      String dataSource,
      SegmentMetadataReadTransaction transaction,
      Set<String> segmentIds
  )
  {
    if (segmentIds.isEmpty()) {
      return Collections.emptyList();
    }

    // Validate segment IDs
    final Set<SegmentId> validSegmentIds = IdUtils.getValidSegmentIds(dataSource, segmentIds);

    if (schemaPersistEnabled) {
      return transaction.findSegmentsWithSchema(validSegmentIds);
    } else {
      return transaction.findSegments(validSegmentIds);
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
      SegmentMetadataTransaction transaction,
      String taskId
  )
  {
    final String sql = StringUtils.format(
        "SELECT segment_id, lock_version FROM %1$s WHERE task_id = :task_id",
        dbTables.getUpgradeSegmentsTable()
    );

    ResultIterator<Pair<String, String>> resultIterator = transaction.getHandle()
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

  /**
   * Read dataSource metadata for the given supervisorId. Returns null if there is no metadata.
   */
  @Override
  public @Nullable DataSourceMetadata retrieveDataSourceMetadata(final String supervisorId)
  {
    final byte[] bytes = connector.lookup(
        dbTables.getDataSourceTable(),
        "dataSource",
        "commit_metadata_payload",
        supervisorId
    );

    if (bytes == null) {
      return null;
    }

    return JacksonUtils.readValue(jsonMapper, bytes, DataSourceMetadata.class);
  }

  /**
   * Read supervisor datasource metadata as bytes, from a specific handle. Returns null if there is no metadata.
   */
  private @Nullable byte[] retrieveDataSourceMetadataAsBytes(
      final SegmentMetadataTransaction transaction,
      final String supervisorId
  )
  {
    return connector.lookupWithHandle(
        transaction.getHandle(),
        dbTables.getDataSourceTable(),
        "dataSource",
        "commit_metadata_payload",
        supervisorId
    );
  }

  /**
   * Compare-and-swap {@link DataSourceMetadata} for a datasource in a transaction.
   * This method updates the metadata for the given datasource only if the
   * currently persisted entry {@link DataSourceMetadata#matches matches} the
   * {@code startMetadata}. If the current entry in the DB
   * {@link DataSourceMetadata#matches matches} the {@code endMetadata}, this
   * method returns immediately with success.
   *
   * @param supervisorId The supervisor ID. Used as the PK for the corresponding metadata entry in the DB.
   * @param dataSource The dataSource. Currently used only for logging purposes.
   * @param startMetadata Current entry in the DB must
   *                      {@link DataSourceMetadata#matches match} this value.
   * @param endMetadata   The updated entry will be equal to the current entry
   *                      {@link DataSourceMetadata#plus(DataSourceMetadata) plus}
   *                      this value.
   * @return Successful {@link SegmentPublishResult} if the metadata in the DB
   * was successfully updated, or if the current entry already matches endMetadata.
   * Otherwise, returns a failed {@link SegmentPublishResult} (retryable or not).
   * @throws IOException          if an error occurred while serializing or deserializing.
   * @throws NullPointerException if any of the arguments is null.
   */
  protected SegmentPublishResult updateDataSourceMetadataInTransaction(
      final SegmentMetadataTransaction transaction,
      final String supervisorId,
      final String dataSource,
      final DataSourceMetadata startMetadata,
      final DataSourceMetadata endMetadata
  ) throws IOException
  {
    Preconditions.checkNotNull(supervisorId, "supervisorId");
    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(startMetadata, "startMetadata");
    Preconditions.checkNotNull(endMetadata, "endMetadata");

    final byte[] oldCommitMetadataBytesFromDb =
        retrieveDataSourceMetadataAsBytes(transaction, supervisorId);
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

    if (startMetadataMatchesExisting) {
      // Proceed with the commit
    } else if (startMetadataGreaterThanExisting) {
      // Offsets stored in startMetadata is greater than the last committed metadata.
      // This can happen because the previous task is still publishing its segments and can resolve once
      // the previous task finishes publishing.
      return SegmentPublishResult.retryableFailure(
          "The new start metadata state[%s] is ahead of the last committed"
          + " end state[%s]. Try resetting the supervisor.",
          startMetadata, oldCommitMetadataFromDb
      );
    } else {
      // startMetadata is older than committed metadata
      // The task trying to publish is probably a replica trying to commit offsets already published by another task.
      // OR the metadata has been updated manually
      return SegmentPublishResult.fail(
          "Stored metadata state[%s] has already been updated by other tasks and"
          + " has diverged from the expected start metadata state[%s]."
          + " This task will be replaced by the supervisor with a new task using updated start offsets."
          + " Try resetting the supervisor if the issue persists.",
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

    final SegmentPublishResult publishResult;
    if (oldCommitMetadataBytesFromDb == null) {
      // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
      final String insertSql = StringUtils.format(
          "INSERT INTO %s (dataSource, created_date, commit_metadata_payload, commit_metadata_sha1) "
          + "VALUES (:dataSource, :created_date, :commit_metadata_payload, :commit_metadata_sha1)",
          dbTables.getDataSourceTable()
      );
      final int numRows = transaction.getHandle().createStatement(insertSql)
                                     .bind("dataSource", supervisorId)
                                     .bind("created_date", DateTimes.nowUtc().toString())
                                     .bind("commit_metadata_payload", newCommitMetadataBytes)
                                     .bind("commit_metadata_sha1", newCommitMetadataSha1)
                                     .execute();

      publishResult = numRows == 1
          ? SegmentPublishResult.ok(Set.of())
          : SegmentPublishResult.retryableFailure("Insert failed");
    } else {
      // Expecting a particular old metadata; use the SHA1 in a compare-and-swap UPDATE
      final String updateSql = StringUtils.format(
          "UPDATE %s SET "
          + "commit_metadata_payload = :new_commit_metadata_payload, "
          + "commit_metadata_sha1 = :new_commit_metadata_sha1 "
          + "WHERE dataSource = :dataSource AND commit_metadata_sha1 = :old_commit_metadata_sha1",
          dbTables.getDataSourceTable()
      );
      final int numRows = transaction.getHandle().createStatement(updateSql)
                                     .bind("dataSource", supervisorId)
                                     .bind("old_commit_metadata_sha1", oldCommitMetadataSha1FromDb)
                                     .bind("new_commit_metadata_payload", newCommitMetadataBytes)
                                     .bind("new_commit_metadata_sha1", newCommitMetadataSha1)
                                     .execute();

      publishResult = numRows == 1
          ? SegmentPublishResult.ok(Set.of())
          : SegmentPublishResult.retryableFailure("Compare-and-swap update failed");
    }

    if (publishResult.isSuccess()) {
      log.info(
          "Updated metadata for supervisor[%s], datasource[%s] from[%s] to[%s].",
          supervisorId, dataSource, oldCommitMetadataFromDb, newCommitMetadata
      );
    } else {
      log.info(
          "Failed to update metadata for supervisor[%s], datasource[%s] due to reason[%s].",
          supervisorId, dataSource, publishResult.getErrorMsg()
      );
    }

    return publishResult;
  }

  @Override
  public boolean deleteDataSourceMetadata(final String supervisorId)
  {
    return connector.retryWithHandle(
        handle -> {
          int rows = handle.createStatement(
              StringUtils.format("DELETE from %s WHERE dataSource = :dataSource", dbTables.getDataSourceTable())
          ).bind("dataSource", supervisorId).execute();

          return rows > 0;
        }
    );
  }

  @Override
  public boolean resetDataSourceMetadata(final String supervisorId, final DataSourceMetadata dataSourceMetadata)
      throws IOException
  {
    final byte[] newCommitMetadataBytes = jsonMapper.writeValueAsBytes(dataSourceMetadata);
    final String newCommitMetadataSha1 = BaseEncoding.base16().encode(
        Hashing.sha1().hashBytes(newCommitMetadataBytes).asBytes()
    );

    final String sql = "UPDATE %s SET "
                       + "commit_metadata_payload = :new_commit_metadata_payload, "
                       + "commit_metadata_sha1 = :new_commit_metadata_sha1 "
                       + "WHERE dataSource = :dataSource";
    return connector.retryWithHandle(
        handle -> {
          final int numRows = handle.createStatement(StringUtils.format(sql, dbTables.getDataSourceTable()))
                                    .bind("dataSource", supervisorId)
                                    .bind("new_commit_metadata_payload", newCommitMetadataBytes)
                                    .bind("new_commit_metadata_sha1", newCommitMetadataSha1)
                                    .execute();
          return numRows == 1;
        }
    );
  }

  @Override
  public void updateSegmentMetadata(final Set<DataSegment> segments)
  {
    final String dataSource = verifySegmentsToCommit(segments);
    inReadWriteDatasourceTransaction(
        dataSource,
        transaction -> {
          for (final DataSegment segment : segments) {
            transaction.updateSegmentPayload(segment);
          }

          return 0;
        }
    );
  }

  @Override
  public int deleteSegments(final Set<DataSegment> segments)
  {
    if (segments.isEmpty()) {
      log.info("No segments to delete.");
      return 0;
    }

    final String dataSource = verifySegmentsToCommit(segments);
    final Set<SegmentId> idsToDelete = segments.stream()
                                            .map(DataSegment::getId)
                                            .collect(Collectors.toSet());
    int numDeletedSegments = inReadWriteDatasourceTransaction(
        dataSource,
        transaction -> transaction.deleteSegments(idsToDelete)
    );

    log.debugSegments(segments, "Delete the metadata of segments");
    return numDeletedSegments;
  }

  @Override
  public boolean insertDataSourceMetadata(String supervisorId, DataSourceMetadata metadata)
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
            .bind("dataSource", supervisorId)
            .bind("created_date", DateTimes.nowUtc().toString())
            .bind("commit_metadata_payload", jsonMapper.writeValueAsBytes(metadata))
            .bind("commit_metadata_sha1", BaseEncoding.base16().encode(
                Hashing.sha1().hashBytes(jsonMapper.writeValueAsBytes(metadata)).asBytes()))
            .execute()
    );
  }

  @Override
  public int removeDataSourceMetadataOlderThan(long timestamp, @NotNull Set<String> excludeSupervisorIds)
  {
    DateTime dateTime = DateTimes.utc(timestamp);
    List<String> supervisorsToDelete = connector.getDBI().withHandle(
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
    supervisorsToDelete.removeAll(excludeSupervisorIds);
    return connector.getDBI().withHandle(
        handle -> {
          final PreparedBatch batch = handle.prepareBatch(
              StringUtils.format(
                  "DELETE FROM %1$s WHERE dataSource = :dataSource AND created_date < '%2$s'",
                  dbTables.getDataSourceTable(),
                  dateTime.toString()
              )
          );
          for (String supervisorId : supervisorsToDelete) {
            batch.bind("dataSource", supervisorId).add();
          }
          int[] result = batch.execute();
          return IntStream.of(result).sum();
        }
    );
  }

  @VisibleForTesting
  Set<DataSegment> retrieveUsedSegmentsForAllocation(
      final SegmentMetadataReadTransaction transaction,
      final String dataSource,
      final Interval interval
  )
  {
    final Set<SegmentId> overlappingSegmentIds
        = transaction.findUsedSegmentIdsOverlapping(interval);
    // Map from version -> interval -> segmentId with the smallest partitionNum
    Map<String, Map<Interval, SegmentId>> versionIntervalToSmallestSegmentId = new HashMap<>();
    for (SegmentId segmentId : overlappingSegmentIds) {
      final Map<Interval, SegmentId> map
          = versionIntervalToSmallestSegmentId.computeIfAbsent(segmentId.getVersion(), v -> new HashMap<>());
      final SegmentId value = map.get(segmentId.getInterval());
      if (value == null || value.getPartitionNum() > segmentId.getPartitionNum()) {
        map.put(segmentId.getInterval(), segmentId);
      }
    }

    // Retrieve the segments for the ids stored in the map to get the numCorePartitions
    final Set<SegmentId> segmentIdsToRetrieve = new HashSet<>();
    for (Map<Interval, SegmentId> itvlMap : versionIntervalToSmallestSegmentId.values()) {
      segmentIdsToRetrieve.addAll(itvlMap.values());
    }
    final List<DataSegmentPlus> dataSegments = transaction.findUsedSegments(segmentIdsToRetrieve);
    final Set<SegmentId> retrievedIds = new HashSet<>();
    final Map<String, Map<Interval, Integer>> versionIntervalToNumCorePartitions = new HashMap<>();
    for (DataSegmentPlus segmentPlus : dataSegments) {
      final DataSegment segment = segmentPlus.getDataSegment();
      versionIntervalToNumCorePartitions.computeIfAbsent(segment.getVersion(), v -> new HashMap<>())
                                        .put(segment.getInterval(), segment.getShardSpec().getNumCorePartitions());
      retrievedIds.add(segment.getId());
    }
    if (!retrievedIds.equals(segmentIdsToRetrieve)) {
      throw DruidException.defensive(
          "Used segment IDs for dataSource[%s] and interval[%s] have changed in the metadata store.",
          dataSource, interval
      );
    }

    // Create dummy segments for each segmentId with only the shard spec populated
    Set<DataSegment> segmentsWithAllocationInfo = new HashSet<>();
    for (SegmentId id : overlappingSegmentIds) {
      final int corePartitions = versionIntervalToNumCorePartitions.get(id.getVersion()).get(id.getInterval());
      segmentsWithAllocationInfo.add(DataSegment.builder(id)
                                                .shardSpec(new NumberedShardSpec(id.getPartitionNum(), corePartitions))
                                                .size(1)
                                                .build());
    }
    return segmentsWithAllocationInfo;
  }

  @Override
  public DataSegment retrieveSegmentForId(SegmentId segmentId)
  {
    return inReadOnlyDatasourceTransaction(
        segmentId.getDataSource(),
        transaction -> transaction.findSegment(segmentId)
    );
  }

  @Override
  public DataSegment retrieveUsedSegmentForId(SegmentId segmentId)
  {
    return inReadOnlyDatasourceTransaction(
        segmentId.getDataSource(),
        transaction -> transaction.findUsedSegment(segmentId)
    );
  }

  @Override
  public int deletePendingSegmentsForTaskAllocatorId(final String datasource, final String taskAllocatorId)
  {
    return inReadWriteDatasourceTransaction(
        datasource,
        transaction -> transaction.deletePendingSegments(taskAllocatorId)
    );
  }

  @Override
  public List<PendingSegmentRecord> getPendingSegments(String datasource, Interval interval)
  {
    return inReadOnlyDatasourceTransaction(
        datasource,
        transaction -> transaction.findPendingSegmentsOverlapping(interval)
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

  @Override
  public Map<String, String> retrieveUpgradedFromSegmentIds(
      final String dataSource,
      final Set<String> segmentIds
  )
  {
    if (segmentIds.isEmpty()) {
      return Collections.emptyMap();
    }

    final Map<String, String> upgradedFromSegmentIds = new HashMap<>();
    final List<List<String>> partitions = Lists.partition(ImmutableList.copyOf(segmentIds), 100);
    for (List<String> partition : partitions) {
      final String sql = StringUtils.format(
          "SELECT id, upgraded_from_segment_id FROM %s WHERE dataSource = :dataSource %s",
          dbTables.getSegmentsTable(),
          SqlSegmentsMetadataQuery.getParameterizedInConditionForColumn("id", partition)
      );
      connector.retryWithHandle(
          handle -> {
            Query<Map<String, Object>> query = handle.createQuery(sql)
                                                     .bind("dataSource", dataSource);
            SqlSegmentsMetadataQuery.bindColumnValuesToQueryWithInCondition("id", partition, query);
            return query.map((index, r, ctx) -> {
              final String id = r.getString(1);
              final String upgradedFromSegmentId = r.getString(2);
              if (upgradedFromSegmentId != null) {
                upgradedFromSegmentIds.put(id, upgradedFromSegmentId);
              }
              return null;
            }).list();
          }
      );
    }
    return upgradedFromSegmentIds;
  }

  @Override
  public Map<String, Set<String>> retrieveUpgradedToSegmentIds(
      final String dataSource,
      final Set<String> segmentIds
  )
  {
    if (segmentIds.isEmpty()) {
      return Collections.emptyMap();
    }

    final Map<String, Set<String>> upgradedToSegmentIds = new HashMap<>();
    retrieveSegmentsById(dataSource, segmentIds)
        .stream()
        .map(DataSegment::getId)
        .map(SegmentId::toString)
        .forEach(id -> upgradedToSegmentIds.computeIfAbsent(id, k -> new HashSet<>()).add(id));

    final List<List<String>> partitions = Lists.partition(ImmutableList.copyOf(segmentIds), 100);
    for (List<String> partition : partitions) {
      final String sql = StringUtils.format(
          "SELECT id, upgraded_from_segment_id FROM %s WHERE dataSource = :dataSource %s",
          dbTables.getSegmentsTable(),
          SqlSegmentsMetadataQuery.getParameterizedInConditionForColumn("upgraded_from_segment_id", partition)
      );

      connector.retryWithHandle(
          handle -> {
            Query<Map<String, Object>> query = handle.createQuery(sql)
                                                     .bind("dataSource", dataSource);
            SqlSegmentsMetadataQuery.bindColumnValuesToQueryWithInCondition(
                "upgraded_from_segment_id",
                partition,
                query
            );
            return query.map((index, r, ctx) -> {
              final String upgradedToId = r.getString(1);
              final String id = r.getString(2);
              upgradedToSegmentIds.computeIfAbsent(id, k -> new HashSet<>())
                                  .add(upgradedToId);
              return null;
            }).list();
          }
      );
    }
    return upgradedToSegmentIds;
  }

  /**
   * Marks indexing state fingerprints as active (non-pending) for successfully published segments.
   * <p>
   * Extracts unique indexing state fingerprints from the given segments and marks them as active
   * in the inexing state storage. This is called after successful segment publishing to indicate
   * that the indexing state is no longer pending and can be retained with the regular grace period.
   *
   * @param segments The segments that were successfully published
   */
  private void markIndexingStateFingerprintsAsActive(Set<DataSegment> segments)
  {
    if (segments == null || segments.isEmpty()) {
      return;
    }

    // Collect unique non-null indexing state fingerprints
    final List<String> fingerprints = segments.stream()
                                             .map(DataSegment::getIndexingStateFingerprint)
                                             .filter(fp -> fp != null && !fp.isEmpty())
                                             .distinct()
                                             .collect(Collectors.toList());

    try {
      int rowsUpdated = indexingStateStorage.markIndexingStatesAsActive(fingerprints);
      if (rowsUpdated > 0) {
        log.info("Marked indexing states active for the following fingerprints: %s", fingerprints);
      }
    }
    catch (Exception e) {
      // Log but don't fail the overall operation - the fingerprint will stay pending
      // and be cleaned up by the pending grace period
      log.warn(e, "Failed to mark indexing states for the following fingerprints as active (Future segments publishes may remediate): %s", fingerprints);
    }
  }

  /**
   * Performs a read-write transaction using the {@link SegmentMetadataTransactionFactory},
   * which may use the segment metadata cache, if enabled and ready.
   */
  private <T> T inReadWriteDatasourceTransaction(
      String dataSource,
      SegmentMetadataTransaction.Callback<T> callback
  )
  {
    return transactionFactory.inReadWriteDatasourceTransaction(dataSource, callback);
  }

  /**
   * Performs a read-only transaction using the {@link SegmentMetadataTransactionFactory},
   * which may use the segment metadata cache, if enabled and ready.
   */
  private <T> T inReadOnlyDatasourceTransaction(
      String dataSource,
      SegmentMetadataReadTransaction.Callback<T> callback
  )
  {
    return transactionFactory.inReadOnlyDatasourceTransaction(dataSource, callback);
  }

  /**
   * Performs a read-only transaction using the {@link SqlSegmentsMetadataQuery},
   * which queries the metadata store directly.
   */
  private <T> T inReadOnlyTransaction(Function<SqlSegmentsMetadataQuery, T> sqlQuery)
  {
    try {
      return connector.retryReadOnlyTransaction(
          (handle, status) -> sqlQuery.apply(
              SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables, jsonMapper)
          ),
          2, 3
      );
    }
    catch (Throwable t) {
      Throwable rootCause = Throwables.getRootCause(t);
      if (rootCause instanceof DruidException) {
        throw (DruidException) rootCause;
      } else {
        throw t;
      }
    }
  }

  /**
   * Performs a write transaction using the {@link SqlSegmentsMetadataQuery},
   * which updates the metadata store directly.
   */
  private <T> T inWriteTransaction(Function<SqlSegmentsMetadataQuery, T> sqlUpdate)
  {
    try {
      return connector.retryTransaction(
          (handle, status) -> sqlUpdate.apply(
              SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables, jsonMapper)
          ),
          2, 3
      );
    }
    catch (Throwable t) {
      Throwable rootCause = Throwables.getRootCause(t);
      if (rootCause instanceof DruidException) {
        throw (DruidException) rootCause;
      } else {
        throw t;
      }
    }
  }
}
