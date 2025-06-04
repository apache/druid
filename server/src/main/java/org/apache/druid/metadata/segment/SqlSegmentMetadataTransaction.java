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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.SqlSegmentsMetadataQuery;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.PreparedBatchPart;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of {@link SegmentMetadataTransaction} that reads from and
 * writes to the SQL-based metadata store directly.
 */
class SqlSegmentMetadataTransaction implements SegmentMetadataTransaction
{
  private static final int MAX_SEGMENTS_PER_BATCH = 100;

  private final String dataSource;
  private final Handle handle;
  private final TransactionStatus transactionStatus;
  private final SQLMetadataConnector connector;
  private final MetadataStorageTablesConfig dbTables;
  private final ObjectMapper jsonMapper;

  private final SqlSegmentsMetadataQuery query;

  SqlSegmentMetadataTransaction(
      String dataSource,
      Handle handle,
      TransactionStatus transactionStatus,
      SQLMetadataConnector connector,
      MetadataStorageTablesConfig dbTables,
      ObjectMapper jsonMapper
  )
  {
    this.dataSource = dataSource;
    this.handle = handle;
    this.connector = connector;
    this.dbTables = dbTables;
    this.jsonMapper = jsonMapper;
    this.transactionStatus = transactionStatus;
    this.query = SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables, jsonMapper);
  }

  @Override
  public Handle getHandle()
  {
    return handle;
  }

  @Override
  public void setRollbackOnly()
  {
    transactionStatus.setRollbackOnly();
  }

  @Override
  public void close()
  {
    // Do nothing here, the JDBI Handle will commit or rollback the transaction as needed
  }

  // READ METHODS

  @Override
  public Set<String> findExistingSegmentIds(Set<SegmentId> segmentIds)
  {
    final Set<String> existingSegmentIds = new HashSet<>();
    final String sql = "SELECT id FROM %s WHERE id in (%s)";

    List<List<SegmentId>> partitions = Lists.partition(
        new ArrayList<>(segmentIds),
        MAX_SEGMENTS_PER_BATCH
    );
    for (List<SegmentId> segmentIdList : partitions) {
      String segmentIdsCsv = segmentIdList.stream().map(
          id -> "'" + StringUtils.escapeSql(id.toString()) + "'"
      ).collect(Collectors.joining(","));

      existingSegmentIds.addAll(
          handle.createQuery(StringUtils.format(sql, dbTables.getSegmentsTable(), segmentIdsCsv))
                .mapTo(String.class)
                .list()
      );
    }

    return existingSegmentIds;
  }

  @Override
  public Set<SegmentId> findUsedSegmentIdsOverlapping(Interval interval)
  {
    return query.retrieveUsedSegmentIds(dataSource, interval);
  }

  @Override
  public SegmentId findHighestUnusedSegmentId(Interval interval, String version)
  {
    return query.retrieveHighestUnusedSegmentId(dataSource, interval, version);
  }

  @Override
  public Set<DataSegment> findUsedSegmentsOverlappingAnyOf(List<Interval> intervals)
  {
    try (CloseableIterator<DataSegment> iterator
             = query.retrieveUsedSegments(dataSource, intervals)) {
      final Set<DataSegment> segments = new HashSet<>();
      iterator.forEachRemaining(segments::add);
      return segments;
    }
    catch (IOException e) {
      throw InternalServerError.exception(e, "Error while fetching segments overlapping intervals[%s].", intervals);
    }
  }

  @Override
  public List<DataSegmentPlus> findUsedSegments(Set<SegmentId> segmentIds)
  {
    return query.retrieveSegmentsById(dataSource, segmentIds);
  }

  @Override
  public Set<DataSegmentPlus> findUsedSegmentsPlusOverlappingAnyOf(List<Interval> intervals)
  {
    try (CloseableIterator<DataSegmentPlus> iterator
             = query.retrieveUsedSegmentsPlus(dataSource, intervals)) {
      return ImmutableSet.copyOf(iterator);
    }
    catch (Exception e) {
      throw DruidException.defensive(e, "Error while retrieving used segments");
    }
  }

  @Override
  public DataSegment findSegment(SegmentId segmentId)
  {
    return query.retrieveSegmentForId(segmentId);
  }

  @Override
  public DataSegment findUsedSegment(SegmentId segmentId)
  {
    return query.retrieveUsedSegmentForId(segmentId);
  }

  @Override
  public List<DataSegmentPlus> findSegments(Set<SegmentId> segmentIds)
  {
    return query.retrieveSegmentsById(dataSource, segmentIds);
  }

  @Override
  public List<DataSegmentPlus> findSegmentsWithSchema(Set<SegmentId> segmentIds)
  {
    return query.retrieveSegmentsWithSchemaById(dataSource, segmentIds);
  }

  @Override
  public List<DataSegment> findUnusedSegments(
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUpdatedTime
  )
  {
    try (final CloseableIterator<DataSegment> iterator =
             query.retrieveUnusedSegments(
                 dataSource,
                 List.of(interval),
                 versions,
                 limit,
                 null,
                 null,
                 maxUpdatedTime
             )
    ) {
      return ImmutableList.copyOf(iterator);
    }
    catch (IOException e) {
      throw DruidException.defensive(e, "Error while reading unused segments");
    }
  }

  // WRITE METHODS

  @Override
  public int insertSegments(Set<DataSegmentPlus> segments)
  {
    return insertSegmentsInBatches(
        dataSource,
        segments,
        "INSERT INTO %1$s "
        + "(id, dataSource, created_date, start, %2$send%2$s, partitioned, "
        + "version, used, payload, used_status_last_updated, upgraded_from_segment_id) "
        + "VALUES "
        + "(:id, :dataSource, :created_date, :start, :end, :partitioned, "
        + ":version, :used, :payload, :used_status_last_updated, :upgraded_from_segment_id)"
    );
  }

  @Override
  public int insertSegmentsWithMetadata(Set<DataSegmentPlus> segments)
  {
    return insertSegmentsInBatches(
        dataSource,
        segments,
        "INSERT INTO %1$s "
        + "(id, dataSource, created_date, start, %2$send%2$s, partitioned, "
        + "version, used, payload, used_status_last_updated, upgraded_from_segment_id, "
        + "schema_fingerprint, num_rows) "
        + "VALUES "
        + "(:id, :dataSource, :created_date, :start, :end, :partitioned, "
        + ":version, :used, :payload, :used_status_last_updated, :upgraded_from_segment_id, "
        + ":schema_fingerprint, :num_rows)"
    );
  }

  @Override
  public boolean markSegmentAsUnused(SegmentId segmentId, DateTime updateTime)
  {
    return query.markSegmentsAsUnused(Set.of(segmentId), updateTime) > 0;
  }

  @Override
  public int markSegmentsAsUnused(Set<SegmentId> segmentIds, DateTime updateTime)
  {
    return query.markSegmentsAsUnused(segmentIds, updateTime);
  }

  @Override
  public int markAllSegmentsAsUnused(DateTime updateTime)
  {
    return query.markSegmentsUnused(dataSource, Intervals.ETERNITY, null, updateTime);
  }

  @Override
  public int markSegmentsWithinIntervalAsUnused(Interval interval, @Nullable List<String> versions, DateTime updateTime)
  {
    return query.markSegmentsUnused(dataSource, interval, versions, updateTime);
  }

  @Override
  public int deleteSegments(Set<SegmentId> segmentsIdsToDelete)
  {
    final String deleteSql = StringUtils.format("DELETE from %s WHERE id = :id", dbTables.getSegmentsTable());

    final PreparedBatch batch = handle.prepareBatch(deleteSql);
    for (SegmentId id : segmentsIdsToDelete) {
      batch.bind("id", id.toString()).add();
    }

    int[] deletedRows = batch.execute();
    return Arrays.stream(deletedRows).sum();
  }

  @Override
  public boolean updateSegmentPayload(DataSegment segment)
  {
    final String sql = "UPDATE %s SET payload = :payload WHERE id = :id";
    int updatedCount = handle
        .createStatement(StringUtils.format(sql, dbTables.getSegmentsTable()))
        .bind("id", segment.getId().toString())
        .bind("payload", getJsonBytes(segment))
        .execute();

    return updatedCount > 0;
  }

  @Override
  public List<SegmentIdWithShardSpec> findPendingSegmentIds(
      String sequenceName,
      String sequencePreviousId
  )
  {
    return query.retrievePendingSegmentIds(dataSource, sequenceName, sequencePreviousId);
  }

  @Override
  public List<SegmentIdWithShardSpec> findPendingSegmentIdsWithExactInterval(
      String sequenceName,
      Interval interval
  )
  {
    return query.retrievePendingSegmentIdsWithExactInterval(dataSource, sequenceName, interval);
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegmentsOverlapping(Interval interval)
  {
    return query.retrievePendingSegmentsOverlappingInterval(dataSource, interval);
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegmentsWithExactInterval(Interval interval)
  {
    return query.retrievePendingSegmentsWithExactInterval(dataSource, interval);
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegments(String taskAllocatorId)
  {
    return query.retrievePendingSegmentsForTaskAllocatorId(dataSource, taskAllocatorId);
  }

  @Override
  public boolean insertPendingSegment(
      PendingSegmentRecord pendingSegment,
      boolean skipSegmentLineageCheck
  )
  {
    final SegmentIdWithShardSpec segmentId = pendingSegment.getId();
    final Interval interval = segmentId.getInterval();
    int updatedCount = handle.createStatement(getSqlToInsertPendingSegment())
          .bind("id", segmentId.toString())
          .bind("dataSource", dataSource)
          .bind("created_date", toNonNullString(pendingSegment.getCreatedDate()))
          .bind("start", interval.getStart().toString())
          .bind("end", interval.getEnd().toString())
          .bind("sequence_name", pendingSegment.getSequenceName())
          .bind("sequence_prev_id", pendingSegment.getSequencePrevId())
          .bind(
              "sequence_name_prev_id_sha1",
              pendingSegment.computeSequenceNamePrevIdSha1(skipSegmentLineageCheck)
          )
          .bind("payload", getJsonBytes(segmentId))
          .bind("task_allocator_id", pendingSegment.getTaskAllocatorId())
          .bind("upgraded_from_segment_id", pendingSegment.getUpgradedFromSegmentId())
          .execute();

    return updatedCount > 0;
  }

  @Override
  public int insertPendingSegments(
      List<PendingSegmentRecord> pendingSegments,
      boolean skipSegmentLineageCheck
  )
  {
    final PreparedBatch insertBatch = handle.prepareBatch(getSqlToInsertPendingSegment());

    final Set<SegmentIdWithShardSpec> processedSegmentIds = new HashSet<>();
    for (PendingSegmentRecord pendingSegment : pendingSegments) {
      final SegmentIdWithShardSpec segmentId = pendingSegment.getId();
      if (processedSegmentIds.contains(segmentId)) {
        continue;
      }
      final Interval interval = segmentId.getInterval();

      insertBatch.add()
                 .bind("id", segmentId.toString())
                 .bind("dataSource", dataSource)
                 .bind("created_date", toNonNullString(pendingSegment.getCreatedDate()))
                 .bind("start", interval.getStart().toString())
                 .bind("end", interval.getEnd().toString())
                 .bind("sequence_name", pendingSegment.getSequenceName())
                 .bind("sequence_prev_id", pendingSegment.getSequencePrevId())
                 .bind(
                     "sequence_name_prev_id_sha1",
                     pendingSegment.computeSequenceNamePrevIdSha1(skipSegmentLineageCheck)
                 )
                 .bind("payload", getJsonBytes(segmentId))
                 .bind("task_allocator_id", pendingSegment.getTaskAllocatorId())
                 .bind("upgraded_from_segment_id", pendingSegment.getUpgradedFromSegmentId());

      processedSegmentIds.add(segmentId);
    }
    int[] updated = insertBatch.execute();
    return Arrays.stream(updated).sum();
  }

  @Override
  public int deleteAllPendingSegments()
  {
    final String sql = StringUtils.format(
        "DELETE FROM %s WHERE datasource = :dataSource",
        dbTables.getPendingSegmentsTable()
    );
    return handle.createStatement(sql).bind("dataSource", dataSource).execute();
  }

  @Override
  public int deletePendingSegments(Set<String> segmentIdsToDelete)
  {
    if (segmentIdsToDelete.isEmpty()) {
      return 0;
    }

    final List<List<String>> pendingSegmentIdBatches
        = Lists.partition(List.copyOf(segmentIdsToDelete), MAX_SEGMENTS_PER_BATCH);

    int numDeletedPendingSegments = 0;
    for (List<String> pendingSegmentIdBatch : pendingSegmentIdBatches) {
      numDeletedPendingSegments += deletePendingSegmentsBatch(pendingSegmentIdBatch);
    }

    return numDeletedPendingSegments;
  }

  @Override
  public int deletePendingSegments(String taskAllocatorId)
  {
    final String sql = StringUtils.format(
        "DELETE FROM %s WHERE dataSource = :dataSource"
        + " AND task_allocator_id = :task_allocator_id",
        dbTables.getPendingSegmentsTable()
    );

    return handle
        .createStatement(sql)
        .bind("dataSource", dataSource)
        .bind("task_allocator_id", taskAllocatorId)
        .execute();
  }

  @Override
  public int deletePendingSegmentsCreatedIn(Interval interval)
  {
    final String sql = StringUtils.format(
        "DELETE FROM %s WHERE datasource = :dataSource"
        + " AND created_date >= :start AND created_date < :end",
        dbTables.getPendingSegmentsTable()
    );
    return handle
        .createStatement(sql)
        .bind("dataSource", dataSource)
        .bind("start", interval.getStart().toString())
        .bind("end", interval.getEnd().toString())
        .execute();
  }

  private int deletePendingSegmentsBatch(List<String> segmentIdsToDelete)
  {
    Update query = handle.createStatement(
        StringUtils.format(
            "DELETE FROM %s WHERE dataSource = :dataSource %s",
            dbTables.getPendingSegmentsTable(),
            SqlSegmentsMetadataQuery.getParameterizedInConditionForColumn("id", segmentIdsToDelete)
        )
    ).bind("dataSource", dataSource);
    SqlSegmentsMetadataQuery.bindColumnValuesToQueryWithInCondition("id", segmentIdsToDelete, query);

    return query.execute();
  }

  private int insertSegmentsInBatches(
      final String dataSource,
      final Set<DataSegmentPlus> segments,
      String insertSql
  )
  {
    final List<List<DataSegmentPlus>> partitionedSegments = Lists.partition(
        new ArrayList<>(segments),
        MAX_SEGMENTS_PER_BATCH
    );

    final boolean persistAdditionalMetadata = insertSql.contains(":schema_fingerprint");

    // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
    // Avoiding ON DUPLICATE KEY since it's not portable.
    // Avoiding try/catch since it may cause inadvertent transaction-splitting.
    final PreparedBatch batch = handle.prepareBatch(
        StringUtils.format(insertSql, dbTables.getSegmentsTable(), connector.getQuoteString())
    );

    int numInsertedSegments = 0;
    for (List<DataSegmentPlus> partition : partitionedSegments) {
      for (DataSegmentPlus segmentPlus : partition) {
        final DataSegment segment = segmentPlus.getDataSegment();
        PreparedBatchPart preparedBatchPart =
            batch.add()
                 .bind("id", segment.getId().toString())
                 .bind("dataSource", dataSource)
                 .bind("created_date", toNonNullString(segmentPlus.getCreatedDate()))
                 .bind("start", segment.getInterval().getStart().toString())
                 .bind("end", segment.getInterval().getEnd().toString())
                 .bind("partitioned", true)
                 .bind("version", segment.getVersion())
                 .bind("used", Boolean.TRUE.equals(segmentPlus.getUsed()))
                 .bind("payload", getJsonBytes(segment))
                 .bind("used_status_last_updated", toNonNullString(segmentPlus.getUsedStatusLastUpdatedDate()))
                 .bind("upgraded_from_segment_id", segmentPlus.getUpgradedFromSegmentId());

        if (persistAdditionalMetadata) {
          preparedBatchPart
              .bind("num_rows", segmentPlus.getNumRows())
              .bind("schema_fingerprint", segmentPlus.getSchemaFingerprint());
        }
      }

      // Execute the batch and ensure that all the segments were inserted
      final int[] affectedRows = batch.execute();

      final List<DataSegment> failedInserts = new ArrayList<>();
      for (int i = 0; i < partition.size(); ++i) {
        if (affectedRows[i] == 1) {
          ++numInsertedSegments;
        } else {
          failedInserts.add(partition.get(i).getDataSegment());
        }
      }
      if (!failedInserts.isEmpty()) {
        throw InternalServerError.exception(
            "Failed to insert segments in metadata store: %s",
            SegmentUtils.commaSeparatedIdentifiers(failedInserts)
        );
      }
    }

    return numInsertedSegments;
  }

  private String getSqlToInsertPendingSegment()
  {
    return StringUtils.format(
        "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, sequence_name, sequence_prev_id, "
        + "sequence_name_prev_id_sha1, payload, task_allocator_id, upgraded_from_segment_id) "
        + "VALUES (:id, :dataSource, :created_date, :start, :end, :sequence_name, :sequence_prev_id, "
        + ":sequence_name_prev_id_sha1, :payload, :task_allocator_id, :upgraded_from_segment_id)",
        dbTables.getPendingSegmentsTable(),
        connector.getQuoteString()
    );
  }

  private static String toNonNullString(DateTime date)
  {
    if (date == null) {
      throw DruidException.defensive("Created date cannot be null");
    }
    return date.toString();
  }

  private <T> byte[] getJsonBytes(T object)
  {
    try {
      return jsonMapper.writeValueAsBytes(object);
    }
    catch (JsonProcessingException e) {
      throw InternalServerError.exception("Could not serialize object[%s]", object);
    }
  }
}
