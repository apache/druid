/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.inject.Inject;
import io.druid.common.utils.JodaUtils;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.SegmentPublishResult;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.StringMapper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class IndexerSQLMetadataStorageCoordinator implements IndexerMetadataStorageCoordinator
{
  private static final Logger log = new Logger(IndexerSQLMetadataStorageCoordinator.class);
  private static final int ALLOCATE_SEGMENT_QUIET_TRIES = 3;

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

  enum DataSourceMetadataUpdateResult
  {
    SUCCESS,
    FAILURE,
    TRY_AGAIN
  }

  @LifecycleStart
  public void start()
  {
    connector.createDataSourceTable();
    connector.createPendingSegmentsTable();
    connector.createSegmentTable();
  }

  @Override
  public List<DataSegment> getUsedSegmentsForInterval(
      final String dataSource,
      final Interval interval
  ) throws IOException
  {
    return getUsedSegmentsForIntervals(dataSource, ImmutableList.of(interval));
  }

  @Override
  public List<DataSegment> getUsedSegmentsForIntervals(
      final String dataSource, final List<Interval> intervals
  ) throws IOException
  {
    return connector.retryWithHandle(
        new HandleCallback<List<DataSegment>>()
        {
          @Override
          public List<DataSegment> withHandle(Handle handle) throws Exception
          {
            final VersionedIntervalTimeline<String, DataSegment> timeline = getTimelineForIntervalsWithHandle(
                handle,
                dataSource,
                intervals
            );

            Set<DataSegment> segments = Sets.newHashSet(
                Iterables.concat(
                    Iterables.transform(
                        Iterables.concat(
                            Iterables.transform(
                                intervals,
                                new Function<Interval, Iterable<TimelineObjectHolder<String, DataSegment>>>()
                                {
                                  @Override
                                  public Iterable<TimelineObjectHolder<String, DataSegment>> apply(Interval interval)
                                  {
                                    return timeline.lookup(interval);
                                  }
                                }
                            )
                        ),
                        new Function<TimelineObjectHolder<String, DataSegment>, Iterable<DataSegment>>()
                        {
                          @Override
                          public Iterable<DataSegment> apply(TimelineObjectHolder<String, DataSegment> input)
                          {
                            return input.getObject().payloads();
                          }
                        }
                    )
                )
            );

            return new ArrayList<>(segments);
          }
        }
    );
  }

  private List<SegmentIdentifier> getPendingSegmentsForIntervalWithHandle(
      final Handle handle,
      final String dataSource,
      final Interval interval
  ) throws IOException
  {
    final List<SegmentIdentifier> identifiers = Lists.newArrayList();

    final ResultIterator<byte[]> dbSegments =
        handle.createQuery(
            StringUtils.format(
                "SELECT payload FROM %1$s WHERE dataSource = :dataSource AND start <= :end and %2$send%2$s >= :start",
                dbTables.getPendingSegmentsTable(), connector.getQuoteString()
            )
        )
              .bind("dataSource", dataSource)
              .bind("start", interval.getStart().toString())
              .bind("end", interval.getEnd().toString())
              .map(ByteArrayMapper.FIRST)
              .iterator();

    while (dbSegments.hasNext()) {
      final byte[] payload = dbSegments.next();
      final SegmentIdentifier identifier = jsonMapper.readValue(payload, SegmentIdentifier.class);

      if (interval.overlaps(identifier.getInterval())) {
        identifiers.add(identifier);
      }
    }

    dbSegments.close();

    return identifiers;
  }

  private VersionedIntervalTimeline<String, DataSegment> getTimelineForIntervalsWithHandle(
      final Handle handle,
      final String dataSource,
      final List<Interval> intervals
  ) throws IOException
  {
    if (intervals == null || intervals.isEmpty()) {
      throw new IAE("null/empty intervals");
    }

    final StringBuilder sb = new StringBuilder();
    sb.append("SELECT payload FROM %s WHERE used = true AND dataSource = ? AND (");
    for (int i = 0; i < intervals.size(); i++) {
      sb.append(
          StringUtils.format("(start <= ? AND %1$send%1$s >= ?)", connector.getQuoteString())
      );
      if (i == intervals.size() - 1) {
        sb.append(")");
      } else {
        sb.append(" OR ");
      }
    }

    Query<Map<String, Object>> sql = handle.createQuery(
        StringUtils.format(
            sb.toString(),
            dbTables.getSegmentsTable()
        )
    ).bind(0, dataSource);

    for (int i = 0; i < intervals.size(); i++) {
      Interval interval = intervals.get(i);
      sql = sql
          .bind(2 * i + 1, interval.getEnd().toString())
          .bind(2 * i + 2, interval.getStart().toString());
    }

    final ResultIterator<byte[]> dbSegments = sql
        .map(ByteArrayMapper.FIRST)
        .iterator();

    final VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>(
        Ordering.natural()
    );

    while (dbSegments.hasNext()) {
      final byte[] payload = dbSegments.next();

      DataSegment segment = jsonMapper.readValue(
          payload,
          DataSegment.class
      );

      timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment));

    }

    dbSegments.close();

    return timeline;
  }

  /**
   * Attempts to insert a set of segments to the database. Returns the set of segments actually added (segments
   * with identifiers already in the database will not be added).
   *
   * @param segments set of segments to add
   *
   * @return set of segments actually added
   */
  @Override
  public Set<DataSegment> announceHistoricalSegments(final Set<DataSegment> segments) throws IOException
  {
    final SegmentPublishResult result = announceHistoricalSegments(segments, null, null);

    // Metadata transaction cannot fail because we are not trying to do one.
    if (!result.isSuccess()) {
      throw new ISE("WTF?! announceHistoricalSegments failed with null metadata, should not happen.");
    }

    return result.getSegments();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SegmentPublishResult announceHistoricalSegments(
      final Set<DataSegment> segments,
      final DataSourceMetadata startMetadata,
      final DataSourceMetadata endMetadata
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
    final Set<DataSegment> usedSegments = Sets.newHashSet();
    for (TimelineObjectHolder<String, DataSegment> holder : VersionedIntervalTimeline.forSegments(segments)
                                                                                     .lookupWithIncompletePartitions(JodaUtils.ETERNITY)) {
      for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
        usedSegments.add(chunk.getObject());
      }
    }

    final AtomicBoolean txnFailure = new AtomicBoolean(false);

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
              final Set<DataSegment> inserted = Sets.newHashSet();

              if (startMetadata != null) {
                final DataSourceMetadataUpdateResult result = updateDataSourceMetadataWithHandle(
                    handle,
                    dataSource,
                    startMetadata,
                    endMetadata
                );

                if (result != DataSourceMetadataUpdateResult.SUCCESS) {
                  transactionStatus.setRollbackOnly();
                  txnFailure.set(true);

                  if (result == DataSourceMetadataUpdateResult.FAILURE) {
                    throw new RuntimeException("Aborting transaction!");
                  } else if (result == DataSourceMetadataUpdateResult.TRY_AGAIN) {
                    throw new RetryTransactionException("Aborting transaction!");
                  }
                }
              }

              for (final DataSegment segment : segments) {
                if (announceHistoricalSegment(handle, segment, usedSegments.contains(segment))) {
                  inserted.add(segment);
                }
              }

              return new SegmentPublishResult(ImmutableSet.copyOf(inserted), true);
            }
          },
          3,
          SQLMetadataConnector.DEFAULT_MAX_TRIES
      );
    }
    catch (CallbackFailedException e) {
      if (txnFailure.get()) {
        return new SegmentPublishResult(ImmutableSet.<DataSegment>of(), false);
      } else {
        throw e;
      }
    }
  }

  @Override
  public SegmentIdentifier allocatePendingSegment(
      final String dataSource,
      final String sequenceName,
      final String previousSegmentId,
      final Interval interval,
      final String maxVersion
  ) throws IOException
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(sequenceName, "sequenceName");
    Preconditions.checkNotNull(interval, "interval");
    Preconditions.checkNotNull(maxVersion, "maxVersion");

    final String previousSegmentIdNotNull = previousSegmentId == null ? "" : previousSegmentId;

    return connector.retryTransaction(
        new TransactionCallback<SegmentIdentifier>()
        {
          @Override
          public SegmentIdentifier inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            final List<byte[]> existingBytes = handle
                .createQuery(
                    StringUtils.format(
                        "SELECT payload FROM %s WHERE "
                        + "dataSource = :dataSource AND "
                        + "sequence_name = :sequence_name AND "
                        + "sequence_prev_id = :sequence_prev_id",
                        dbTables.getPendingSegmentsTable()
                    )
                ).bind("dataSource", dataSource)
                .bind("sequence_name", sequenceName)
                .bind("sequence_prev_id", previousSegmentIdNotNull)
                .map(ByteArrayMapper.FIRST)
                .list();

            if (!existingBytes.isEmpty()) {
              final SegmentIdentifier existingIdentifier = jsonMapper.readValue(
                  Iterables.getOnlyElement(existingBytes),
                  SegmentIdentifier.class
              );

              if (existingIdentifier.getInterval().getStartMillis() == interval.getStartMillis()
                  && existingIdentifier.getInterval().getEndMillis() == interval.getEndMillis()) {
                log.info(
                    "Found existing pending segment [%s] for sequence[%s] (previous = [%s]) in DB",
                    existingIdentifier.getIdentifierAsString(),
                    sequenceName,
                    previousSegmentIdNotNull
                );

                return existingIdentifier;
              } else {
                log.warn(
                    "Cannot use existing pending segment [%s] for sequence[%s] (previous = [%s]) in DB, "
                    + "does not match requested interval[%s]",
                    existingIdentifier.getIdentifierAsString(),
                    sequenceName,
                    previousSegmentIdNotNull,
                    interval
                );

                return null;
              }
            }

            // Make up a pending segment based on existing segments and pending segments in the DB. This works
            // assuming that all tasks inserting segments at a particular point in time are going through the
            // allocatePendingSegment flow. This should be assured through some other mechanism (like task locks).

            final SegmentIdentifier newIdentifier;

            final List<TimelineObjectHolder<String, DataSegment>> existingChunks = getTimelineForIntervalsWithHandle(
                handle,
                dataSource,
                ImmutableList.of(interval)
            ).lookup(interval);

            if (existingChunks.size() > 1) {
              // Not possible to expand more than one chunk with a single segment.
              log.warn(
                  "Cannot allocate new segment for dataSource[%s], interval[%s], maxVersion[%s]: already have [%,d] chunks.",
                  dataSource,
                  interval,
                  maxVersion,
                  existingChunks.size()
              );
              return null;
            } else {
              SegmentIdentifier max = null;

              if (!existingChunks.isEmpty()) {
                TimelineObjectHolder<String, DataSegment> existingHolder = Iterables.getOnlyElement(existingChunks);
                for (PartitionChunk<DataSegment> existing : existingHolder.getObject()) {
                  if (max == null || max.getShardSpec().getPartitionNum() < existing.getObject()
                                                                                    .getShardSpec()
                                                                                    .getPartitionNum()) {
                    max = SegmentIdentifier.fromDataSegment(existing.getObject());
                  }
                }
              }

              final List<SegmentIdentifier> pendings = getPendingSegmentsForIntervalWithHandle(
                  handle,
                  dataSource,
                  interval
              );

              for (SegmentIdentifier pending : pendings) {
                if (max == null ||
                    pending.getVersion().compareTo(max.getVersion()) > 0 ||
                    (pending.getVersion().equals(max.getVersion())
                     && pending.getShardSpec().getPartitionNum() > max.getShardSpec().getPartitionNum())) {
                  max = pending;
                }
              }

              if (max == null) {
                newIdentifier = new SegmentIdentifier(
                    dataSource,
                    interval,
                    maxVersion,
                    new NumberedShardSpec(0, 0)
                );
              } else if (!max.getInterval().equals(interval) || max.getVersion().compareTo(maxVersion) > 0) {
                log.warn(
                    "Cannot allocate new segment for dataSource[%s], interval[%s], maxVersion[%s]: conflicting segment[%s].",
                    dataSource,
                    interval,
                    maxVersion,
                    max.getIdentifierAsString()
                );
                return null;
              } else if (max.getShardSpec() instanceof LinearShardSpec) {
                newIdentifier = new SegmentIdentifier(
                    dataSource,
                    max.getInterval(),
                    max.getVersion(),
                    new LinearShardSpec(max.getShardSpec().getPartitionNum() + 1)
                );
              } else if (max.getShardSpec() instanceof NumberedShardSpec) {
                newIdentifier = new SegmentIdentifier(
                    dataSource,
                    max.getInterval(),
                    max.getVersion(),
                    new NumberedShardSpec(
                        max.getShardSpec().getPartitionNum() + 1,
                        ((NumberedShardSpec) max.getShardSpec()).getPartitions()
                    )
                );
              } else {
                log.warn(
                    "Cannot allocate new segment for dataSource[%s], interval[%s], maxVersion[%s]: ShardSpec class[%s] used by [%s].",
                    dataSource,
                    interval,
                    maxVersion,
                    max.getShardSpec().getClass(),
                    max.getIdentifierAsString()
                );
                return null;
              }
            }

            // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
            // Avoiding ON DUPLICATE KEY since it's not portable.
            // Avoiding try/catch since it may cause inadvertent transaction-splitting.

            // UNIQUE key for the row, ensuring sequences do not fork in two directions.
            // Using a single column instead of (sequence_name, sequence_prev_id) as some MySQL storage engines
            // have difficulty with large unique keys (see https://github.com/druid-io/druid/issues/2319)
            final String sequenceNamePrevIdSha1 = BaseEncoding.base16().encode(
                Hashing.sha1()
                       .newHasher()
                       .putBytes(StringUtils.toUtf8(sequenceName))
                       .putByte((byte) 0xff)
                       .putBytes(StringUtils.toUtf8(previousSegmentIdNotNull))
                       .hash()
                       .asBytes()
            );

            handle.createStatement(
                StringUtils.format(
                    "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, sequence_name, sequence_prev_id, sequence_name_prev_id_sha1, payload) "
                    + "VALUES (:id, :dataSource, :created_date, :start, :end, :sequence_name, :sequence_prev_id, :sequence_name_prev_id_sha1, :payload)",
                    dbTables.getPendingSegmentsTable(), connector.getQuoteString()
                )
            )
                  .bind("id", newIdentifier.getIdentifierAsString())
                  .bind("dataSource", dataSource)
                  .bind("created_date", new DateTime().toString())
                  .bind("start", interval.getStart().toString())
                  .bind("end", interval.getEnd().toString())
                  .bind("sequence_name", sequenceName)
                  .bind("sequence_prev_id", previousSegmentIdNotNull)
                  .bind("sequence_name_prev_id_sha1", sequenceNamePrevIdSha1)
                  .bind("payload", jsonMapper.writeValueAsBytes(newIdentifier))
                  .execute();

            log.info(
                "Allocated pending segment [%s] for sequence[%s] (previous = [%s]) in DB",
                newIdentifier.getIdentifierAsString(),
                sequenceName,
                previousSegmentIdNotNull
            );

            return newIdentifier;
          }
        },
        ALLOCATE_SEGMENT_QUIET_TRIES,
        SQLMetadataConnector.DEFAULT_MAX_TRIES
    );
  }

  /**
   * Attempts to insert a single segment to the database. If the segment already exists, will do nothing; although,
   * this checking is imperfect and callers must be prepared to retry their entire transaction on exceptions.
   *
   * @return true if the segment was added, false if it already existed
   */
  private boolean announceHistoricalSegment(
      final Handle handle,
      final DataSegment segment,
      final boolean used
  ) throws IOException
  {
    try {
      if (segmentExists(handle, segment)) {
        log.info("Found [%s] in DB, not updating DB", segment.getIdentifier());
        return false;
      }

      // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
      // Avoiding ON DUPLICATE KEY since it's not portable.
      // Avoiding try/catch since it may cause inadvertent transaction-splitting.
      handle.createStatement(
          StringUtils.format(
              "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, partitioned, version, used, payload) "
              + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
              dbTables.getSegmentsTable(), connector.getQuoteString()
          )
      )
            .bind("id", segment.getIdentifier())
            .bind("dataSource", segment.getDataSource())
            .bind("created_date", new DateTime().toString())
            .bind("start", segment.getInterval().getStart().toString())
            .bind("end", segment.getInterval().getEnd().toString())
            .bind("partitioned", (segment.getShardSpec() instanceof NoneShardSpec) ? false : true)
            .bind("version", segment.getVersion())
            .bind("used", used)
            .bind("payload", jsonMapper.writeValueAsBytes(segment))
            .execute();

      log.info("Published segment [%s] to DB with used flag [%s]", segment.getIdentifier(), used);
    }
    catch (Exception e) {
      log.error(e, "Exception inserting segment [%s] with used flag [%s] into DB", segment.getIdentifier(), used);
      throw e;
    }

    return true;
  }

  private boolean segmentExists(final Handle handle, final DataSegment segment)
  {
    return !handle
        .createQuery(
            StringUtils.format(
                "SELECT id FROM %s WHERE id = :identifier",
                dbTables.getSegmentsTable()
            )
        ).bind("identifier", segment.getIdentifier())
        .map(StringMapper.FIRST)
        .list()
        .isEmpty();
  }

  /**
   * Read dataSource metadata. Returns null if there is no metadata.
   */
  @Override
  public DataSourceMetadata getDataSourceMetadata(final String dataSource)
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

    try {
      return jsonMapper.readValue(bytes, DataSourceMetadata.class);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Read dataSource metadata as bytes, from a specific handle. Returns null if there is no metadata.
   */
  private byte[] getDataSourceMetadataWithHandleAsBytes(
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
   * @return true if dataSource metadata was updated from matching startMetadata to matching endMetadata
   */
  protected DataSourceMetadataUpdateResult updateDataSourceMetadataWithHandle(
      final Handle handle,
      final String dataSource,
      final DataSourceMetadata startMetadata,
      final DataSourceMetadata endMetadata
  ) throws IOException
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(startMetadata, "startMetadata");
    Preconditions.checkNotNull(endMetadata, "endMetadata");

    final byte[] oldCommitMetadataBytesFromDb = getDataSourceMetadataWithHandleAsBytes(handle, dataSource);
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

    final boolean startMetadataMatchesExisting = oldCommitMetadataFromDb == null
                                                 ? startMetadata.isValidStart()
                                                 : startMetadata.matches(oldCommitMetadataFromDb);

    if (!startMetadataMatchesExisting) {
      // Not in the desired start state.
      log.info("Not updating metadata, existing state is not the expected start state.");
      return DataSourceMetadataUpdateResult.FAILURE;
    }

    final DataSourceMetadata newCommitMetadata = oldCommitMetadataFromDb == null
                                                 ? endMetadata
                                                 : oldCommitMetadataFromDb.plus(endMetadata);
    final byte[] newCommitMetadataBytes = jsonMapper.writeValueAsBytes(newCommitMetadata);
    final String newCommitMetadataSha1 = BaseEncoding.base16().encode(
        Hashing.sha1().hashBytes(newCommitMetadataBytes).asBytes()
    );

    final DataSourceMetadataUpdateResult retVal;
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
                                .bind("created_date", new DateTime().toString())
                                .bind("commit_metadata_payload", newCommitMetadataBytes)
                                .bind("commit_metadata_sha1", newCommitMetadataSha1)
                                .execute();

      retVal = numRows == 1 ? DataSourceMetadataUpdateResult.SUCCESS : DataSourceMetadataUpdateResult.TRY_AGAIN;
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

      retVal = numRows == 1 ? DataSourceMetadataUpdateResult.SUCCESS : DataSourceMetadataUpdateResult.TRY_AGAIN;
    }

    if (retVal == DataSourceMetadataUpdateResult.SUCCESS) {
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
          public Boolean withHandle(Handle handle) throws Exception
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
  public boolean resetDataSourceMetadata(
      final String dataSource, final DataSourceMetadata dataSourceMetadata
  ) throws IOException
  {
    final byte[] newCommitMetadataBytes = jsonMapper.writeValueAsBytes(dataSourceMetadata);
    final String newCommitMetadataSha1 = BaseEncoding.base16().encode(
        Hashing.sha1().hashBytes(newCommitMetadataBytes).asBytes()
    );

    return connector.retryWithHandle(
        new HandleCallback<Boolean>()
        {
          @Override
          public Boolean withHandle(Handle handle) throws Exception
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
  public void updateSegmentMetadata(final Set<DataSegment> segments) throws IOException
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
  public void deleteSegments(final Set<DataSegment> segments) throws IOException
  {
    connector.getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws IOException
          {
            for (final DataSegment segment : segments) {
              deleteSegment(handle, segment);
            }

            return null;
          }
        }
    );
  }

  private void deleteSegment(final Handle handle, final DataSegment segment)
  {
    handle.createStatement(
        StringUtils.format("DELETE from %s WHERE id = :id", dbTables.getSegmentsTable())
    )
          .bind("id", segment.getIdentifier())
          .execute();
  }

  private void updatePayload(final Handle handle, final DataSegment segment) throws IOException
  {
    try {
      handle.createStatement(
          StringUtils.format("UPDATE %s SET payload = :payload WHERE id = :id", dbTables.getSegmentsTable())
      )
            .bind("id", segment.getIdentifier())
            .bind("payload", jsonMapper.writeValueAsBytes(segment))
            .execute();
    }
    catch (IOException e) {
      log.error(e, "Exception inserting into DB");
      throw e;
    }
  }

  @Override
  public List<DataSegment> getUnusedSegmentsForInterval(final String dataSource, final Interval interval)
  {
    List<DataSegment> matchingSegments = connector.inReadOnlyTransaction(
        new TransactionCallback<List<DataSegment>>()
        {
          @Override
          public List<DataSegment> inTransaction(final Handle handle, final TransactionStatus status) throws Exception
          {
            // 2 range conditions are used on different columns, but not all SQL databases properly optimize it.
            // Some databases can only use an index on one of the columns. An additional condition provides
            // explicit knowledge that 'start' cannot be greater than 'end'.
            return handle
                .createQuery(
                    StringUtils.format(
                        "SELECT payload FROM %1$s WHERE dataSource = :dataSource and start >= :start "
                        + "and start <= :end and %2$send%2$s <= :end and used = false",
                        dbTables.getSegmentsTable(), connector.getQuoteString()
                    )
                )
                .setFetchSize(connector.getStreamingFetchSize())
                .bind("dataSource", dataSource)
                .bind("start", interval.getStart().toString())
                .bind("end", interval.getEnd().toString())
                .map(ByteArrayMapper.FIRST)
                .fold(
                    Lists.<DataSegment>newArrayList(),
                    new Folder3<List<DataSegment>, byte[]>()
                    {
                      @Override
                      public List<DataSegment> fold(
                          List<DataSegment> accumulator,
                          byte[] payload,
                          FoldController foldController,
                          StatementContext statementContext
                      ) throws SQLException
                      {
                        try {
                          accumulator.add(jsonMapper.readValue(payload, DataSegment.class));
                          return accumulator;
                        }
                        catch (Exception e) {
                          throw Throwables.propagate(e);
                        }
                      }
                    }
                );
          }
        }
    );

    log.info("Found %,d segments for %s for interval %s.", matchingSegments.size(), dataSource, interval);
    return matchingSegments;
  }
}
