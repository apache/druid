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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.inject.Inject;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
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
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.StringMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
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
  public List<DataSegment> getUsedSegmentsForIntervals(final String dataSource, final List<Interval> intervals)
  {
    return connector.retryWithHandle(
        new HandleCallback<List<DataSegment>>()
        {
          @Override
          public List<DataSegment> withHandle(Handle handle)
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
    final List<SegmentIdentifier> identifiers = new ArrayList<>();

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
  )
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

    try (final ResultIterator<byte[]> dbSegments = sql.map(ByteArrayMapper.FIRST).iterator()) {
      return VersionedIntervalTimeline.forSegments(
          Iterators.transform(
              dbSegments,
              payload -> {
                try {
                  return jsonMapper.readValue(payload, DataSegment.class);
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
          )
      );
    }
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
    final SegmentPublishResult result = announceHistoricalSegments(segments, null, null, true);

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
      final DataSourceMetadata endMetadata,
      final boolean defaultUsed
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
        VersionedIntervalTimeline.forSegments(segments).lookupWithIncompletePartitions(Intervals.ETERNITY);
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

              final Set<DataSegment> inserted = new HashSet<>();

              if (startMetadata != null) {
                final DataSourceMetadataUpdateResult result = updateDataSourceMetadataWithHandle(
                    handle,
                    dataSource,
                    startMetadata,
                    endMetadata
                );

                if (result != DataSourceMetadataUpdateResult.SUCCESS) {
                  // Metadata was definitely not updated.
                  transactionStatus.setRollbackOnly();
                  definitelyNotUpdated.set(true);

                  if (result == DataSourceMetadataUpdateResult.FAILURE) {
                    throw new RuntimeException("Aborting transaction!");
                  } else if (result == DataSourceMetadataUpdateResult.TRY_AGAIN) {
                    throw new RetryTransactionException("Aborting transaction!");
                  }
                }
              }

              for (final DataSegment segment : segments) {
                if (announceHistoricalSegment(handle, segment, defaultUsed && usedSegments.contains(segment))) {
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
      if (definitelyNotUpdated.get()) {
        return SegmentPublishResult.fail();
      } else {
        // Must throw exception if we are not sure if we updated or not.
        throw e;
      }
    }
  }

  @Override
  public SegmentIdentifier allocatePendingSegment(
      final String dataSource,
      final String sequenceName,
      @Nullable final String previousSegmentId,
      final Interval interval,
      final String maxVersion,
      final boolean skipSegmentLineageCheck
  )
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(sequenceName, "sequenceName");
    Preconditions.checkNotNull(interval, "interval");
    Preconditions.checkNotNull(maxVersion, "maxVersion");

    return connector.retryWithHandle(
        new HandleCallback<SegmentIdentifier>()
        {
          @Override
          public SegmentIdentifier withHandle(Handle handle) throws Exception
          {
            return skipSegmentLineageCheck ?
                   allocatePendingSegment(handle, dataSource, sequenceName, interval, maxVersion) :
                   allocatePendingSegmentWithSegmentLineageCheck(
                       handle,
                       dataSource,
                       sequenceName,
                       previousSegmentId,
                       interval,
                       maxVersion
                   );
          }
        }
    );
  }

  @Nullable
  private SegmentIdentifier allocatePendingSegmentWithSegmentLineageCheck(
      final Handle handle,
      final String dataSource,
      final String sequenceName,
      @Nullable final String previousSegmentId,
      final Interval interval,
      final String maxVersion
  ) throws IOException
  {
    final String previousSegmentIdNotNull = previousSegmentId == null ? "" : previousSegmentId;
    final CheckExistingSegmentIdResult result = checkAndGetExistingSegmentId(
        handle.createQuery(
            StringUtils.format(
                "SELECT payload FROM %s WHERE "
                + "dataSource = :dataSource AND "
                + "sequence_name = :sequence_name AND "
                + "sequence_prev_id = :sequence_prev_id",
                dbTables.getPendingSegmentsTable()
            )
        ),
        interval,
        sequenceName,
        previousSegmentIdNotNull,
        Pair.of("dataSource", dataSource),
        Pair.of("sequence_name", sequenceName),
        Pair.of("sequence_prev_id", previousSegmentIdNotNull)
    );

    if (result.found) {
      // The found existing segment identifier can be null if its interval doesn't match with the given interval
      return result.segmentIdentifier;
    }

    final SegmentIdentifier newIdentifier = createNewSegment(handle, dataSource, interval, maxVersion);
    if (newIdentifier == null) {
      return null;
    }

    // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
    // Avoiding ON DUPLICATE KEY since it's not portable.
    // Avoiding try/catch since it may cause inadvertent transaction-splitting.

    // UNIQUE key for the row, ensuring sequences do not fork in two directions.
    // Using a single column instead of (sequence_name, sequence_prev_id) as some MySQL storage engines
    // have difficulty with large unique keys (see https://github.com/apache/incubator-druid/issues/2319)
    final String sequenceNamePrevIdSha1 = BaseEncoding.base16().encode(
        Hashing.sha1()
               .newHasher()
               .putBytes(StringUtils.toUtf8(sequenceName))
               .putByte((byte) 0xff)
               .putBytes(StringUtils.toUtf8(previousSegmentIdNotNull))
               .hash()
               .asBytes()
    );

    insertToMetastore(
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

  @Nullable
  private SegmentIdentifier allocatePendingSegment(
      final Handle handle,
      final String dataSource,
      final String sequenceName,
      final Interval interval,
      final String maxVersion
  ) throws IOException
  {
    final CheckExistingSegmentIdResult result = checkAndGetExistingSegmentId(
        handle.createQuery(
            StringUtils.format(
                "SELECT payload FROM %s WHERE "
                + "dataSource = :dataSource AND "
                + "sequence_name = :sequence_name AND "
                + "start = :start AND "
                + "%2$send%2$s = :end",
                dbTables.getPendingSegmentsTable(),
                connector.getQuoteString()
            )
        ),
        interval,
        sequenceName,
        null,
        Pair.of("dataSource", dataSource),
        Pair.of("sequence_name", sequenceName),
        Pair.of("start", interval.getStart().toString()),
        Pair.of("end", interval.getEnd().toString())
    );

    if (result.found) {
      // The found existing segment identifier can be null if its interval doesn't match with the given interval
      return result.segmentIdentifier;
    }

    final SegmentIdentifier newIdentifier = createNewSegment(handle, dataSource, interval, maxVersion);
    if (newIdentifier == null) {
      return null;
    }

    // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
    // Avoiding ON DUPLICATE KEY since it's not portable.
    // Avoiding try/catch since it may cause inadvertent transaction-splitting.

    // UNIQUE key for the row, ensuring we don't have more than one segment per sequence per interval.
    // Using a single column instead of (sequence_name, sequence_prev_id) as some MySQL storage engines
    // have difficulty with large unique keys (see https://github.com/apache/incubator-druid/issues/2319)
    final String sequenceNamePrevIdSha1 = BaseEncoding.base16().encode(
        Hashing.sha1()
               .newHasher()
               .putBytes(StringUtils.toUtf8(sequenceName))
               .putByte((byte) 0xff)
               .putLong(interval.getStartMillis())
               .putLong(interval.getEndMillis())
               .hash()
               .asBytes()
    );

    // always insert empty previous sequence id
    insertToMetastore(handle, newIdentifier, dataSource, interval, "", sequenceName, sequenceNamePrevIdSha1);

    log.info(
        "Allocated pending segment [%s] for sequence[%s] in DB",
        newIdentifier.getIdentifierAsString(),
        sequenceName
    );

    return newIdentifier;
  }

  private CheckExistingSegmentIdResult checkAndGetExistingSegmentId(
      final Query<Map<String, Object>> query,
      final Interval interval,
      final String sequenceName,
      final @Nullable String previousSegmentId,
      final Pair<String, String>... queryVars
  ) throws IOException
  {
    Query<Map<String, Object>> boundQuery = query;
    for (Pair<String, String> var : queryVars) {
      boundQuery = boundQuery.bind(var.lhs, var.rhs);
    }
    final List<byte[]> existingBytes = boundQuery.map(ByteArrayMapper.FIRST).list();

    if (!existingBytes.isEmpty()) {
      final SegmentIdentifier existingIdentifier = jsonMapper.readValue(
          Iterables.getOnlyElement(existingBytes),
          SegmentIdentifier.class
      );

      if (existingIdentifier.getInterval().getStartMillis() == interval.getStartMillis()
          && existingIdentifier.getInterval().getEndMillis() == interval.getEndMillis()) {
        if (previousSegmentId == null) {
          log.info(
              "Found existing pending segment [%s] for sequence[%s] in DB",
              existingIdentifier.getIdentifierAsString(),
              sequenceName
          );
        } else {
          log.info(
              "Found existing pending segment [%s] for sequence[%s] (previous = [%s]) in DB",
              existingIdentifier.getIdentifierAsString(),
              sequenceName,
              previousSegmentId
          );
        }

        return new CheckExistingSegmentIdResult(true, existingIdentifier);
      } else {
        if (previousSegmentId == null) {
          log.warn(
              "Cannot use existing pending segment [%s] for sequence[%s] in DB, "
              + "does not match requested interval[%s]",
              existingIdentifier.getIdentifierAsString(),
              sequenceName,
              interval
          );
        } else {
          log.warn(
              "Cannot use existing pending segment [%s] for sequence[%s] (previous = [%s]) in DB, "
              + "does not match requested interval[%s]",
              existingIdentifier.getIdentifierAsString(),
              sequenceName,
              previousSegmentId,
              interval
          );
        }

        return new CheckExistingSegmentIdResult(true, null);
      }
    }
    return new CheckExistingSegmentIdResult(false, null);
  }

  private static class CheckExistingSegmentIdResult
  {
    private final boolean found;
    @Nullable
    private final SegmentIdentifier segmentIdentifier;

    CheckExistingSegmentIdResult(boolean found, @Nullable SegmentIdentifier segmentIdentifier)
    {
      this.found = found;
      this.segmentIdentifier = segmentIdentifier;
    }
  }

  private void insertToMetastore(
      Handle handle,
      SegmentIdentifier newIdentifier,
      String dataSource,
      Interval interval,
      String previousSegmentId,
      String sequenceName,
      String sequenceNamePrevIdSha1
  ) throws JsonProcessingException
  {
    handle.createStatement(
        StringUtils.format(
            "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, sequence_name, sequence_prev_id, sequence_name_prev_id_sha1, payload) "
            + "VALUES (:id, :dataSource, :created_date, :start, :end, :sequence_name, :sequence_prev_id, :sequence_name_prev_id_sha1, :payload)",
            dbTables.getPendingSegmentsTable(),
            connector.getQuoteString()
        )
    )
          .bind("id", newIdentifier.getIdentifierAsString())
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

  @Nullable
  private SegmentIdentifier createNewSegment(
      final Handle handle,
      final String dataSource,
      final Interval interval,
      final String maxVersion
  ) throws IOException
  {
    // Make up a pending segment based on existing segments and pending segments in the DB. This works
    // assuming that all tasks inserting segments at a particular point in time are going through the
    // allocatePendingSegment flow. This should be assured through some other mechanism (like task locks).

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
        return new SegmentIdentifier(
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
        return new SegmentIdentifier(
            dataSource,
            max.getInterval(),
            max.getVersion(),
            new LinearShardSpec(max.getShardSpec().getPartitionNum() + 1)
        );
      } else if (max.getShardSpec() instanceof NumberedShardSpec) {
        return new SegmentIdentifier(
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
  }

  @Override
  public int deletePendingSegments(String dataSource, Interval deleteInterval)
  {
    return connector.getDBI().inTransaction(
        (handle, status) -> handle
            .createStatement(
                StringUtils.format(
                    "delete from %s where datasource = :dataSource and created_date >= :start and created_date < :end",
                    dbTables.getPendingSegmentsTable()
                )
            )
            .bind("dataSource", dataSource)
            .bind("start", deleteInterval.getStart().toString())
            .bind("end", deleteInterval.getEnd().toString())
            .execute()
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
              dbTables.getSegmentsTable(),
              connector.getQuoteString()
          )
      )
            .bind("id", segment.getIdentifier())
            .bind("dataSource", segment.getDataSource())
            .bind("created_date", DateTimes.nowUtc().toString())
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
   * @return SUCCESS if dataSource metadata was updated from matching startMetadata to matching endMetadata, FAILURE or
   * TRY_AGAIN if it definitely was not updated. This guarantee is meant to help
   * {@link #announceHistoricalSegments(Set, DataSourceMetadata, DataSourceMetadata)}
   * achieve its own guarantee.
   *
   * @throws RuntimeException if state is unknown after this call
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
      log.debug("Existing database state [%s], request's start metadata [%s]", oldCommitMetadataFromDb, startMetadata);
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
                                .bind("created_date", DateTimes.nowUtc().toString())
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
    connector.getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus)
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
          public List<DataSegment> inTransaction(final Handle handle, final TransactionStatus status)
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
                    new ArrayList<>(),
                    new Folder3<List<DataSegment>, byte[]>()
                    {
                      @Override
                      public List<DataSegment> fold(
                          List<DataSegment> accumulator,
                          byte[] payload,
                          FoldController foldController,
                          StatementContext statementContext
                      )
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

  @Override
  public List<Pair<DataSegment, String>> getUsedSegmentAndCreatedDateForInterval(String dataSource, Interval interval)
  {
    return connector.retryWithHandle(
        handle -> handle.createQuery(
            StringUtils.format(
                "SELECT created_date, payload FROM %1$s WHERE dataSource = :dataSource " +
                "AND start >= :start AND %2$send%2$s <= :end AND used = true",
                dbTables.getSegmentsTable(), connector.getQuoteString()
            )
        )
                        .bind("dataSource", dataSource)
                        .bind("start", interval.getStart().toString())
                        .bind("end", interval.getEnd().toString())
                        .map(new ResultSetMapper<Pair<DataSegment, String>>()
                        {
                          @Override
                          public Pair<DataSegment, String> map(int index, ResultSet r, StatementContext ctx)
                              throws SQLException
                          {
                            try {
                              return new Pair<>(
                                  jsonMapper.readValue(r.getBytes("payload"), DataSegment.class),
                                  r.getString("created_date")
                              );
                            }
                            catch (IOException e) {
                              throw new RuntimeException(e);
                            }
                          }
                        })
                        .list()
    );
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
}
