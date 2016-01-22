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
import com.metamx.common.IAE;
import com.metamx.common.StringUtils;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.logger.Logger;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
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
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.StringMapper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class IndexerSQLMetadataStorageCoordinator implements IndexerMetadataStorageCoordinator
{
  private static final Logger log = new Logger(IndexerSQLMetadataStorageCoordinator.class);

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
            String.format(
                "SELECT payload FROM %s WHERE dataSource = :dataSource AND start <= :end and \"end\" >= :start",
                dbTables.getPendingSegmentsTable()
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
          "(start <= ? AND \"end\" >= ?)"
      );
      if (i == intervals.size() - 1) {
        sb.append(")");
      } else {
        sb.append(" OR ");
      }
    }

    Query<Map<String, Object>> sql = handle.createQuery(
        String.format(
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
  public Set<DataSegment> announceHistoricalSegments(final Set<DataSegment> segments) throws IOException
  {
    return connector.getDBI().inTransaction(
        new TransactionCallback<Set<DataSegment>>()
        {
          @Override
          public Set<DataSegment> inTransaction(Handle handle, TransactionStatus transactionStatus) throws IOException
          {
            final Set<DataSegment> inserted = Sets.newHashSet();

            for (final DataSegment segment : segments) {
              if (announceHistoricalSegment(handle, segment)) {
                inserted.add(segment);
              }
            }

            return ImmutableSet.copyOf(inserted);
          }
        }
    );
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
                    String.format(
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
                String.format(
                    "INSERT INTO %s (id, dataSource, created_date, start, \"end\", sequence_name, sequence_prev_id, sequence_name_prev_id_sha1, payload) "
                    + "VALUES (:id, :dataSource, :created_date, :start, :end, :sequence_name, :sequence_prev_id, :sequence_name_prev_id_sha1, :payload)",
                    dbTables.getPendingSegmentsTable()
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
        }
    );
  }

  /**
   * Attempts to insert a single segment to the database. If the segment already exists, will do nothing. Meant
   * to be called from within a transaction.
   *
   * @return true if the segment was added, false otherwise
   */
  private boolean announceHistoricalSegment(final Handle handle, final DataSegment segment) throws IOException
  {
    try {
      if (segmentExists(handle, segment)) {
        log.info("Found [%s] in DB, not updating DB", segment.getIdentifier());
        return false;
      }

      // Try/catch to work around races due to SELECT -> INSERT. Avoid ON DUPLICATE KEY since it's not portable.
      try {
        handle.createStatement(
            String.format(
                "INSERT INTO %s (id, dataSource, created_date, start, \"end\", partitioned, version, used, payload) "
                + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
                dbTables.getSegmentsTable()
            )
        )
              .bind("id", segment.getIdentifier())
              .bind("dataSource", segment.getDataSource())
              .bind("created_date", new DateTime().toString())
              .bind("start", segment.getInterval().getStart().toString())
              .bind("end", segment.getInterval().getEnd().toString())
              .bind("partitioned", (segment.getShardSpec() instanceof NoneShardSpec) ? false : true)
              .bind("version", segment.getVersion())
              .bind("used", true)
              .bind("payload", jsonMapper.writeValueAsBytes(segment))
              .execute();

        log.info("Published segment [%s] to DB", segment.getIdentifier());
      }
      catch (Exception e) {
        if (e.getCause() instanceof SQLException && segmentExists(handle, segment)) {
          log.info("Found [%s] in DB, not updating DB", segment.getIdentifier());
        } else {
          throw e;
        }
      }
    }
    catch (IOException e) {
      log.error(e, "Exception inserting into DB");
      throw e;
    }

    return true;
  }

  private boolean segmentExists(final Handle handle, final DataSegment segment)
  {
    return !handle
        .createQuery(
            String.format(
                "SELECT id FROM %s WHERE id = :identifier",
                dbTables.getSegmentsTable()
            )
        ).bind("identifier", segment.getIdentifier())
        .map(StringMapper.FIRST)
        .list()
        .isEmpty();
  }

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
        String.format("DELETE from %s WHERE id = :id", dbTables.getSegmentsTable())
    )
          .bind("id", segment.getIdentifier())
          .execute();
  }

  private void updatePayload(final Handle handle, final DataSegment segment) throws IOException
  {
    try {
      handle.createStatement(
          String.format("UPDATE %s SET payload = :payload WHERE id = :id", dbTables.getSegmentsTable())
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

  public List<DataSegment> getUnusedSegmentsForInterval(final String dataSource, final Interval interval)
  {
    List<DataSegment> matchingSegments = connector.getDBI().withHandle(
        new HandleCallback<List<DataSegment>>()
        {
          @Override
          public List<DataSegment> withHandle(Handle handle) throws IOException, SQLException
          {
            return handle
                .createQuery(
                    String.format(
                        "SELECT payload FROM %s WHERE dataSource = :dataSource and start >= :start and \"end\" <= :end and used = false",
                        dbTables.getSegmentsTable()
                    )
                )
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
