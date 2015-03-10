/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.skife.jdbi.v2.util.StringMapper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
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

  public List<DataSegment> getUsedSegmentsForInterval(final String dataSource, final Interval interval)
      throws IOException
  {
    final VersionedIntervalTimeline<String, DataSegment> timeline = connector.getDBI().withHandle(
        new HandleCallback<VersionedIntervalTimeline<String, DataSegment>>()
        {
          @Override
          public VersionedIntervalTimeline<String, DataSegment> withHandle(Handle handle) throws IOException
          {
            final VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<String, DataSegment>(
                Ordering.natural()
            );

            final ResultIterator<byte[]> dbSegments =
                handle.createQuery(
                    String.format(
                        "SELECT payload FROM %s WHERE used = true AND dataSource = :dataSource",
                        dbTables.getSegmentsTable()
                    )
                )
                      .bind("dataSource", dataSource)
                      .map(ByteArrayMapper.FIRST)
                      .iterator();

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
        }
    );

    return Lists.newArrayList(
        Iterables.concat(
            Iterables.transform(
                timeline.lookup(interval),
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
  }

  /**
   * Attempts to insert a set of segments to the database. Returns the set of segments actually added (segments
   * with identifiers already in the database will not be added).
   *
   * @param segments set of segments to add
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
      } catch(Exception e) {
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
            for(final DataSegment segment : segments) {
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
            for(final DataSegment segment : segments) {
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
