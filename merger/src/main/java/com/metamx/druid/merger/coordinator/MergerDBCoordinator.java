/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.merger.coordinator;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.common.logger.Logger;
import com.metamx.druid.TimelineObjectHolder;
import com.metamx.druid.VersionedIntervalTimeline;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.db.DbConnectorConfig;
import com.metamx.druid.merger.common.TaskStatus;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 */
public class MergerDBCoordinator
{
  private static final Logger log = new Logger(MergerDBCoordinator.class);

  private final ObjectMapper jsonMapper;
  private final DbConnectorConfig dbConnectorConfig;
  private final DBI dbi;

  public MergerDBCoordinator(
      ObjectMapper jsonMapper,
      DbConnectorConfig dbConnectorConfig,
      DBI dbi
  )
  {
    this.jsonMapper = jsonMapper;
    this.dbConnectorConfig = dbConnectorConfig;
    this.dbi = dbi;
  }

  public List<DataSegment> getUsedSegmentsForInterval(final String dataSource, final Interval interval)
      throws IOException
  {
    // XXX Could be reading from a cache if we can assume we're the only one editing the DB

    final VersionedIntervalTimeline<String, DataSegment> timeline = dbi.withHandle(
        new HandleCallback<VersionedIntervalTimeline<String, DataSegment>>()
        {
          @Override
          public VersionedIntervalTimeline<String, DataSegment> withHandle(Handle handle) throws Exception
          {
            final VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<String, DataSegment>(
                Ordering.natural()
            );

            final ResultIterator<Map<String, Object>> dbSegments =
                handle.createQuery(
                    String.format(
                        "SELECT payload FROM %s WHERE used = 1 AND dataSource = :dataSource",
                        dbConnectorConfig.getSegmentTable()
                    )
                )
                      .bind("dataSource", dataSource)
                      .iterator();

            while (dbSegments.hasNext()) {

              final Map<String, Object> dbSegment = dbSegments.next();

              DataSegment segment = jsonMapper.readValue(
                  (String) dbSegment.get("payload"),
                  DataSegment.class
              );

              timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment));

            }

            dbSegments.close();

            return timeline;

          }
        }
    );

    final List<DataSegment> segments = Lists.transform(
        timeline.lookup(interval),
        new Function<TimelineObjectHolder<String, DataSegment>, DataSegment>()
        {
          @Override
          public DataSegment apply(TimelineObjectHolder<String, DataSegment> input)
          {
            return input.getObject().getChunk(0).getObject();
          }
        }
    );

    return segments;
  }

  public void commitTaskStatus(final TaskStatus taskStatus)
  {
    try {
      dbi.inTransaction(
          new TransactionCallback<Void>()
          {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
            {
              for(final DataSegment segment : taskStatus.getSegments())
              {
                log.info("Publishing segment[%s] for task[%s]", segment.getIdentifier(), taskStatus.getId());
                announceHistoricalSegment(handle, segment);
              }

              for(final DataSegment segment : taskStatus.getSegmentsNuked())
              {
                log.info("Deleting segment[%s] for task[%s]", segment.getIdentifier(), taskStatus.getId());
                deleteSegment(handle, segment);
              }

              return null;
            }
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(String.format("Exception commit task to DB: %s", taskStatus.getId()), e);
    }
  }

  public void announceHistoricalSegment(final DataSegment segment) throws Exception
  {
    dbi.withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            announceHistoricalSegment(handle, segment);
            return null;
          }
        }
    );
  }

  private void announceHistoricalSegment(final Handle handle, final DataSegment segment) throws Exception
  {
    try {
      final List<Map<String, Object>> exists = handle.createQuery(
          String.format(
              "SELECT id FROM %s WHERE id = ':identifier'",
              dbConnectorConfig.getSegmentTable()
          )
      ).bind(
          "identifier",
          segment.getIdentifier()
      ).list();

      if (!exists.isEmpty()) {
        log.info("Found [%s] in DB, not updating DB", segment.getIdentifier());
        return;
      }

      handle.createStatement(
          String.format(
              "INSERT INTO %s (id, dataSource, created_date, start, end, partitioned, version, used, payload) VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
              dbConnectorConfig.getSegmentTable()
          )
      )
            .bind("id", segment.getIdentifier())
            .bind("dataSource", segment.getDataSource())
            .bind("created_date", new DateTime().toString())
            .bind("start", segment.getInterval().getStart().toString())
            .bind("end", segment.getInterval().getEnd().toString())
            .bind("partitioned", segment.getShardSpec().getPartitionNum())
            .bind("version", segment.getVersion())
            .bind("used", true)
            .bind("payload", jsonMapper.writeValueAsString(segment))
            .execute();

      log.info("Published segment [%s] to DB", segment.getIdentifier());
    }
    catch (Exception e) {
      log.error(e, "Exception inserting into DB");
      throw e;
    }
  }

  public void deleteSegment(final DataSegment segment)
  {
    dbi.withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            deleteSegment(handle, segment);
            return null;
          }
        }
    );
  }

  private void deleteSegment(final Handle handle, final DataSegment segment)
  {
    handle.createStatement(
        String.format("DELETE from %s WHERE id = :id", dbConnectorConfig.getSegmentTable())
    ).bind("id", segment.getIdentifier())
          .execute();
  }

  public List<DataSegment> getUnusedSegmentsForInterval(final String dataSource, final Interval interval)
  {
    List<DataSegment> matchingSegments = dbi.withHandle(
        new HandleCallback<List<DataSegment>>()
        {
          @Override
          public List<DataSegment> withHandle(Handle handle) throws Exception
          {
            return handle.createQuery(
                String.format(
                    "SELECT payload FROM %s WHERE dataSource = :dataSource and start >= :start and end <= :end and used = 0",
                    dbConnectorConfig.getSegmentTable()
                )
            )
                         .bind("dataSource", dataSource)
                         .bind("start", interval.getStart().toString())
                         .bind("end", interval.getEnd().toString())
                         .fold(
                             Lists.<DataSegment>newArrayList(),
                             new Folder3<List<DataSegment>, Map<String, Object>>()
                             {
                               @Override
                               public List<DataSegment> fold(
                                   List<DataSegment> accumulator,
                                   Map<String, Object> stringObjectMap,
                                   FoldController foldController,
                                   StatementContext statementContext
                               ) throws SQLException
                               {
                                 try {
                                   DataSegment segment = jsonMapper.readValue(
                                       (String) stringObjectMap.get("payload"),
                                       DataSegment.class
                                   );

                                   accumulator.add(segment);

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
