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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.tweak.HandleCallback;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.common.logger.Logger;
import com.metamx.druid.TimelineObjectHolder;
import com.metamx.druid.VersionedIntervalTimeline;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.db.DbConnectorConfig;

/**
 */
public class MergerDBCoordinator
{
  private static final Logger log = new Logger(MergerDBCoordinator.class);

  private final Object lock = new Object();

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

  public List<DataSegment> getSegmentsForInterval(final String dataSource, final Interval interval) throws IOException
  {
    synchronized (lock) {

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
            public DataSegment apply(@Nullable TimelineObjectHolder<String, DataSegment> input)
            {
              return input.getObject().getChunk(0).getObject();
            }
          }
      );

      return segments;

    }
  }

  public void announceHistoricalSegment(final DataSegment segment) throws Exception
  {
    synchronized (lock) {
      try {
        List<Map<String, Object>> exists = dbi.withHandle(
            new HandleCallback<List<Map<String, Object>>>()
            {
              @Override
              public List<Map<String, Object>> withHandle(Handle handle) throws Exception
              {
                return handle.createQuery(
                    String.format(
                        "SELECT id FROM %s WHERE id = ':identifier'",
                        dbConnectorConfig.getSegmentTable()
                    )
                ).bind(
                    "identifier",
                    segment.getIdentifier()
                ).list();
              }
            }
        );

        if (!exists.isEmpty()) {
          log.info("Found [%s] in DB, not updating DB", segment.getIdentifier());
          return;
        }

        dbi.withHandle(
            new HandleCallback<Void>()
            {
              @Override
              public Void withHandle(Handle handle) throws Exception
              {
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

                return null;
              }
            }
        );

        log.info("Published segment [%s] to DB", segment.getIdentifier());
      }
      catch (Exception e) {
        log.error(e, "Exception inserting into DB");
        throw new RuntimeException(e);
      }
    }
  }
}
