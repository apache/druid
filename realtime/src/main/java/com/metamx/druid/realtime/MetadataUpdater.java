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

package com.metamx.druid.realtime;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.phonebook.PhoneBook;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class MetadataUpdater
{
  private static final Logger log = new Logger(MetadataUpdater.class);

  private final Object lock = new Object();

  private final ObjectMapper jsonMapper;
  private final MetadataUpdaterConfig config;
  private final PhoneBook yp;
  private final String servedSegmentsLocation;
  private final DBI dbi;

  private volatile boolean started = false;

  public MetadataUpdater(
      ObjectMapper jsonMapper,
      MetadataUpdaterConfig config,
      PhoneBook yp,
      DBI dbi
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.yp = yp;
    this.servedSegmentsLocation = yp.combineParts(
        Arrays.asList(
            config.getServedSegmentsLocation(), config.getServerName()
        )
    );

    this.dbi = dbi;
  }

  public Map<String, String> getStringProps()
  {
    return ImmutableMap.of(
        "name", config.getServerName(),
        "host", config.getHost(),
        "maxSize", String.valueOf(config.getMaxSize()),
        "type", "realtime"
    );
  }

  public boolean hasStarted()
  {
    return started;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      log.info("Starting zkCoordinator for server[%s] with config[%s]", config.getServerName(), config);
      if (yp.lookup(servedSegmentsLocation, Object.class) == null) {
        yp.post(
            config.getServedSegmentsLocation(),
            config.getServerName(),
            ImmutableMap.of("created", new DateTime().toString())
        );
      }

      yp.announce(
          config.getAnnounceLocation(),
          config.getServerName(),
          getStringProps()
      );

      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      log.info("Stopping MetadataUpdater with config[%s]", config);
      yp.unannounce(config.getAnnounceLocation(), config.getServerName());

      started = false;
    }
  }

  public void announceSegment(DataSegment segment) throws IOException
  {
    log.info("Announcing realtime segment %s", segment.getIdentifier());
    yp.announce(servedSegmentsLocation, segment.getIdentifier(), segment);
  }

  public void unannounceSegment(DataSegment segment) throws IOException
  {
    log.info("Unannouncing realtime segment %s", segment.getIdentifier());
    yp.unannounce(servedSegmentsLocation, segment.getIdentifier());
  }

  public void publishSegment(final DataSegment segment) throws IOException
  {
    try {
      List<Map<String, Object>> exists = dbi.withHandle(
          new HandleCallback<List<Map<String, Object>>>()
          {
            @Override
            public List<Map<String, Object>> withHandle(Handle handle) throws Exception
            {
              return handle.createQuery(
                  String.format("SELECT id FROM %s WHERE id=:id", config.getSegmentTable())
              )
                  .bind("id", segment.getIdentifier())
                  .list();
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
                      "INSERT INTO %s (id, dataSource, created_date, start, end, partitioned, version, used, payload) "
                      + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
                      config.getSegmentTable()
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
    }
    catch (Exception e) {
      log.error(e, "Exception inserting into DB");
      throw new RuntimeException(e);
    }
  }
}
