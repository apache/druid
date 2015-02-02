

/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SQLMetadataSegmentPublisher implements MetadataSegmentPublisher
{
  private static final Logger log = new Logger(SQLMetadataSegmentPublisher.class);

  private final ObjectMapper jsonMapper;
  private final MetadataStorageTablesConfig config;
  private final SQLMetadataConnector connector;
  private final String statement;

  @Inject
  public SQLMetadataSegmentPublisher(
      ObjectMapper jsonMapper,
      MetadataStorageTablesConfig config,
      SQLMetadataConnector connector
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.connector = connector;
    this.statement = String.format(
        "INSERT INTO %s (id, dataSource, created_date, start, \"end\", partitioned, version, used, payload) "
        + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
        config.getSegmentsTable()
    );
  }

  @Override
  public void publishSegment(final DataSegment segment) throws IOException
  {
    try {
      final DBI dbi = connector.getDBI();
      List<Map<String, Object>> exists = dbi.withHandle(
          new HandleCallback<List<Map<String, Object>>>()
          {
            @Override
            public List<Map<String, Object>> withHandle(Handle handle) throws Exception
            {
              return handle.createQuery(
                  String.format("SELECT id FROM %s WHERE id=:id", config.getSegmentsTable())
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
              handle.createStatement(statement)
                    .bind("id", segment.getIdentifier())
                    .bind("dataSource", segment.getDataSource())
                    .bind("created_date", new DateTime().toString())
                    .bind("start", segment.getInterval().getStart().toString())
                    .bind("end", segment.getInterval().getEnd().toString())
                    .bind("partitioned", (segment.getShardSpec() instanceof NoneShardSpec) ? 0 : 1)
                    .bind("version", segment.getVersion())
                    .bind("used", true)
                    .bind("payload", jsonMapper.writeValueAsBytes(segment))
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
