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
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;

import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
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
    this.statement = StringUtils.format(
        "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, partitioned, version, used, payload) "
        + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
        config.getSegmentsTable(), connector.getQuoteString()
    );
  }

  @Override
  public void publishSegment(final DataSegment segment) throws IOException
  {
    publishSegment(
        segment.getIdentifier(),
        segment.getDataSource(),
        new DateTime().toString(),
        segment.getInterval().getStart().toString(),
        segment.getInterval().getEnd().toString(),
        (segment.getShardSpec() instanceof NoneShardSpec) ? false : true,
        segment.getVersion(),
        true,
        jsonMapper.writeValueAsBytes(segment)
    );
  }

  @VisibleForTesting
  void publishSegment(
      final String identifier,
      final String dataSource,
      final String createdDate,
      final String start,
      final String end,
      final boolean partitioned,
      final String version,
      final boolean used,
      final byte[] payload
  )
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
                  StringUtils.format("SELECT id FROM %s WHERE id=:id", config.getSegmentsTable())
              )
                           .bind("id", identifier)
                           .list();
            }
          }
      );

      if (!exists.isEmpty()) {
        log.info("Found [%s] in DB, not updating DB", identifier);
        return;
      }

      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              handle.createStatement(statement)
                    .bind("id", identifier)
                    .bind("dataSource", dataSource)
                    .bind("created_date", createdDate)
                    .bind("start", start)
                    .bind("end", end)
                    .bind("partitioned", partitioned)
                    .bind("version", version)
                    .bind("used", used)
                    .bind("payload", payload)
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
