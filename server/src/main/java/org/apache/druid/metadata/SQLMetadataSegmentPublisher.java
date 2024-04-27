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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.skife.jdbi.v2.DBI;

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
        "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, partitioned, version, used, payload, used_status_last_updated) "
        + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload, :used_status_last_updated)",
        config.getSegmentsTable(), connector.getQuoteString()
    );
  }

  @Override
  public void publishSegment(final DataSegment segment) throws IOException
  {
    String now = DateTimes.nowUtc().toString();
    publishSegment(
        segment.getId().toString(),
        segment.getDataSource(),
        now,
        segment.getInterval().getStart().toString(),
        segment.getInterval().getEnd().toString(),
        (segment.getShardSpec() instanceof NoneShardSpec) ? false : true,
        segment.getVersion(),
        true,
        jsonMapper.writeValueAsBytes(segment),
        now
    );
  }

  void publishSegment(
      final String segmentId,
      final String dataSource,
      final String createdDate,
      final String start,
      final String end,
      final boolean partitioned,
      final String version,
      final boolean used,
      final byte[] payload,
      final String usedFlagLastUpdated
  )
  {
    try {
      final DBI dbi = connector.getDBI();
      List<Map<String, Object>> exists = dbi.withHandle(
          handle -> handle.createQuery(
              StringUtils.format("SELECT id FROM %s WHERE id=:id", config.getSegmentsTable())
          ).bind("id", segmentId).list()
      );

      if (!exists.isEmpty()) {
        log.info("Skipping publish of segment[%s] as it already exists in the metadata store.", segmentId);
        return;
      }

      dbi.withHandle(
          handle ->
              handle.createStatement(statement)
                    .bind("id", segmentId)
                    .bind("dataSource", dataSource)
                    .bind("created_date", createdDate)
                    .bind("start", start)
                    .bind("end", end)
                    .bind("partitioned", partitioned)
                    .bind("version", version)
                    .bind("used", used)
                    .bind("payload", payload)
                    .bind("used_status_last_updated", usedFlagLastUpdated)
                    .execute()
      );
    }
    catch (Exception e) {
      log.error(e, "Exception inserting into DB");
      throw new RuntimeException(e);
    }
  }
}
