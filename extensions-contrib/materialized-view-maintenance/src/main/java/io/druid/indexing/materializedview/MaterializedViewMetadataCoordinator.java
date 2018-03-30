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

package io.druid.indexing.materializedview;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.inject.Inject;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class MaterializedViewMetadataCoordinator 
{
  
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final SQLMetadataConnector connector;
  private final ObjectMapper objectMapper;

  @Inject
  public MaterializedViewMetadataCoordinator(
      ObjectMapper objectMapper,
      Supplier<MetadataStorageTablesConfig> dbTables,
      SQLMetadataConnector connector
  )
  {
    this.objectMapper = objectMapper;
    this.dbTables = dbTables;
    this.connector = connector;
  }
  
  public void insertDataSourceMetadata(String dataSource, DataSourceMetadata metadata) 
  {
    connector.getDBI().inTransaction(
        (handle, status) -> handle
            .createStatement(
                StringUtils.format(
                    "INSERT INTO %s (dataSource, created_date, commit_metadata_payload, commit_metadata_sha1) VALUES" +
                        " (:dataSource, :created_date, :commit_metadata_payload, :commit_metadata_sha1)",
                    dbTables.get().getDataSourceTable()
                )
            )
            .bind("dataSource", dataSource)
            .bind("created_date", DateTimes.nowUtc().toString())
            .bind("commit_metadata_payload", objectMapper.writeValueAsBytes(metadata))
            .bind("commit_metadata_sha1", BaseEncoding.base16().encode(
                Hashing.sha1().hashBytes(objectMapper.writeValueAsBytes(metadata)).asBytes()))
            .execute()
    );
  }
  
  public Map<DataSegment, String> getSegmentAndCreatedDate(String dataSource, Interval interval)
  {
    List<Pair<DataSegment, String>> maxCreatedDate = connector.retryWithHandle(
        new HandleCallback<List<Pair<DataSegment, String>>>() 
        {
          @Override
          public List<Pair<DataSegment, String>> withHandle(Handle handle) throws Exception 
          {
            return handle.createQuery(
                StringUtils.format("SELECT created_date, payload FROM %1$s WHERE dataSource = :dataSource " +
                    "AND start >= :start AND %2$send%2$s <= :end AND used = true", 
                    dbTables.get().getSegmentsTable(), connector.getQuoteString()
                )
            )
                .bind("dataSource", dataSource)
                .bind("start", interval.getStart().toString())
                .bind("end", interval.getEnd().toString())
                .map(new ResultSetMapper<Pair<DataSegment, String>>() 
                {
                  @Override
                  public Pair<DataSegment, String> map(int index, ResultSet r, StatementContext ctx) throws SQLException 
                  {
                    try {
                      return new Pair<DataSegment, String>(
                         objectMapper.readValue(r.getBytes("payload"), DataSegment.class),
                          r.getString("created_date"));
                    } 
                    catch (Exception e) {
                      throw Throwables.propagate(e);
                    }
                  }
                })
                .list();
          }
        }
    );
    Map<DataSegment, String> result = Maps.newHashMap();
    for (Pair<DataSegment, String> data : maxCreatedDate) {
      result.put(data.lhs, data.rhs);
    }
    return result;
  }
}
