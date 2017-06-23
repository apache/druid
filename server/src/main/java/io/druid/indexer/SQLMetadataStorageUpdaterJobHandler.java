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

package io.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.List;

public class SQLMetadataStorageUpdaterJobHandler implements MetadataStorageUpdaterJobHandler
{
  private static final Logger log = new Logger(SQLMetadataStorageUpdaterJobHandler.class);
  private final SQLMetadataConnector connector;
  private final IDBI dbi;

  @Inject
  public SQLMetadataStorageUpdaterJobHandler(SQLMetadataConnector connector)
  {
    this.connector = connector;
    this.dbi = connector.getDBI();
  }

  @Override
  public void publishSegments(final String tableName, final List<DataSegment> segments, final ObjectMapper mapper)
  {
    dbi.withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            final PreparedBatch batch = handle.prepareBatch(
                StringUtils.format(
                    "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, partitioned, version, used, payload) "
                    + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
                    tableName, connector.getQuoteString()
                )
            );
            for (final DataSegment segment : segments) {

              batch.add(
                  new ImmutableMap.Builder<String, Object>()
                      .put("id", segment.getIdentifier())
                      .put("dataSource", segment.getDataSource())
                      .put("created_date", new DateTime().toString())
                      .put("start", segment.getInterval().getStart().toString())
                      .put("end", segment.getInterval().getEnd().toString())
                      .put("partitioned", (segment.getShardSpec() instanceof NoneShardSpec) ? false : true)
                      .put("version", segment.getVersion())
                      .put("used", true)
                      .put("payload", mapper.writeValueAsBytes(segment))
                      .build()
              );

              log.info("Published %s", segment.getIdentifier());

            }
            batch.execute();

            return null;
          }
        }
    );
  }
}
