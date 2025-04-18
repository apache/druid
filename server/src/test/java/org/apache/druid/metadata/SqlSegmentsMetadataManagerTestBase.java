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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaCache;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Period;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SqlSegmentsMetadataManagerTestBase
{
  protected SqlSegmentsMetadataManager sqlSegmentsMetadataManager;

  protected SegmentSchemaCache segmentSchemaCache;
  protected SegmentSchemaManager segmentSchemaManager;
  protected TestDerbyConnector connector;
  protected SegmentsMetadataManagerConfig config;
  protected MetadataStorageTablesConfig storageConfig;
  protected final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

  protected final DataSegment segment1 = createSegment(
      "wikipedia",
      "2012-03-15T00:00:00.000/2012-03-16T00:00:00.000",
      "2012-03-16T00:36:30.848Z",
      "index/y=2012/m=03/d=15/2012-03-16T00:36:30.848Z/0/index.zip",
      0
  );

  protected final DataSegment segment2 = createSegment(
      "wikipedia",
      "2012-01-05T00:00:00.000/2012-01-06T00:00:00.000",
      "2012-01-06T22:19:12.565Z",
      "wikipedia/index/y=2012/m=01/d=05/2012-01-06T22:19:12.565Z/0/index.zip",
      0
  );

  protected void setUp(TestDerbyConnector.DerbyConnectorRule derbyConnectorRule) throws Exception
  {
    config = new SegmentsMetadataManagerConfig(Period.seconds(3), null);
    connector = derbyConnectorRule.getConnector();
    storageConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();

    segmentSchemaCache = new SegmentSchemaCache(NoopServiceEmitter.instance());
    segmentSchemaManager = new SegmentSchemaManager(
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        jsonMapper,
        connector
    );

    sqlSegmentsMetadataManager = new SqlSegmentsMetadataManager(
        jsonMapper,
        Suppliers.ofInstance(config),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        connector,
        segmentSchemaCache,
        CentralizedDatasourceSchemaConfig.create(),
        NoopServiceEmitter.instance()
    );
    sqlSegmentsMetadataManager.start();

    connector.createSegmentSchemasTable();
    connector.createSegmentTable();
  }

  protected void teardownManager()
  {
    if (sqlSegmentsMetadataManager.isPollingDatabasePeriodically()) {
      sqlSegmentsMetadataManager.stopPollingDatabasePeriodically();
    }
    sqlSegmentsMetadataManager.stop();
  }

  protected void publishSegment(final DataSegment segment)
  {
    publishSegment(connector, storageConfig, jsonMapper, segment);
  }

  protected int markSegmentsAsUnused(SegmentId... segmentIds)
  {
    return markSegmentsAsUnused(Set.of(segmentIds), connector, storageConfig, jsonMapper);
  }

  public static int markSegmentsAsUnused(
      Set<SegmentId> segmentIds,
      SQLMetadataConnector connector,
      MetadataStorageTablesConfig storageConfig,
      ObjectMapper jsonMapper
  )
  {
    return connector.retryWithHandle(
        handle -> SqlSegmentsMetadataQuery.forHandle(handle, connector, storageConfig, jsonMapper)
                                          .markSegmentsAsUnused(segmentIds, DateTimes.nowUtc())
    );
  }

  protected static DataSegment createSegment(
      String dataSource,
      String interval,
      String version,
      String bucketKey,
      int binaryVersion
  )
  {
    return new DataSegment(
        dataSource,
        Intervals.of(interval),
        version,
        ImmutableMap.of(
            "type", "s3_zip",
            "bucket", "test",
            "key", dataSource + "/" + bucketKey
        ),
        ImmutableList.of("dim1", "dim2", "dim3"),
        ImmutableList.of("count", "value"),
        NoneShardSpec.instance(),
        binaryVersion,
        1234L
    );
  }

  public static void publishSegment(
      final SQLMetadataConnector connector,
      final MetadataStorageTablesConfig config,
      final ObjectMapper jsonMapper,
      final DataSegment segment
  )
  {
    final String now = DateTimes.nowUtc().toString();
    try {
      final DBI dbi = connector.getDBI();
      List<Map<String, Object>> exists = dbi.withHandle(
          handle ->
              handle.createQuery(StringUtils.format("SELECT id FROM %s WHERE id=:id", config.getSegmentsTable()))
                    .bind("id", segment.getId().toString())
                    .list()
      );

      if (!exists.isEmpty()) {
        return;
      }

      final String publishStatement = StringUtils.format(
          "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, partitioned, version, used, payload, used_status_last_updated) "
          + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload, :used_status_last_updated)",
          config.getSegmentsTable(),
          connector.getQuoteString()
      );

      final byte[] payload = jsonMapper.writeValueAsBytes(segment);
      dbi.withHandle(
          (HandleCallback<Void>) handle -> {
            handle.createStatement(publishStatement)
                  .bind("id", segment.getId().toString())
                  .bind("dataSource", segment.getDataSource())
                  .bind("created_date", now)
                  .bind("start", segment.getInterval().getStart().toString())
                  .bind("end", segment.getInterval().getEnd().toString())
                  .bind("partitioned", !(segment.getShardSpec() instanceof NoneShardSpec))
                  .bind("version", segment.getVersion())
                  .bind("used", true)
                  .bind("payload", payload)
                  .bind("used_status_last_updated", now)
                  .execute();

            return null;
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
