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

package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.metadata.storage.derby.DerbyConnector;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.Assert;
import org.skife.jdbi.v2.PreparedBatch;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SegmentSchemaTestUtils
{
  private final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule;
  private final DerbyConnector derbyConnector;
  private final ObjectMapper mapper;

  public SegmentSchemaTestUtils(
      TestDerbyConnector.DerbyConnectorRule derbyConnectorRule,
      DerbyConnector derbyConnector,
      ObjectMapper mapper
  )
  {
    this.derbyConnectorRule = derbyConnectorRule;
    this.derbyConnector = derbyConnector;
    this.mapper = mapper;
  }

  public Boolean insertUsedSegments(Set<DataSegment> dataSegments, Map<String, Pair<String, Long>> segmentMetadata)
  {
    if (!segmentMetadata.isEmpty()) {
      final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
      return derbyConnector.retryWithHandle(
          handle -> {
            PreparedBatch preparedBatch = handle.prepareBatch(
                StringUtils.format(
                    "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, partitioned, version, used, payload, used_status_last_updated, schema_fingerprint, num_rows) "
                    + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload, :used_status_last_updated, :schema_fingerprint, :num_rows)",
                    table,
                    derbyConnector.getQuoteString()
                )
            );
            for (DataSegment segment : dataSegments) {
              String id = segment.getId().toString();
              preparedBatch.add()
                           .bind("id", id)
                           .bind("dataSource", segment.getDataSource())
                           .bind("created_date", DateTimes.nowUtc().toString())
                           .bind("start", segment.getInterval().getStart().toString())
                           .bind("end", segment.getInterval().getEnd().toString())
                           .bind("partitioned", !(segment.getShardSpec() instanceof NoneShardSpec))
                           .bind("version", segment.getVersion())
                           .bind("used", true)
                           .bind("payload", mapper.writeValueAsBytes(segment))
                           .bind("used_status_last_updated", DateTimes.nowUtc().toString())
                           .bind("schema_fingerprint", segmentMetadata.containsKey(id) ? segmentMetadata.get(id).lhs : null)
                           .bind("num_rows", segmentMetadata.containsKey(id) ? segmentMetadata.get(id).rhs : null);
            }

            final int[] affectedRows = preparedBatch.execute();
            final boolean succeeded = Arrays.stream(affectedRows).allMatch(eachAffectedRows -> eachAffectedRows == 1);
            if (!succeeded) {
              throw new ISE("Failed to publish segments to DB");
            }
            return true;
          }
      );
    } else {
      final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
      return derbyConnector.retryWithHandle(
          handle -> {
            PreparedBatch preparedBatch = handle.prepareBatch(
                StringUtils.format(
                    "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, partitioned, version, used, payload, used_status_last_updated) "
                    + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload, :used_status_last_updated)",
                    table,
                    derbyConnector.getQuoteString()
                )
            );
            for (DataSegment segment : dataSegments) {
              String id = segment.getId().toString();
              preparedBatch.add()
                           .bind("id", id)
                           .bind("dataSource", segment.getDataSource())
                           .bind("created_date", DateTimes.nowUtc().toString())
                           .bind("start", segment.getInterval().getStart().toString())
                           .bind("end", segment.getInterval().getEnd().toString())
                           .bind("partitioned", !(segment.getShardSpec() instanceof NoneShardSpec))
                           .bind("version", segment.getVersion())
                           .bind("used", true)
                           .bind("payload", mapper.writeValueAsBytes(segment))
                           .bind("used_status_last_updated", DateTimes.nowUtc().toString());
            }

            final int[] affectedRows = preparedBatch.execute();
            final boolean succeeded = Arrays.stream(affectedRows).allMatch(eachAffectedRows -> eachAffectedRows == 1);
            if (!succeeded) {
              throw new ISE("Failed to publish segments to DB");
            }
            return true;
          }
      );
    }
  }

  public void insertSegmentSchema(
      String dataSource,
      Map<String, SchemaPayload> schemaPayloadMap,
      Set<String> usedFingerprints
  )
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentSchemasTable();
    derbyConnector.retryWithHandle(
        handle -> {
          PreparedBatch preparedBatch = handle.prepareBatch(
              StringUtils.format(
                  "INSERT INTO %1$s (created_date, datasource, fingerprint, payload, used, used_status_last_updated, version) "
                  + "VALUES (:created_date, :datasource, :fingerprint, :payload, :used, :used_status_last_updated, :version)",
                  table
              )
          );

          for (Map.Entry<String, SchemaPayload> entry : schemaPayloadMap.entrySet()) {
            String fingerprint = entry.getKey();
            SchemaPayload payload = entry.getValue();
            String now = DateTimes.nowUtc().toString();
            preparedBatch.add()
                         .bind("created_date", now)
                         .bind("datasource", dataSource)
                         .bind("fingerprint", fingerprint)
                         .bind("payload", mapper.writeValueAsBytes(payload))
                         .bind("used", usedFingerprints.contains(fingerprint))
                         .bind("used_status_last_updated", now)
                         .bind("version", CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);
          }

          final int[] affectedRows = preparedBatch.execute();
          final boolean succeeded = Arrays.stream(affectedRows).allMatch(eachAffectedRows -> eachAffectedRows == 1);
          if (!succeeded) {
            throw new ISE("Failed to publish segments to DB");
          }
          return true;
        }
    );
  }

  public void verifySegmentSchema(Map<String, Pair<SchemaPayload, Integer>> segmentIdSchemaMap)
  {
    final String segmentsTable = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
    // segmentId -> schemaFingerprint, numRows
    Map<String, Pair<String, Long>> segmentStats = new HashMap<>();

    derbyConnector.retryWithHandle(
        handle -> handle.createQuery("SELECT id, schema_fingerprint, num_rows FROM " + segmentsTable + " WHERE used = true ORDER BY id")
                        .map((index, result, context) -> segmentStats.put(result.getString(1), Pair.of(result.getString(2), result.getLong(3))))
                        .list()
    );

    // schemaFingerprint -> schema details
    Map<String, SegmentSchemaRecord> schemaRepresentationMap = new HashMap<>();

    final String schemaTable = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentSchemasTable();

    derbyConnector.retryWithHandle(
        handle -> handle.createQuery("SELECT fingerprint, payload, created_date, used, version FROM "
                                     + schemaTable)
                        .map(((index, r, ctx) ->
                            schemaRepresentationMap.put(
                                r.getString(1),
                                new SegmentSchemaRecord(
                                    r.getString(1),
                                    JacksonUtils.readValue(
                                        mapper,
                                        r.getBytes(2),
                                        SchemaPayload.class
                                    ),
                                    r.getString(3),
                                    r.getBoolean(4),
                                    r.getInt(5)
                                )
                            )))
                        .list());

    for (Map.Entry<String, Pair<SchemaPayload, Integer>> entry : segmentIdSchemaMap.entrySet()) {
      String id = entry.getKey();
      SchemaPayload schemaPayload = entry.getValue().lhs;
      Integer random = entry.getValue().rhs;

      Assert.assertTrue(segmentStats.containsKey(id));

      Assert.assertEquals(random.intValue(), segmentStats.get(id).rhs.intValue());
      Assert.assertTrue(schemaRepresentationMap.containsKey(segmentStats.get(id).lhs));

      SegmentSchemaRecord schemaRepresentation = schemaRepresentationMap.get(segmentStats.get(id).lhs);
      Assert.assertEquals(schemaPayload, schemaRepresentation.getSchemaPayload());
      Assert.assertTrue(schemaRepresentation.isUsed());
      Assert.assertEquals(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION, schemaRepresentation.getVersion());
    }
  }

  public static class SegmentSchemaRecord
  {
    private final String fingerprint;
    private final SchemaPayload schemaPayload;
    private final String createdDate;
    private final boolean used;
    private final int version;

    public SegmentSchemaRecord(String fingerprint, SchemaPayload schemaPayload, String createdDate, Boolean used, int version)
    {
      this.fingerprint = fingerprint;
      this.schemaPayload = schemaPayload;
      this.createdDate = createdDate;
      this.used = used;
      this.version = version;
    }

    public String getFingerprint()
    {
      return fingerprint;
    }

    public SchemaPayload getSchemaPayload()
    {
      return schemaPayload;
    }

    public String getCreatedDate()
    {
      return createdDate;
    }

    public boolean isUsed()
    {
      return used;
    }

    public int getVersion()
    {
      return version;
    }
  }
}
