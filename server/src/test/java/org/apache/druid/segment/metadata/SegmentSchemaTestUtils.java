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
import org.apache.druid.segment.column.SchemaPayload;
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

  public Boolean insertUsedSegments(Set<DataSegment> dataSegments, Map<String, Pair<Long, Long>> segmentStats)
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
    return derbyConnector.retryWithHandle(
        handle -> {
          PreparedBatch preparedBatch = handle.prepareBatch(
              StringUtils.format(
                  "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, partitioned, version, used, payload, used_status_last_updated, schema_id, num_rows) "
                  + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload, :used_status_last_updated, :schema_id, :num_rows)",
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
                         .bind("schema_id", segmentStats.containsKey(id) ? segmentStats.get(id).lhs : null)
                         .bind("num_rows", segmentStats.containsKey(id) ? segmentStats.get(id).rhs : null);
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

  public Map<String, Long> insertSegmentSchema(Map<String, SchemaPayload> schemaPayloadMap)
  {
    final String table = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentSchemaTable();
    derbyConnector.retryWithHandle(
        handle -> {
          PreparedBatch preparedBatch = handle.prepareBatch(
              StringUtils.format(
                  "INSERT INTO %1$s (fingerprint, created_date, payload) "
                  + "VALUES (:fingerprint, :created_date, :payload)",
                  table
              )
          );

          for (Map.Entry<String, SchemaPayload> entry : schemaPayloadMap.entrySet()) {
            String fingerprint = entry.getKey();
            SchemaPayload payload = entry.getValue();
            preparedBatch.add()
                         .bind("fingerprint", fingerprint)
                         .bind("created_date", DateTimes.nowUtc().toString())
                         .bind("payload", mapper.writeValueAsBytes(payload));
          }

          final int[] affectedRows = preparedBatch.execute();
          final boolean succeeded = Arrays.stream(affectedRows).allMatch(eachAffectedRows -> eachAffectedRows == 1);
          if (!succeeded) {
            throw new ISE("Failed to publish segments to DB");
          }
          return true;
        }
    );

    Map<String, Long> fingerprintSchemaIdMap = new HashMap<>();
    derbyConnector.retryWithHandle(
        handle ->
            handle.createQuery("SELECT fingerprint, id FROM " + table)
                  .map((index, result, context) -> fingerprintSchemaIdMap.put(result.getString(1), result.getLong(2)))
                  .list()
    );
    return fingerprintSchemaIdMap;
  }

  public void verifySegmentSchema(Map<String, Pair<SchemaPayload, Integer>> segmentIdSchemaMap)
  {
    final String segmentsTable = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable();
    // segmentId -> schemaId, numRows
    Map<String, Pair<Long, Long>> segmentStats = new HashMap<>();

    derbyConnector.retryWithHandle(
        handle -> handle.createQuery("SELECT id, schema_id, num_rows FROM " + segmentsTable + " WHERE used = true ORDER BY id")
                        .map((index, result, context) -> segmentStats.put(result.getString(1), Pair.of(result.getLong(2), result.getLong(3))))
                        .list()
    );

    // schemaId -> schema details
    Map<Long, SegmentSchemaRepresentation> schemaRepresentationMap = new HashMap<>();

    final String schemaTable = derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentSchemaTable();

    derbyConnector.retryWithHandle(
        handle -> handle.createQuery("SELECT id, fingerprint, payload, created_date FROM "
                                     + schemaTable)
                        .map(((index, r, ctx) ->
                            schemaRepresentationMap.put(
                                r.getLong(1),
                                new SegmentSchemaRepresentation(
                                    r.getString(2),
                                    JacksonUtils.readValue(
                                        mapper,
                                        r.getBytes(3),
                                        SchemaPayload.class
                                    ),
                                    r.getString(4)
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

      SegmentSchemaRepresentation schemaRepresentation = schemaRepresentationMap.get(segmentStats.get(id).lhs);
      Assert.assertEquals(schemaPayload, schemaRepresentation.getSchemaPayload());
    }
  }

  public static class SegmentSchemaRepresentation
  {
    String fingerprint;
    SchemaPayload schemaPayload;
    String createdDate;

    public SegmentSchemaRepresentation(String fingerprint, SchemaPayload schemaPayload, String createdDate)
    {
      this.fingerprint = fingerprint;
      this.schemaPayload = schemaPayload;
      this.createdDate = createdDate;
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
  }
}
