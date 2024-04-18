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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.timeline.SegmentId;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.TransactionCallback;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Handles segment schema persistence and cleanup.
 */
@LazySingleton
public class SegmentSchemaManager
{
  private static final EmittingLogger log = new EmittingLogger(SegmentSchemaManager.class);
  private static final int DB_ACTION_PARTITION_SIZE = 100;
  private final MetadataStorageTablesConfig dbTables;
  private final ObjectMapper jsonMapper;
  private final SQLMetadataConnector connector;

  @Inject
  public SegmentSchemaManager(
      MetadataStorageTablesConfig dbTables,
      ObjectMapper jsonMapper,
      SQLMetadataConnector connector
  )
  {
    this.dbTables = dbTables;
    this.jsonMapper = jsonMapper;
    this.connector = connector;
  }

  public List<Long> findReferencedSchemaIdsMarkedAsUnused()
  {
    return connector.retryWithHandle(
        handle ->
            handle.createQuery(
                      StringUtils.format(
                          "SELECT DISTINCT(schema_id) FROM %s WHERE used = true AND schema_id IN (SELECT id FROM %s WHERE used = false)",
                          dbTables.getSegmentsTable(),
                          dbTables.getSegmentSchemasTable()
                      ))
                  .mapTo(Long.class)
                  .list()
    );
  }

  public int markSchemaIdsAsUsed(List<Long> schemaIds)
  {
    if (schemaIds.isEmpty()) {
      return 0;
    }
    String inClause = schemaIds.stream().map(Object::toString).collect(Collectors.joining(","));

    return connector.retryWithHandle(
        handle ->
            handle.createStatement(
                      StringUtils.format(
                          "UPDATE %s SET used = true, used_status_last_updated = :now"
                          + " WHERE id IN (%s)",
                          dbTables.getSegmentSchemasTable(), inClause
                      )
                  )
                  .bind("now", DateTimes.nowUtc().toString())
                  .execute()
    );
  }

  public int deleteSchemasOlderThan(long timestamp)
  {
    return connector.retryWithHandle(
        handle -> handle.createStatement(
                            StringUtils.format(
                                "DELETE FROM %s WHERE used = false AND used_status_last_updated < :now",
                                dbTables.getSegmentSchemasTable()
                            ))
                        .bind("now", DateTimes.utc(timestamp).toString())
                        .execute());
  }

  public int markUnreferencedSchemasAsUnused()
  {
    return connector.retryWithHandle(
        handle ->
            handle.createStatement(
                      StringUtils.format(
                          "UPDATE %s SET used = false, used_status_last_updated = :now  WHERE used != false "
                          + "AND id NOT IN (SELECT DISTINCT(schema_id) FROM %s WHERE used=true AND schema_id IS NOT NULL)",
                          dbTables.getSegmentSchemasTable(),
                          dbTables.getSegmentsTable()
                      )
                  )
                  .bind("now", DateTimes.nowUtc().toString())
                  .execute());
  }

  /**
   * Persist segment schema and update segments in a transaction.
   */
  public void persistSchemaAndUpdateSegmentsTable(String dataSource, List<SegmentSchemaMetadataPlus> segmentSchemas, int schemaVersion)
  {
    connector.retryTransaction((TransactionCallback<Void>) (handle, status) -> {
      Map<String, SchemaPayload> schemaPayloadMap = new HashMap<>();

      for (SegmentSchemaMetadataPlus segmentSchema : segmentSchemas) {
        schemaPayloadMap.put(
            segmentSchema.getFingerprint(),
            segmentSchema.getSegmentSchemaMetadata().getSchemaPayload()
        );
      }
      persistSegmentSchema(handle, dataSource, schemaPayloadMap, schemaVersion);
      updateSegmentWithSchemaInformation(handle, dataSource, schemaVersion, segmentSchemas);

      return null;
    }, 1, 3);
  }

  /**
   * Persist unique segment schema in the DB.
   */
  public void persistSegmentSchema(
      Handle handle,
      String dataSource,
      Map<String, SchemaPayload> fingerprintSchemaPayloadMap,
      int schemaVersion
  ) throws JsonProcessingException
  {
    try {
      // Filter already existing schema
      Map<Boolean, Set<String>> existingFingerprintsAndUsedStatus = fingerprintExistBatch(handle, dataSource, schemaVersion, fingerprintSchemaPayloadMap.keySet());
      Set<String> usedExistingFingerprints = existingFingerprintsAndUsedStatus.containsKey(true) ? existingFingerprintsAndUsedStatus.get(true) : new HashSet<>();
      Set<String> unusedExistingFingerprints = existingFingerprintsAndUsedStatus.containsKey(false) ? existingFingerprintsAndUsedStatus.get(false) : new HashSet<>();
      Set<String> existingFingerprints = Sets.union(usedExistingFingerprints, unusedExistingFingerprints);
      if (existingFingerprints.size() > 0) {
        log.info(
            "Found already existing schema in the DB for dataSource [%1$s]. Used fingeprints: [%2$s], Unused fingerprints: [%3$s]",
            dataSource,
            usedExistingFingerprints,
            unusedExistingFingerprints
        );
      }

      // There is a possibility of race with schema cleanup Coordinator duty.
      // The duty could delete the unused schema. We try to mark them used.
      // However, if the duty succeeds in deleting it the transaction fails due to consistency guarantees.
      // The failed transaction is retried.
      // Since the deletion period would be at least > 1h, we are sure that the race wouldn't arise on retry.
      // There is another race, wherein used schema could be marked as unused by the cleanup duty.
      // The implication is that a segment could reference an unused schema. Since there is a significant gap
      // between marking the segment as unused and deletion, the schema won't be lost during retry.
      // There is no functional problem as such, since the duty would itself mark those schema as used.
      if (unusedExistingFingerprints.size() > 0) {
        // make the unused schema as used to prevent deletion
        String inClause = unusedExistingFingerprints.stream()
                                .map(value -> "'" + StringEscapeUtils.escapeSql(value) + "'")
                                .collect(Collectors.joining(","));

        handle.createStatement(
                  StringUtils.format(
                      "UPDATE %s SET used = true, used_status_last_updated = :now"
                      + " WHERE datasource = :datasource AND version = :version AND fingerprint IN (%s)",
                      dbTables.getSegmentSchemasTable(), inClause)
              )
              .bind("now", DateTimes.nowUtc().toString())
              .bind("datasource", dataSource)
              .bind("version", schemaVersion)
              .execute();
      }

      Map<String, SchemaPayload> schemaPayloadToCreate = new HashMap<>();

      for (Map.Entry<String, SchemaPayload> entry : fingerprintSchemaPayloadMap.entrySet()) {
        if (!existingFingerprints.contains(entry.getKey())) {
          schemaPayloadToCreate.put(entry.getKey(), entry.getValue());
        }
      }

      if (schemaPayloadToCreate.isEmpty()) {
        log.info("No schema to persist for dataSource [%s].", dataSource);
        return;
      }

      final List<List<String>> partitionedFingerprints = Lists.partition(
          new ArrayList<>(schemaPayloadToCreate.keySet()),
          DB_ACTION_PARTITION_SIZE
      );

      String insertSql = StringUtils.format(
          "INSERT INTO %s (created_date, datasource, fingerprint, payload, used, used_status_last_updated, version) "
          + "VALUES (:created_date, :datasource, :fingerprint, :payload, :used, :used_status_last_updated, :version)",
          dbTables.getSegmentSchemasTable()
      );

      // insert schemas
      PreparedBatch schemaInsertBatch = handle.prepareBatch(insertSql);
      for (List<String> partition : partitionedFingerprints) {
        for (String fingerprint : partition) {
          final String now = DateTimes.nowUtc().toString();
          schemaInsertBatch.add()
                           .bind("created_date", now)
                           .bind("datasource", dataSource)
                           .bind("fingerprint", fingerprint)
                           .bind("payload", jsonMapper.writeValueAsBytes(fingerprintSchemaPayloadMap.get(fingerprint)))
                           .bind("used", true)
                           .bind("used_status_last_updated", now)
                           .bind("version", schemaVersion);
        }
        final int[] affectedRows = schemaInsertBatch.execute();
        final boolean succeeded = Arrays.stream(affectedRows).allMatch(eachAffectedRows -> eachAffectedRows == 1);
        if (succeeded) {
          log.info("Published schemas to DB: %s", partition);
        } else {
          final List<String> failedToPublish =
              IntStream.range(0, partition.size())
                       .filter(i -> affectedRows[i] != 1)
                       .mapToObj(partition::get)
                       .collect(Collectors.toList());
          throw new ISE("Failed to publish schemas to DB: %s", failedToPublish);
        }
      }
    }
    catch (Exception e) {
      log.error("Exception inserting schemas to DB: %s", fingerprintSchemaPayloadMap);
      throw e;
    }
  }

  /**
   * Update segment with schemaId and numRows information.
   */
  public void updateSegmentWithSchemaInformation(Handle handle, String dataSource, int schemaVersion, List<SegmentSchemaMetadataPlus> batch)
  {
    log.debug("Updating segment with schema and numRows information: [%s].", batch);

    // fetch schemaId
    Map<String, Long> fingerprintSchemaIdMap =
        schemaIdFetchBatch(
            handle,
            dataSource,
            schemaVersion,
            batch
                .stream()
                .map(SegmentSchemaMetadataPlus::getFingerprint)
                .collect(Collectors.toSet())
        );

    log.debug("FingerprintSchemaIdMap: [%s].", fingerprintSchemaIdMap);

    // update schemaId and numRows in segments table
    String updateSql =
        StringUtils.format(
            "UPDATE %s SET schema_id = :schema_id, num_rows = :num_rows WHERE id = :id",
            dbTables.getSegmentsTable()
        );

    PreparedBatch segmentUpdateBatch = handle.prepareBatch(updateSql);

    List<List<SegmentSchemaMetadataPlus>> partitionedSegmentIds =
        Lists.partition(
            batch,
            DB_ACTION_PARTITION_SIZE
        );

    for (List<SegmentSchemaMetadataPlus> partition : partitionedSegmentIds) {
      for (SegmentSchemaMetadataPlus segmentSchema : batch) {
        String fingerprint = segmentSchema.getFingerprint();
        if (!fingerprintSchemaIdMap.containsKey(fingerprint)) {
          log.error(
              "Fingerprint [%s] for segmentId [%s] and datasource [%s] is not associated with any schemaId.",
                    fingerprint, dataSource, segmentSchema.getSegmentId()
          );
          continue;
        }

        segmentUpdateBatch.add()
                          .bind("id", segmentSchema.getSegmentId().toString())
                          .bind("schema_id", fingerprintSchemaIdMap.get(fingerprint))
                          .bind("num_rows", segmentSchema.getSegmentSchemaMetadata().getNumRows());
      }

      final int[] affectedRows = segmentUpdateBatch.execute();
      final boolean succeeded = Arrays.stream(affectedRows).allMatch(eachAffectedRows -> eachAffectedRows == 1);

      if (succeeded) {
        log.info("Updated segments with schemaId & numRows in the DB: %s", partition);
      } else {
        final List<String> failedToUpdate =
            IntStream.range(0, partition.size())
                     .filter(i -> affectedRows[i] != 1)
                     .mapToObj(partition::get)
                     .map(plus -> plus.getSegmentId().toString())
                     .collect(Collectors.toList());
        throw new ISE("Failed to update segments with schema information: %s", failedToUpdate);
      }
    }
  }

  /**
   * Query the metadata DB to filter the fingerprints that exists.
   * It returns separate set for used and unused fingerprints in a map.
   */
  private Map<Boolean, Set<String>> fingerprintExistBatch(Handle handle, String dataSource, int schemaVersion, Set<String> fingerprintsToInsert)
  {
    List<List<String>> partitionedFingerprints = Lists.partition(
        new ArrayList<>(fingerprintsToInsert),
        DB_ACTION_PARTITION_SIZE
    );

    Map<Boolean, Set<String>> existingFingerprints = new HashMap<>();
    for (List<String> fingerprintList : partitionedFingerprints) {
      String fingerprints = fingerprintList.stream()
                                           .map(fingerprint -> "'" + StringEscapeUtils.escapeSql(fingerprint) + "'")
                                           .collect(Collectors.joining(","));
      handle.createQuery(
                StringUtils.format(
                    "SELECT used, fingerprint FROM %s WHERE datasource = :datasource AND version = :version AND fingerprint IN (%s)",
                    dbTables.getSegmentSchemasTable(), fingerprints
                )
            )
            .bind("datasource", dataSource)
            .bind("version", schemaVersion)
            .map((index, r, ctx) -> existingFingerprints.computeIfAbsent(
                r.getBoolean(1), value -> new HashSet<>()).add(r.getString(2)))
            .list();
    }
    return existingFingerprints;
  }

  public Map<String, Long> schemaIdFetchBatch(Handle handle, String dataSource, int schemaVersion, Set<String> fingerprintsToQuery)
  {
    List<List<String>> partitionedFingerprints = Lists.partition(
        new ArrayList<>(fingerprintsToQuery),
        DB_ACTION_PARTITION_SIZE
    );

    Map<String, Long> fingerprintIdMap = new HashMap<>();
    for (List<String> fingerprintList : partitionedFingerprints) {
      String fingerprints = fingerprintList.stream()
                                           .map(fingerprint -> "'" + StringEscapeUtils.escapeSql(fingerprint) + "'")
                                           .collect(Collectors.joining(","));
      Map<String, Long> partitionFingerprintIdMap =
          handle.createQuery(
                    StringUtils.format(
                        "SELECT fingerprint, id FROM %s WHERE fingerprint IN (%s) AND datasource = :datasource AND version = :version",
                        dbTables.getSegmentSchemasTable(), fingerprints
                    )
                )
                .bind("datasource", dataSource)
                .bind("version", schemaVersion)
                .map((index, r, ctx) -> Pair.of(r.getString("fingerprint"), r.getLong("id")))
                .fold(
                    new HashMap<>(), (accumulator, rs, control, ctx) -> {
                      accumulator.put(rs.lhs, rs.rhs);
                      return accumulator;
                    }
                );
      fingerprintIdMap.putAll(partitionFingerprintIdMap);
    }
    return fingerprintIdMap;
  }

  /**
   * Wrapper over {@link SchemaPayloadPlus} class to include segmentId and fingerprint information.
   */
  public static class SegmentSchemaMetadataPlus
  {
    private final SegmentId segmentId;
    private final String fingerprint;
    private final SchemaPayloadPlus schemaPayloadPlus;

    public SegmentSchemaMetadataPlus(
        SegmentId segmentId,
        String fingerprint,
        SchemaPayloadPlus schemaPayloadPlus
    )
    {
      this.segmentId = segmentId;
      this.schemaPayloadPlus = schemaPayloadPlus;
      this.fingerprint = fingerprint;
    }

    public SegmentId getSegmentId()
    {
      return segmentId;
    }

    public SchemaPayloadPlus getSegmentSchemaMetadata()
    {
      return schemaPayloadPlus;
    }

    public String getFingerprint()
    {
      return fingerprint;
    }

    @Override
    public String toString()
    {
      return "SegmentSchemaMetadataPlus{" +
             "segmentId='" + segmentId + '\'' +
             ", fingerprint='" + fingerprint + '\'' +
             ", segmentSchemaMetadata=" + schemaPayloadPlus +
             '}';
    }
  }
}
