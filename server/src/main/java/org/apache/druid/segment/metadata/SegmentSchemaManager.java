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
import com.google.common.base.Functions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

  /**
   * Return a list of schema fingerprints
   */
  public List<String> findReferencedSchemaMarkedAsUnused()
  {
    return connector.retryWithHandle(
        handle ->
            handle.createQuery(
                      StringUtils.format(
                          "SELECT DISTINCT(schema_fingerprint) FROM %s WHERE used = true AND schema_fingerprint IN (SELECT fingerprint FROM %s WHERE used = false)",
                          dbTables.getSegmentsTable(),
                          dbTables.getSegmentSchemasTable()
                      ))
                  .mapTo(String.class)
                  .list()
    );
  }

  public int markSchemaAsUsed(List<String> schemaFingerprints)
  {
    if (schemaFingerprints.isEmpty()) {
      return 0;
    }
    String inClause = getInClause(schemaFingerprints.stream());

    return connector.retryWithHandle(
        handle ->
            handle.createStatement(
                      StringUtils.format(
                          "UPDATE %s SET used = true, used_status_last_updated = :now"
                          + " WHERE fingerprint IN (%s)",
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
                          + "AND fingerprint NOT IN (SELECT DISTINCT(schema_fingerprint) FROM %s WHERE used=true AND schema_fingerprint IS NOT NULL)",
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
  public void persistSchemaAndUpdateSegmentsTable(
      final String dataSource,
      final List<SegmentSchemaMetadataPlus> segmentSchemas,
      final int version
  )
  {
    connector.retryTransaction((TransactionCallback<Void>) (handle, status) -> {
      Map<String, SchemaPayload> schemaPayloadMap = new HashMap<>();

      for (SegmentSchemaMetadataPlus segmentSchema : segmentSchemas) {
        schemaPayloadMap.put(
            segmentSchema.getFingerprint(),
            segmentSchema.getSegmentSchemaMetadata().getSchemaPayload()
        );
      }
      persistSegmentSchema(handle, dataSource, version, schemaPayloadMap);
      updateSegmentWithSchemaInformation(handle, segmentSchemas);

      return null;
    }, 1, 3);
  }

  /**
   * Persist unique segment schema in the DB.
   */
  public void persistSegmentSchema(
      final Handle handle,
      final String dataSource,
      final int version,
      final Map<String, SchemaPayload> fingerprintSchemaPayloadMap
  ) throws JsonProcessingException
  {
    if (fingerprintSchemaPayloadMap.isEmpty()) {
      return;
    }
    // Filter already existing schema
    Map<Boolean, Set<String>> existingFingerprintsAndUsedStatus = fingerprintExistBatch(
        handle,
        fingerprintSchemaPayloadMap.keySet()
    );

    // Used schema can also be marked as unused by the schema cleanup duty in parallel.
    // Refer to the javadocs in org.apache.druid.server.coordinator.duty.KillUnreferencedSegmentSchemaDuty for more details.
    Set<String> usedExistingFingerprints = existingFingerprintsAndUsedStatus.containsKey(true)
                                           ? existingFingerprintsAndUsedStatus.get(true)
                                           : new HashSet<>();
    Set<String> unusedExistingFingerprints = existingFingerprintsAndUsedStatus.containsKey(false)
                                             ? existingFingerprintsAndUsedStatus.get(false)
                                             : new HashSet<>();
    Set<String> existingFingerprints = Sets.union(usedExistingFingerprints, unusedExistingFingerprints);
    if (existingFingerprints.size() > 0) {
      log.info(
          "Found already existing schema in the DB for dataSource [%1$s]. "
          + "Used fingeprints: [%2$s], Unused fingerprints: [%3$s].",
          dataSource,
          usedExistingFingerprints,
          unusedExistingFingerprints
      );
    }

    // Unused schema can be deleted by the schema cleanup duty in parallel.
    // Refer to the javadocs in org.apache.druid.server.coordinator.duty.KillUnreferencedSegmentSchemaDuty for more details.
    if (unusedExistingFingerprints.size() > 0) {
      // make the unused schema as used to prevent deletion
      markSchemaAsUsed(new ArrayList<>(unusedExistingFingerprints));
    }

    Map<String, SchemaPayload> schemaPayloadToPersist = new HashMap<>();

    for (Map.Entry<String, SchemaPayload> entry : fingerprintSchemaPayloadMap.entrySet()) {
      if (!existingFingerprints.contains(entry.getKey())) {
        schemaPayloadToPersist.put(entry.getKey(), entry.getValue());
      }
    }

    if (schemaPayloadToPersist.isEmpty()) {
      log.info("No schema to persist for dataSource [%s] and version [%s].", dataSource, version);
      return;
    }

    final List<List<String>> partitionedFingerprints = Lists.partition(
        new ArrayList<>(schemaPayloadToPersist.keySet()),
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
                         .bind("version", version);
      }
      final int[] affectedRows = schemaInsertBatch.execute();
      final List<String> failedInserts = new ArrayList<>();
      for (int i = 0; i < partition.size(); ++i) {
        if (affectedRows[i] != 1) {
          failedInserts.add(partition.get(i));
        }
      }
      if (failedInserts.isEmpty()) {
        log.info(
            "Published schemas [%s] to DB for datasource [%s] and version [%s]",
            partition,
            dataSource,
            version
        );
      } else {
        throw new ISE(
            "Failed to publish schemas [%s] to DB for datasource [%s] and version [%s]",
            failedInserts,
            dataSource,
            version
        );
      }
    }
  }

  /**
   * Update segment with schemaFingerprint and numRows information.
   */
  public void updateSegmentWithSchemaInformation(
      final Handle handle,
      final List<SegmentSchemaMetadataPlus> batch
  )
  {
    log.debug("Updating segment with schemaFingerprint and numRows information: [%s].", batch);

    // update schemaFingerprint and numRows in segments table
    String updateSql =
        StringUtils.format(
            "UPDATE %s SET schema_fingerprint = :schema_fingerprint, num_rows = :num_rows WHERE id = :id",
            dbTables.getSegmentsTable()
        );

    PreparedBatch segmentUpdateBatch = handle.prepareBatch(updateSql);

    List<List<SegmentSchemaMetadataPlus>> partitionedSegmentIds =
        Lists.partition(
            batch,
            DB_ACTION_PARTITION_SIZE
        );

    for (List<SegmentSchemaMetadataPlus> partition : partitionedSegmentIds) {
      for (SegmentSchemaMetadataPlus segmentSchema : partition) {
        String fingerprint = segmentSchema.getFingerprint();

        segmentUpdateBatch.add()
                          .bind("id", segmentSchema.getSegmentId().toString())
                          .bind("schema_fingerprint", fingerprint)
                          .bind("num_rows", segmentSchema.getSegmentSchemaMetadata().getNumRows());
      }

      final int[] affectedRows = segmentUpdateBatch.execute();
      final List<SegmentId> failedUpdates = new ArrayList<>();
      for (int i = 0; i < partition.size(); ++i) {
        if (affectedRows[i] != 1) {
          failedUpdates.add(partition.get(i).getSegmentId());
        }
      }

      if (failedUpdates.isEmpty()) {
        log.infoSegmentIds(
            partition.stream().map(SegmentSchemaMetadataPlus::getSegmentId),
            "Updated segments with schema information in the DB"
        );
      } else {
        throw new ISE(
            "Failed to update segments with schema information: %s",
            getCommaSeparatedIdentifiers(failedUpdates));
      }
    }
  }

  private Object getCommaSeparatedIdentifiers(final Collection<SegmentId> ids)
  {
    if (ids == null || ids.isEmpty()) {
      return null;
    }

    return Collections2.transform(ids, Functions.identity());
  }

  /**
   * Query the metadata DB to filter the fingerprints that exists.
   * It returns separate set for used and unused fingerprints in a map.
   */
  private Map<Boolean, Set<String>> fingerprintExistBatch(
      final Handle handle,
      final Set<String> fingerprintsToInsert
  )
  {
    if (fingerprintsToInsert.isEmpty()) {
      return Collections.emptyMap();
    }

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
                    "SELECT used, fingerprint FROM %s WHERE fingerprint IN (%s)",
                    dbTables.getSegmentSchemasTable(), fingerprints
                )
            )
            .map((index, r, ctx) -> existingFingerprints.computeIfAbsent(
                r.getBoolean(1), value -> new HashSet<>()).add(r.getString(2)))
            .list();
    }
    return existingFingerprints;
  }

  private String getInClause(final Stream<String> ids)
  {
    return ids
        .map(value -> "'" + StringEscapeUtils.escapeSql(value) + "'")
        .collect(Collectors.joining(","));
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
             ", schemaPayloadPlus=" + schemaPayloadPlus +
             '}';
    }
  }
}
