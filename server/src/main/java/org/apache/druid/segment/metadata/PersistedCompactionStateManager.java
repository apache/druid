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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.Striped;
import com.google.inject.Inject;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Deterministic;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.timeline.CompactionState;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.SQLStatement;
import org.skife.jdbi.v2.Update;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * Database-backed implementation of {@link CompactionStateManager}.
 * <p>
 * Manages the persistence and retrieval of {@link CompactionState} objects in the metadata storage.
 * Compaction states are uniquely identified by their fingerprints, which are SHA-256 hashes of their content. A cache
 * of compaction states using the fingerprints as keys is maintained in memory to optimize retrieval performance.
 * </p>
 * <p>
 * A striped locking mechanism is used to ensure thread-safe persistence of compaction states on a per-datasource basis.
 * </p>
 */
@ManageLifecycle
public class PersistedCompactionStateManager implements CompactionStateManager
{
  private static final EmittingLogger log = new EmittingLogger(PersistedCompactionStateManager.class);
  private static final int DB_ACTION_PARTITION_SIZE = 100;

  private final MetadataStorageTablesConfig dbTables;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper deterministicMapper;
  private final SQLMetadataConnector connector;
  private final Striped<Lock> datasourceLocks = Striped.lock(128);

  @Inject
  public PersistedCompactionStateManager(
      @Nonnull MetadataStorageTablesConfig dbTables,
      @Nonnull ObjectMapper jsonMapper,
      @Deterministic @Nonnull ObjectMapper deterministicMapper,
      @Nonnull SQLMetadataConnector connector
  )
  {
    this.dbTables = dbTables;
    this.jsonMapper = jsonMapper;
    this.deterministicMapper = deterministicMapper;
    this.connector = connector;
  }

  @LifecycleStart
  public void start()
  {
  }

  @LifecycleStop
  public void stop()
  {
  }

  @VisibleForTesting
  PersistedCompactionStateManager()
  {
    this.dbTables = null;
    this.jsonMapper = null;
    this.deterministicMapper = null;
    this.connector = null;
  }

  @Override
  public void persistCompactionState(
      final String dataSource,
      final Map<String, CompactionState> fingerprintToStateMap,
      final DateTime updateTime
  )
  {
    if (fingerprintToStateMap.isEmpty()) {
      return;
    }

    final Lock lock = datasourceLocks.get(dataSource);
    lock.lock();
    try {
      connector.retryWithHandle(handle -> {
        // Fetch already existing compaction state fingerprints
        final Set<String> existingFingerprints = getExistingFingerprints(
            handle,
            fingerprintToStateMap.keySet()
        );

        if (!existingFingerprints.isEmpty()) {
          log.info(
              "Found already existing compaction state in the DB for dataSource[%s]. Fingerprints: %s.",
              dataSource,
              existingFingerprints
          );
          String setFingerprintsUsedSql = StringUtils.format(
              "UPDATE %s SET used = :used, used_status_last_updated = :used_status_last_updated "
              + "WHERE fingerprint = :fingerprint",
              dbTables.getCompactionStatesTable()
          );
          PreparedBatch markUsedBatch = handle.prepareBatch(setFingerprintsUsedSql);
          for (String fingerprint : existingFingerprints) {
            final String now = updateTime.toString();
            markUsedBatch.add()
                         .bind("used", true)
                         .bind("used_status_last_updated", now)
                         .bind("fingerprint", fingerprint);
          }
          markUsedBatch.execute();
        }

        Map<String, CompactionState> statesToPersist = new HashMap<>();

        for (Map.Entry<String, CompactionState> entry : fingerprintToStateMap.entrySet()) {
          if (!existingFingerprints.contains(entry.getKey())) {
            statesToPersist.put(entry.getKey(), entry.getValue());
          }
        }

        if (statesToPersist.isEmpty()) {
          log.info("No compaction state to persist for dataSource [%s].", dataSource);
          return null;
        }

        final List<List<String>> partitionedFingerprints = Lists.partition(
            new ArrayList<>(statesToPersist.keySet()),
            DB_ACTION_PARTITION_SIZE
        );

        String insertSql = StringUtils.format(
            "INSERT INTO %s (created_date, datasource, fingerprint, payload, used, used_status_last_updated) "
            + "VALUES (:created_date, :datasource, :fingerprint, :payload, :used, :used_status_last_updated)",
            dbTables.getCompactionStatesTable()
        );

        // Insert compaction states
        PreparedBatch stateInsertBatch = handle.prepareBatch(insertSql);
        for (List<String> partition : partitionedFingerprints) {
          for (String fingerprint : partition) {
            final String now = updateTime.toString();
            try {
              stateInsertBatch.add()
                              .bind("created_date", now)
                              .bind("datasource", dataSource)
                              .bind("fingerprint", fingerprint)
                              .bind("payload", jsonMapper.writeValueAsBytes(fingerprintToStateMap.get(fingerprint)))
                              .bind("used", true)
                              .bind("used_status_last_updated", now);
            }
            catch (JsonProcessingException e) {
              throw InternalServerError.exception(
                  e,
                  "Failed to serialize compaction state for fingerprint[%s]",
                  fingerprint
              );
            }
          }
          final int[] affectedRows = stateInsertBatch.execute();
          final List<String> failedInserts = new ArrayList<>();
          for (int i = 0; i < partition.size(); ++i) {
            if (affectedRows[i] != 1) {
              failedInserts.add(partition.get(i));
            }
          }
          if (failedInserts.isEmpty()) {
            log.info(
                "Published compaction states %s to DB for datasource[%s].",
                partition,
                dataSource
            );
          } else {
            throw new ISE(
                "Failed to publish compaction states[%s] to DB for datasource[%s]",
                failedInserts,
                dataSource
            );
          }
        }
        return null;
      });
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public int markUnreferencedCompactionStatesAsUnused()
  {
    return connector.retryWithHandle(
        handle ->
            handle.createStatement(
                      StringUtils.format(
                          "UPDATE %s SET used = false, used_status_last_updated = :now WHERE used = true "
                          + "AND fingerprint NOT IN (SELECT DISTINCT compaction_state_fingerprint FROM %s WHERE used = true AND compaction_state_fingerprint IS NOT NULL)",
                          dbTables.getCompactionStatesTable(),
                          dbTables.getSegmentsTable()
                      )
                  )
                  .bind("now", DateTimes.nowUtc().toString())
                  .execute());
  }

  @Override
  public List<String> findReferencedCompactionStateMarkedAsUnused()
  {
    return connector.retryWithHandle(
        handle ->
            handle.createQuery(
                      StringUtils.format(
                          "SELECT DISTINCT compaction_state_fingerprint FROM %s WHERE used = true AND compaction_state_fingerprint IN (SELECT fingerprint FROM %s WHERE used = false)",
                          dbTables.getSegmentsTable(),
                          dbTables.getCompactionStatesTable()
                      ))
                  .mapTo(String.class)
                  .list()
    );
  }

  @Override
  public int markCompactionStatesAsUsed(List<String> stateFingerprints)
  {
    if (stateFingerprints.isEmpty()) {
      return 0;
    }

    return connector.retryWithHandle(
        handle -> {
          Update statement = handle.createStatement(
              StringUtils.format(
                  "UPDATE %s SET used = true, used_status_last_updated = :now"
                  + " WHERE fingerprint IN (%s)",
                  dbTables.getCompactionStatesTable(),
                  buildParameterizedInClause("fp", stateFingerprints.size())
              )
          ).bind("now", DateTimes.nowUtc().toString());

          bindValuesToInClause(stateFingerprints, "fp", statement);

          return statement.execute();
        }
    );
  }

  @Override
  public int deleteUnusedCompactionStatesOlderThan(long timestamp)
  {
    return connector.retryWithHandle(
        handle -> handle.createStatement(
                            StringUtils.format(
                                "DELETE FROM %s WHERE used = false AND used_status_last_updated < :maxUpdateTime",
                                dbTables.getCompactionStatesTable()
                            ))
                        .bind("maxUpdateTime", DateTimes.utc(timestamp).toString())
                        .execute());
  }

  @Override
  @SuppressWarnings("UnstableApiUsage")
  public String generateCompactionStateFingerprint(
      final CompactionState compactionState,
      final String dataSource
  )
  {
    final Hasher hasher = Hashing.sha256().newHasher();

    hasher.putBytes(StringUtils.toUtf8(dataSource));
    hasher.putByte((byte) 0xff);

    try {
      hasher.putBytes(deterministicMapper.writeValueAsBytes(compactionState));
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize CompactionState for fingerprinting", e);
    }
    hasher.putByte((byte) 0xff);

    return BaseEncoding.base16().encode(hasher.hash().asBytes());
  }


  /**
   * Query the metadata DB to filter the fingerprints that already exist.
   **/
  private Set<String> getExistingFingerprints(
      final Handle handle,
      final Set<String> fingerprintsToInsert
  )
  {
    if (fingerprintsToInsert.isEmpty()) {
      return Collections.emptySet();
    }

    List<List<String>> partitionedFingerprints = Lists.partition(
        new ArrayList<>(fingerprintsToInsert),
        DB_ACTION_PARTITION_SIZE
    );

    final Set<String> existingFingerprints = new HashSet<>();
    for (List<String> fingerprintList : partitionedFingerprints) {
      Query<?> query = handle.createQuery(
          StringUtils.format(
              "SELECT fingerprint FROM %s WHERE fingerprint IN (%s)",
              dbTables.getCompactionStatesTable(),
              buildParameterizedInClause("fp", fingerprintList.size())
          )
      );

      bindValuesToInClause(fingerprintList, "fp", query);

      query.map((index, r, ctx) -> existingFingerprints.add(r.getString(1)))
           .list();
    }
    return existingFingerprints;
  }

  /**
   * Builds a parameterized IN clause for the specified column with placeholders.
   * Must be followed by a call to {@link #bindValuesToInClause(List, String, SQLStatement)}.
   *
   * @param parameterPrefix prefix for parameter names (e.g., "fingerprint")
   * @param valueCount      number of values in the IN clause
   * @return parameterized IN clause like "(?, ?, ?)" but with named parameters
   */
  private static String buildParameterizedInClause(String parameterPrefix, int valueCount)
  {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < valueCount; i++) {
      sb.append(":").append(parameterPrefix).append(i);
      if (i != valueCount - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  /**
   * Binds values to a parameterized IN clause in a SQL query.
   *
   * @param values          list of values to bind
   * @param parameterPrefix prefix used when building the IN clause
   * @param query           the SQL statement to bind values to
   */
  private static void bindValuesToInClause(
      List<String> values,
      String parameterPrefix,
      SQLStatement<?> query
  )
  {
    for (int i = 0; i < values.size(); i++) {
      query.bind(parameterPrefix + i, values.get(i));
    }
  }
}
