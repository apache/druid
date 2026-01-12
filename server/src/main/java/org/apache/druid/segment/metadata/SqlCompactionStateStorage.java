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
import com.google.inject.Inject;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Deterministic;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.timeline.CompactionState;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.SQLStatement;
import org.skife.jdbi.v2.Update;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.List;

/**
 * Database-backed implementation of {@link CompactionStateStorage}.
 * <p>
 * Manages the persistence and retrieval of {@link CompactionState} objects in the metadata storage.
 * Compaction states are uniquely identified by their fingerprints, which are SHA-256 hashes of their content.
 * </p>
 * <p>
 * This implementation is designed to be called from a single thread (CompactionJobQueue) and relies on
 * database constraints and the retry mechanism to handle any conflicts. Operations are idempotent - concurrent
 * upserts for the same fingerprint will either succeed or fail with a constraint violation that is safely ignored.
 * </p>
 */
@LazySingleton
public class SqlCompactionStateStorage implements CompactionStateStorage
{
  private static final EmittingLogger log = new EmittingLogger(SqlCompactionStateStorage.class);

  private final MetadataStorageTablesConfig dbTables;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper deterministicMapper;
  private final SQLMetadataConnector connector;

  @Inject
  public SqlCompactionStateStorage(
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

  @Override
  public void upsertCompactionState(
      final String dataSource,
      final String fingerprint,
      final CompactionState compactionState,
      final DateTime updateTime
  )
  {
    if (compactionState == null || fingerprint == null || fingerprint.isEmpty()) {
      return;
    }

    try {
      connector.retryWithHandle(handle -> {
        // Check if the fingerprint already exists and its used status
        final FingerprintState state = getFingerprintState(handle, fingerprint);
        final String now = updateTime.toString();

        switch (state) {
          case EXISTS_AND_USED:
            // Fingerprint exists and is already marked as used - no operation needed
            log.debug(
                "Compaction state for fingerprint[%s] in dataSource[%s] already exists and is marked as used. Skipping update.",
                fingerprint,
                dataSource
            );
            break;

          case EXISTS_AND_UNUSED:
            // Fingerprint exists but is marked as unused - update the used flag
            log.info(
                "Found existing compaction state in DB for fingerprint[%s] in dataSource[%s]. Marking as used.",
                fingerprint,
                dataSource
            );
            String updateSql = StringUtils.format(
                "UPDATE %s SET used = :used, used_status_last_updated = :used_status_last_updated "
                + "WHERE fingerprint = :fingerprint",
                dbTables.getCompactionStatesTable()
            );
            handle.createStatement(updateSql)
                  .bind("used", true)
                  .bind("used_status_last_updated", now)
                  .bind("fingerprint", fingerprint)
                  .execute();

            log.info("Updated existing compaction state for datasource[%s].", dataSource);
            break;

          case DOES_NOT_EXIST:
            // Fingerprint doesn't exist - insert new state
            log.info("Inserting new compaction state for fingerprint[%s] in dataSource[%s].", fingerprint, dataSource);

            String insertSql = StringUtils.format(
                "INSERT INTO %s (created_date, dataSource, fingerprint, payload, used, used_status_last_updated) "
                + "VALUES (:created_date, :dataSource, :fingerprint, :payload, :used, :used_status_last_updated)",
                dbTables.getCompactionStatesTable()
            );

            try {
              handle.createStatement(insertSql)
                    .bind("created_date", now)
                    .bind("dataSource", dataSource)
                    .bind("fingerprint", fingerprint)
                    .bind("payload", jsonMapper.writeValueAsBytes(compactionState))
                    .bind("used", true)
                    .bind("used_status_last_updated", now)
                    .execute();

              log.info(
                  "Published compaction state for fingerprint[%s] to DB for datasource[%s].",
                  fingerprint,
                  dataSource
              );
            }
            catch (JsonProcessingException e) {
              throw InternalServerError.exception(
                  e,
                  "Failed to serialize compaction state for fingerprint[%s]",
                  fingerprint
              );
            }
            break;

          default:
            throw new IllegalStateException("Unknown fingerprint state: " + state);
        }
        return null;
      });
    }
    catch (Exception e) {
      if (isUniqueConstraintViolation(e)) {
        log.info(
            "Fingerprints already exist for datasource[%s] (likely concurrent insert). "
            + "Treating as success since operation is idempotent.",
            dataSource
        );
        // Swallow exception - another thread already persisted the same data
        return;
      }
      // For other exceptions, let them propagate
      throw e;
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
  public String generateCompactionStateFingerprint(
      final CompactionState compactionState,
      final String dataSource
  )
  {
    return CompactionStateFingerprints.generate(compactionState, dataSource, deterministicMapper);
  }


  /**
   * Represents the state of an indexing state fingerprint in the database.
   * <p>
   * Intent is to help upsert logic decide whether to insert, update, or skip operations.
   */
  private enum FingerprintState
  {
    /** Fingerprint does not exist in the database */
    DOES_NOT_EXIST,
    /** Fingerprint exists and is marked as used */
    EXISTS_AND_USED,
    /** Fingerprint exists but is marked as unused */
    EXISTS_AND_UNUSED
  }

  /**
   * Checks the state of a fingerprint in the metadata DB.
   *
   * @param handle             Database handle
   * @param fingerprintToCheck The fingerprint to check
   * @return The state of the fingerprint (exists and used, exists and unused, or does not exist)
   */
  private FingerprintState getFingerprintState(
      final Handle handle,
      @Nonnull final String fingerprintToCheck
  )
  {
    if (fingerprintToCheck.isEmpty()) {
      return FingerprintState.DOES_NOT_EXIST;
    }

    String sql = StringUtils.format(
        "SELECT used FROM %s WHERE fingerprint = :fingerprint",
        dbTables.getCompactionStatesTable()
    );

    Boolean used = handle.createQuery(sql)
                         .bind("fingerprint", fingerprintToCheck)
                         .mapTo(Boolean.class)
                         .first();

    if (used == null) {
      return FingerprintState.DOES_NOT_EXIST;
    }

    return used ? FingerprintState.EXISTS_AND_USED : FingerprintState.EXISTS_AND_UNUSED;
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

  /**
   * Checks if an exception is a unique constraint violation.
   * This is expected when multiple threads try to insert the same fingerprint concurrently.
   * Since operations are idempotent, these violations can be safely ignored.
   */
  private boolean isUniqueConstraintViolation(Exception e)
  {
    // Look for SQLException in the cause chain
    Throwable cause = e;
    while (cause != null) {
      if (cause instanceof SQLException) {
        SQLException sqlException = (SQLException) cause;
        String sqlState = sqlException.getSQLState();

        // SQL standard unique constraint violation codes
        // 23505 = unique_violation (PostgreSQL, Derby)
        // 23000 = integrity_constraint_violation (MySQL and others)
        if ("23505".equals(sqlState) || "23000".equals(sqlState)) {
          return true;
        }
      }
      cause = cause.getCause();
    }

    // Also check exception message as fallback
    String message = e.getMessage();
    if (message != null) {
      String lowerMessage = StringUtils.toLowerCase(message);
      return lowerMessage.contains("unique constraint")
          || lowerMessage.contains("duplicate key")
          || lowerMessage.contains("duplicate entry");
    }

    return false;
  }
}
