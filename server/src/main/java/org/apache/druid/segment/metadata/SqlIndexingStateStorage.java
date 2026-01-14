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
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.guice.LazySingleton;
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
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import java.util.List;

/**
 * Database-backed implementation of {@link IndexingStateStorage}.
 * <p>
 * Manages the persistence and retrieval of {@link CompactionState} (AKA IndexinState) objects in the metadata storage.
 * Indexing states are uniquely identified by their fingerprints, which are SHA-256 hashes of their content.
 * </p>
 * <p>
 * This implementation is designed to be called from a single thread and relies on
 * database constraints and the retry mechanism to handle any conflicts. Operations are idempotent - concurrent
 * upserts for the same fingerprint will either succeed or fail with a constraint violation that is safely ignored.
 * </p>
 */
@LazySingleton
public class SqlIndexingStateStorage implements IndexingStateStorage
{
  private static final EmittingLogger log = new EmittingLogger(SqlIndexingStateStorage.class);

  private final MetadataStorageTablesConfig dbTables;
  private final ObjectMapper jsonMapper;
  private final SQLMetadataConnector connector;

  @Inject
  public SqlIndexingStateStorage(
      @Nonnull MetadataStorageTablesConfig dbTables,
      @Nonnull ObjectMapper jsonMapper,
      @Nonnull SQLMetadataConnector connector
  )
  {
    this.dbTables = dbTables;
    this.jsonMapper = jsonMapper;
    this.connector = connector;
  }

  @Override
  public void upsertIndexingState(
      @NotEmpty final String dataSource,
      @NotEmpty final String fingerprint,
      @Nonnull final CompactionState compactionState,
      @Nonnull final DateTime updateTime
  )
  {
    // Strictly sanitize inputs to avoid writing junk data to the rdbms
    StringBuilder errors = new StringBuilder();
    if (dataSource == null || dataSource.isEmpty()) {
      errors.append("dataSource cannot be empty; ");
    }
    if (fingerprint == null || fingerprint.isEmpty()) {
      errors.append("fingerprint cannot be empty; ");
    }
    if (compactionState == null) {
      errors.append("compactionState cannot be null; ");
    }
    if (updateTime == null) {
      errors.append("updateTime cannot be null; ");
    }
    if (errors.length() > 0) {
      throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(errors.toString().trim());
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
                "Indexing state for fingerprint[%s] in dataSource[%s] already exists and is marked as used. Skipping update.",
                fingerprint,
                dataSource
            );
            break;

          case EXISTS_AND_UNUSED:
            // Fingerprint exists but is marked as unused - update the used flag
            log.info(
                "Found existing indexing state in DB for fingerprint[%s] in dataSource[%s]. Marking as used.",
                fingerprint,
                dataSource
            );
            String updateSql = StringUtils.format(
                "UPDATE %s SET used = :used, used_status_last_updated = :used_status_last_updated "
                + "WHERE fingerprint = :fingerprint",
                dbTables.getIndexingStatesTable()
            );
            handle.createStatement(updateSql)
                  .bind("used", true)
                  .bind("used_status_last_updated", now)
                  .bind("fingerprint", fingerprint)
                  .execute();

            log.info("Updated existing indexing state for datasource[%s].", dataSource);
            break;

          case DOES_NOT_EXIST:
            // Fingerprint doesn't exist - insert new state
            log.info("Inserting new indexing state for fingerprint[%s] in dataSource[%s].", fingerprint, dataSource);

            String insertSql = StringUtils.format(
                "INSERT INTO %s (created_date, dataSource, fingerprint, payload, used, pending, used_status_last_updated) "
                + "VALUES (:created_date, :dataSource, :fingerprint, :payload, :used, :pending, :used_status_last_updated)",
                dbTables.getIndexingStatesTable()
            );

            try {
              handle.createStatement(insertSql)
                    .bind("created_date", now)
                    .bind("dataSource", dataSource)
                    .bind("fingerprint", fingerprint)
                    .bind("payload", jsonMapper.writeValueAsBytes(compactionState))
                    .bind("used", true)
                    .bind("pending", true)
                    .bind("used_status_last_updated", now)
                    .execute();

              log.info(
                  "Published indexing state for fingerprint[%s] to DB for datasource[%s].",
                  fingerprint,
                  dataSource
              );
            }
            catch (JsonProcessingException e) {
              throw InternalServerError.exception(
                  e,
                  "Failed to serialize indexing state for fingerprint[%s]",
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
    catch (Throwable e) {
      if (connector.isUniqueConstraintViolation(e)) {
        // Swallow exception - another thread already persisted the same data
        log.info(
            "Fingerprints already exist for datasource[%s] (likely concurrent insert). "
            + "Treating as success since operation is idempotent.",
            dataSource
        );
      } else {
        // For other exceptions, let them propagate
        throw e;
      }
    }
  }

  @Override
  public int markUnreferencedIndexingStatesAsUnused()
  {
    return connector.retryWithHandle(
        handle ->
            handle.createStatement(
                      StringUtils.format(
                          "UPDATE %s SET used = false, used_status_last_updated = :now WHERE used = true AND pending = false "
                          + "AND fingerprint NOT IN (SELECT DISTINCT indexing_state_fingerprint FROM %s WHERE used = true AND indexing_state_fingerprint IS NOT NULL)",
                          dbTables.getIndexingStatesTable(),
                          dbTables.getSegmentsTable()
                      )
                  )
                  .bind("now", DateTimes.nowUtc().toString())
                  .execute());
  }

  @Override
  public List<String> findReferencedIndexingStateMarkedAsUnused()
  {
    return connector.retryWithHandle(
        handle ->
            handle.createQuery(
                      StringUtils.format(
                          "SELECT DISTINCT indexing_state_fingerprint FROM %s WHERE used = true AND indexing_state_fingerprint IN (SELECT fingerprint FROM %s WHERE used = false)",
                          dbTables.getSegmentsTable(),
                          dbTables.getIndexingStatesTable()
                      ))
                  .mapTo(String.class)
                  .list()
    );
  }

  @Override
  public int markIndexingStatesAsUsed(List<String> stateFingerprints)
  {
    if (stateFingerprints.isEmpty()) {
      return 0;
    }

    return connector.retryWithHandle(
        handle -> {
          Update statement = handle.createStatement(
              StringUtils.format(
                  "UPDATE %s SET used = true, pending = false, used_status_last_updated = :now"
                  + " WHERE fingerprint IN (%s)",
                  dbTables.getIndexingStatesTable(),
                  buildParameterizedInClause("fp", stateFingerprints.size())
              )
          ).bind("now", DateTimes.nowUtc().toString());

          bindValuesToInClause(stateFingerprints, "fp", statement);

          return statement.execute();
        }
    );
  }

  @Override
  public int markIndexingStatesAsActive(String stateFingerprint)
  {
    return connector.retryWithHandle(
        handle -> handle.createStatement(
                            StringUtils.format(
                                "UPDATE %s SET pending = false WHERE fingerprint = :fingerprint AND pending = true",
                                dbTables.getIndexingStatesTable()
                            ))
                        .bind("fingerprint", stateFingerprint)
                        .execute()
    );
  }

  @Override
  public int deleteUnusedIndexingStatesOlderThan(long timestamp)
  {
    return connector.retryWithHandle(
        handle -> handle.createStatement(
                            StringUtils.format(
                                "DELETE FROM %s WHERE used = false AND pending = false AND used_status_last_updated < :maxUpdateTime",
                                dbTables.getIndexingStatesTable()
                            ))
                        .bind("maxUpdateTime", DateTimes.utc(timestamp).toString())
                        .execute());
  }

  @Override
  public int deletePendingIndexingStatesOlderThan(long timestamp)
  {
    return connector.retryWithHandle(
        handle -> handle.createStatement(
                            StringUtils.format(
                                "DELETE FROM %s WHERE pending = true AND used_status_last_updated < :maxUpdateTime",
                                dbTables.getIndexingStatesTable()
                            ))
                        .bind("maxUpdateTime", DateTimes.utc(timestamp).toString())
                        .execute());
  }

  /**
   * Checks if the indexing state for the given fingerprint is pending.
   * <p>
   * Useful for testing purposes to verify the pending status of an indexing state.
   * </p>
   */
  @Nullable
  @VisibleForTesting
  public Boolean isIndexingStatePending(final String fingerprint)
  {
    return connector.retryWithHandle(
        handle -> {
          String sql = StringUtils.format(
              "SELECT pending FROM %s WHERE fingerprint = :fingerprint",
              dbTables.getIndexingStatesTable()
          );

          return handle.createQuery(sql)
                       .bind("fingerprint", fingerprint)
                       .mapTo(Boolean.class)
                       .first();
        }
    );
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
        dbTables.getIndexingStatesTable()
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
}
