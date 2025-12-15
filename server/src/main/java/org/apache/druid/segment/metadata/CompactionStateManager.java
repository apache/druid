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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Striped;
import com.google.inject.Inject;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.guice.ManageLifecycle;
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
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;

/**
 * Handles compaction state persistence on the Coordinator.
 */
@ManageLifecycle
public class CompactionStateManager
{
  private static final EmittingLogger log = new EmittingLogger(CompactionStateManager.class);
  private static final int DB_ACTION_PARTITION_SIZE = 100;
  private static final int DEFAULT_PREWARM_SIZE = 100;

  private final MetadataStorageTablesConfig dbTables;
  private final ObjectMapper jsonMapper;
  private final SQLMetadataConnector connector;
  private final CompactionStateManagerConfig config;
  private final Cache<String, CompactionState> fingerprintCache;
  private final Striped<Lock> datasourceLocks = Striped.lock(128);

  @Inject
  public CompactionStateManager(
      @Nonnull MetadataStorageTablesConfig dbTables,
      @Nonnull ObjectMapper jsonMapper,
      @Nonnull SQLMetadataConnector connector,
      @Nonnull CompactionStateManagerConfig config
  )
  {
    this.dbTables = dbTables;
    this.jsonMapper = jsonMapper;
    this.connector = connector;
    this.config = config;

    this.fingerprintCache = CacheBuilder.newBuilder()
                                        .maximumSize(config.getCacheSize())
                                        .build();
  }

  @LifecycleStart
  public void start()
  {
    // This is defensive. Since the new table is created during startup after upgrade, we need to defend against
    // the table not existing yet. If that is the case we do not pre-warm the cache.
    try {
      boolean tableExists = connector.retryWithHandle(
          handle -> connector.tableExists(handle, dbTables.getCompactionStatesTable())
      );
      if (tableExists) {
        log.info("Pre-warming compaction state cache");
        prewarmCache(DEFAULT_PREWARM_SIZE);
      } else {
        log.info("Compaction states table does not exist, skipping pre-warm");
      }
    }
    catch (Exception e) {
      log.warn(e, "Failed to prewarm cache, will load lazily");
    }
  }

  @LifecycleStop
  public void stop()
  {
    fingerprintCache.invalidateAll();
  }

  @VisibleForTesting
  CompactionStateManager()
  {
    this.dbTables = null;
    this.jsonMapper = null;
    this.connector = null;
    this.config = null;
    this.fingerprintCache = null;
  }

  /**
   * Persist unique compaction state fingerprints in the DB.
   * <p>
   * This method uses per-datasource locking to prevent concurrent insert race conditions
   * when multiple threads attempt to persist the same fingerprints simultaneously.
   */
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
        warmCache(fingerprintToStateMap);
        return null;
      });
    }
    finally {
      lock.unlock();
    }
  }

  /**
   * Marks compaction states as unused if they are not referenced by any used segments.
   *
   * @return Number of rows updated
   */
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

  /**
   * Finds all compaction state fingerprints which have been marked as unused but are
   * still referenced by some used segments.
   *
   * @return Empty list if no such fingerprint exists
   */
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

  /**
   * Gets a compaction state by fingerprint, checking cache first.
   */
  @Nullable
  public CompactionState getCompactionStateByFingerprint(String fingerprint)
  {
    try {
      return fingerprintCache.get(
          fingerprint,
          () -> {
            CompactionState fromDb = loadCompactionStateFromDatabase(fingerprint);
            if (fromDb == null) {
              throw new CacheLoader.InvalidCacheLoadException("Fingerprint not found"); // Guava won't cache nulls
            }
            return fromDb;
          }
      );
    }
    catch (ExecutionException | CacheLoader.InvalidCacheLoadException e) {
      return null;
    }
  }

  /**
   * Warms cache with specific states (after persisting).
   */
  private void warmCache(Map<String, CompactionState> fingerprintToStateMap)
  {
    fingerprintCache.putAll(fingerprintToStateMap);
    log.debug("Warmed cache with [%d] compaction states", fingerprintToStateMap.size());
  }

  /**
   * Pre-warms the cache by loading the N most recently used fingerprints.
   */
  private void prewarmCache(int limit)
  {
    final long startTime = System.currentTimeMillis();
    log.info("Pre-warming compaction state cache with up to [%d] most recent fingerprints", limit);

    final Map<String, CompactionState> recentStates = connector.retryWithHandle(
        handle -> {
          final String sql = StringUtils.format(
              "SELECT fingerprint, payload FROM %s "
              + "WHERE used = true "
              + "ORDER BY used_status_last_updated DESC "
              + "%s",
              dbTables.getCompactionStatesTable(),
              connector.limitClause(limit)
          );

          final Map<String, CompactionState> states = new HashMap<>();
          handle.createQuery(sql)
                .map((index, r, ctx) -> {
                  String fingerprint = r.getString("fingerprint");
                  byte[] payload = r.getBytes("payload");

                  try {
                    CompactionState state = jsonMapper.readValue(payload, CompactionState.class);
                    states.put(fingerprint, state);
                  }
                  catch (IOException e) {
                    log.warn(e, "Failed to deserialize compaction state for fingerprint[%s], skipping", fingerprint);
                  }
                  return null;
                })
                .list();

          return states;
        }
    );

    // Populate cache
    fingerprintCache.putAll(recentStates);

    final long duration = System.currentTimeMillis() - startTime;
    log.info(
        "Pre-warmed cache with [%d] compaction states in [%d]ms",
        recentStates.size(),
        duration
    );

  }

  /**
   * Invalidates a fingerprint from cache.
   */
  public void invalidateFingerprint(String fingerprint)
  {
    fingerprintCache.invalidate(fingerprint);
  }

  /**
   * Loads from database. Returns null if not found or unused.
   */
  @Nullable
  private CompactionState loadCompactionStateFromDatabase(String fingerprint)
  {
    return connector.retryWithHandle(
        handle -> {
          List<byte[]> results = handle.createQuery(
                                           StringUtils.format(
                                               "SELECT payload FROM %s WHERE fingerprint = :fingerprint AND used = true",
                                               dbTables.getCompactionStatesTable()
                                           ))
                                       .bind("fingerprint", fingerprint)
                                       .map((index, r, ctx) -> r.getBytes("payload"))
                                       .list();

          if (results.isEmpty()) {
            return null;
          }

          try {
            return jsonMapper.readValue(results.get(0), CompactionState.class);
          }
          catch (IOException e) {
            log.error(e, "Failed to deserialize compaction state for fingerprint[%s]", fingerprint);
            return null;
          }
        }
    );
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

  @VisibleForTesting
  protected boolean isCached(String fingerprint)
  {
    return fingerprintCache.getIfPresent(fingerprint) != null;
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
