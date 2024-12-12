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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.client.DruidServer;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
@ManageLifecycle
public class SQLMetadataRuleManager implements MetadataRuleManager
{

  public static void createDefaultRule(
      final IDBI dbi,
      final String ruleTable,
      final String defaultDatasourceName,
      final ObjectMapper jsonMapper
  )
  {
    try {
      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              List<Map<String, Object>> existing = handle
                  .createQuery(
                      StringUtils.format(
                          "SELECT id from %s where datasource=:dataSource",
                          ruleTable
                      )
                  )
                  .bind("dataSource", defaultDatasourceName)
                  .list();

              if (!existing.isEmpty()) {
                return null;
              }

              final List<Rule> defaultRules = Collections.singletonList(
                  new ForeverLoadRule(
                      ImmutableMap.of(
                          DruidServer.DEFAULT_TIER,
                          DruidServer.DEFAULT_NUM_REPLICANTS
                      ),
                      null
                  )
              );
              final String version = DateTimes.nowUtc().toString();
              handle.createStatement(
                  StringUtils.format(
                      "INSERT INTO %s (id, dataSource, version, payload) VALUES (:id, :dataSource, :version, :payload)",
                      ruleTable
                  )
              )
                    .bind("id", StringUtils.format("%s_%s", defaultDatasourceName, version))
                    .bind("dataSource", defaultDatasourceName)
                    .bind("version", version)
                    .bind("payload", jsonMapper.writeValueAsBytes(defaultRules))
                    .execute();

              return null;
            }
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final EmittingLogger log = new EmittingLogger(SQLMetadataRuleManager.class);

  private final ObjectMapper jsonMapper;
  private final MetadataRuleManagerConfig config;
  private final MetadataStorageTablesConfig dbTables;
  private final IDBI dbi;
  private final AtomicReference<ImmutableMap<String, List<Rule>>> rules;
  private final AuditManager auditManager;

  private final Object lock = new Object();
  /** The number of times this SQLMetadataRuleManager was started. */
  private long startCount = 0;
  /**
   * Equal to the current {@link #startCount} value, if the SQLMetadataRuleManager is currently started; -1 if
   * currently stopped.
   *
   * This field is used to implement a simple stamp mechanism instead of just a boolean "started" flag to prevent
   * the theoretical situation of two tasks scheduled in {@link #start()} calling {@link #poll()} concurrently, if
   * the sequence of {@link #start()} - {@link #stop()} - {@link #start()} actions occurs quickly.
   *
   * {@link SqlSegmentsMetadataManager} also have a similar issue.
   */
  private long currentStartOrder = -1;
  private ScheduledExecutorService exec = null;
  private long failStartTimeMs = 0;

  @Inject
  public SQLMetadataRuleManager(
      @Json ObjectMapper jsonMapper,
      MetadataRuleManagerConfig config,
      MetadataStorageTablesConfig dbTables,
      SQLMetadataConnector connector,
      AuditManager auditManager
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbTables = dbTables;
    this.dbi = connector.getDBI();
    this.auditManager = auditManager;

    // Verify configured Periods can be treated as Durations (fail-fast before they're needed).
    Preconditions.checkNotNull(config.getAlertThreshold().toStandardDuration());
    Preconditions.checkNotNull(config.getPollDuration().toStandardDuration());

    String defaultRule = config.getDefaultRule();
    Preconditions.checkState(
        defaultRule != null && !defaultRule.isEmpty(),
        "If specified, 'druid.manager.rules.defaultRule' must have a non-empty value."
    );

    this.rules = new AtomicReference<>(ImmutableMap.of());
  }

  @Override
  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (currentStartOrder >= 0) {
        return;
      }

      startCount++;
      currentStartOrder = startCount;
      long localStartedOrder = currentStartOrder;

      exec = Execs.scheduledSingleThreaded("DatabaseRuleManager-Exec--%d");

      createDefaultRule(dbi, getRulesTable(), config.getDefaultRule(), jsonMapper);
      exec.scheduleWithFixedDelay(
          () -> {
            try {
              // Do not poll if already stopped
              // See https://github.com/apache/druid/issues/6028
              synchronized (lock) {
                if (localStartedOrder == currentStartOrder) {
                  poll();
                }
              }
            }
            catch (Exception e) {
              log.error(e, "uncaught exception in rule manager polling thread");
            }
          },
          0,
          config.getPollDuration().toStandardDuration().getMillis(),
          TimeUnit.MILLISECONDS
      );
    }
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (currentStartOrder == -1) {
        return;
      }
      rules.set(ImmutableMap.of());
      currentStartOrder = -1;
      // This call cancels the periodic poll() task, scheduled in start().
      exec.shutdownNow();
      exec = null;
    }
  }

  @Override
  public void poll()
  {
    try {
    
      Map<String, List<Rule>> newRulesMap =
          dbi.withHandle(
              handle -> handle.createQuery(
                  // Return latest version rule by dataSource
                  StringUtils.format(
                      "SELECT r.dataSource, r.payload "
                      + "FROM %1$s r "
                      + "INNER JOIN(SELECT dataSource, max(version) as version FROM %1$s GROUP BY dataSource) ds "
                      + "ON r.datasource = ds.datasource and r.version = ds.version",
                      getRulesTable()
                  )
              ).map(
                  (index, r, ctx) -> {
                    try {
                      return Pair.of(
                          r.getString("dataSource"),
                          jsonMapper.readValue(r.getBytes("payload"), new TypeReference<List<Rule>>() {})
                      );
                    }
                    catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  }
              ).fold(
                  new HashMap<>(),
                  (retVal, stringObjectMap, foldController, statementContext) -> {
                    try {
                      String dataSource = stringObjectMap.lhs;
                      retVal.put(dataSource, stringObjectMap.rhs);
                      return retVal;
                    }
                    catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  }
              )
      );

      ImmutableMap<String, List<Rule>> newRules = ImmutableMap.copyOf(newRulesMap);

      final int newRuleCount = newRules.values().stream().mapToInt(List::size).sum();
      log.info("Polled and found [%d] rule(s) for [%d] datasource(s).", newRuleCount, newRules.size());

      rules.set(newRules);
      failStartTimeMs = 0;
    }
    catch (Exception e) {
      if (failStartTimeMs == 0) {
        failStartTimeMs = System.currentTimeMillis();
      }

      final long alertPeriodMillis = config.getAlertThreshold().toStandardDuration().getMillis();
      if (System.currentTimeMillis() - failStartTimeMs > alertPeriodMillis) {
        log.makeAlert(e, "Exception while polling for rules").emit();
        failStartTimeMs = 0;
      } else {
        log.error(e, "Exception while polling for rules");
      }
    }
  }

  @Override
  public Map<String, List<Rule>> getAllRules()
  {
    return rules.get();
  }

  @Override
  public List<Rule> getRules(final String dataSource)
  {
    List<Rule> retVal = rules.get().get(dataSource);
    return retVal == null ? new ArrayList<>() : retVal;
  }

  @Override
  public List<Rule> getRulesWithDefault(final String dataSource)
  {
    List<Rule> retVal = new ArrayList<>();
    Map<String, List<Rule>> theRules = rules.get();
    if (theRules.get(dataSource) != null) {
      retVal.addAll(theRules.get(dataSource));
    }
    if (theRules.get(config.getDefaultRule()) != null) {
      retVal.addAll(theRules.get(config.getDefaultRule()));
    }
    return retVal;
  }

  @Override
  public boolean overrideRule(final String dataSource, final List<Rule> newRules, final AuditInfo auditInfo)
  {
    if (newRules == null) {
      throw new IAE("Rules cannot be null.");
    } else if (newRules.isEmpty() && config.getDefaultRule().equals(dataSource)) {
      throw new IAE("Cluster-level rules cannot be empty.");
    }

    final String ruleString;
    try {
      ruleString = jsonMapper.writeValueAsString(newRules);
      log.info("Updating datasource[%s] with rules[%s] as per [%s]", dataSource, ruleString, auditInfo);
    }
    catch (JsonProcessingException e) {
      log.error(e, "Unable to write rules as string for datasource[%s]", dataSource);
      return false;
    }
    synchronized (lock) {
      try {
        dbi.inTransaction(
            (handle, transactionStatus) -> {
              final DateTime auditTime = DateTimes.nowUtc();
              auditManager.doAudit(
                  AuditEntry.builder()
                            .key(dataSource)
                            .type("rules")
                            .auditInfo(auditInfo)
                            .serializedPayload(ruleString)
                            .auditTime(auditTime)
                            .build(),
                  handle
              );
              // Note that the method removeRulesForEmptyDatasourcesOlderThan
              // depends on the version field to be a timestamp
              String version = auditTime.toString();
              handle.createStatement(
                  StringUtils.format(
                      "INSERT INTO %s (id, dataSource, version, payload)"
                      + " VALUES (:id, :dataSource, :version, :payload)",
                      getRulesTable()
                  )
              )
                    .bind("id", StringUtils.format("%s_%s", dataSource, version))
                    .bind("dataSource", dataSource)
                    .bind("version", version)
                    .bind("payload", jsonMapper.writeValueAsBytes(newRules))
                    .execute();

              return null;
            }
        );
      }
      catch (Exception e) {
        log.error(e, "Exception while overriding rule for %s", dataSource);
        return false;
      }
    }
    try {
      poll();
    }
    catch (Exception e) {
      log.error(e, "Exception while polling for rules after overriding the rule for %s", dataSource);
    }
    return true;
  }

  @Override
  public int removeRulesForEmptyDatasourcesOlderThan(long timestamp)
  {
    // Note that this DELETE SQL depends on the version field to be a timestamp. Hence, this
    // method depends on overrideRule method to set version to timestamp when the rule entry is created
    DateTime dateTime = DateTimes.utc(timestamp);
    synchronized (lock) {
      return dbi.withHandle(
          handle -> {
            Update sql = handle.createStatement(
                // Note that this query could be expensive when the segments table is large
                // However, since currently this query is run very infrequent (by default once a day by the KillRules Coordinator duty)
                // and the inner query on segment table is a READ (no locking), it is keep this way.
                StringUtils.format(
                    "DELETE FROM %1$s WHERE datasource NOT IN (SELECT DISTINCT datasource from %2$s)"
                    + " and datasource!=:default_rule and version < :date_time",
                    getRulesTable(),
                    getSegmentsTable()
                )
            );
            return sql.bind("default_rule", config.getDefaultRule())
                      .bind("date_time", dateTime.toString())
                      .execute();
          }
      );
    }
  }

  private String getRulesTable()
  {
    return dbTables.getRulesTable();
  }

  private String getSegmentsTable()
  {
    return dbTables.getSegmentsTable();
  }
}
