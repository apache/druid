
/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.audit.AuditEntry;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditManager;
import io.druid.client.DruidServer;
import io.druid.concurrent.Execs;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.coordinator.rules.ForeverLoadRule;
import io.druid.server.coordinator.rules.Rule;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
                      String.format(
                          "SELECT id from %s where datasource=:dataSource",
                          ruleTable
                      )
                  )
                  .bind("dataSource", defaultDatasourceName)
                  .list();

              if (!existing.isEmpty()) {
                return null;
              }

              final List<Rule> defaultRules = Arrays.<Rule>asList(
                  new ForeverLoadRule(
                      ImmutableMap.<String, Integer>of(
                          DruidServer.DEFAULT_TIER,
                          DruidServer.DEFAULT_NUM_REPLICANTS
                      )
                  )
              );
              final String version = new DateTime().toString();
              handle.createStatement(
                  String.format(
                      "INSERT INTO %s (id, dataSource, version, payload) VALUES (:id, :dataSource, :version, :payload)",
                      ruleTable
                  )
              )
                    .bind("id", String.format("%s_%s", defaultDatasourceName, version))
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
      throw Throwables.propagate(e);
    }
  }

  private static final EmittingLogger log = new EmittingLogger(SQLMetadataRuleManager.class);

  private final ObjectMapper jsonMapper;
  private final Supplier<MetadataRuleManagerConfig> config;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final IDBI dbi;
  private final AtomicReference<ImmutableMap<String, List<Rule>>> rules;
  private final AuditManager auditManager;

  private final Object lock = new Object();

  private volatile boolean started = false;

  private volatile ListeningScheduledExecutorService exec = null;
  private volatile ListenableFuture<?> future = null;

  private volatile long retryStartTime = 0;

  @Inject
  public SQLMetadataRuleManager(
      @Json ObjectMapper jsonMapper,
      Supplier<MetadataRuleManagerConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      SQLMetadataConnector connector,
      AuditManager auditManager
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbTables = dbTables;
    this.dbi = connector.getDBI();
    this.auditManager = auditManager;

    this.rules = new AtomicReference<>(
        ImmutableMap.<String, List<Rule>>of()
    );
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      exec = MoreExecutors.listeningDecorator(Execs.scheduledSingleThreaded("DatabaseRuleManager-Exec--%d"));

      createDefaultRule(dbi, getRulesTable(), config.get().getDefaultRule(), jsonMapper);
      future = exec.scheduleWithFixedDelay(
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                poll();
              }
              catch (Exception e) {
                log.error(e, "uncaught exception in rule manager polling thread");
              }
            }
          },
          0,
          config.get().getPollDuration().toStandardDuration().getMillis(),
          TimeUnit.MILLISECONDS
      );

      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      rules.set(ImmutableMap.<String, List<Rule>>of());

      future.cancel(false);
      future = null;
      started = false;
      exec.shutdownNow();
      exec = null;
    }
  }

  public void poll()
  {
    try {
      ImmutableMap<String, List<Rule>> newRules = ImmutableMap.copyOf(
          dbi.withHandle(
              new HandleCallback<Map<String, List<Rule>>>()
              {
                @Override
                public Map<String, List<Rule>> withHandle(Handle handle) throws Exception
                {
                  return handle.createQuery(
                      // Return latest version rule by dataSource
                      String.format(
                          "SELECT r.dataSource, r.payload "
                          + "FROM %1$s r "
                          + "INNER JOIN(SELECT dataSource, max(version) as version FROM %1$s GROUP BY dataSource) ds "
                          + "ON r.datasource = ds.datasource and r.version = ds.version",
                          getRulesTable()
                      )
                  ).map(
                      new ResultSetMapper<Pair<String, List<Rule>>>()
                      {
                        @Override
                        public Pair<String, List<Rule>> map(int index, ResultSet r, StatementContext ctx)
                            throws SQLException
                        {
                          try {
                            return Pair.of(
                                r.getString("dataSource"),
                                jsonMapper.<List<Rule>>readValue(
                                    r.getBytes("payload"), new TypeReference<List<Rule>>()
                                    {
                                    }
                                )
                            );
                          }
                          catch (IOException e) {
                            throw Throwables.propagate(e);
                          }
                        }
                      }
                  )
                               .fold(
                                   Maps.<String, List<Rule>>newHashMap(),
                                   new Folder3<Map<String, List<Rule>>, Pair<String, List<Rule>>>()
                                   {
                                     @Override
                                     public Map<String, List<Rule>> fold(
                                         Map<String, List<Rule>> retVal,
                                         Pair<String, List<Rule>> stringObjectMap,
                                         FoldController foldController,
                                         StatementContext statementContext
                                     ) throws SQLException
                                     {
                                       try {
                                         String dataSource = stringObjectMap.lhs;
                                         retVal.put(dataSource, stringObjectMap.rhs);
                                         return retVal;
                                       }
                                       catch (Exception e) {
                                         throw Throwables.propagate(e);
                                       }
                                     }
                                   }
                               );
                }
              }
          )
      );

      log.info("Polled and found rules for %,d datasource(s)", newRules.size());

      rules.set(newRules);
      retryStartTime = 0;
    }
    catch (Exception e) {
      if (retryStartTime == 0) {
        retryStartTime = System.currentTimeMillis();
      }

      if (System.currentTimeMillis() - retryStartTime > config.get().getAlertThreshold().getMillis()) {
        log.makeAlert(e, "Exception while polling for rules")
           .emit();
        retryStartTime = 0;
      } else {
        log.error(e, "Exception while polling for rules");
      }
    }
  }

  public Map<String, List<Rule>> getAllRules()
  {
    return rules.get();
  }

  public List<Rule> getRules(final String dataSource)
  {
    List<Rule> retVal = rules.get().get(dataSource);
    return retVal == null ? Lists.<Rule>newArrayList() : retVal;
  }

  public List<Rule> getRulesWithDefault(final String dataSource)
  {
    List<Rule> retVal = Lists.newArrayList();
    Map<String, List<Rule>> theRules = rules.get();
    if (theRules.get(dataSource) != null) {
      retVal.addAll(theRules.get(dataSource));
    }
    if (theRules.get(config.get().getDefaultRule()) != null) {
      retVal.addAll(theRules.get(config.get().getDefaultRule()));
    }
    return retVal;
  }

  public boolean overrideRule(final String dataSource, final List<Rule> newRules, final AuditInfo auditInfo)
  {
    final String ruleString;
    try {
      ruleString = jsonMapper.writeValueAsString(newRules);
      log.info("Updating [%s] with rules [%s] as per [%s]", dataSource, ruleString, auditInfo);
    }
    catch (JsonProcessingException e) {
      log.error(e, "Unable to write rules as string for [%s]", dataSource);
      return false;
    }
    synchronized (lock) {
      try {
        dbi.inTransaction(
            new TransactionCallback<Void>()
            {
              @Override
              public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
              {
                final DateTime auditTime = DateTime.now();
                auditManager.doAudit(
                    AuditEntry.builder()
                              .key(dataSource)
                              .type("rules")
                              .auditInfo(auditInfo)
                              .payload(ruleString)
                              .auditTime(auditTime)
                              .build(),
                    handle
                );
                String version = auditTime.toString();
                handle.createStatement(
                    String.format(
                        "INSERT INTO %s (id, dataSource, version, payload) VALUES (:id, :dataSource, :version, :payload)",
                        getRulesTable()
                    )
                )
                      .bind("id", String.format("%s_%s", dataSource, version))
                      .bind("dataSource", dataSource)
                      .bind("version", version)
                      .bind("payload", jsonMapper.writeValueAsBytes(newRules))
                      .execute();

                return null;
              }
            }
        );
      }
      catch (Exception e) {
        log.error(e, String.format("Exception while overriding rule for %s", dataSource));
        return false;
      }
    }
    try {
      poll();
    }
    catch (Exception e) {
      log.error(e, String.format("Exception while polling for rules after overriding the rule for %s", dataSource));
    }
    return true;
  }

  private String getRulesTable()
  {
    return dbTables.get().getRulesTable();
  }
}
