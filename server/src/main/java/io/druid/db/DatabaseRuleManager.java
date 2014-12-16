/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.db;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.MapUtils;
import com.metamx.common.Pair;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.client.DruidServer;
import io.druid.concurrent.Execs;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;
import io.druid.server.coordinator.rules.ForeverLoadRule;
import io.druid.server.coordinator.rules.Rule;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
@ManageLifecycle
public class DatabaseRuleManager
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
                          "SELECT id from %s where datasource=:dataSource;",
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

  private static final Logger log = new Logger(DatabaseRuleManager.class);

  private final ObjectMapper jsonMapper;
  private final Supplier<DatabaseRuleManagerConfig> config;
  private final Supplier<DbTablesConfig> dbTables;
  private final IDBI dbi;
  private final AtomicReference<ImmutableMap<String, List<Rule>>> rules;

  private volatile ScheduledExecutorService exec;

  private final Object lock = new Object();

  private volatile boolean started = false;

  @Inject
  public DatabaseRuleManager(
      @Json ObjectMapper jsonMapper,
      Supplier<DatabaseRuleManagerConfig> config,
      Supplier<DbTablesConfig> dbTables,
      IDBI dbi
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbTables = dbTables;
    this.dbi = dbi;

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

      this.exec = Execs.scheduledSingleThreaded("DatabaseRuleManager-Exec--%d");

      createDefaultRule(dbi, getRulesTable(), config.get().getDefaultRule(), jsonMapper);
      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(0),
          config.get().getPollDuration().toStandardDuration(),
          new Runnable()
          {
            @Override
            public void run()
            {
              poll();
            }
          }
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
                  )
                      .map(
                          new ResultSetMapper<Pair<String, byte[]>>()
                          {
                            @Override
                            public Pair<String, byte[]> map(int index, ResultSet r, StatementContext ctx)
                                throws SQLException
                            {
                              return Pair.of(
                                  r.getString("dataSource"),
                                  r.getBytes("payload")
                              );
                            }
                          }
                      )
                               .fold(
                                   Maps.<String, List<Rule>>newHashMap(),
                                   new Folder3<Map<String, List<Rule>>, Pair<String, byte[]>>()
                                   {
                                     @Override
                                     public Map<String, List<Rule>> fold(
                                         Map<String, List<Rule>> retVal,
                                         Pair<String, byte[]> dataSourcePayload,
                                         FoldController foldController,
                                         StatementContext statementContext
                                     ) throws SQLException
                                     {

                                       try {
                                         String dataSource = dataSourcePayload.lhs;
                                         if(dataSource == null) {
                                           throw new IAE("dataSource cannot be null");
                                         }
                                         List<Rule> rules = jsonMapper.readValue(
                                             dataSourcePayload.rhs,
                                             new TypeReference<List<Rule>>() {}
                                         );
                                         retVal.put(dataSource, rules);
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
    }
    catch (Exception e) {
      log.error(e, "Exception while polling for rules");
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

  public boolean overrideRule(final String dataSource, final List<Rule> newRules)
  {
    synchronized (lock) {
      try {
        dbi.withHandle(
            new HandleCallback<Void>()
            {
              @Override
              public Void withHandle(Handle handle) throws Exception
              {
                final String version = new DateTime().toString();
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

    return true;
  }

  private String getRulesTable() {return dbTables.get().getRulesTable();}
}
