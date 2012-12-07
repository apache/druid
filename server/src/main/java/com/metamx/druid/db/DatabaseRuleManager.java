/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.db;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.MapUtils;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.master.rules.PeriodLoadRule;
import com.metamx.druid.master.rules.Rule;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class DatabaseRuleManager
{
  public static void createDefaultRule(
      final DBI dbi,
      final String ruleTable,
      final String defaultDatasource,
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
              List<Map<String, Object>> existing = handle.select(
                  String.format(
                      "SELECT id from %s where datasource='%s';",
                      ruleTable,
                      defaultDatasource
                  )
              );

              if (!existing.isEmpty()) {
                return null;
              }

              final List<Rule> defaultRules = Arrays.<Rule>asList(
                  new PeriodLoadRule(
                      new Period("P5000Y"),
                      2,
                      "_default_tier"
                  )
              );
              final String version = new DateTime().toString();
              handle.createStatement(
                  String.format(
                      "INSERT INTO %s (id, dataSource, version, payload) VALUES (:id, :dataSource, :version, :payload)",
                      ruleTable
                  )
              )
                    .bind("id", String.format("%s_%s", defaultDatasource, version))
                    .bind("dataSource", defaultDatasource)
                    .bind("version", version)
                    .bind("payload", jsonMapper.writeValueAsString(defaultRules))
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
  private final ScheduledExecutorService exec;
  private final DatabaseRuleManagerConfig config;
  private final DBI dbi;
  private final AtomicReference<ConcurrentHashMap<String, List<Rule>>> rules;

  private final Object lock = new Object();

  private volatile boolean started = false;

  public DatabaseRuleManager(
      ObjectMapper jsonMapper,
      ScheduledExecutorService exec,
      DatabaseRuleManagerConfig config,
      DBI dbi
  )
  {
    this.jsonMapper = jsonMapper;
    this.exec = exec;
    this.config = config;
    this.dbi = dbi;

    this.rules = new AtomicReference<ConcurrentHashMap<String, List<Rule>>>(
        new ConcurrentHashMap<String, List<Rule>>()
    );
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(0),
          config.getRulesPollDuration(),
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

      rules.set(new ConcurrentHashMap<String, List<Rule>>());

      started = false;
    }
  }

  public void poll()
  {
    try {
      ConcurrentHashMap<String, List<Rule>> newRules = new ConcurrentHashMap<String, List<Rule>>(
          dbi.withHandle(
              new HandleCallback<Map<String, List<Rule>>>()
              {
                @Override
                public Map<String, List<Rule>> withHandle(Handle handle) throws Exception
                {
                  return handle.createQuery(
                      String.format("SELECT dataSource, payload FROM %s", config.getRuleTable())
                  ).fold(
                      Maps.<String, List<Rule>>newHashMap(),
                      new Folder3<Map<String, List<Rule>>, Map<String, Object>>()
                      {
                        @Override
                        public Map<String, List<Rule>> fold(
                            Map<String, List<Rule>> retVal,
                            Map<String, Object> stringObjectMap,
                            FoldController foldController,
                            StatementContext statementContext
                        ) throws SQLException
                        {

                          try {
                            String dataSource = MapUtils.getString(stringObjectMap, "dataSource");
                            List<Rule> rules = jsonMapper.readValue(
                                MapUtils.getString(stringObjectMap, "payload"), new TypeReference<List<Rule>>()
                            {
                            }
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
    return rules.get().get(dataSource);
  }

  public List<Rule> getRulesWithDefault(final String dataSource)
  {
    List<Rule> retVal = Lists.newArrayList();
    Map<String, List<Rule>> theRules = rules.get();
    if (theRules.get(dataSource) != null) {
      retVal.addAll(theRules.get(dataSource));
    }
    if (theRules.get(config.getDefaultDatasource()) != null) {
      retVal.addAll(theRules.get(config.getDefaultDatasource()));
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
                        config.getRuleTable()
                    )
                )
                      .bind("id", String.format("%s_%s", dataSource, version))
                      .bind("dataSource", dataSource)
                      .bind("version", version)
                      .bind("payload", jsonMapper.writeValueAsString(newRules))
                      .execute();

                return null;
              }
            }
        );

        ConcurrentHashMap<String, List<Rule>> existingRules = rules.get();
        if (existingRules == null) {
          existingRules = new ConcurrentHashMap<String, List<Rule>>();
        }
        existingRules.put(dataSource, newRules);
      }
      catch (Exception e) {
        log.error(e, String.format("Exception while overriding rule for %s", dataSource));
        return false;
      }
    }

    return true;
  }
}
