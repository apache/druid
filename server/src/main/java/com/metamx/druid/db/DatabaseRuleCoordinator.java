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
import com.metamx.common.logger.Logger;
import com.metamx.druid.master.rules.Rule;
import com.metamx.druid.master.rules.RuleMap;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 */
public class DatabaseRuleCoordinator
{
  private static final Logger log = new Logger(DatabaseRuleCoordinator.class);

  private final ObjectMapper jsonMapper;
  private final DatabaseRuleCoordinatorConfig config;
  private final DBI dbi;

  private final Object lock = new Object();

  public DatabaseRuleCoordinator(ObjectMapper jsonMapper, DatabaseRuleCoordinatorConfig config, DBI dbi)
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbi = dbi;
  }

  public RuleMap getRuleMap()
  {
    Map<String, List<Rule>> assignmentRules = getAllRules();
    return new RuleMap(
        assignmentRules,
        assignmentRules.get(config.getDefaultDatasource())
    );
  }

  public Map<String, List<Rule>> getAllRules()
  {
    synchronized (lock) {
      return dbi.withHandle(
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
      );
    }
  }

  public List<Rule> getRules(final String dataSource)
  {
    synchronized (lock) {
      return dbi.withHandle(
          new HandleCallback<List<Rule>>()
          {
            @Override
            public List<Rule> withHandle(Handle handle) throws Exception
            {
              return handle.createQuery(
                  String.format("SELECT payload FROM %s WHERE dataSource = :dataSource", config.getRuleTable())
              )
                           .bind("dataSource", dataSource)
                           .fold(
                               Lists.<Rule>newArrayList(),
                               new Folder3<ArrayList<Rule>, Map<String, Object>>()
                               {
                                 @Override
                                 public ArrayList<Rule> fold(
                                     ArrayList<Rule> rules,
                                     Map<String, Object> stringObjectMap,
                                     FoldController foldController,
                                     StatementContext statementContext
                                 ) throws SQLException
                                 {
                                   try {
                                     return jsonMapper.readValue(
                                         MapUtils.getString(stringObjectMap, "payload"), new TypeReference<List<Rule>>()
                                     {
                                     }
                                     );
                                   }
                                   catch (Exception e) {
                                     throw Throwables.propagate(e);
                                   }
                                 }
                               }
                           );
            }
          }
      );
    }
  }

  public boolean overrideRule(final String dataSource, final List<Rule> rules)
  {
    synchronized (lock) {
      try {
        dbi.withHandle(
            new HandleCallback<Void>()
            {
              @Override
              public Void withHandle(Handle handle) throws Exception
              {
                handle.createStatement(
                    String.format(
                        "INSERT INTO %s (id, dataSource, ruleVersion, payload) VALUES (:id, :dataSource, :ruleVersion, :payload)",
                        config.getRuleTable()
                    )
                )
                      .bind("id", String.format("%s_%s", dataSource, config.getRuleVersion()))
                      .bind("dataSource", dataSource)
                      .bind("ruleVersion", config.getRuleVersion())
                      .bind("payload", jsonMapper.writeValueAsString(rules))
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
}
