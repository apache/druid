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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.metamx.common.lifecycle.Lifecycle;

/**
 */
public class DatabaseRuleManagerProvider implements Provider<DatabaseRuleManager>
{
  private final ObjectMapper jsonMapper;
  private final Supplier<DatabaseRuleManagerConfig> config;
  private final Supplier<DbTablesConfig> dbTables;
  private final DbConnector dbConnector;
  private final Lifecycle lifecycle;

  @Inject
  public DatabaseRuleManagerProvider(
      ObjectMapper jsonMapper,
      Supplier<DatabaseRuleManagerConfig> config,
      Supplier<DbTablesConfig> dbTables,
      DbConnector dbConnector,
      Lifecycle lifecycle
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbTables = dbTables;
    this.dbConnector = dbConnector;
    this.lifecycle = lifecycle;
  }

  @Override
  public DatabaseRuleManager get()
  {
    try {
      lifecycle.addMaybeStartHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start() throws Exception
            {
              dbConnector.createRulesTable();
              DatabaseRuleManager.createDefaultRule(
                  dbConnector.getDBI(), dbTables.get().getRulesTable(), config.get().getDefaultRule(), jsonMapper
              );
            }

            @Override
            public void stop()
            {

            }
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return new DatabaseRuleManager(jsonMapper, config, dbTables, dbConnector.getDBI());
  }
}
