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

package com.metamx.druid.config;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.db.DbTablesConfig;

/**
 */
public class ConfigManagerProvider implements Provider<ConfigManager>
{
  private final DbConnector dbConnector;
  private final Supplier<DbTablesConfig> dbTables;
  private final Supplier<ConfigManagerConfig> config;
  private final Lifecycle lifecycle;

  @Inject
  ConfigManagerProvider(
      DbConnector dbConnector,
      Supplier<DbTablesConfig> dbTables,
      Supplier<ConfigManagerConfig> config,
      Lifecycle lifecycle
  )
  {
    this.dbConnector = dbConnector;
    this.dbTables = dbTables;
    this.config = config;
    this.lifecycle = lifecycle;
  }

  @Override
  public ConfigManager get()
  {
    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            dbConnector.createConfigTable();
          }

          @Override
          public void stop()
          {

          }
        }
    );

    return new ConfigManager(dbConnector.getDBI(), dbTables, config);
  }
}
