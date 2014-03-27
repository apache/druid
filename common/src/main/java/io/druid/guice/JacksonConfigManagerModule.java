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

package io.druid.guice;

import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.common.config.ConfigManager;
import io.druid.common.config.ConfigManagerConfig;
import io.druid.common.config.JacksonConfigManager;
import io.druid.db.DbConnector;
import io.druid.db.DbTablesConfig;

/**
 */
public class JacksonConfigManagerModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.manager.config", ConfigManagerConfig.class);
    binder.bind(JacksonConfigManager.class).in(LazySingleton.class);
  }

  @Provides @ManageLifecycle
  public ConfigManager getConfigManager(
      final DbConnector dbConnector,
      final Supplier<DbTablesConfig> dbTables,
      final Supplier<ConfigManagerConfig> config,
      final Lifecycle lifecycle
  )
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

    return new ConfigManager(dbConnector, dbTables, config);
  }
}
