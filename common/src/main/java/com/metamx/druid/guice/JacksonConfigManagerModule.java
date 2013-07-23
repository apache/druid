package com.metamx.druid.guice;

import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.druid.config.ConfigManager;
import com.metamx.druid.config.ConfigManagerConfig;
import com.metamx.druid.config.JacksonConfigManager;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.db.DbTablesConfig;

/**
 */
public class JacksonConfigManagerModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.manager.config", ConfigManagerConfig.class);
    binder.bind(JacksonConfigManager.class);
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

    return new ConfigManager(dbConnector.getDBI(), dbTables, config);
  }
}
