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

package io.druid.guice;

import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.druid.common.config.ConfigManager;
import io.druid.common.config.ConfigManagerConfig;
import io.druid.common.config.JacksonConfigManager;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataStorageTablesConfig;

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
      final MetadataStorageConnector dbConnector,
      final Supplier<MetadataStorageTablesConfig> dbTables,
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
