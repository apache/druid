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

package org.apache.druid.guice;

import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.ConfigManagerConfig;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;

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
          public void start()
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
