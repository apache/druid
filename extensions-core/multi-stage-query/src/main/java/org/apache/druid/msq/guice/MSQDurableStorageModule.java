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

package org.apache.druid.msq.guice;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.StorageConnectorProvider;

import java.util.List;
import java.util.Properties;

/**
 * Module for functionality related to durable storage for stage output data.
 */
public class MSQDurableStorageModule implements DruidModule
{
  public static final String MSQ_INTERMEDIATE_STORAGE_PREFIX =
      String.join(".", MSQIndexingModule.BASE_MSQ_KEY, "intermediate.storage");

  public static final String MSQ_INTERMEDIATE_STORAGE_ENABLED =
      String.join(".", MSQ_INTERMEDIATE_STORAGE_PREFIX, "enable");

  @Inject
  private Properties properties;

  public MSQDurableStorageModule()
  {
  }

  public MSQDurableStorageModule(Properties properties)
  {
    this.properties = properties;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    if (isDurableShuffleStorageEnabled()) {
      JsonConfigProvider.bind(
          binder,
          MSQ_INTERMEDIATE_STORAGE_PREFIX,
          StorageConnectorProvider.class,
          MultiStageQuery.class
      );

      binder.bind(Key.get(StorageConnector.class, MultiStageQuery.class))
            .toProvider(Key.get(StorageConnectorProvider.class, MultiStageQuery.class))
            .in(LazySingleton.class);
    }
  }

  private boolean isDurableShuffleStorageEnabled()
  {
    return Boolean.parseBoolean((String) properties.getOrDefault(MSQ_INTERMEDIATE_STORAGE_ENABLED, "false"));
  }
}
