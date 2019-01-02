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

package org.apache.druid.extensions.watermarking.storage.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.extensions.watermarking.storage.WatermarkSource;
import org.apache.druid.extensions.watermarking.storage.WatermarkStoreModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

public class MySqlWatermarkStoreModule extends WatermarkStoreModule implements DruidModule
{
  public static final String TYPE = "mysql";

  public MySqlWatermarkStoreModule()
  {
    super(TYPE);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    super.configure(binder);

    JsonConfigProvider.bind(
        binder,
        STORE_PROPERTY_BASE + TYPE,
        SqlWatermarkStoreConfig.class
    );

    PolyBind.optionBinder(binder, Key.get(WatermarkSink.class))
            .addBinding(TYPE)
            .to(MySqlWatermarkStore.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(WatermarkSource.class))
            .addBinding(TYPE)
            .to(MySqlWatermarkStore.class)
            .in(LazySingleton.class);
  }
}
