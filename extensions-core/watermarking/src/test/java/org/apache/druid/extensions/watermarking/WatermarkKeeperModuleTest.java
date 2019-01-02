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

package org.apache.druid.extensions.watermarking;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.druid.extensions.watermarking.storage.WatermarkSource;
import org.apache.druid.extensions.watermarking.storage.google.DatastoreWatermarkStore;
import org.apache.druid.extensions.watermarking.storage.sql.MySqlWatermarkStore;
import org.apache.druid.extensions.watermarking.storage.sql.SqlWatermarkStoreConfig;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.initialization.Initialization;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Properties;

public class WatermarkKeeperModuleTest extends WatermarkingModuleTestBase
{
  @Test
  public void defaultStore()
  {
    Properties props = new Properties();
    Injector injector = newInjector(props);

    injector.getInstance(getType());
    WatermarkSource source = injector.getInstance(WatermarkSource.class);

    // todo: explode or default to memory store without config?
    assertMemorySource(source);
  }

  @Test
  public void memoryStore()
  {
    Properties props = new Properties();
    setMemorySource(props);
    setMemoryStore(props);
    Injector injector = newInjector(props);

    injector.getInstance(getType());
    WatermarkSource source = injector.getInstance(WatermarkSource.class);

    assertMemorySource(source);
  }

  @Test
  public void mysqlStore()
  {
    Properties props = new Properties();
    setMysqlSource(props);
    setMysqlStore(props);
    Injector injector = newInjector(props);

    injector.getInstance(getType());
    WatermarkSource source = injector.getInstance(WatermarkSource.class);

    assertMysqlSource(source);
    assertMysqlConfig((SqlWatermarkStoreConfig) ((MySqlWatermarkStore) source).getConfig());
  }

  @Ignore
  @Test
  public void datastoreStore()
  {
    Properties props = new Properties();
    setDatastoreSource(props);
    setDatastoreStore(props);
    Injector injector = newInjector(props);

    injector.getInstance(getType());
    WatermarkSource source = injector.getInstance(WatermarkSource.class);

    assertDatastoreSource(source);
    assertDatastoreConfig(((DatastoreWatermarkStore) source).getConfig());
  }


  @Override
  public java.lang.Class getType()
  {
    return WatermarkKeeper.class;
  }

  @Override
  public Injector newInjector(final Properties props)
  {
    return Initialization.makeInjectorWithModules(
        Guice.createInjector(
            Modules.override(GuiceInjectors.makeDefaultStartupModules())
                   .with((Module) binder ->
                       binder.bind(Properties.class).toInstance(props))
        ),
        ImmutableList.of(
            new WatermarkKeeperModule()
        )
    );
  }
}
