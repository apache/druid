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
package io.druid.metadata.storage.sqlserver;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import io.druid.guice.LazySingleton;
import io.druid.guice.PolyBind;
import io.druid.guice.SQLMetadataStorageDruidModule;
import io.druid.initialization.DruidModule;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataStorageProvider;
import io.druid.metadata.NoopMetadataStorageProvider;
import io.druid.metadata.SQLMetadataConnector;

import java.util.List;

@SuppressWarnings("nls")
public class SQLServerMetadataStorageModule extends SQLMetadataStorageDruidModule implements DruidModule
{

  public static final String TYPE = "sqlserver";

  public SQLServerMetadataStorageModule()
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

    PolyBind
        .optionBinder(binder, Key.get(MetadataStorageProvider.class))
        .addBinding(TYPE)
        .to(NoopMetadataStorageProvider.class)
        .in(LazySingleton.class);

    PolyBind
        .optionBinder(binder, Key.get(MetadataStorageConnector.class))
        .addBinding(TYPE)
        .to(SQLServerConnector.class)
        .in(LazySingleton.class);

    PolyBind
        .optionBinder(binder, Key.get(SQLMetadataConnector.class))
        .addBinding(TYPE)
        .to(SQLServerConnector.class)
        .in(LazySingleton.class);
  }
}
