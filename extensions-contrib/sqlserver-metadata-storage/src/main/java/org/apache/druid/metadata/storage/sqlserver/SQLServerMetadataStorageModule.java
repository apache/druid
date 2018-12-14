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

package org.apache.druid.metadata.storage.sqlserver;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.SQLMetadataStorageDruidModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.metadata.MetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageProvider;
import org.apache.druid.metadata.NoopMetadataStorageProvider;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.SQLServerMetadataStorageActionHandlerFactory;

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

    PolyBind.optionBinder(binder, Key.get(MetadataStorageActionHandlerFactory.class))
            .addBinding(TYPE)
            .to(SQLServerMetadataStorageActionHandlerFactory.class)
            .in(LazySingleton.class);
  }
}
