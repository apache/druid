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

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import org.apache.druid.data.SearchableVersionedDataFinder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentKiller;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.LocalFileTimestampVersionFinder;
import org.apache.druid.segment.loading.LocalLoadSpec;

import java.util.List;
import java.util.Properties;

/**
 */
public class LocalDataStorageDruidModule implements DruidModule
{
  public static final String SCHEME = "local";
  public static final String STORAGE_TYPE_PROPERTY = "druid.storage.type";

  @Override
  public void configure(Binder binder)
  {
    bindDeepStorageLocal(binder);

    PolyBind.createChoice(
        binder,
        STORAGE_TYPE_PROPERTY,
        Key.get(DataSegmentPusher.class),
        Key.get(LocalDataSegmentPusher.class)
    );

    PolyBind.createChoice(
        binder,
        STORAGE_TYPE_PROPERTY,
        Key.get(DataSegmentKiller.class),
        Key.get(LocalDataSegmentKiller.class)
    );
  }

  @Provides
  @Named(STORAGE_TYPE_PROPERTY)
  public String getDeepStorageType(Properties properties)
  {
    return properties.getProperty(STORAGE_TYPE_PROPERTY, SCHEME);
  }

  private static void bindDeepStorageLocal(Binder binder)
  {
    MapBinder.newMapBinder(binder, String.class, SearchableVersionedDataFinder.class)
             .addBinding(LocalFileTimestampVersionFinder.URI_SCHEME)
             .to(LocalFileTimestampVersionFinder.class)
             .in(LazySingleton.class);

    Binders.dataSegmentKillerBinder(binder)
           .addBinding(SCHEME)
           .to(LocalDataSegmentKiller.class)
           .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DataSegmentPusher.class))
            .addBinding(SCHEME)
            .to(LocalDataSegmentPusher.class)
            .in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "druid.storage", LocalDataSegmentPusherConfig.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new Module()
        {
          @Override
          public String getModuleName()
          {
            return "DruidLocalStorage-" + System.identityHashCode(this);
          }

          @Override
          public Version version()
          {
            return Version.unknownVersion();
          }

          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(LocalLoadSpec.class);
          }
        }
    );
  }
}
