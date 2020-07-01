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

package org.apache.druid.storage.aliyun;

import com.aliyun.oss.OSS;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.data.SearchableVersionedDataFinder;
import org.apache.druid.data.input.aliyun.OssClientConfig;
import org.apache.druid.guice.Binders;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

public class OssStorageDruidModule implements DruidModule
{
  public static final String SCHEME = "oss";
  public static final String SCHEME_ZIP = "oss_zip";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new Module()
        {
          @Override
          public String getModuleName()
          {
            return "DruidAliyunOss-" + System.identityHashCode(this);
          }

          @Override
          public Version version()
          {
            return Version.unknownVersion();
          }

          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(OssLoadSpec.class);
          }
        }
    );
  }

  @Override
  public void configure(Binder binder)
  {
    MapBinder.newMapBinder(binder, String.class, SearchableVersionedDataFinder.class)
             .addBinding(SCHEME)
             .to(OssTimestampVersionedDataFinder.class)
             .in(LazySingleton.class);
    Binders.dataSegmentKillerBinder(binder)
           .addBinding(SCHEME_ZIP)
           .to(OssDataSegmentKiller.class)
           .in(LazySingleton.class);
    Binders.dataSegmentMoverBinder(binder)
           .addBinding(SCHEME_ZIP)
           .to(OssDataSegmentMover.class)
           .in(LazySingleton.class);
    Binders.dataSegmentArchiverBinder(binder)
           .addBinding(SCHEME_ZIP)
           .to(OssDataSegmentArchiver.class)
           .in(LazySingleton.class);
    Binders.dataSegmentPusherBinder(binder).addBinding(SCHEME).to(OssDataSegmentPusher.class).in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "druid.oss", OssClientConfig.class);
    JsonConfigProvider.bind(binder, "druid.storage.oss", OssInputDataConfig.class);
    JsonConfigProvider.bind(binder, "druid.storage.oss", OssStorageConfig.class);
    JsonConfigProvider.bind(binder, "druid.storage.oss", OssDataSegmentArchiverConfig.class);

    Binders.taskLogsBinder(binder).addBinding(SCHEME).to(OssTaskLogs.class);
    JsonConfigProvider.bind(binder, "druid.indexer.logs.oss", OssTaskLogsConfig.class);
    binder.bind(OssTaskLogs.class).in(LazySingleton.class);
  }

  @Provides
  @LazySingleton
  public OSS initializeOssClient(OssClientConfig inputSourceConfig)
  {
    return inputSourceConfig.buildClient();
  }
}
