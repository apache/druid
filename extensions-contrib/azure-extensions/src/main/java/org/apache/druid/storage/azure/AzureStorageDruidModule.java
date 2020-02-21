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

package org.apache.druid.storage.azure;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import org.apache.druid.data.input.azure.AzureEntityFactory;
import org.apache.druid.data.input.azure.AzureInputSource;
import org.apache.druid.firehose.azure.StaticAzureBlobStoreFirehoseFactory;
import org.apache.druid.guice.Binders;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.azure.blob.ListBlobItemHolderFactory;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.List;

/**
 * Binds objects related to dealing with the Azure file system.
 */
public class AzureStorageDruidModule implements DruidModule
{

  static final String SCHEME = "azure";
  public static final String STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s";
  public static final String INDEX_ZIP_FILE_NAME = "index.zip";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new Module()
        {
          @Override
          public String getModuleName()
          {
            return "DruidAzure-" + System.identityHashCode(this);
          }

          @Override
          public Version version()
          {
            return Version.unknownVersion();
          }

          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(AzureLoadSpec.class);
          }
        },
        new SimpleModule().registerSubtypes(
            new NamedType(StaticAzureBlobStoreFirehoseFactory.class, "static-azure-blobstore"),
            new NamedType(AzureInputSource.class, SCHEME)
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.azure", AzureInputDataConfig.class);
    JsonConfigProvider.bind(binder, "druid.azure", AzureDataSegmentConfig.class);
    JsonConfigProvider.bind(binder, "druid.azure", AzureAccountConfig.class);

    Binders.dataSegmentPusherBinder(binder)
           .addBinding(SCHEME)
           .to(AzureDataSegmentPusher.class).in(LazySingleton.class);

    Binders.dataSegmentKillerBinder(binder)
           .addBinding(SCHEME)
           .to(AzureDataSegmentKiller.class).in(LazySingleton.class);

    Binders.taskLogsBinder(binder).addBinding(SCHEME).to(AzureTaskLogs.class);
    JsonConfigProvider.bind(binder, "druid.indexer.logs", AzureTaskLogsConfig.class);
    binder.bind(AzureTaskLogs.class).in(LazySingleton.class);
    binder.bind(AzureCloudBlobHolderToCloudObjectLocationConverter.class).in(LazySingleton.class);
    binder.install(new FactoryModuleBuilder()
                       .build(AzureByteSourceFactory.class));
    binder.install(new FactoryModuleBuilder()
                       .build(AzureEntityFactory.class));
    binder.install(new FactoryModuleBuilder()
                       .build(AzureCloudBlobIteratorFactory.class));
    binder.install(new FactoryModuleBuilder()
                       .build(AzureCloudBlobIterableFactory.class));
    binder.install(new FactoryModuleBuilder()
                       .build(ListBlobItemHolderFactory.class));
  }

  @Provides
  @LazySingleton
  public CloudBlobClient getCloudBlobClient(final AzureAccountConfig config)
      throws URISyntaxException, InvalidKeyException
  {
    CloudStorageAccount account = CloudStorageAccount.parse(
        StringUtils.format(
            STORAGE_CONNECTION_STRING,
            config.getProtocol(),
            config.getAccount(),
            config.getKey()
        )
    );

    return account.createCloudBlobClient();
  }

  @Provides
  @LazySingleton
  public AzureStorage getAzureStorageContainer(
      final CloudBlobClient cloudBlobClient
  )
  {
    return new AzureStorage(cloudBlobClient);
  }
}
