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

package io.druid.storage.azure;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import io.druid.firehose.azure.StaticAzureBlobStoreFirehoseFactory;
import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.StringUtils;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.List;

public class AzureStorageDruidModule implements DruidModule
{

  public static final String SCHEME = "azure";
  public static final String STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s";
  public static final String DESCRIPTOR_FILE_NAME = "descriptor.json";
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
            new NamedType(StaticAzureBlobStoreFirehoseFactory.class, "static-azure-blobstore"))
    );
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.azure", AzureAccountConfig.class);

    Binders.dataSegmentPullerBinder(binder)
           .addBinding(SCHEME)
           .to(AzureDataSegmentPuller.class).in(LazySingleton.class);

    Binders.dataSegmentPusherBinder(binder)
           .addBinding(SCHEME)
           .to(AzureDataSegmentPusher.class).in(LazySingleton.class);

    Binders.dataSegmentKillerBinder(binder)
           .addBinding(SCHEME)
           .to(AzureDataSegmentKiller.class).in(LazySingleton.class);

    Binders.taskLogsBinder(binder).addBinding(SCHEME).to(AzureTaskLogs.class);
    JsonConfigProvider.bind(binder, "druid.indexer.logs", AzureTaskLogsConfig.class);
    binder.bind(AzureTaskLogs.class).in(LazySingleton.class);
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
