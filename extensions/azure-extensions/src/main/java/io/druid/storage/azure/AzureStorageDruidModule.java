/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.storage.azure;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.initialization.DruidModule;

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
        }
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
  }

  @Provides
  @LazySingleton
  public CloudStorageAccount getCloudStorageAccount(final AzureAccountConfig config)
      throws URISyntaxException, InvalidKeyException
  {
    return CloudStorageAccount.parse(
        String.format(
            STORAGE_CONNECTION_STRING,
            config.getProtocol(),
            config.getAccount(),
            config.getKey()
        )
    );
  }

  @Provides
  @LazySingleton
  public CloudBlobContainer getCloudBlobContainer(
      final AzureAccountConfig config,
      final CloudStorageAccount cloudStorageAccount
  )
      throws URISyntaxException, StorageException
  {
    CloudBlobClient blobClient = cloudStorageAccount.createCloudBlobClient();
    CloudBlobContainer cloudBlobContainer = blobClient.getContainerReference(config.getContainer());

    cloudBlobContainer.createIfNotExists();

    return cloudBlobContainer;
  }

  @Provides
  @LazySingleton
  public AzureStorageContainer getAzureStorageContainer(
      final CloudBlobContainer cloudBlobContainer
  )
  {
    return new AzureStorageContainer(cloudBlobContainer);
  }
}
