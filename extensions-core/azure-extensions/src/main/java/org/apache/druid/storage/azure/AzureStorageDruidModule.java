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
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.data.input.azure.AzureEntityFactory;
import org.apache.druid.data.input.azure.AzureInputSource;
import org.apache.druid.data.input.azure.AzureStorageAccountInputSource;
import org.apache.druid.guice.Binders;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;

import java.util.List;

/**
 * Binds objects related to dealing with the Azure file system.
 */
public class AzureStorageDruidModule implements DruidModule
{

  public static final String SCHEME = "azure";
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
            new NamedType(AzureInputSource.class, SCHEME),
            new NamedType(AzureStorageAccountInputSource.class, AzureStorageAccountInputSource.SCHEME)
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
    binder.install(new FactoryModuleBuilder()
                       .build(AzureByteSourceFactory.class));
    binder.install(new FactoryModuleBuilder()
                       .build(AzureEntityFactory.class));
    binder.install(new FactoryModuleBuilder()
                       .build(AzureCloudBlobIteratorFactory.class));
    binder.install(new FactoryModuleBuilder()
                       .build(AzureCloudBlobIterableFactory.class));
  }


  @Provides
  @LazySingleton
  public AzureClientFactory getAzureClientFactory(final AzureAccountConfig config)
  {
    if (StringUtils.isEmpty(config.getAccount())) {
      throw new ISE("Set 'account' to the storage account that needs to be configured in the azure config."
          + " Please refer to azure documentation.");
    }

    if (StringUtils.isEmpty(config.getKey()) && StringUtils.isEmpty(config.getSharedAccessStorageToken()) && BooleanUtils.isNotTrue(config.getUseAzureCredentialsChain())) {
      throw new ISE("Either set 'key' or 'sharedAccessStorageToken' or 'useAzureCredentialsChain' in the azure config."
          + " Please refer to azure documentation.");
    }

    /* Azure named keys and sas tokens are mutually exclusive with each other and with azure keychain auth,
    but any form of auth supported by the DefaultAzureCredentialChain is not mutually exclusive, e.g. you can have
    environment credentials or workload credentials or managed credentials using the same chain.
    **/
    if (!StringUtils.isEmpty(config.getKey()) && !StringUtils.isEmpty(config.getSharedAccessStorageToken()) ||
        !StringUtils.isEmpty(config.getKey()) && BooleanUtils.isTrue(config.getUseAzureCredentialsChain()) ||
        !StringUtils.isEmpty(config.getSharedAccessStorageToken()) && BooleanUtils.isTrue(config.getUseAzureCredentialsChain())
    ) {
      throw new ISE("Set only one of 'key' or 'sharedAccessStorageToken' or 'useAzureCredentialsChain' in the azure config."
          + " Please refer to azure documentation.");
    }
    return new AzureClientFactory(config);
  }

  @Provides
  @Global
  @LazySingleton
  public AzureStorage getAzureStorageContainer(
      final AzureClientFactory azureClientFactory,
      final AzureAccountConfig azureAccountConfig
  )
  {
    return new AzureStorage(azureClientFactory, azureAccountConfig.getAccount());
  }
}
