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

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

public class AzureStorageDruidModuleTest
{
  private static final String AZURE_ACCOUNT_NAME;
  private static final String AZURE_ACCOUNT_KEY;
  private static final String AZURE_CONTAINER;
  private Injector injector;

  static {
    try {
      AZURE_ACCOUNT_NAME = "azureAccount1";
      AZURE_ACCOUNT_KEY = Base64.getUrlEncoder()
                                .encodeToString("azureKey1".getBytes(StandardCharsets.UTF_8.toString()));
      AZURE_CONTAINER = "azureContainer1";
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void test_getBlobClient_expectedClient()
  {
    final Properties props = new Properties();
    props.put("druid.azure.account", AZURE_ACCOUNT_NAME);
    props.put("druid.azure.key", AZURE_ACCOUNT_KEY);
    props.put("druid.azure.container", AZURE_CONTAINER);
    injector = makeInjectorWithProperties(props);
    AzureAccountConfig azureAccountConfig = injector.getInstance(Key.get(AzureAccountConfig.class));

    Assert.assertEquals(AZURE_ACCOUNT_NAME, azureAccountConfig.getAccount());
    Assert.assertEquals(AZURE_ACCOUNT_KEY, azureAccountConfig.getKey());
    Assert.assertEquals(AZURE_CONTAINER, azureAccountConfig.getContainer());

    CloudBlobClient cloudBlobClient = injector.getInstance(CloudBlobClient.class);
    StorageCredentials storageCredentials = cloudBlobClient.getCredentials();

    Assert.assertEquals(AZURE_ACCOUNT_NAME, storageCredentials.getAccountName());
  }

  @Test
  public void test_getAzureStorageContainer_expectedClient()
  {
    final Properties props = new Properties();
    props.put("druid.azure.account", AZURE_ACCOUNT_NAME);
    props.put("druid.azure.key", AZURE_ACCOUNT_KEY);
    props.put("druid.azure.container", AZURE_CONTAINER);
    injector = makeInjectorWithProperties(props);
    AzureAccountConfig azureAccountConfig = injector.getInstance(Key.get(AzureAccountConfig.class));

    Assert.assertEquals(AZURE_ACCOUNT_NAME, azureAccountConfig.getAccount());
    Assert.assertEquals(AZURE_ACCOUNT_KEY, azureAccountConfig.getKey());
    Assert.assertEquals(AZURE_CONTAINER, azureAccountConfig.getContainer());

    CloudBlobClient cloudBlobClient = injector.getInstance(CloudBlobClient.class);
    StorageCredentials storageCredentials = cloudBlobClient.getCredentials();

    Assert.assertEquals(AZURE_ACCOUNT_NAME, storageCredentials.getAccountName());

    AzureStorage azureStorage = injector.getInstance(AzureStorage.class);
    Assert.assertSame(cloudBlobClient, azureStorage.getCloudBlobClient());
  }

  private Injector makeInjectorWithProperties(final Properties props)
  {
    return Guice.createInjector(
        ImmutableList.of(
            new DruidGuiceExtensions(),
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
                binder.bind(JsonConfigurator.class).in(LazySingleton.class);
                binder.bind(Properties.class).toInstance(props);
              }
            },
            new AzureStorageDruidModule()
        ));
  }
}
