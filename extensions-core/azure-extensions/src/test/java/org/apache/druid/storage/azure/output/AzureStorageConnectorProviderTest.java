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

package org.apache.druid.storage.azure.output;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.ProvisionException;
import com.google.inject.name.Names;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.storage.StorageConnectorModule;
import org.apache.druid.storage.StorageConnectorProvider;
import org.apache.druid.storage.azure.AzureStorage;
import org.apache.druid.storage.azure.AzureStorageDruidModule;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AzureStorageConnectorProviderTest
{
  private static final String CUSTOM_NAMESPACE = "custom";

  @Test
  public void createAzureStorageFactoryWithRequiredProperties()
  {

    final Properties properties = new Properties();
    properties.setProperty(CUSTOM_NAMESPACE + ".type", "azure");
    properties.setProperty(CUSTOM_NAMESPACE + ".container", "container");
    properties.setProperty(CUSTOM_NAMESPACE + ".prefix", "prefix");
    properties.setProperty(CUSTOM_NAMESPACE + ".tempDir", "/tmp");
    StorageConnectorProvider storageConnectorProvider = getStorageConnectorProvider(properties);

    assertInstanceOf(AzureStorageConnectorProvider.class, storageConnectorProvider);
    assertInstanceOf(AzureStorageConnector.class, storageConnectorProvider.createStorageConnector(new File("/tmp")));
    assertEquals("container", ((AzureStorageConnectorProvider) storageConnectorProvider).getContainer());
    assertEquals("prefix", ((AzureStorageConnectorProvider) storageConnectorProvider).getPrefix());
    assertEquals(new File("/tmp"),
                            ((AzureStorageConnectorProvider) storageConnectorProvider).getTempDir());
  }

  @Test
  public void createAzureStorageFactoryWithMissingPrefix()
  {

    final Properties properties = new Properties();
    properties.setProperty(CUSTOM_NAMESPACE + ".type", "azure");
    properties.setProperty(CUSTOM_NAMESPACE + ".container", "container");
    properties.setProperty(CUSTOM_NAMESPACE + ".tempDir", "/tmp");
    assertThrows(
        ProvisionException.class,
        () -> getStorageConnectorProvider(properties),
        "Missing required creator property 'prefix'"
    );
  }


  @Test
  public void createAzureStorageFactoryWithMissingContainer()
  {

    final Properties properties = new Properties();
    properties.setProperty(CUSTOM_NAMESPACE + ".type", "azure");
    properties.setProperty(CUSTOM_NAMESPACE + ".prefix", "prefix");
    properties.setProperty(CUSTOM_NAMESPACE + ".tempDir", "/tmp");
    assertThrows(
        ProvisionException.class,
        () -> getStorageConnectorProvider(properties),
        "Missing required creator property 'container'"
    );
  }

  @Test
  public void createAzureStorageFactoryWithMissingTempDir()
  {

    final Properties properties = new Properties();
    properties.setProperty(CUSTOM_NAMESPACE + ".type", "azure");
    properties.setProperty(CUSTOM_NAMESPACE + ".container", "container");
    properties.setProperty(CUSTOM_NAMESPACE + ".prefix", "prefix");

    assertThrows(
        DruidException.class,
        () -> getStorageConnectorProvider(properties).createStorageConnector(null),
        "The runtime property `druid.msq.intermediate.storage.tempDir` must be configured."
    );
  }

  @Test
  public void createAzureStorageFactoryWithMissingTempDirButProvidedDuringRuntime()
  {

    final Properties properties = new Properties();
    properties.setProperty(CUSTOM_NAMESPACE + ".type", "azure");
    properties.setProperty(CUSTOM_NAMESPACE + ".container", "container");
    properties.setProperty(CUSTOM_NAMESPACE + ".prefix", "prefix");

    getStorageConnectorProvider(properties).createStorageConnector(new File("/tmp"));
  }

  private StorageConnectorProvider getStorageConnectorProvider(Properties properties)
  {
    StartupInjectorBuilder startupInjectorBuilder = new StartupInjectorBuilder().add(
        new AzureStorageDruidModule(),
        new StorageConnectorModule(),
        new AzureStorageConnectorModule(),
        binder -> JsonConfigProvider.bind(
            binder,
            CUSTOM_NAMESPACE,
            StorageConnectorProvider.class,
            Names.named(CUSTOM_NAMESPACE)
        )
    ).withProperties(properties);

    Injector injector = startupInjectorBuilder.build();
    injector.getInstance(ObjectMapper.class).registerModules(new AzureStorageConnectorModule().getJacksonModules());
    injector.getInstance(ObjectMapper.class).setInjectableValues(
        new InjectableValues.Std()
            .addValue(
                AzureStorage.class,
                EasyMock.mock(AzureStorage.class)
            ));


    return injector.getInstance(Key.get(
        StorageConnectorProvider.class,
        Names.named(CUSTOM_NAMESPACE)
    ));
  }
}
