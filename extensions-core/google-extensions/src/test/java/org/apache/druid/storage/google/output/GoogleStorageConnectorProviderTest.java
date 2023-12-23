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

package org.apache.druid.storage.google.output;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.ProvisionException;
import com.google.inject.name.Names;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.StorageConnectorModule;
import org.apache.druid.storage.StorageConnectorProvider;
import org.apache.druid.storage.google.GoogleInputDataConfig;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleStorageDruidModule;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Properties;

public class GoogleStorageConnectorProviderTest
{
  private static final String CUSTOM_NAMESPACE = "custom";

  @Test
  public void createGoogleStorageFactoryWithRequiredProperties()
  {

    final Properties properties = new Properties();
    properties.setProperty(CUSTOM_NAMESPACE + ".type", "google");
    properties.setProperty(CUSTOM_NAMESPACE + ".bucket", "bucket");
    properties.setProperty(CUSTOM_NAMESPACE + ".prefix", "prefix");
    properties.setProperty(CUSTOM_NAMESPACE + ".tempDir", "/tmp");
    StorageConnectorProvider googleStorageConnectorProvider = getStorageConnectorProvider(properties);

    Assert.assertTrue(googleStorageConnectorProvider instanceof GoogleStorageConnectorProvider);
    Assert.assertTrue(googleStorageConnectorProvider.get() instanceof GoogleStorageConnector);
    Assert.assertEquals("bucket", ((GoogleStorageConnectorProvider) googleStorageConnectorProvider).getBucket());
    Assert.assertEquals("prefix", ((GoogleStorageConnectorProvider) googleStorageConnectorProvider).getPrefix());
    Assert.assertEquals(new File("/tmp"), ((GoogleStorageConnectorProvider) googleStorageConnectorProvider).getTempDir());

  }

  @Test
  public void createGoogleStorageFactoryWithMissingPrefix()
  {

    final Properties properties = new Properties();
    properties.setProperty(CUSTOM_NAMESPACE + ".type", "bucket");
    properties.setProperty(CUSTOM_NAMESPACE + ".bucket", "bucket");
    properties.setProperty(CUSTOM_NAMESPACE + ".tempDir", "/tmp");
    Assert.assertThrows(
        "Missing required creator property 'prefix'",
        ProvisionException.class,
        () -> getStorageConnectorProvider(properties)
    );
  }


  @Test
  public void createGoogleStorageFactoryWithMissingbucket()
  {

    final Properties properties = new Properties();
    properties.setProperty(CUSTOM_NAMESPACE + ".type", "Google");
    properties.setProperty(CUSTOM_NAMESPACE + ".prefix", "prefix");
    properties.setProperty(CUSTOM_NAMESPACE + ".tempDir", "/tmp");
    Assert.assertThrows(
        "Missing required creator property 'bucket'",
        ProvisionException.class,
        () -> getStorageConnectorProvider(properties)
    );
  }

  @Test
  public void createGoogleStorageFactoryWithMissingTempDir()
  {

    final Properties properties = new Properties();
    properties.setProperty(CUSTOM_NAMESPACE + ".type", "Google");
    properties.setProperty(CUSTOM_NAMESPACE + ".bucket", "bucket");
    properties.setProperty(CUSTOM_NAMESPACE + ".prefix", "prefix");

    Assert.assertThrows(
        "Missing required creator property 'tempDir'",
        ProvisionException.class,
        () -> getStorageConnectorProvider(properties)
    );
  }

  private StorageConnectorProvider getStorageConnectorProvider(Properties properties)
  {
    StartupInjectorBuilder startupInjectorBuilder = new StartupInjectorBuilder().add(
        new GoogleStorageDruidModule(),
        new StorageConnectorModule(),
        new GoogleStorageConnectorModule(),
        binder -> {
          JsonConfigProvider.bind(
              binder,
              CUSTOM_NAMESPACE,
              StorageConnectorProvider.class,
              Names.named(CUSTOM_NAMESPACE)
          );

          binder.bind(Key.get(StorageConnector.class, Names.named(CUSTOM_NAMESPACE)))
                .toProvider(Key.get(StorageConnectorProvider.class, Names.named(CUSTOM_NAMESPACE)))
                .in(LazySingleton.class);
        }
    ).withProperties(properties);

    Injector injector = startupInjectorBuilder.build();
    injector.getInstance(ObjectMapper.class).registerModules(new GoogleStorageConnectorModule().getJacksonModules());
    injector.getInstance(ObjectMapper.class).setInjectableValues(
        new InjectableValues.Std()
            .addValue(
                GoogleStorage.class,
                EasyMock.mock(GoogleStorage.class)
            ).addValue(
                GoogleInputDataConfig.class,
                EasyMock.mock(GoogleInputDataConfig.class)
            ));


    return injector.getInstance(Key.get(
        StorageConnectorProvider.class,
        Names.named(CUSTOM_NAMESPACE)
    ));
  }
}
