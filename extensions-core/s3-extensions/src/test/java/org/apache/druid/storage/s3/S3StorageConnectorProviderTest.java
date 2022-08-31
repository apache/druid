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

package org.apache.druid.storage.s3;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.ProvisionException;
import com.google.inject.name.Names;
import org.apache.druid.common.aws.AWSModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.StorageConnectorModule;
import org.apache.druid.storage.StorageConnectorProvider;
import org.apache.druid.storage.s3.output.S3StorageConnector;
import org.apache.druid.storage.s3.output.S3StorageConnectorModule;
import org.apache.druid.storage.s3.output.S3StorageConnectorProvider;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Properties;

public class S3StorageConnectorProviderTest
{

  private static final String CUSTOM_NAMESPACE = "custom";

  @Test
  public void createS3StorageFactoryWithRequiredProperties()
  {

    final Properties properties = new Properties();
    properties.setProperty(CUSTOM_NAMESPACE + ".type", "s3");
    properties.setProperty(CUSTOM_NAMESPACE + ".bucket", "bucket");
    properties.setProperty(CUSTOM_NAMESPACE + ".prefix", "prefix");
    properties.setProperty(CUSTOM_NAMESPACE + ".tempDir", "/tmp");
    StorageConnectorProvider s3StorageConnectorProvider = getStorageConnectorProvider(properties);

    Assert.assertTrue(s3StorageConnectorProvider instanceof S3StorageConnectorProvider);
    Assert.assertTrue(s3StorageConnectorProvider.get() instanceof S3StorageConnector);
    Assert.assertEquals("bucket", ((S3StorageConnectorProvider) s3StorageConnectorProvider).getBucket());
    Assert.assertEquals("prefix", ((S3StorageConnectorProvider) s3StorageConnectorProvider).getPrefix());
    Assert.assertEquals(new File("/tmp"), ((S3StorageConnectorProvider) s3StorageConnectorProvider).getTempDir());

  }

  @Test
  public void createS3StorageFactoryWithMissingPrefix()
  {

    final Properties properties = new Properties();
    properties.setProperty(CUSTOM_NAMESPACE + ".type", "s3");
    properties.setProperty(CUSTOM_NAMESPACE + ".bucket", "bucket");
    properties.setProperty(CUSTOM_NAMESPACE + ".tempDir", "/tmp");
    Assert.assertThrows(
        "Missing required creator property 'prefix'",
        ProvisionException.class,
        () -> getStorageConnectorProvider(properties)
    );
  }


  @Test
  public void createS3StorageFactoryWithMissingBucket()
  {

    final Properties properties = new Properties();
    properties.setProperty(CUSTOM_NAMESPACE + ".type", "s3");
    properties.setProperty(CUSTOM_NAMESPACE + ".prefix", "prefix");
    properties.setProperty(CUSTOM_NAMESPACE + ".tempDir", "/tmp");
    Assert.assertThrows(
        "Missing required creator property 'bucket'",
        ProvisionException.class,
        () -> getStorageConnectorProvider(properties)
    );
  }

  @Test
  public void createS3StorageFactoryWithMissingTempDir()
  {

    final Properties properties = new Properties();
    properties.setProperty(CUSTOM_NAMESPACE + ".type", "s3");
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
        new AWSModule(),
        new StorageConnectorModule(),
        new S3StorageConnectorModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
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
        }
    ).withProperties(properties);

    Injector injector = startupInjectorBuilder.build();
    injector.getInstance(ObjectMapper.class).registerModules(new S3StorageConnectorModule().getJacksonModules());
    injector.getInstance(ObjectMapper.class).setInjectableValues(
        new InjectableValues.Std()
            .addValue(
                ServerSideEncryptingAmazonS3.class,
                new ServerSideEncryptingAmazonS3(null, new NoopServerSideEncryption())
            ));


    StorageConnectorProvider storageConnectorProvider = injector.getInstance(Key.get(
        StorageConnectorProvider.class,
        Names.named(CUSTOM_NAMESPACE)
    ));
    return storageConnectorProvider;
  }
}
