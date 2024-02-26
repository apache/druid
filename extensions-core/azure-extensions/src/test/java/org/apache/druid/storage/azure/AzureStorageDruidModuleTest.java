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

import com.azure.storage.blob.BlobServiceClient;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.ProvisionException;
import com.google.inject.TypeLiteral;
import org.apache.druid.data.input.azure.AzureEntityFactory;
import org.apache.druid.data.input.azure.AzureInputSource;
import org.apache.druid.data.input.azure.AzureStorageAccountInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.segment.loading.OmniDataSegmentKiller;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.validation.Validation;
import javax.validation.Validator;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

public class AzureStorageDruidModuleTest extends EasyMockSupport
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final String AZURE_ACCOUNT_NAME;
  private static final String AZURE_ACCOUNT_KEY;
  private static final String AZURE_SHARED_ACCESS_TOKEN;
  private static final String AZURE_MANAGED_CREDENTIAL_CLIENT_ID;
  private static final String AZURE_CONTAINER;
  private static final String AZURE_PREFIX;
  private static final int AZURE_MAX_LISTING_LENGTH;
  private static final String PATH = "path/subpath";
  private static final Iterable<URI> EMPTY_PREFIXES_ITERABLE = ImmutableList.of();
  private static final Properties PROPERTIES;

  private CloudObjectLocation cloudObjectLocation1;
  private CloudObjectLocation cloudObjectLocation2;
  private AzureStorage azureStorage;
  private Injector injector;

  static {
    try {
      AZURE_ACCOUNT_NAME = "azureAccount1";
      AZURE_ACCOUNT_KEY = Base64.getUrlEncoder()
                                .encodeToString("azureKey1".getBytes(StandardCharsets.UTF_8));
      AZURE_SHARED_ACCESS_TOKEN = "dummyToken";
      AZURE_MANAGED_CREDENTIAL_CLIENT_ID = "clientId";
      AZURE_CONTAINER = "azureContainer1";
      AZURE_PREFIX = "azurePrefix1";
      AZURE_MAX_LISTING_LENGTH = 10;
      PROPERTIES = initializePropertes();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setup()
  {
    cloudObjectLocation1 = createMock(CloudObjectLocation.class);
    cloudObjectLocation2 = createMock(CloudObjectLocation.class);
    azureStorage = createMock(AzureStorage.class);
  }

  @Test
  public void testGetAzureAccountConfigExpectedConfig()
  {
    injector = makeInjectorWithProperties(PROPERTIES);
    AzureAccountConfig azureAccountConfig = injector.getInstance(AzureAccountConfig.class);

    Assert.assertEquals(AZURE_ACCOUNT_NAME, azureAccountConfig.getAccount());
    Assert.assertEquals(AZURE_ACCOUNT_KEY, azureAccountConfig.getKey());
  }

  @Test
  public void testGetAzureAccountConfigExpectedConfigWithSAS()
  {
    Properties properties = initializePropertes();
    properties.setProperty("druid.azure.sharedAccessStorageToken", AZURE_SHARED_ACCESS_TOKEN);
    properties.remove("druid.azure.key");

    injector = makeInjectorWithProperties(properties);
    AzureAccountConfig azureAccountConfig = injector.getInstance(AzureAccountConfig.class);

    Assert.assertEquals(AZURE_ACCOUNT_NAME, azureAccountConfig.getAccount());
    Assert.assertEquals(AZURE_SHARED_ACCESS_TOKEN, azureAccountConfig.getSharedAccessStorageToken());
  }

  @Test
  public void testGetAzureDataSegmentConfigExpectedConfig()
  {
    injector = makeInjectorWithProperties(PROPERTIES);
    AzureDataSegmentConfig segmentConfig = injector.getInstance(AzureDataSegmentConfig.class);

    Assert.assertEquals(AZURE_CONTAINER, segmentConfig.getContainer());
    Assert.assertEquals(AZURE_PREFIX, segmentConfig.getPrefix());
  }

  @Test
  public void testGetAzureInputDataConfigExpectedConfig()
  {
    injector = makeInjectorWithProperties(PROPERTIES);
    AzureInputDataConfig inputDataConfig = injector.getInstance(AzureInputDataConfig.class);

    Assert.assertEquals(AZURE_MAX_LISTING_LENGTH, inputDataConfig.getMaxListingLength());
  }

  @Test
  public void testGetAzureByteSourceFactoryCanCreateAzureByteSource()
  {
    injector = makeInjectorWithProperties(PROPERTIES);
    AzureByteSourceFactory factory = injector.getInstance(AzureByteSourceFactory.class);
    Object object1 = factory.create("container1", "blob1", azureStorage);
    Object object2 = factory.create("container2", "blob2", azureStorage);
    Assert.assertNotNull(object1);
    Assert.assertNotNull(object2);
    Assert.assertNotSame(object1, object2);
  }

  @Test
  public void testGetAzureEntityFactoryCanCreateAzureEntity()
  {
    EasyMock.expect(cloudObjectLocation1.getBucket()).andReturn(AZURE_CONTAINER);
    EasyMock.expect(cloudObjectLocation2.getBucket()).andReturn(AZURE_CONTAINER);
    EasyMock.expect(cloudObjectLocation1.getPath()).andReturn(PATH).times(2);
    EasyMock.expect(cloudObjectLocation2.getPath()).andReturn(PATH);
    replayAll();

    injector = makeInjectorWithProperties(PROPERTIES);
    AzureEntityFactory factory = injector.getInstance(AzureEntityFactory.class);
    Object object1 = factory.create(cloudObjectLocation1, azureStorage, AzureInputSource.SCHEME);
    Object object2 = factory.create(cloudObjectLocation2, azureStorage, AzureInputSource.SCHEME);
    Object object3 = factory.create(cloudObjectLocation1, azureStorage, AzureStorageAccountInputSource.SCHEME);
    Assert.assertNotNull(object1);
    Assert.assertNotNull(object2);
    Assert.assertNotNull(object3);
    Assert.assertNotSame(object1, object2);
    Assert.assertNotSame(object1, object3);
  }

  @Test
  public void testGetAzureCloudBlobIteratorFactoryCanCreateAzureCloudBlobIterator()
  {
    injector = makeInjectorWithProperties(PROPERTIES);
    AzureCloudBlobIteratorFactory factory = injector.getInstance(AzureCloudBlobIteratorFactory.class);
    Object object1 = factory.create(EMPTY_PREFIXES_ITERABLE, 10, azureStorage);
    Object object2 = factory.create(EMPTY_PREFIXES_ITERABLE, 10, azureStorage);
    Assert.assertNotNull(object1);
    Assert.assertNotNull(object2);
    Assert.assertNotSame(object1, object2);
  }

  @Test
  public void testGetAzureCloudBlobIterableFactoryCanCreateAzureCloudBlobIterable()
  {
    injector = makeInjectorWithProperties(PROPERTIES);
    AzureCloudBlobIterableFactory factory = injector.getInstance(AzureCloudBlobIterableFactory.class);
    AzureCloudBlobIterable object1 = factory.create(EMPTY_PREFIXES_ITERABLE, 10, azureStorage);
    AzureCloudBlobIterable object2 = factory.create(EMPTY_PREFIXES_ITERABLE, 10, azureStorage);
    Assert.assertNotNull(object1);
    Assert.assertNotNull(object2);
    Assert.assertNotSame(object1, object2);
  }

  @Test
  public void testSegmentKillerBoundSingleton()
  {
    Injector injector = makeInjectorWithProperties(PROPERTIES);
    OmniDataSegmentKiller killer = injector.getInstance(OmniDataSegmentKiller.class);
    Assert.assertTrue(killer.getKillers().containsKey(AzureStorageDruidModule.SCHEME));
    Assert.assertSame(
        AzureDataSegmentKiller.class,
        killer.getKillers().get(AzureStorageDruidModule.SCHEME).get().getClass()
    );
    Assert.assertSame(
        killer.getKillers().get(AzureStorageDruidModule.SCHEME).get(),
        killer.getKillers().get(AzureStorageDruidModule.SCHEME).get()
    );
  }

  @Test
  public void testMultipleCredentialsSet()
  {
    String message = "Set only one of 'key' or 'sharedAccessStorageToken' or 'useAzureCredentialsChain' in the azure config.";
    Properties properties = initializePropertes();
    properties.setProperty("druid.azure.sharedAccessStorageToken", AZURE_SHARED_ACCESS_TOKEN);
    expectedException.expect(ProvisionException.class);
    expectedException.expectMessage(message);
    makeInjectorWithProperties(properties).getInstance(
        Key.get(new TypeLiteral<AzureClientFactory>()
        {
        })
    );

    properties = initializePropertes();
    properties.setProperty("druid.azure.managedIdentityClientId", AZURE_MANAGED_CREDENTIAL_CLIENT_ID);
    expectedException.expect(ProvisionException.class);
    expectedException.expectMessage(message);
    makeInjectorWithProperties(properties).getInstance(
        Key.get(new TypeLiteral<Supplier<BlobServiceClient>>()
        {
        })
    );

    properties = initializePropertes();
    properties.remove("druid.azure.key");
    properties.setProperty("druid.azure.managedIdentityClientId", AZURE_MANAGED_CREDENTIAL_CLIENT_ID);
    properties.setProperty("druid.azure.sharedAccessStorageToken", AZURE_SHARED_ACCESS_TOKEN);
    expectedException.expect(ProvisionException.class);
    expectedException.expectMessage(message);
    makeInjectorWithProperties(properties).getInstance(
        Key.get(new TypeLiteral<AzureClientFactory>()
        {
        })
    );
  }

  @Test
  public void testAllCredentialsUnset()
  {
    Properties properties = initializePropertes();
    properties.remove("druid.azure.key");
    expectedException.expect(ProvisionException.class);
    expectedException.expectMessage("Either set 'key' or 'sharedAccessStorageToken' or 'useAzureCredentialsChain' in the azure config.");
    makeInjectorWithProperties(properties).getInstance(
        Key.get(new TypeLiteral<AzureClientFactory>()
        {
        })
    );
  }

  @Test
  public void testAccountUnset()
  {
    Properties properties = initializePropertes();
    properties.remove("druid.azure.account");
    expectedException.expect(ProvisionException.class);
    expectedException.expectMessage("Set 'account' to the storage account that needs to be configured in the azure config. Please refer to azure documentation.");
    makeInjectorWithProperties(properties).getInstance(
        Key.get(new TypeLiteral<AzureClientFactory>()
        {
        })
    );
  }

  @Test
  public void testGetBlobStorageEndpointWithDefaultProperties()
  {
    Properties properties = initializePropertes();
    AzureAccountConfig config = makeInjectorWithProperties(properties).getInstance(AzureAccountConfig.class);
    Assert.assertEquals(config.getEndpointSuffix(), AzureUtils.DEFAULT_AZURE_ENDPOINT_SUFFIX);
    Assert.assertEquals(config.getBlobStorageEndpoint(), AzureUtils.AZURE_STORAGE_HOST_ADDRESS);
  }

  @Test
  public void testGetBlobStorageEndpointWithCustomBlobPath()
  {
    Properties properties = initializePropertes();
    final String customSuffix = "core.usgovcloudapi.net";
    properties.setProperty("druid.azure.endpointSuffix", customSuffix);
    AzureAccountConfig config = makeInjectorWithProperties(properties).getInstance(AzureAccountConfig.class);
    Assert.assertEquals(config.getEndpointSuffix(), customSuffix);
    Assert.assertEquals(config.getBlobStorageEndpoint(), "blob." + customSuffix);
  }

  private Injector makeInjectorWithProperties(final Properties props)
  {
    return Guice.createInjector(
        ImmutableList.of(
            new DruidGuiceExtensions(),
            new JacksonModule(),
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

  private static Properties initializePropertes()
  {
    final Properties props = new Properties();
    props.put("druid.azure.account", AZURE_ACCOUNT_NAME);
    props.put("druid.azure.key", AZURE_ACCOUNT_KEY);
    props.put("druid.azure.container", AZURE_CONTAINER);
    props.put("druid.azure.prefix", AZURE_PREFIX);
    props.put("druid.azure.maxListingLength", String.valueOf(AZURE_MAX_LISTING_LENGTH));
    return props;
  }
}
