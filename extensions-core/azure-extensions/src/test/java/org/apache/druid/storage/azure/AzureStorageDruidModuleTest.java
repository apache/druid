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
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.validation.Validation;
import javax.validation.Validator;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AzureStorageDruidModuleTest extends EasyMockSupport
{
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

  @BeforeEach
  public void setup()
  {
    cloudObjectLocation1 = createMock(CloudObjectLocation.class);
    cloudObjectLocation2 = createMock(CloudObjectLocation.class);
    azureStorage = createMock(AzureStorage.class);
  }

  @Test
  public void testGetAzureAccountConfigExpectedConfig()
  {
    final Injector injector = makeInjectorWithProperties(PROPERTIES);
    AzureAccountConfig azureAccountConfig = injector.getInstance(AzureAccountConfig.class);

    assertEquals(AZURE_ACCOUNT_NAME, azureAccountConfig.getAccount());
    assertEquals(AZURE_ACCOUNT_KEY, azureAccountConfig.getKey());
  }

  @Test
  public void testGetAzureAccountConfigExpectedConfigWithSAS()
  {
    Properties properties = initializePropertes();
    properties.setProperty("druid.azure.sharedAccessStorageToken", AZURE_SHARED_ACCESS_TOKEN);
    properties.remove("druid.azure.key");

    final Injector injector = makeInjectorWithProperties(properties);
    AzureAccountConfig azureAccountConfig = injector.getInstance(AzureAccountConfig.class);

    assertEquals(AZURE_ACCOUNT_NAME, azureAccountConfig.getAccount());
    assertEquals(AZURE_SHARED_ACCESS_TOKEN, azureAccountConfig.getSharedAccessStorageToken());
  }

  @Test
  public void testGetAzureDataSegmentConfigExpectedConfig()
  {
    final Injector injector = makeInjectorWithProperties(PROPERTIES);
    AzureDataSegmentConfig segmentConfig = injector.getInstance(AzureDataSegmentConfig.class);

    assertEquals(AZURE_CONTAINER, segmentConfig.getContainer());
    assertEquals(AZURE_PREFIX, segmentConfig.getPrefix());
  }

  @Test
  public void testGetAzureInputDataConfigExpectedConfig()
  {
    final Injector injector = makeInjectorWithProperties(PROPERTIES);
    AzureInputDataConfig inputDataConfig = injector.getInstance(AzureInputDataConfig.class);

    assertEquals(AZURE_MAX_LISTING_LENGTH, inputDataConfig.getMaxListingLength());
  }

  @Test
  public void testGetAzureByteSourceFactoryCanCreateAzureByteSource()
  {
    final Injector injector = makeInjectorWithProperties(PROPERTIES);
    AzureByteSourceFactory factory = injector.getInstance(AzureByteSourceFactory.class);
    Object object1 = factory.create("container1", "blob1", azureStorage);
    Object object2 = factory.create("container2", "blob2", azureStorage);
    assertNotNull(object1);
    assertNotNull(object2);
    assertNotSame(object1, object2);
  }

  @Test
  public void testGetAzureEntityFactoryCanCreateAzureEntity()
  {
    EasyMock.expect(cloudObjectLocation1.getBucket()).andReturn(AZURE_CONTAINER);
    EasyMock.expect(cloudObjectLocation2.getBucket()).andReturn(AZURE_CONTAINER);
    EasyMock.expect(cloudObjectLocation1.getPath()).andReturn(PATH).times(2);
    EasyMock.expect(cloudObjectLocation2.getPath()).andReturn(PATH);
    replayAll();

    final Injector injector = makeInjectorWithProperties(PROPERTIES);
    AzureEntityFactory factory = injector.getInstance(AzureEntityFactory.class);
    Object object1 = factory.create(cloudObjectLocation1, azureStorage, AzureInputSource.SCHEME);
    Object object2 = factory.create(cloudObjectLocation2, azureStorage, AzureInputSource.SCHEME);
    Object object3 = factory.create(cloudObjectLocation1, azureStorage, AzureStorageAccountInputSource.SCHEME);
    assertNotNull(object1);
    assertNotNull(object2);
    assertNotNull(object3);
    assertNotSame(object1, object2);
    assertNotSame(object1, object3);
  }

  @Test
  public void testGetAzureCloudBlobIteratorFactoryCanCreateAzureCloudBlobIterator()
  {
    final Injector injector = makeInjectorWithProperties(PROPERTIES);
    AzureCloudBlobIteratorFactory factory = injector.getInstance(AzureCloudBlobIteratorFactory.class);
    Object object1 = factory.create(EMPTY_PREFIXES_ITERABLE, 10, azureStorage);
    Object object2 = factory.create(EMPTY_PREFIXES_ITERABLE, 10, azureStorage);
    assertNotNull(object1);
    assertNotNull(object2);
    assertNotSame(object1, object2);
  }

  @Test
  public void testGetAzureCloudBlobIterableFactoryCanCreateAzureCloudBlobIterable()
  {
    final Injector injector = makeInjectorWithProperties(PROPERTIES);
    AzureCloudBlobIterableFactory factory = injector.getInstance(AzureCloudBlobIterableFactory.class);
    AzureCloudBlobIterable object1 = factory.create(EMPTY_PREFIXES_ITERABLE, 10, azureStorage);
    AzureCloudBlobIterable object2 = factory.create(EMPTY_PREFIXES_ITERABLE, 10, azureStorage);
    assertNotNull(object1);
    assertNotNull(object2);
    assertNotSame(object1, object2);
  }

  @Test
  public void testSegmentKillerBoundSingleton()
  {
    Injector injector = makeInjectorWithProperties(PROPERTIES);
    OmniDataSegmentKiller killer = injector.getInstance(OmniDataSegmentKiller.class);
    assertTrue(killer.getKillers().containsKey(AzureStorageDruidModule.SCHEME));
    assertSame(
        AzureDataSegmentKiller.class,
        killer.getKillers().get(AzureStorageDruidModule.SCHEME).get().getClass()
    );
    assertSame(
        killer.getKillers().get(AzureStorageDruidModule.SCHEME).get(),
        killer.getKillers().get(AzureStorageDruidModule.SCHEME).get()
    );
  }

  @ParameterizedTest
  @MethodSource("propertiesWithMultipleCredentials")
  public void testMultipleCredentialsSet(final Properties properties)
  {
    final ProvisionException exception = assertThrows(
        ProvisionException.class,
        () -> makeInjectorWithProperties(properties).getInstance(
            Key.get(new TypeLiteral<AzureClientFactory>()
            {
            })
        )
    );

    assertEquals(
        "Set only one of 'key' or 'sharedAccessStorageToken' or 'useAzureCredentialsChain' in the azure config. Please refer to azure documentation.",
        exception.getCause().getMessage()
    );
  }

  @Test
  public void testAllCredentialsUnset()
  {
    final Properties properties = initializePropertes();
    properties.remove("druid.azure.key");

    final ProvisionException exception = assertThrows(
        ProvisionException.class,
        () -> makeInjectorWithProperties(properties).getInstance(
            Key.get(new TypeLiteral<AzureClientFactory>()
            {
            })
        )
    );

    assertEquals(
        "Either set 'key' or 'sharedAccessStorageToken' or 'useAzureCredentialsChain' in the azure config. Please refer to azure documentation.",
        exception.getCause().getMessage()
    );
  }

  @Test
  public void testGetBlobStorageEndpointWithDefaultProperties()
  {
    Properties properties = initializePropertes();
    AzureAccountConfig config = makeInjectorWithProperties(properties).getInstance(AzureAccountConfig.class);
    assertNull(config.getEndpointSuffix());
    assertEquals(config.getStorageAccountEndpointSuffix(), AzureUtils.AZURE_STORAGE_HOST_ADDRESS);
    assertEquals(config.getBlobStorageEndpoint(), AzureUtils.AZURE_STORAGE_HOST_ADDRESS);
  }

  @Test
  public void testGetBlobStorageEndpointWithCustomBlobPath()
  {
    Properties properties = initializePropertes();
    final String customSuffix = "core.usgovcloudapi.net";
    properties.setProperty("druid.azure.endpointSuffix", customSuffix);
    AzureAccountConfig config = makeInjectorWithProperties(properties).getInstance(AzureAccountConfig.class);
    assertEquals(config.getEndpointSuffix(), customSuffix);
    assertEquals(config.getBlobStorageEndpoint(), "blob." + customSuffix);
  }

  private Injector makeInjectorWithProperties(final Properties props)
  {
    return Guice.createInjector(
        ImmutableList.of(
            new DruidGuiceExtensions(),
            new JacksonModule(),
            binder -> {
              binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
              binder.bind(JsonConfigurator.class).in(LazySingleton.class);
              binder.bind(Properties.class).toInstance(props);
            },
            new AzureStorageDruidModule()
        ));
  }

  private static Properties initializePropertes()
  {
    final Properties props = new Properties();
    props.setProperty("druid.azure.account", AZURE_ACCOUNT_NAME);
    props.setProperty("druid.azure.key", AZURE_ACCOUNT_KEY);
    props.setProperty("druid.azure.container", AZURE_CONTAINER);
    props.setProperty("druid.azure.prefix", AZURE_PREFIX);
    props.setProperty("druid.azure.maxListingLength", String.valueOf(AZURE_MAX_LISTING_LENGTH));
    return props;
  }

  private static Stream<Named<Properties>> propertiesWithMultipleCredentials()
  {
    final Properties propertiesWithKeyAndToken = initializePropertes();
    propertiesWithKeyAndToken.setProperty("druid.azure.sharedAccessStorageToken", AZURE_SHARED_ACCESS_TOKEN);

    final Properties propertiesWithKeyAndCredentialChain = initializePropertes();
    propertiesWithKeyAndCredentialChain.setProperty("druid.azure.useAzureCredentialsChain", Boolean.TRUE.toString());

    final Properties propertiesWithTokenAndCredentialChain = initializePropertes();
    propertiesWithTokenAndCredentialChain.remove("druid.azure.key");
    propertiesWithTokenAndCredentialChain.setProperty("druid.azure.useAzureCredentialsChain", Boolean.TRUE.toString());
    propertiesWithTokenAndCredentialChain.setProperty("druid.azure.sharedAccessStorageToken", AZURE_SHARED_ACCESS_TOKEN);

    return Stream.of(
        Named.of("Key and storage token", propertiesWithKeyAndToken),
        Named.of("Key and credential chain", propertiesWithKeyAndCredentialChain),
        Named.of("Storage token and credential chain", propertiesWithTokenAndCredentialChain)
    );
  }
}
