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

package org.apache.druid.data.input.azure;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.systemfield.SystemField;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.storage.azure.AzureAccountConfig;
import org.apache.druid.storage.azure.AzureCloudBlobIterableFactory;
import org.apache.druid.storage.azure.AzureDataSegmentConfig;
import org.apache.druid.storage.azure.AzureInputDataConfig;
import org.apache.druid.storage.azure.AzureStorage;
import org.apache.druid.storage.azure.AzureStorageDruidModule;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;

public class AzureInputSourceSerdeTest extends EasyMockSupport
{
  private static final String JSON_WITH_URIS = "{\n"
                                               + "        \"type\": \"azure\",\n"
                                               + "        \"uris\": [\"azure://datacontainer2/wikipedia.json\"]\n"
                                               + "}";

  private static final String JSON_WITH_PREFIXES = "{\n"
                                                   + "        \"type\": \"azure\",\n"
                                                   + "        \"prefixes\": [\"azure://datacontainer2\"]\n"
                                                   + "}";

  private static final String JSON_WITH_OBJECTS = "{\n"
                                                  + "        \"type\": \"azure\",\n"
                                                  + "        \"objects\": [\n"
                                                  + "          { \"bucket\": \"container1\", \"path\": \"bar/file1.json\"},\n"
                                                  + "          { \"bucket\": \"conatiner2\", \"path\": \"foo/file2.json\"}\n"
                                                  + "        ]\n"
                                                  + "      }";

  private static final String JSON_WITH_URIS_AND_SYSFIELDS =
      "{\n"
      + "        \"type\": \"azure\",\n"
      + "        \"uris\": [\"azure://datacontainer2/wikipedia.json\"],\n"
      + "        \"systemFields\": [\"__file_uri\"]\n"
      + "}";

  private static final List<URI> EXPECTED_URIS;
  private static final List<URI> EXPECTED_PREFIXES;
  private static final List<CloudObjectLocation> EXPECTED_CLOUD_OBJECTS;

  private AzureStorage azureStorage;
  private AzureEntityFactory entityFactory;
  private AzureCloudBlobIterableFactory azureCloudBlobIterableFactory;
  private AzureInputDataConfig inputDataConfig;
  private AzureAccountConfig accountConfig;


  static {
    try {
      EXPECTED_URIS = ImmutableList.of(new URI("azure://datacontainer2/wikipedia.json"));
      EXPECTED_PREFIXES = ImmutableList.of(new URI("azure://datacontainer2"));
      EXPECTED_CLOUD_OBJECTS = ImmutableList.of(
          new CloudObjectLocation("container1", "bar/file1.json"),
          new CloudObjectLocation("conatiner2", "foo/file2.json")
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setup()
  {
    azureStorage = createMock(AzureStorage.class);
    entityFactory = createMock(AzureEntityFactory.class);
    azureCloudBlobIterableFactory = createMock(AzureCloudBlobIterableFactory.class);
    inputDataConfig = createMock(AzureInputDataConfig.class);
    accountConfig = createMock(AzureAccountConfig.class);
  }

  @Test
  public void test_uriSerde_constructsProperAzureInputSource() throws Exception
  {
    final InjectableValues.Std injectableValues = initInjectableValues();
    final ObjectMapper objectMapper = new DefaultObjectMapper()
        .registerModules(new AzureStorageDruidModule().getJacksonModules());
    objectMapper.setInjectableValues(injectableValues);

    final AzureInputSource inputSource = objectMapper.readValue(JSON_WITH_URIS, AzureInputSource.class);
    verifyInputSourceWithUris(inputSource);

    final AzureInputSource roundTripInputSource = objectMapper.readValue(
        objectMapper.writeValueAsBytes(inputSource),
        AzureInputSource.class);
    verifyInputSourceWithUris(roundTripInputSource);

  }

  @Test
  public void test_uriAndSystemFieldsSerde_constructsProperAzureInputSource() throws Exception
  {
    final InjectableValues.Std injectableValues = initInjectableValues();
    final ObjectMapper objectMapper = new DefaultObjectMapper()
        .registerModules(new AzureStorageDruidModule().getJacksonModules());
    objectMapper.setInjectableValues(injectableValues);

    final AzureInputSource inputSource = objectMapper.readValue(JSON_WITH_URIS_AND_SYSFIELDS, AzureInputSource.class);
    Assert.assertEquals(Collections.singleton(SystemField.URI), inputSource.getConfiguredSystemFields());

    final AzureInputSource roundTripInputSource = objectMapper.readValue(
        objectMapper.writeValueAsBytes(inputSource),
        AzureInputSource.class);
    Assert.assertEquals(Collections.singleton(SystemField.URI), roundTripInputSource.getConfiguredSystemFields());
  }

  @Test
  public void test_prefixSerde_constructsProperAzureInputSource() throws Exception
  {
    final InjectableValues.Std injectableValues = initInjectableValues();
    injectableValues.addValue(AzureDataSegmentConfig.class, inputDataConfig);
    final ObjectMapper objectMapper = new DefaultObjectMapper()
        .registerModules(new AzureStorageDruidModule().getJacksonModules());
    objectMapper.setInjectableValues(injectableValues);

    final AzureInputSource inputSource = objectMapper.readValue(JSON_WITH_PREFIXES, AzureInputSource.class);
    verifyInputSourceWithPrefixes(inputSource);

    final AzureInputSource roundTripInputSource = objectMapper.readValue(
        objectMapper.writeValueAsBytes(inputSource),
        AzureInputSource.class);
    verifyInputSourceWithPrefixes(roundTripInputSource);

  }

  @Test
  public void test_objectSerde_constructsProperAzureInputSource() throws Exception
  {
    final InjectableValues.Std injectableValues = initInjectableValues();
    final ObjectMapper objectMapper = new DefaultObjectMapper()
        .registerModules(new AzureStorageDruidModule().getJacksonModules());
    objectMapper.setInjectableValues(injectableValues);

    final AzureInputSource inputSource = objectMapper.readValue(JSON_WITH_OBJECTS, AzureInputSource.class);
    verifyInputSourceWithObjects(inputSource);

    final AzureInputSource roundTripInputSource = objectMapper.readValue(
        objectMapper.writeValueAsBytes(inputSource),
        AzureInputSource.class);
    verifyInputSourceWithObjects(roundTripInputSource);
  }

  private InjectableValues.Std initInjectableValues()
  {
    final InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(AzureStorage.class, azureStorage);
    injectableValues.addValue(AzureEntityFactory.class, entityFactory);
    injectableValues.addValue(AzureCloudBlobIterableFactory.class, azureCloudBlobIterableFactory);
    injectableValues.addValue(AzureInputDataConfig.class, inputDataConfig);
    injectableValues.addValue(AzureAccountConfig.class, accountConfig);
    return injectableValues;
  }

  private static void verifyInputSourceWithUris(final AzureInputSource inputSource)
  {

    Assert.assertEquals(EXPECTED_URIS, inputSource.getUris());
    Assert.assertNull(inputSource.getPrefixes());
    Assert.assertNull(inputSource.getObjects());
  }

  private static void verifyInputSourceWithPrefixes(final AzureInputSource inputSource)
  {

    Assert.assertNull(inputSource.getUris());
    Assert.assertEquals(EXPECTED_PREFIXES, inputSource.getPrefixes());
    Assert.assertNull(inputSource.getObjects());
  }

  private static void verifyInputSourceWithObjects(final AzureInputSource inputSource)
  {
    Assert.assertNull(inputSource.getUris());
    Assert.assertNull(inputSource.getPrefixes());
    Assert.assertEquals(EXPECTED_CLOUD_OBJECTS, inputSource.getObjects());
  }
}
