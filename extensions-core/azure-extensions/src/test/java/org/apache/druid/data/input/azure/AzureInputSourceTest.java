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

import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.azure.AzureCloudBlobHolderToCloudObjectLocationConverter;
import org.apache.druid.storage.azure.AzureCloudBlobIterable;
import org.apache.druid.storage.azure.AzureCloudBlobIterableFactory;
import org.apache.druid.storage.azure.AzureInputDataConfig;
import org.apache.druid.storage.azure.AzureStorage;
import org.apache.druid.storage.azure.blob.CloudBlobHolder;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AzureInputSourceTest extends EasyMockSupport
{
  private static final String CONTAINER_NAME = "container";
  private static final String BLOB_NAME = "blob";
  private static final URI PREFIX_URI;
  private final List<URI> EMPTY_URIS = ImmutableList.of();
  private final List<URI> EMPTY_PREFIXES = ImmutableList.of();
  private final List<CloudObjectLocation> EMPTY_OBJECTS = ImmutableList.of();
  private static final String CONTAINER = "CONTAINER";
  private static final String BLOB_PATH = "BLOB_PATH";
  private static final CloudObjectLocation CLOUD_OBJECT_LOCATION_1 = new CloudObjectLocation(CONTAINER, BLOB_PATH);
  private static final int MAX_LISTING_LENGTH = 10;

  private AzureStorage storage;
  private AzureEntityFactory entityFactory;
  private AzureCloudBlobIterableFactory azureCloudBlobIterableFactory;
  private AzureCloudBlobHolderToCloudObjectLocationConverter azureCloudBlobToLocationConverter;
  private AzureInputDataConfig inputDataConfig;

  private InputSplit<List<CloudObjectLocation>> inputSplit;
  private AzureEntity azureEntity1;
  private CloudBlobHolder cloudBlobDruid1;
  private AzureCloudBlobIterable azureCloudBlobIterable;

  private AzureInputSource azureInputSource;

  static {
    try {
      PREFIX_URI = new URI(AzureInputSource.SCHEME + "://" + CONTAINER_NAME + "/" + BLOB_NAME);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setup()
  {
    storage = createMock(AzureStorage.class);
    entityFactory = createMock(AzureEntityFactory.class);
    inputSplit = createMock(InputSplit.class);
    azureEntity1 = createMock(AzureEntity.class);
    azureCloudBlobIterableFactory = createMock(AzureCloudBlobIterableFactory.class);
    azureCloudBlobToLocationConverter = createMock(AzureCloudBlobHolderToCloudObjectLocationConverter.class);
    inputDataConfig = createMock(AzureInputDataConfig.class);
    cloudBlobDruid1 = createMock(CloudBlobHolder.class);
    azureCloudBlobIterable = createMock(AzureCloudBlobIterable.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void test_constructor_emptyUrisEmptyPrefixesEmptyObjects_throwsIllegalArgumentException()
  {
    replayAll();
    azureInputSource = new AzureInputSource(
        storage,
        entityFactory,
        azureCloudBlobIterableFactory,
        azureCloudBlobToLocationConverter,
        inputDataConfig,
        EMPTY_URIS,
        EMPTY_PREFIXES,
        EMPTY_OBJECTS
    );
  }

  @Test
  public void test_createEntity_returnsExpectedEntity()
  {
    EasyMock.expect(entityFactory.create(CLOUD_OBJECT_LOCATION_1)).andReturn(azureEntity1);
    EasyMock.expect(inputSplit.get()).andReturn(ImmutableList.of(CLOUD_OBJECT_LOCATION_1)).times(2);
    replayAll();

    List<CloudObjectLocation> objects = ImmutableList.of(CLOUD_OBJECT_LOCATION_1);
    azureInputSource = new AzureInputSource(
        storage,
        entityFactory,
        azureCloudBlobIterableFactory,
        azureCloudBlobToLocationConverter,
        inputDataConfig,
        EMPTY_URIS,
        EMPTY_PREFIXES,
        objects
    );

    Assert.assertEquals(1, inputSplit.get().size());
    AzureEntity actualAzureEntity = azureInputSource.createEntity(inputSplit.get().get(0));
    Assert.assertSame(azureEntity1, actualAzureEntity);
    verifyAll();
  }

  @Test
  public void test_getPrefixesSplitStream_successfullyCreatesCloudLocation_returnsExpectedLocations()
  {
    List<URI> prefixes = ImmutableList.of(PREFIX_URI);
    List<List<CloudObjectLocation>> expectedCloudLocations = ImmutableList.of(ImmutableList.of(CLOUD_OBJECT_LOCATION_1));
    List<CloudBlobHolder> expectedCloudBlobs = ImmutableList.of(cloudBlobDruid1);
    Iterator<CloudBlobHolder> expectedCloudBlobsIterator = expectedCloudBlobs.iterator();
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_LISTING_LENGTH);
    EasyMock.expect(azureCloudBlobIterableFactory.create(prefixes, MAX_LISTING_LENGTH)).andReturn(
        azureCloudBlobIterable);
    EasyMock.expect(azureCloudBlobIterable.iterator()).andReturn(expectedCloudBlobsIterator);
    EasyMock.expect(azureCloudBlobToLocationConverter.createCloudObjectLocation(cloudBlobDruid1))
            .andReturn(CLOUD_OBJECT_LOCATION_1);
    EasyMock.expect(cloudBlobDruid1.getBlobLength()).andReturn(100L).anyTimes();
    replayAll();

    azureInputSource = new AzureInputSource(
        storage,
        entityFactory,
        azureCloudBlobIterableFactory,
        azureCloudBlobToLocationConverter,
        inputDataConfig,
        EMPTY_URIS,
        prefixes,
        EMPTY_OBJECTS
    );

    Stream<InputSplit<List<CloudObjectLocation>>> cloudObjectStream = azureInputSource.getPrefixesSplitStream(
        new MaxSizeSplitHintSpec(1L)
    );

    List<List<CloudObjectLocation>> actualCloudLocationList = cloudObjectStream.map(InputSplit::get)
                                                                               .collect(Collectors.toList());
    verifyAll();
    Assert.assertEquals(expectedCloudLocations, actualCloudLocationList);
  }

  @Test
  public void test_withSplit_constructsExpectedInputSource()
  {
    List<URI> prefixes = ImmutableList.of(PREFIX_URI);
    EasyMock.expect(inputSplit.get()).andReturn(ImmutableList.of(CLOUD_OBJECT_LOCATION_1));
    replayAll();

    azureInputSource = new AzureInputSource(
        storage,
        entityFactory,
        azureCloudBlobIterableFactory,
        azureCloudBlobToLocationConverter,
        inputDataConfig,
        EMPTY_URIS,
        prefixes,
        EMPTY_OBJECTS
    );

    SplittableInputSource<List<CloudObjectLocation>> newInputSource = azureInputSource.withSplit(inputSplit);
    Assert.assertTrue(newInputSource.isSplittable());
    verifyAll();
  }

  @Test
  public void test_toString_returnsExpectedString()
  {
    List<URI> prefixes = ImmutableList.of(PREFIX_URI);
    azureInputSource = new AzureInputSource(
        storage,
        entityFactory,
        azureCloudBlobIterableFactory,
        azureCloudBlobToLocationConverter,
        inputDataConfig,
        EMPTY_URIS,
        prefixes,
        EMPTY_OBJECTS
    );

    String actualToString = azureInputSource.toString();
    Assert.assertEquals("AzureInputSource{uris=[], prefixes=[azure://container/blob], objects=[]}", actualToString);
  }

  @Test
  public void abidesEqualsContract()
  {
    EqualsVerifier.forClass(AzureInputSource.class)
                  .usingGetClass()
                  .withPrefabValues(Logger.class, new Logger(AzureStorage.class), new Logger(AzureStorage.class))
                  .withNonnullFields("storage")
                  .withNonnullFields("entityFactory")
                  .withNonnullFields("azureCloudBlobIterableFactory")
                  .withNonnullFields("azureCloudBlobToLocationConverter")
                  .withNonnullFields("inputDataConfig")
                  .verify();
  }

  @After
  public void cleanup()
  {
    resetAll();
  }
}
