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

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.data.input.impl.systemfield.SystemField;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
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
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.EnumSet;
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
  private static final String BLOB_PATH = "BLOB_PATH.csv";
  private static final CloudObjectLocation CLOUD_OBJECT_LOCATION_1 = new CloudObjectLocation(CONTAINER, BLOB_PATH);
  private static final int MAX_LISTING_LENGTH = 10;

  private static final InputFormat INPUT_FORMAT = new JsonInputFormat(
      new JSONPathSpec(true, null),
      null,
      false,
      null,
      null
  );

  private AzureStorage storage;
  private AzureEntityFactory entityFactory;
  private AzureCloudBlobIterableFactory azureCloudBlobIterableFactory;
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
        inputDataConfig,
        EMPTY_URIS,
        EMPTY_PREFIXES,
        EMPTY_OBJECTS,
        null,
        null
    );
  }

  @Test
  public void test_createEntity_returnsExpectedEntity()
  {
    EasyMock.expect(entityFactory.create(CLOUD_OBJECT_LOCATION_1, storage, AzureInputSource.SCHEME)).andReturn(azureEntity1);
    EasyMock.expect(inputSplit.get()).andReturn(ImmutableList.of(CLOUD_OBJECT_LOCATION_1)).times(2);
    replayAll();

    List<CloudObjectLocation> objects = ImmutableList.of(CLOUD_OBJECT_LOCATION_1);
    azureInputSource = new AzureInputSource(
        storage,
        entityFactory,
        azureCloudBlobIterableFactory,
        inputDataConfig,
        EMPTY_URIS,
        EMPTY_PREFIXES,
        objects,
        null,
        null
    );

    Assert.assertEquals(1, inputSplit.get().size());
    AzureEntity actualAzureEntity = azureInputSource.createEntity(inputSplit.get().get(0));
    Assert.assertSame(azureEntity1, actualAzureEntity);
    verifyAll();
  }

  @Test
  public void test_createSplits_successfullyCreatesCloudLocation_returnsExpectedLocations()
  {
    List<URI> prefixes = ImmutableList.of(PREFIX_URI);
    List<List<CloudObjectLocation>> expectedCloudLocations = ImmutableList.of(ImmutableList.of(CLOUD_OBJECT_LOCATION_1));
    List<CloudBlobHolder> expectedCloudBlobs = ImmutableList.of(cloudBlobDruid1);
    Iterator<CloudBlobHolder> expectedCloudBlobsIterator = expectedCloudBlobs.iterator();
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_LISTING_LENGTH);
    EasyMock.expect(azureCloudBlobIterableFactory.create(prefixes, MAX_LISTING_LENGTH, storage)).andReturn(
        azureCloudBlobIterable);
    EasyMock.expect(azureCloudBlobIterable.iterator()).andReturn(expectedCloudBlobsIterator);
    EasyMock.expect(cloudBlobDruid1.getContainerName()).andReturn(CONTAINER).anyTimes();
    EasyMock.expect(cloudBlobDruid1.getName()).andReturn(BLOB_PATH).anyTimes();
    EasyMock.expect(cloudBlobDruid1.getBlobLength()).andReturn(100L).anyTimes();
    replayAll();

    azureInputSource = new AzureInputSource(
        storage,
        entityFactory,
        azureCloudBlobIterableFactory,
        inputDataConfig,
        EMPTY_URIS,
        prefixes,
        EMPTY_OBJECTS,
        null,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> cloudObjectStream = azureInputSource.createSplits(
        INPUT_FORMAT,
        new MaxSizeSplitHintSpec(null, 1)
    );

    List<List<CloudObjectLocation>> actualCloudLocationList = cloudObjectStream.map(InputSplit::get)
                                                                               .collect(Collectors.toList());
    verifyAll();
    Assert.assertEquals(expectedCloudLocations, actualCloudLocationList);
  }

  @Test
  public void test_getPrefixesSplitStream_withObjectGlob_successfullyCreatesCloudLocation_returnsExpectedLocations()
  {
    List<URI> prefixes = ImmutableList.of(PREFIX_URI);
    List<List<CloudObjectLocation>> expectedCloudLocations = ImmutableList.of(ImmutableList.of(CLOUD_OBJECT_LOCATION_1));
    List<CloudBlobHolder> expectedCloudBlobs = ImmutableList.of(cloudBlobDruid1);
    Iterator<CloudBlobHolder> expectedCloudBlobsIterator = expectedCloudBlobs.iterator();
    String objectGlob = "**.csv";

    PathMatcher m = FileSystems.getDefault().getPathMatcher("glob:" + objectGlob);

    expectedCloudBlobsIterator = Iterators.filter(
        expectedCloudBlobsIterator,
        object -> m.matches(Paths.get(object.getName()))
    );

    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_LISTING_LENGTH);
    EasyMock.expect(azureCloudBlobIterableFactory.create(prefixes, MAX_LISTING_LENGTH, storage)).andReturn(
        azureCloudBlobIterable);
    EasyMock.expect(azureCloudBlobIterable.iterator()).andReturn(expectedCloudBlobsIterator);
    EasyMock.expect(cloudBlobDruid1.getBlobLength()).andReturn(100L).anyTimes();
    EasyMock.expect(cloudBlobDruid1.getContainerName()).andReturn(CONTAINER).anyTimes();
    EasyMock.expect(cloudBlobDruid1.getName()).andReturn(BLOB_PATH).anyTimes();

    replayAll();

    azureInputSource = new AzureInputSource(
        storage,
        entityFactory,
        azureCloudBlobIterableFactory,
        inputDataConfig,
        EMPTY_URIS,
        prefixes,
        EMPTY_OBJECTS,
        objectGlob,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> cloudObjectStream = azureInputSource.createSplits(
        INPUT_FORMAT,
        new MaxSizeSplitHintSpec(null, 1)
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
        inputDataConfig,
        EMPTY_URIS,
        prefixes,
        EMPTY_OBJECTS,
        null,
        null
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
        inputDataConfig,
        EMPTY_URIS,
        prefixes,
        EMPTY_OBJECTS,
        null,
        null
    );

    String actualToString = azureInputSource.toString();
    Assert.assertEquals(
        "AzureInputSource{uris=[], prefixes=[azure://container/blob], objects=[], objectGlob=null}",
        actualToString
    );
  }

  @Test
  public void test_toString_withAllSystemFields_returnsExpectedString()
  {
    List<URI> prefixes = ImmutableList.of(PREFIX_URI);
    azureInputSource = new AzureInputSource(
        storage,
        entityFactory,
        azureCloudBlobIterableFactory,
        inputDataConfig,
        EMPTY_URIS,
        prefixes,
        EMPTY_OBJECTS,
        null,
        new SystemFields(EnumSet.of(SystemField.URI, SystemField.BUCKET, SystemField.PATH))
    );

    String actualToString = azureInputSource.toString();
    Assert.assertEquals(
        "AzureInputSource{"
        + "uris=[], "
        + "prefixes=[azure://container/blob], "
        + "objects=[], "
        + "objectGlob=null, "
        + "systemFields=[__file_uri, __file_bucket, __file_path]"
        + "}",
        actualToString
    );
  }

  @Test
  public void test_getTypes_returnsExpectedTypes()
  {
    List<URI> prefixes = ImmutableList.of(PREFIX_URI);
    azureInputSource = new AzureInputSource(
        storage,
        entityFactory,
        azureCloudBlobIterableFactory,
        inputDataConfig,
        EMPTY_URIS,
        prefixes,
        EMPTY_OBJECTS,
        null,
        null
    );
    Assert.assertEquals(ImmutableSet.of(AzureInputSource.SCHEME), azureInputSource.getTypes());
  }

  @Test
  public void test_systemFields()
  {
    azureInputSource = (AzureInputSource) new AzureInputSource(
        storage,
        entityFactory,
        azureCloudBlobIterableFactory,
        inputDataConfig,
        EMPTY_URIS,
        ImmutableList.of(PREFIX_URI),
        EMPTY_OBJECTS,
        null,
        new SystemFields(EnumSet.of(SystemField.URI, SystemField.BUCKET, SystemField.PATH))
    );

    Assert.assertEquals(
        EnumSet.of(SystemField.URI, SystemField.BUCKET, SystemField.PATH),
        azureInputSource.getConfiguredSystemFields()
    );

    final AzureEntity entity = new AzureEntity(
        new CloudObjectLocation("foo", "bar"),
        storage,
        AzureInputSource.SCHEME,
        (containerName, blobPath, storage) -> null
    );

    Assert.assertEquals("azure://foo/bar", azureInputSource.getSystemFieldValue(entity, SystemField.URI));
    Assert.assertEquals("foo", azureInputSource.getSystemFieldValue(entity, SystemField.BUCKET));
    Assert.assertEquals("bar", azureInputSource.getSystemFieldValue(entity, SystemField.PATH));
  }

  @Test
  public void abidesEqualsContract()
  {
    EqualsVerifier.forClass(AzureInputSource.class)
                  .usingGetClass()
                  .withPrefabValues(Logger.class, new Logger(AzureStorage.class), new Logger(AzureStorage.class))
                  .withPrefabValues(BlobContainerClient.class, new BlobContainerClientBuilder().buildClient(), new BlobContainerClientBuilder().buildClient())
                  .withPrefabValues(AzureStorage.class, new AzureStorage(null, null), new AzureStorage(null, null))
                  .withNonnullFields("storage")
                  .withNonnullFields("entityFactory")
                  .withNonnullFields("azureCloudBlobIterableFactory")
                  .withNonnullFields("inputDataConfig")
                  .withNonnullFields("objectGlob")
                  .withNonnullFields("scheme")
                  .verify();
  }

  @After
  public void cleanup()
  {
    resetAll();
  }
}
