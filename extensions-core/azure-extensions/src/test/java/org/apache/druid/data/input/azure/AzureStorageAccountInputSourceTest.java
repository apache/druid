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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.storage.azure.AzureAccountConfig;
import org.apache.druid.storage.azure.AzureCloudBlobIterable;
import org.apache.druid.storage.azure.AzureCloudBlobIterableFactory;
import org.apache.druid.storage.azure.AzureIngestClientFactory;
import org.apache.druid.storage.azure.AzureInputDataConfig;
import org.apache.druid.storage.azure.AzureStorage;
import org.apache.druid.storage.azure.blob.CloudBlobHolder;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AzureStorageAccountInputSourceTest extends EasyMockSupport
{
  private static final String BLOB_NAME = "blob";
  private static final URI PREFIX_URI;
  private static final String STORAGE_ACCOUNT = "STORAGE_ACCOUNT";
  private static final String DEFAULT_STORAGE_ACCOUNT = "DEFAULT_STORAGE_ACCOUNT";
  private static final String CONTAINER = "CONTAINER";
  private static final String BLOB_PATH = "BLOB_PATH.csv";
  private static final CloudObjectLocation CLOUD_OBJECT_LOCATION_1 = new CloudObjectLocation(
      STORAGE_ACCOUNT,
      CONTAINER + "/" + BLOB_PATH
  );
  private static final int MAX_LISTING_LENGTH = 10;
  private static final InputFormat INPUT_FORMAT = new JsonInputFormat(
      new JSONPathSpec(true, null),
      null,
      false,
      null,
      null
  );

  private final List<URI> EMPTY_URIS = ImmutableList.of();
  private final List<URI> EMPTY_PREFIXES = ImmutableList.of();
  private final List<CloudObjectLocation> EMPTY_OBJECTS = ImmutableList.of();
  private AzureStorage storage;
  private AzureEntityFactory entityFactory;
  private AzureCloudBlobIterableFactory azureCloudBlobIterableFactory;
  private AzureInputDataConfig inputDataConfig;
  private AzureStorageAccountInputSourceConfig azureStorageAccountInputSourceConfig;
  private AzureAccountConfig azureAccountConfig;
  private InputSplit<List<CloudObjectLocation>> inputSplit;
  private AzureEntity azureEntity;
  private CloudBlobHolder cloudBlobDruid;
  private AzureCloudBlobIterable azureCloudBlobIterable;

  static {
    try {
      PREFIX_URI = new URI(
          AzureStorageAccountInputSource.SCHEME + "://" + STORAGE_ACCOUNT + "/" + CONTAINER + "/" + BLOB_NAME
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeEach
  public void setup()
  {
    storage = createMock(AzureStorage.class);
    entityFactory = createMock(AzureEntityFactory.class);
    inputSplit = createMock(InputSplit.class);
    azureEntity = createMock(AzureEntity.class);
    azureCloudBlobIterableFactory = createMock(AzureCloudBlobIterableFactory.class);
    inputDataConfig = createMock(AzureInputDataConfig.class);
    cloudBlobDruid = createMock(CloudBlobHolder.class);
    azureCloudBlobIterable = createMock(AzureCloudBlobIterable.class);
    azureStorageAccountInputSourceConfig = createMock(AzureStorageAccountInputSourceConfig.class);
    azureAccountConfig = createMock(AzureAccountConfig.class);
    EasyMock.expect(azureAccountConfig.getAccount()).andReturn(DEFAULT_STORAGE_ACCOUNT).anyTimes();
  }

  @Test
  public void test_constructor_emptyUrisEmptyPrefixesEmptyObjects_throwsIllegalArgumentException()
  {
    replayAll();

    //noinspection ResultOfObjectAllocationIgnored
    assertThrows(
        IllegalArgumentException.class,
        () -> new AzureStorageAccountInputSource(
            entityFactory,
            azureCloudBlobIterableFactory,
            inputDataConfig,
            azureAccountConfig,
            EMPTY_URIS,
            EMPTY_PREFIXES,
            EMPTY_OBJECTS,
            null,
            azureStorageAccountInputSourceConfig,
            null
        )
    );
  }

  @Test
  public void test_createEntity_returnsExpectedEntity()
  {
    EasyMock.expect(
        entityFactory.create(
            EasyMock.eq(CLOUD_OBJECT_LOCATION_1),
            EasyMock.anyObject(AzureStorage.class),
            EasyMock.eq(AzureStorageAccountInputSource.SCHEME)
        )
    ).andReturn(azureEntity);
    EasyMock.expect(inputSplit.get()).andReturn(ImmutableList.of(CLOUD_OBJECT_LOCATION_1)).times(2);
    replayAll();

    List<CloudObjectLocation> objects = ImmutableList.of(CLOUD_OBJECT_LOCATION_1);
    final AzureStorageAccountInputSource azureInputSource = new AzureStorageAccountInputSource(
        entityFactory,
        azureCloudBlobIterableFactory,
        inputDataConfig,
        azureAccountConfig,
        EMPTY_URIS,
        EMPTY_PREFIXES,
        objects,
        null,
        azureStorageAccountInputSourceConfig,
        null
    );

    assertEquals(1, inputSplit.get().size());
    AzureEntity actualAzureEntity = azureInputSource.createEntity(inputSplit.get().get(0));
    assertSame(azureEntity, actualAzureEntity);
    verifyAll();
  }

  @Test
  public void test_createSplits_successfullyCreatesCloudLocation_returnsExpectedLocations()
  {
    List<URI> prefixes = ImmutableList.of(PREFIX_URI);
    List<List<CloudObjectLocation>> expectedCloudLocations = ImmutableList.of(ImmutableList.of(CLOUD_OBJECT_LOCATION_1));
    List<CloudBlobHolder> expectedCloudBlobs = ImmutableList.of(cloudBlobDruid);
    Iterator<CloudBlobHolder> expectedCloudBlobsIterator = expectedCloudBlobs.iterator();
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_LISTING_LENGTH);
    EasyMock.expect(
        azureCloudBlobIterableFactory.create(
            EasyMock.eq(prefixes),
            EasyMock.eq(MAX_LISTING_LENGTH),
            EasyMock.anyObject(AzureStorage.class)
        )
    ).andReturn(azureCloudBlobIterable);
    EasyMock.expect(azureCloudBlobIterable.iterator()).andReturn(expectedCloudBlobsIterator);
    EasyMock.expect(cloudBlobDruid.getStorageAccount()).andReturn(STORAGE_ACCOUNT).anyTimes();
    EasyMock.expect(cloudBlobDruid.getContainerName()).andReturn(CONTAINER).anyTimes();
    EasyMock.expect(cloudBlobDruid.getName()).andReturn(BLOB_PATH).anyTimes();
    EasyMock.expect(cloudBlobDruid.getBlobLength()).andReturn(100L).anyTimes();
    replayAll();

    final AzureStorageAccountInputSource azureInputSource = new AzureStorageAccountInputSource(
        entityFactory,
        azureCloudBlobIterableFactory,
        inputDataConfig,
        azureAccountConfig,
        EMPTY_URIS,
        prefixes,
        EMPTY_OBJECTS,
        null,
        azureStorageAccountInputSourceConfig,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> cloudObjectStream = azureInputSource.createSplits(
        INPUT_FORMAT,
        new MaxSizeSplitHintSpec(null, 1)
    );

    List<List<CloudObjectLocation>> actualCloudLocationList = cloudObjectStream
        .map(InputSplit::get)
        .collect(Collectors.toList());
    verifyAll();
    assertEquals(expectedCloudLocations, actualCloudLocationList);
  }

  @Test
  public void test_getPrefixesSplitStream_withObjectGlob_successfullyCreatesCloudLocation_returnsExpectedLocations()
  {
    List<URI> prefixes = ImmutableList.of(PREFIX_URI);
    List<List<CloudObjectLocation>> expectedCloudLocations = ImmutableList.of(ImmutableList.of(CLOUD_OBJECT_LOCATION_1));
    List<CloudBlobHolder> expectedCloudBlobs = ImmutableList.of(cloudBlobDruid);
    Iterator<CloudBlobHolder> expectedCloudBlobsIterator = expectedCloudBlobs.iterator();
    String objectGlob = "**.csv";

    PathMatcher m = FileSystems.getDefault().getPathMatcher("glob:" + objectGlob);

    expectedCloudBlobsIterator = Iterators.filter(
        expectedCloudBlobsIterator,
        object -> m.matches(Paths.get(object.getName()))
    );

    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_LISTING_LENGTH);
    EasyMock.expect(
        azureCloudBlobIterableFactory.create(
            EasyMock.eq(prefixes),
            EasyMock.eq(MAX_LISTING_LENGTH),
            EasyMock.anyObject(AzureStorage.class)
        )
    ).andReturn(azureCloudBlobIterable);
    EasyMock.expect(azureCloudBlobIterable.iterator()).andReturn(expectedCloudBlobsIterator);
    EasyMock.expect(cloudBlobDruid.getStorageAccount()).andReturn(STORAGE_ACCOUNT).anyTimes();
    EasyMock.expect(cloudBlobDruid.getBlobLength()).andReturn(100L).anyTimes();
    EasyMock.expect(cloudBlobDruid.getContainerName()).andReturn(CONTAINER).anyTimes();
    EasyMock.expect(cloudBlobDruid.getName()).andReturn(BLOB_PATH).anyTimes();

    replayAll();

    final AzureStorageAccountInputSource azureInputSource = new AzureStorageAccountInputSource(
        entityFactory,
        azureCloudBlobIterableFactory,
        inputDataConfig,
        azureAccountConfig,
        EMPTY_URIS,
        prefixes,
        EMPTY_OBJECTS,
        objectGlob,
        azureStorageAccountInputSourceConfig,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> cloudObjectStream = azureInputSource.createSplits(
        INPUT_FORMAT,
        new MaxSizeSplitHintSpec(null, 1)
    );

    List<List<CloudObjectLocation>> actualCloudLocationList = cloudObjectStream
        .map(InputSplit::get)
        .collect(Collectors.toList());
    verifyAll();
    assertEquals(expectedCloudLocations, actualCloudLocationList);
  }

  @Test
  public void test_withSplit_constructsExpectedInputSource()
  {
    List<URI> prefixes = ImmutableList.of(PREFIX_URI);
    EasyMock.expect(inputSplit.get()).andReturn(ImmutableList.of(CLOUD_OBJECT_LOCATION_1));
    replayAll();

    final AzureStorageAccountInputSource azureInputSource = new AzureStorageAccountInputSource(
        entityFactory,
        azureCloudBlobIterableFactory,
        inputDataConfig,
        azureAccountConfig,
        EMPTY_URIS,
        prefixes,
        EMPTY_OBJECTS,
        null,
        azureStorageAccountInputSourceConfig,
        null
    );

    SplittableInputSource<List<CloudObjectLocation>> newInputSource = azureInputSource.withSplit(inputSplit);
    assertTrue(newInputSource.isSplittable());
    verifyAll();
  }

  @Test
  public void test_toString_returnsExpectedString()
  {
    final AzureStorageAccountInputSource azureInputSource = new AzureStorageAccountInputSource(
        entityFactory,
        azureCloudBlobIterableFactory,
        inputDataConfig,
        azureAccountConfig,
        EMPTY_URIS,
        ImmutableList.of(PREFIX_URI),
        EMPTY_OBJECTS,
        null,
        azureStorageAccountInputSourceConfig,
        null
    );

    assertEquals(
        String.format(
            Locale.ENGLISH,
            "AzureStorageAccountInputSource{uris=[], prefixes=[azureStorage://STORAGE_ACCOUNT/CONTAINER/blob], objects=[], objectGlob=null, azureStorageAccountInputSourceConfig=%s}",
            azureStorageAccountInputSourceConfig
        ),
        azureInputSource.toString()
    );
  }

  @Test
  public void test_toString_withAllSystemFields_returnsExpectedString()
  {
    List<URI> prefixes = ImmutableList.of(PREFIX_URI);
    final AzureStorageAccountInputSource azureInputSource = new AzureStorageAccountInputSource(
        entityFactory,
        azureCloudBlobIterableFactory,
        inputDataConfig,
        azureAccountConfig,
        EMPTY_URIS,
        prefixes,
        EMPTY_OBJECTS,
        null,
        azureStorageAccountInputSourceConfig,
        new SystemFields(EnumSet.of(SystemField.URI, SystemField.BUCKET, SystemField.PATH))
    );

    String azureStorageAccountInputSourceString = azureInputSource.toString();

    assertEquals(
        "AzureStorageAccountInputSource{"
        + "uris=[], "
        + "prefixes=[azureStorage://STORAGE_ACCOUNT/CONTAINER/blob], "
        + "objects=[], "
        + "objectGlob=null, "
        + "azureStorageAccountInputSourceConfig=" + azureStorageAccountInputSourceConfig + ", "
        + "systemFields=[__file_uri, __file_bucket, __file_path]"
        + "}",
        azureStorageAccountInputSourceString
    );
  }

  @Test
  public void test_getTypes_returnsExpectedTypes()
  {
    List<URI> prefixes = ImmutableList.of(PREFIX_URI);
    final AzureStorageAccountInputSource azureInputSource = new AzureStorageAccountInputSource(
        entityFactory,
        azureCloudBlobIterableFactory,
        inputDataConfig,
        azureAccountConfig,
        EMPTY_URIS,
        prefixes,
        EMPTY_OBJECTS,
        null,
        azureStorageAccountInputSourceConfig,
        null
    );
    assertEquals(ImmutableSet.of(AzureStorageAccountInputSource.SCHEME), azureInputSource.getTypes());
  }

  @Test
  public void test_systemFields()
  {
    final AzureStorageAccountInputSource azureInputSource = new AzureStorageAccountInputSource(
        entityFactory,
        azureCloudBlobIterableFactory,
        inputDataConfig,
        azureAccountConfig,
        EMPTY_URIS,
        ImmutableList.of(PREFIX_URI),
        EMPTY_OBJECTS,
        null,
        azureStorageAccountInputSourceConfig,
        new SystemFields(EnumSet.of(SystemField.URI, SystemField.BUCKET, SystemField.PATH))
    );

    assertEquals(
        EnumSet.of(SystemField.URI, SystemField.BUCKET, SystemField.PATH),
        azureInputSource.getConfiguredSystemFields()
    );

    final AzureEntity entity = new AzureEntity(
        new CloudObjectLocation("foo", "container/bar"),
        storage,
        AzureStorageAccountInputSource.SCHEME,
        (containerName, blobPath, storage) -> null
    );

    assertEquals(
        "azureStorage://foo/container/bar",
        azureInputSource.getSystemFieldValue(entity, SystemField.URI)
    );
    assertEquals("foo", azureInputSource.getSystemFieldValue(entity, SystemField.BUCKET));
    assertEquals("container/bar", azureInputSource.getSystemFieldValue(entity, SystemField.PATH));
  }

  @Test
  public void abidesEqualsContract()
  {
    EqualsVerifier
        .forClass(AzureStorageAccountInputSource.class)
        .usingGetClass()
        .withPrefabValues(Logger.class, new Logger(AzureStorage.class), new Logger(AzureStorage.class))
        .withPrefabValues(
            BlobContainerClient.class,
            new BlobContainerClientBuilder().buildClient(),
            new BlobContainerClientBuilder().buildClient()
        )
        .withPrefabValues(
            AzureIngestClientFactory.class,
            new AzureIngestClientFactory(null, null),
            new AzureIngestClientFactory(null, null)
        )
        .withIgnoredFields("entityFactory")
        .withIgnoredFields("azureCloudBlobIterableFactory")
        .withNonnullFields("inputDataConfig")
        .withNonnullFields("objectGlob")
        .withNonnullFields("scheme")
        .withNonnullFields("azureStorageAccountInputSourceConfig")
        .withNonnullFields("azureAccountConfig")
        .withNonnullFields("azureIngestClientFactory")
        .verify();
  }

  @Test
  public void test_getContainerAndPathFromObjectLocation()
  {
    Pair<String, String> storageLocation = AzureStorageAccountInputSource.getContainerAndPathFromObjectLocation(
        CLOUD_OBJECT_LOCATION_1
    );
    assertEquals(CONTAINER, storageLocation.lhs);
    assertEquals(BLOB_PATH, storageLocation.rhs);
  }

  @Test
  public void test_getContainerAndPathFromObjectLocatio_nullpath()
  {
    Pair<String, String> storageLocation = AzureStorageAccountInputSource.getContainerAndPathFromObjectLocation(
        new CloudObjectLocation(STORAGE_ACCOUNT, CONTAINER)
    );
    assertEquals(CONTAINER, storageLocation.lhs);
    assertEquals("", storageLocation.rhs);
  }

  @AfterEach
  public void cleanup()
  {
    resetAll();
  }
}
