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

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.azure.blob.CloudBlobHolder;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

@RunWith(EasyMockRunner.class)
public class AzureCloudBlobIteratorTest extends EasyMockSupport
{
  @Mock
  private AzureStorage storage;

  private AzureCloudBlobIterator azureCloudBlobIterator;
  private final AzureAccountConfig config = new AzureAccountConfig();
  private final Integer MAX_TRIES = 3;
  private final Integer MAX_LISTING_LENGTH = 10;
  private final String CONTAINER = "container";
  private final String STORAGE_ACCOUNT = "storageAccount";
  private final String DEFAULT_STORAGE_ACCOUNT = "defaultStorageAccount";


  @Before
  public void setup()
  {
    config.setMaxTries(MAX_TRIES);
    config.setAccount(DEFAULT_STORAGE_ACCOUNT);

  }

  @Test
  public void test_hasNext_noBlobs_returnsFalse()
  {
    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        config,
        ImmutableList.of(),
        1
    );
    boolean hasNext = azureCloudBlobIterator.hasNext();
    Assert.assertFalse(hasNext);
  }

  @Test
  public void test_next_prefixesWithMultipleBlobsAndSomeDirectories_returnsExpectedBlobs() throws Exception
  {
    List<URI> prefixes = ImmutableList.of(
        new URI(StringUtils.format("azure://%s/dir1", CONTAINER)),
        new URI(StringUtils.format("azure://%s/dir2", CONTAINER))
    );

    BlobItem blobItem = new BlobItem().setName("blobName").setProperties(new BlobItemProperties().setContentLength(10L));
    SettableSupplier<PagedResponse<BlobItem>> supplier = new SettableSupplier<>();
    supplier.set(new TestPagedResponse<>(ImmutableList.of(blobItem)));
    PagedIterable<BlobItem> pagedIterable = new PagedIterable<>(supplier);
    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(DEFAULT_STORAGE_ACCOUNT, CONTAINER, "dir1", MAX_LISTING_LENGTH, MAX_TRIES))
        .andReturn(pagedIterable);

    BlobItem blobPrefixItem = new BlobItem().setIsPrefix(true).setName("subdir").setProperties(new BlobItemProperties());
    BlobItem blobItem2 = new BlobItem().setName("blobName2").setProperties(new BlobItemProperties().setContentLength(10L));
    SettableSupplier<PagedResponse<BlobItem>> supplier2 = new SettableSupplier<>();
    supplier2.set(new TestPagedResponse<>(ImmutableList.of(blobPrefixItem, blobItem2)));
    PagedIterable<BlobItem> pagedIterable2 = new PagedIterable<>(supplier2);
    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(DEFAULT_STORAGE_ACCOUNT, CONTAINER, "dir2", MAX_LISTING_LENGTH, MAX_TRIES))
        .andReturn(pagedIterable2);

    replayAll();
    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        config,
        prefixes,
        MAX_LISTING_LENGTH
    );
    List<CloudBlobHolder> actualBlobItems = new ArrayList<>();
    while (azureCloudBlobIterator.hasNext()) {
      actualBlobItems.add(azureCloudBlobIterator.next());
    }
    verifyAll();
    List<CloudBlobHolder> expectedBlobItems = ImmutableList.of(
        new CloudBlobHolder(blobItem, CONTAINER, DEFAULT_STORAGE_ACCOUNT),
        new CloudBlobHolder(blobItem2, CONTAINER, DEFAULT_STORAGE_ACCOUNT)
    );
    Assert.assertEquals(expectedBlobItems.size(), actualBlobItems.size());
    Assert.assertEquals(
        expectedBlobItems.stream().map(CloudBlobHolder::getName).collect(Collectors.toSet()),
        actualBlobItems.stream().map(CloudBlobHolder::getName).collect(Collectors.toSet())
    );
    Assert.assertEquals(
        expectedBlobItems.stream().map(CloudBlobHolder::getStorageAccount).collect(Collectors.toSet()),
        actualBlobItems.stream().map(CloudBlobHolder::getStorageAccount).collect(Collectors.toSet())
    );
    Assert.assertEquals(
        expectedBlobItems.stream().map(CloudBlobHolder::getContainerName).collect(Collectors.toSet()),
        actualBlobItems.stream().map(CloudBlobHolder::getContainerName).collect(Collectors.toSet())
    );
  }

  @Test
  public void test_next_prefixesWithMultipleBlobsAndOneDirectory_returnsExpectedBlobs() throws Exception
  {
    List<URI> prefixes = ImmutableList.of(
        new URI(StringUtils.format("azure://%s/dir1", CONTAINER))
    );

    BlobItem blobItem = new BlobItem().setName("blobName").setProperties(new BlobItemProperties().setContentLength(10L));
    BlobItem blobItem2 = new BlobItem().setName("blobName2").setProperties(new BlobItemProperties().setContentLength(10L));
    SettableSupplier<PagedResponse<BlobItem>> supplier = new SettableSupplier<>();
    supplier.set(new TestPagedResponse<>(ImmutableList.of(blobItem, blobItem2)));
    PagedIterable<BlobItem> pagedIterable = new PagedIterable<>(supplier);
    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(DEFAULT_STORAGE_ACCOUNT, CONTAINER, "dir1", MAX_LISTING_LENGTH, MAX_TRIES))
        .andReturn(pagedIterable);


    replayAll();
    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        config,
        prefixes,
        MAX_LISTING_LENGTH
    );
    List<CloudBlobHolder> actualBlobItems = new ArrayList<>();
    while (azureCloudBlobIterator.hasNext()) {
      actualBlobItems.add(azureCloudBlobIterator.next());
    }
    verifyAll();
    List<CloudBlobHolder> expectedBlobItems = ImmutableList.of(
        new CloudBlobHolder(blobItem, CONTAINER, DEFAULT_STORAGE_ACCOUNT),
        new CloudBlobHolder(blobItem2, CONTAINER, DEFAULT_STORAGE_ACCOUNT)
    );
    Assert.assertEquals(expectedBlobItems.size(), actualBlobItems.size());
    Assert.assertEquals(
        expectedBlobItems.stream().map(CloudBlobHolder::getName).collect(Collectors.toSet()),
        actualBlobItems.stream().map(CloudBlobHolder::getName).collect(Collectors.toSet())
    );
    Assert.assertEquals(
        expectedBlobItems.stream().map(CloudBlobHolder::getStorageAccount).collect(Collectors.toSet()),
        actualBlobItems.stream().map(CloudBlobHolder::getStorageAccount).collect(Collectors.toSet())
    );
    Assert.assertEquals(
        expectedBlobItems.stream().map(CloudBlobHolder::getContainerName).collect(Collectors.toSet()),
        actualBlobItems.stream().map(CloudBlobHolder::getContainerName).collect(Collectors.toSet())
    );
  }

  @Test
  public void test_next_prefixesWithMultipleBlobsAndSomeDirectories_returnsExpectedBlobs_azureStorage() throws Exception
  {
    List<URI> prefixes = ImmutableList.of(
        new URI(StringUtils.format("azureStorage://%s/%s/dir1", STORAGE_ACCOUNT, CONTAINER)),
        new URI(StringUtils.format("azureStorage://%s/%s/dir2", STORAGE_ACCOUNT, CONTAINER))
    );

    BlobItem blobItem = new BlobItem().setName("blobName").setProperties(new BlobItemProperties().setContentLength(10L));
    SettableSupplier<PagedResponse<BlobItem>> supplier = new SettableSupplier<>();
    supplier.set(new TestPagedResponse<>(ImmutableList.of(blobItem)));
    PagedIterable<BlobItem> pagedIterable = new PagedIterable<>(supplier);
    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(STORAGE_ACCOUNT, CONTAINER, "dir1", MAX_LISTING_LENGTH, MAX_TRIES))
        .andReturn(pagedIterable);

    BlobItem blobPrefixItem = new BlobItem().setIsPrefix(true).setName("subdir").setProperties(new BlobItemProperties());
    BlobItem blobItem2 = new BlobItem().setName("blobName2").setProperties(new BlobItemProperties().setContentLength(10L));
    SettableSupplier<PagedResponse<BlobItem>> supplier2 = new SettableSupplier<>();
    supplier2.set(new TestPagedResponse<>(ImmutableList.of(blobPrefixItem, blobItem2)));
    PagedIterable<BlobItem> pagedIterable2 = new PagedIterable<>(supplier2);
    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(STORAGE_ACCOUNT, CONTAINER, "dir2", MAX_LISTING_LENGTH, MAX_TRIES))
        .andReturn(pagedIterable2);

    replayAll();
    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        config,
        prefixes,
        MAX_LISTING_LENGTH
    );
    List<CloudBlobHolder> actualBlobItems = new ArrayList<>();
    while (azureCloudBlobIterator.hasNext()) {
      actualBlobItems.add(azureCloudBlobIterator.next());
    }
    verifyAll();
    List<CloudBlobHolder> expectedBlobItems = ImmutableList.of(
        new CloudBlobHolder(blobItem, CONTAINER, STORAGE_ACCOUNT),
        new CloudBlobHolder(blobItem2, CONTAINER, STORAGE_ACCOUNT)
    );
    Assert.assertEquals(expectedBlobItems.size(), actualBlobItems.size());
    Assert.assertEquals(
        expectedBlobItems.stream().map(CloudBlobHolder::getName).collect(Collectors.toSet()),
        actualBlobItems.stream().map(CloudBlobHolder::getName).collect(Collectors.toSet())
    );
    Assert.assertEquals(
        expectedBlobItems.stream().map(CloudBlobHolder::getStorageAccount).collect(Collectors.toSet()),
        actualBlobItems.stream().map(CloudBlobHolder::getStorageAccount).collect(Collectors.toSet())
    );
    Assert.assertEquals(
        expectedBlobItems.stream().map(CloudBlobHolder::getContainerName).collect(Collectors.toSet()),
        actualBlobItems.stream().map(CloudBlobHolder::getContainerName).collect(Collectors.toSet())
    );
  }

  @Test
  public void test_next_emptyObjects_skipEmptyObjects() throws Exception
  {
    List<URI> prefixes = ImmutableList.of(
        new URI(StringUtils.format("azure://%s/dir1", CONTAINER))
    );

    BlobItem blobItem = new BlobItem().setName("blobName").setProperties(new BlobItemProperties().setContentLength(10L));
    BlobItem blobItem2 = new BlobItem().setName("blobName2").setProperties(new BlobItemProperties().setContentLength(0L));

    SettableSupplier<PagedResponse<BlobItem>> supplier = new SettableSupplier<>();
    supplier.set(new TestPagedResponse<>(ImmutableList.of(blobItem, blobItem2)));
    PagedIterable<BlobItem> pagedIterable = new PagedIterable<>(supplier);
    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(DEFAULT_STORAGE_ACCOUNT, CONTAINER, "dir1", MAX_LISTING_LENGTH, MAX_TRIES))
        .andReturn(pagedIterable);

    replayAll();
    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        config,
        prefixes,
        MAX_LISTING_LENGTH
    );
    List<CloudBlobHolder> actualBlobItems = new ArrayList<>();
    while (azureCloudBlobIterator.hasNext()) {
      actualBlobItems.add(azureCloudBlobIterator.next());
    }
    verifyAll();
    List<CloudBlobHolder> expectedBlobItems = ImmutableList.of(
        new CloudBlobHolder(blobItem, CONTAINER, DEFAULT_STORAGE_ACCOUNT)
    );
    Assert.assertEquals(expectedBlobItems.size(), actualBlobItems.size());
    Assert.assertEquals(
        expectedBlobItems.stream().map(CloudBlobHolder::getName).collect(Collectors.toSet()),
        actualBlobItems.stream().map(CloudBlobHolder::getName).collect(Collectors.toSet())
    );
    Assert.assertEquals(
        expectedBlobItems.stream().map(CloudBlobHolder::getStorageAccount).collect(Collectors.toSet()),
        actualBlobItems.stream().map(CloudBlobHolder::getStorageAccount).collect(Collectors.toSet())
    );
    Assert.assertEquals(
        expectedBlobItems.stream().map(CloudBlobHolder::getContainerName).collect(Collectors.toSet()),
        actualBlobItems.stream().map(CloudBlobHolder::getContainerName).collect(Collectors.toSet())
    );
  }

  @Test(expected = NoSuchElementException.class)
  public void test_next_emptyPrefixes_throwsNoSuchElementException()
  {
    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        config,
        ImmutableList.of(),
        MAX_LISTING_LENGTH
    );
    azureCloudBlobIterator.next();
  }

  @Test(expected = RE.class)
  public void test_fetchNextBatch_moreThanMaxTriesRetryableExceptionsThrownInStorage_throwsREException() throws Exception
  {
    List<URI> prefixes = ImmutableList.of(
        new URI(StringUtils.format("azure://%s/dir1", CONTAINER))
    );

    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(
        EasyMock.anyString(),
        EasyMock.anyString(),
        EasyMock.anyString(),
        EasyMock.anyInt(),
        EasyMock.anyInt()
    )).andThrow(new BlobStorageException("", null, null)).times(3);

    replayAll();
    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        config,
        prefixes,
        MAX_LISTING_LENGTH
    );
    verifyAll();
  }

  @Test(expected = RE.class)
  public void test_fetchNextBatch_nonRetryableExceptionThrownInStorage_throwsREException() throws Exception
  {
    List<URI> prefixes = ImmutableList.of(
        new URI(StringUtils.format("azure://%s/dir1", CONTAINER))
    );
    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(
        EasyMock.anyString(),
        EasyMock.anyString(),
        EasyMock.anyString(),
        EasyMock.anyInt(),
        EasyMock.anyInt()
    )).andThrow(new RuntimeException(""));
    replayAll();
    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        config,
        prefixes,
        MAX_LISTING_LENGTH
    );
    verifyAll();
  }
}
