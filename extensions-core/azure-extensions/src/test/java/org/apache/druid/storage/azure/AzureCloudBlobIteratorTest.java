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
import com.google.common.collect.Lists;
import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.storage.azure.blob.CloudBlobHolder;
import org.apache.druid.storage.azure.blob.ListBlobItemHolder;
import org.apache.druid.storage.azure.blob.ListBlobItemHolderFactory;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class AzureCloudBlobIteratorTest extends EasyMockSupport
{
  private static final String AZURE = "azure";
  private static final String CONTAINER1 = "container1";
  private static final String PREFIX_ONLY_CLOUD_BLOBS = "prefixOnlyCloudBlobs";
  private static final String PREFIX_WITH_NO_BLOBS = "prefixWithNoBlobs";
  private static final String PREFIX_WITH_CLOUD_BLOBS_AND_DIRECTORIES = "prefixWithCloudBlobsAndDirectories";
  private static final URI PREFIX_ONLY_CLOUD_BLOBS_URI;
  private static final URI PREFIX_WITH_NO_BLOBS_URI;
  private static final URI PREFIX_WITH_CLOUD_BLOBS_AND_DIRECTORIES_URI;
  private static final List<URI> EMPTY_URI_PREFIXES = ImmutableList.of();
  private static final List<URI> PREFIXES;
  private static final int MAX_LISTING_LENGTH = 10;
  private static final int MAX_TRIES = 2;
  private static final StorageException RETRYABLE_EXCEPTION = new StorageException("", "", new IOException());
  private static final URISyntaxException NON_RETRYABLE_EXCEPTION = new URISyntaxException("", "");

  private AzureStorage storage;
  private ListBlobItemHolderFactory blobItemDruidFactory;
  private AzureAccountConfig config;
  private ResultSegment<ListBlobItem> resultSegmentPrefixOnlyAndFailLessThanMaxTriesCloudBlobs1;
  private ResultSegment<ListBlobItem> resultSegmentPrefixOnlyAndFailLessThanMaxTriesCloudBlobs2;
  private ResultSegment<ListBlobItem> resultSegmentPrefixWithNoBlobs;
  private ResultSegment<ListBlobItem> resultSegmentPrefixWithCloudBlobsAndDirectories;

  private ResultContinuation resultContinuationPrefixOnlyCloudBlobs = new ResultContinuation();
  private ResultContinuation nullResultContinuationToken = null;

  private ListBlobItem blobItemPrefixWithOnlyCloudBlobs1;
  private ListBlobItemHolder cloudBlobItemPrefixWithOnlyCloudBlobs1;
  private CloudBlobHolder cloudBlobDruidPrefixWithOnlyCloudBlobs1;

  private ListBlobItem blobItemPrefixWithOnlyCloudBlobs2;
  private ListBlobItemHolder cloudBlobItemPrefixWithOnlyCloudBlobs2;
  private CloudBlobHolder cloudBlobDruidPrefixWithOnlyCloudBlobs2;

  private ListBlobItem blobItemPrefixWithCloudBlobsAndDirectories1;
  private ListBlobItemHolder directoryItemPrefixWithCloudBlobsAndDirectories;

  private ListBlobItem blobItemPrefixWithCloudBlobsAndDirectories2;
  private ListBlobItemHolder cloudBlobItemPrefixWithCloudBlobsAndDirectories;
  private CloudBlobHolder cloudBlobDruidPrefixWithCloudBlobsAndDirectories;

  private ListBlobItem blobItemPrefixWithCloudBlobsAndDirectories3;
  private ListBlobItemHolder directoryItemPrefixWithCloudBlobsAndDirectories3;


  private AzureCloudBlobIterator azureCloudBlobIterator;

  static {
    try {
      PREFIX_ONLY_CLOUD_BLOBS_URI = new URI(AZURE + "://" + CONTAINER1 + "/" + PREFIX_ONLY_CLOUD_BLOBS);
      PREFIX_WITH_NO_BLOBS_URI = new URI(AZURE + "://" + CONTAINER1 + "/" + PREFIX_WITH_NO_BLOBS);
      PREFIX_WITH_CLOUD_BLOBS_AND_DIRECTORIES_URI = new URI(AZURE
                                                            + "://"
                                                            + CONTAINER1
                                                            + "/"
                                                            + PREFIX_WITH_CLOUD_BLOBS_AND_DIRECTORIES);
      PREFIXES = ImmutableList.of(
          PREFIX_ONLY_CLOUD_BLOBS_URI,
          PREFIX_WITH_NO_BLOBS_URI,
          PREFIX_WITH_CLOUD_BLOBS_AND_DIRECTORIES_URI
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setup()
  {
    storage = createMock(AzureStorage.class);
    config = createMock(AzureAccountConfig.class);
    blobItemDruidFactory = createMock(ListBlobItemHolderFactory.class);

    resultSegmentPrefixOnlyAndFailLessThanMaxTriesCloudBlobs1 = createMock(ResultSegment.class);
    resultSegmentPrefixOnlyAndFailLessThanMaxTriesCloudBlobs2 = createMock(ResultSegment.class);
    resultSegmentPrefixWithNoBlobs = createMock(ResultSegment.class);
    resultSegmentPrefixWithCloudBlobsAndDirectories = createMock(ResultSegment.class);
    cloudBlobItemPrefixWithOnlyCloudBlobs1 = createMock(ListBlobItemHolder.class);

    blobItemPrefixWithOnlyCloudBlobs1 = createMock(ListBlobItem.class);
    cloudBlobItemPrefixWithOnlyCloudBlobs1 = createMock(ListBlobItemHolder.class);
    cloudBlobDruidPrefixWithOnlyCloudBlobs1 = createMock(CloudBlobHolder.class);
    EasyMock.expect(cloudBlobDruidPrefixWithOnlyCloudBlobs1.getBlobLength()).andReturn(10L).anyTimes();

    blobItemPrefixWithOnlyCloudBlobs2 = createMock(ListBlobItem.class);
    cloudBlobItemPrefixWithOnlyCloudBlobs2 = createMock(ListBlobItemHolder.class);
    cloudBlobDruidPrefixWithOnlyCloudBlobs2 = createMock(CloudBlobHolder.class);
    EasyMock.expect(cloudBlobDruidPrefixWithOnlyCloudBlobs2.getBlobLength()).andReturn(10L).anyTimes();

    blobItemPrefixWithCloudBlobsAndDirectories1 = createMock(ListBlobItem.class);
    directoryItemPrefixWithCloudBlobsAndDirectories = createMock(ListBlobItemHolder.class);

    blobItemPrefixWithCloudBlobsAndDirectories2 = createMock(ListBlobItem.class);
    cloudBlobItemPrefixWithCloudBlobsAndDirectories = createMock(ListBlobItemHolder.class);
    cloudBlobDruidPrefixWithCloudBlobsAndDirectories = createMock(CloudBlobHolder.class);
    EasyMock.expect(cloudBlobDruidPrefixWithCloudBlobsAndDirectories.getBlobLength()).andReturn(10L).anyTimes();

    blobItemPrefixWithCloudBlobsAndDirectories3 = createMock(ListBlobItem.class);
    directoryItemPrefixWithCloudBlobsAndDirectories3 = createMock(ListBlobItemHolder.class);
  }

  @Test
  public void test_hasNext_noBlobs_returnsFalse()
  {
    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        blobItemDruidFactory,
        config,
        EMPTY_URI_PREFIXES,
        MAX_LISTING_LENGTH
    );
    boolean hasNext = azureCloudBlobIterator.hasNext();
    Assert.assertFalse(hasNext);
  }

  @Test
  public void test_next_prefixesWithMultipleBlobsAndSomeDirectories_returnsExpectedBlobs() throws Exception
  {
    EasyMock.expect(config.getMaxTries()).andReturn(MAX_TRIES).atLeastOnce();
    EasyMock.expect(cloudBlobItemPrefixWithOnlyCloudBlobs1.isCloudBlob()).andReturn(true);
    EasyMock.expect(cloudBlobItemPrefixWithOnlyCloudBlobs1.getCloudBlob()).andReturn(
        cloudBlobDruidPrefixWithOnlyCloudBlobs1).anyTimes();
    EasyMock.expect(blobItemDruidFactory.create(blobItemPrefixWithOnlyCloudBlobs1)).andReturn(
        cloudBlobItemPrefixWithOnlyCloudBlobs1);

    EasyMock.expect(cloudBlobItemPrefixWithOnlyCloudBlobs2.isCloudBlob()).andReturn(true);
    EasyMock.expect(cloudBlobItemPrefixWithOnlyCloudBlobs2.getCloudBlob()).andReturn(
        cloudBlobDruidPrefixWithOnlyCloudBlobs2).anyTimes();
    EasyMock.expect(blobItemDruidFactory.create(blobItemPrefixWithOnlyCloudBlobs2)).andReturn(
        cloudBlobItemPrefixWithOnlyCloudBlobs2);

    EasyMock.expect(directoryItemPrefixWithCloudBlobsAndDirectories.isCloudBlob()).andReturn(false);
    EasyMock.expect(blobItemDruidFactory.create(blobItemPrefixWithCloudBlobsAndDirectories1)).andReturn(
        directoryItemPrefixWithCloudBlobsAndDirectories);

    EasyMock.expect(cloudBlobItemPrefixWithCloudBlobsAndDirectories.isCloudBlob()).andReturn(true);
    EasyMock.expect(cloudBlobItemPrefixWithCloudBlobsAndDirectories.getCloudBlob()).andReturn(
        cloudBlobDruidPrefixWithCloudBlobsAndDirectories).anyTimes();
    EasyMock.expect(blobItemDruidFactory.create(blobItemPrefixWithCloudBlobsAndDirectories2)).andReturn(
        cloudBlobItemPrefixWithCloudBlobsAndDirectories);

    EasyMock.expect(directoryItemPrefixWithCloudBlobsAndDirectories3.isCloudBlob()).andReturn(false);
    EasyMock.expect(blobItemDruidFactory.create(blobItemPrefixWithCloudBlobsAndDirectories3)).andReturn(
        directoryItemPrefixWithCloudBlobsAndDirectories3);

    ArrayList<ListBlobItem> resultBlobItemsPrefixWithOnlyCloudBlobs1 = new ArrayList<>();
    resultBlobItemsPrefixWithOnlyCloudBlobs1.add(blobItemPrefixWithOnlyCloudBlobs1);
    ArrayList<ListBlobItem> resultBlobItemsPrefixWithOnlyCloudBlobs2 = new ArrayList<>();
    resultBlobItemsPrefixWithOnlyCloudBlobs2.add(blobItemPrefixWithOnlyCloudBlobs2);
    ArrayList<ListBlobItem> resultBlobItemsPrefixWithNoBlobs = new ArrayList<>();
    ArrayList<ListBlobItem> resultBlobItemsPrefixWithCloudBlobsAndDirectories = new ArrayList<>();
    resultBlobItemsPrefixWithCloudBlobsAndDirectories.add(blobItemPrefixWithCloudBlobsAndDirectories1);
    resultBlobItemsPrefixWithCloudBlobsAndDirectories.add(blobItemPrefixWithCloudBlobsAndDirectories2);
    resultBlobItemsPrefixWithCloudBlobsAndDirectories.add(blobItemPrefixWithCloudBlobsAndDirectories3);
    EasyMock.expect(resultSegmentPrefixOnlyAndFailLessThanMaxTriesCloudBlobs1.getContinuationToken())
            .andReturn(resultContinuationPrefixOnlyCloudBlobs);
    EasyMock.expect(resultSegmentPrefixOnlyAndFailLessThanMaxTriesCloudBlobs1.getResults())
            .andReturn(resultBlobItemsPrefixWithOnlyCloudBlobs1);

    EasyMock.expect(resultSegmentPrefixOnlyAndFailLessThanMaxTriesCloudBlobs2.getContinuationToken()).andReturn(nullResultContinuationToken);
    EasyMock.expect(resultSegmentPrefixOnlyAndFailLessThanMaxTriesCloudBlobs2.getResults())
            .andReturn(resultBlobItemsPrefixWithOnlyCloudBlobs2);

    EasyMock.expect(resultSegmentPrefixWithNoBlobs.getContinuationToken()).andReturn(nullResultContinuationToken);
    EasyMock.expect(resultSegmentPrefixWithNoBlobs.getResults()).andReturn(resultBlobItemsPrefixWithNoBlobs);

    EasyMock.expect(resultSegmentPrefixWithCloudBlobsAndDirectories.getContinuationToken())
            .andReturn(nullResultContinuationToken);
    EasyMock.expect(resultSegmentPrefixWithCloudBlobsAndDirectories.getResults())
            .andReturn(resultBlobItemsPrefixWithCloudBlobsAndDirectories);

    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(
        CONTAINER1,
        PREFIX_ONLY_CLOUD_BLOBS,
        nullResultContinuationToken,
        MAX_LISTING_LENGTH
    )).andThrow(RETRYABLE_EXCEPTION);

    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(
        CONTAINER1,
        PREFIX_ONLY_CLOUD_BLOBS,
        nullResultContinuationToken,
        MAX_LISTING_LENGTH
    )).andReturn(resultSegmentPrefixOnlyAndFailLessThanMaxTriesCloudBlobs1);


    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(
        CONTAINER1,
        PREFIX_ONLY_CLOUD_BLOBS,
        resultContinuationPrefixOnlyCloudBlobs,
        MAX_LISTING_LENGTH
    )).andReturn(resultSegmentPrefixOnlyAndFailLessThanMaxTriesCloudBlobs2);

    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(
        CONTAINER1,
        PREFIX_WITH_NO_BLOBS,
        nullResultContinuationToken,
        MAX_LISTING_LENGTH
    )).andReturn(resultSegmentPrefixWithNoBlobs);

    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(
        CONTAINER1,
        PREFIX_WITH_CLOUD_BLOBS_AND_DIRECTORIES,
        nullResultContinuationToken,
        MAX_LISTING_LENGTH
    )).andReturn(resultSegmentPrefixWithCloudBlobsAndDirectories);

    replayAll();

    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        blobItemDruidFactory,
        config,
        PREFIXES,
        MAX_LISTING_LENGTH
    );

    List<CloudBlobHolder> expectedBlobItems = ImmutableList.of(
        cloudBlobDruidPrefixWithOnlyCloudBlobs1,
        cloudBlobDruidPrefixWithOnlyCloudBlobs2,
        cloudBlobDruidPrefixWithCloudBlobsAndDirectories
    );
    List<CloudBlobHolder> actualBlobItems = new ArrayList<>();
    while (azureCloudBlobIterator.hasNext()) {
      actualBlobItems.add(azureCloudBlobIterator.next());
    }
    Assert.assertEquals(expectedBlobItems.size(), actualBlobItems.size());
    Assert.assertTrue(expectedBlobItems.containsAll(actualBlobItems));
    verifyAll();
  }

  @Test
  public void test_next_emptyObjects_skipEmptyObjects() throws URISyntaxException, StorageException
  {
    EasyMock.expect(config.getMaxTries()).andReturn(MAX_TRIES).atLeastOnce();
    EasyMock.expect(cloudBlobItemPrefixWithOnlyCloudBlobs1.isCloudBlob()).andReturn(true);
    EasyMock.expect(cloudBlobItemPrefixWithOnlyCloudBlobs1.getCloudBlob()).andReturn(
        cloudBlobDruidPrefixWithOnlyCloudBlobs1).anyTimes();
    EasyMock.expect(blobItemDruidFactory.create(blobItemPrefixWithOnlyCloudBlobs1)).andReturn(
        cloudBlobItemPrefixWithOnlyCloudBlobs1);

    ListBlobItem emptyBlobItem = createMock(ListBlobItem.class);
    ListBlobItemHolder emptyBlobItemHolder = createMock(ListBlobItemHolder.class);
    CloudBlobHolder emptyBlobHolder = createMock(CloudBlobHolder.class);
    EasyMock.expect(emptyBlobHolder.getBlobLength()).andReturn(0L).anyTimes();
    EasyMock.expect(emptyBlobItemHolder.isCloudBlob()).andReturn(true);
    EasyMock.expect(emptyBlobItemHolder.getCloudBlob()).andReturn(emptyBlobHolder).anyTimes();

    EasyMock.expect(blobItemDruidFactory.create(emptyBlobItem)).andReturn(emptyBlobItemHolder);

    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(
        CONTAINER1,
        PREFIX_ONLY_CLOUD_BLOBS,
        nullResultContinuationToken,
        MAX_LISTING_LENGTH
    )).andReturn(resultSegmentPrefixOnlyAndFailLessThanMaxTriesCloudBlobs1);

    EasyMock.expect(resultSegmentPrefixOnlyAndFailLessThanMaxTriesCloudBlobs1.getContinuationToken())
            .andReturn(nullResultContinuationToken);
    ArrayList<ListBlobItem> resultBlobItemsPrefixWithOnlyCloudBlobs1 = new ArrayList<>();
    resultBlobItemsPrefixWithOnlyCloudBlobs1.add(blobItemPrefixWithOnlyCloudBlobs1);
    resultBlobItemsPrefixWithOnlyCloudBlobs1.add(emptyBlobItem);
    EasyMock.expect(resultSegmentPrefixOnlyAndFailLessThanMaxTriesCloudBlobs1.getResults())
            .andReturn(resultBlobItemsPrefixWithOnlyCloudBlobs1);

    replayAll();

    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        blobItemDruidFactory,
        config,
        ImmutableList.of(PREFIX_ONLY_CLOUD_BLOBS_URI),
        MAX_LISTING_LENGTH
    );

    List<CloudBlobHolder> expectedBlobItems = ImmutableList.of(cloudBlobDruidPrefixWithOnlyCloudBlobs1);
    List<CloudBlobHolder> actualBlobItems = Lists.newArrayList(azureCloudBlobIterator);
    Assert.assertEquals(expectedBlobItems.size(), actualBlobItems.size());
    Assert.assertTrue(expectedBlobItems.containsAll(actualBlobItems));
    verifyAll();
  }

  @Test(expected = NoSuchElementException.class)
  public void test_next_emptyPrefixes_throwsNoSuchElementException()
  {
    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        blobItemDruidFactory,
        config,
        EMPTY_URI_PREFIXES,
        MAX_LISTING_LENGTH
    );
    azureCloudBlobIterator.next();
  }

  @Test(expected = RE.class)
  public void test_fetchNextBatch_moreThanMaxTriesRetryableExceptionsThrownInStorage_throwsREException() throws Exception
  {
    EasyMock.expect(config.getMaxTries()).andReturn(MAX_TRIES).atLeastOnce();
    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(
        EasyMock.anyString(),
        EasyMock.anyString(),
        EasyMock.anyObject(),
        EasyMock.anyInt()
    )).andThrow(RETRYABLE_EXCEPTION);
    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(
        EasyMock.anyString(),
        EasyMock.anyString(),
        EasyMock.anyObject(),
        EasyMock.anyInt()
    )).andThrow(RETRYABLE_EXCEPTION);
    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        blobItemDruidFactory,
        config,
        PREFIXES,
        MAX_LISTING_LENGTH
    );
  }

  @Test(expected = RE.class)
  public void test_fetchNextBatch_nonRetryableExceptionThrownInStorage_throwsREException() throws Exception
  {
    EasyMock.expect(config.getMaxTries()).andReturn(MAX_TRIES).atLeastOnce();
    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(
        EasyMock.anyString(),
        EasyMock.anyString(),
        EasyMock.anyObject(),
        EasyMock.anyInt()
    )).andThrow(NON_RETRYABLE_EXCEPTION);
    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        blobItemDruidFactory,
        config,
        PREFIXES,
        MAX_LISTING_LENGTH
    );
  }

  @After
  public void cleanup()
  {
    resetAll();
  }
}
