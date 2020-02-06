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
import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.druid.java.util.common.RE;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

  private AzureStorage storage;
  private ListBlobItemDruidFactory blobItemDruidFactory;
  private ResultSegment<ListBlobItem> resultSegmentPrefixOnlyCloudBlobs1;
  private ResultSegment<ListBlobItem> resultSegmentPrefixOnlyCloudBlobs2;
  private ResultSegment<ListBlobItem> resultSegmentPrefixWithNoBlobs;
  private ResultSegment<ListBlobItem> resultSegmentPrefixWithCloudBlobsAndDirectories;

  private ResultContinuation resultContinuationPrefixOnlyCloudBlobs = new ResultContinuation();
  private ResultContinuation nullResultContinuationToken = null;

  private ListBlobItem blobItemPrefixWithOnlyCloudBlobs1;
  private ListBlobItemDruid cloudBlobItemPrefixWithOnlyCloudBlobs1;
  private CloudBlobDruid cloudBlobDruidPrefixWithOnlyCloudBlobs1;

  private ListBlobItem blobItemPrefixWithOnlyCloudBlobs2;
  private ListBlobItemDruid cloudBlobItemPrefixWithOnlyCloudBlobs2;
  private CloudBlobDruid cloudBlobDruidPrefixWithOnlyCloudBlobs2;

  private ListBlobItem blobItemPrefixWithCloudBlobsAndDirectories1;
  private ListBlobItemDruid directoryItemPrefixWithCloudBlobsAndDirectories;

  private ListBlobItem blobItemPrefixWithCloudBlobsAndDirectories2;
  private ListBlobItemDruid cloudBlobItemPrefixWithCloudBlobsAndDirectories;
  private CloudBlobDruid cloudBlobDruidPrefixWithCloudBlobsAndDirectories;

  private ListBlobItem blobItemPrefixWithCloudBlobsAndDirectories3;
  private ListBlobItemDruid directoryItemPrefixWithCloudBlobsAndDirectories3;


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
    resultSegmentPrefixOnlyCloudBlobs1 = createMock(ResultSegment.class);
    resultSegmentPrefixOnlyCloudBlobs2 = createMock(ResultSegment.class);
    resultSegmentPrefixWithNoBlobs = createMock(ResultSegment.class);
    resultSegmentPrefixWithCloudBlobsAndDirectories = createMock(ResultSegment.class);
    cloudBlobItemPrefixWithOnlyCloudBlobs1 = createMock(ListBlobItemDruid.class);

    blobItemPrefixWithOnlyCloudBlobs1 = createMock(ListBlobItem.class);
    cloudBlobItemPrefixWithOnlyCloudBlobs1 = createMock(ListBlobItemDruid.class);
    cloudBlobDruidPrefixWithOnlyCloudBlobs1 = createMock(CloudBlobDruid.class);

    blobItemPrefixWithOnlyCloudBlobs2 = createMock(ListBlobItem.class);
    cloudBlobItemPrefixWithOnlyCloudBlobs2 = createMock(ListBlobItemDruid.class);
    cloudBlobDruidPrefixWithOnlyCloudBlobs2 = createMock(CloudBlobDruid.class);

    blobItemPrefixWithCloudBlobsAndDirectories1 = createMock(ListBlobItem.class);
    directoryItemPrefixWithCloudBlobsAndDirectories = createMock(ListBlobItemDruid.class);

    blobItemPrefixWithCloudBlobsAndDirectories2 = createMock(ListBlobItem.class);
    cloudBlobItemPrefixWithCloudBlobsAndDirectories = createMock(ListBlobItemDruid.class);
    cloudBlobDruidPrefixWithCloudBlobsAndDirectories = createMock(CloudBlobDruid.class);

    blobItemPrefixWithCloudBlobsAndDirectories3 = createMock(ListBlobItem.class);
    directoryItemPrefixWithCloudBlobsAndDirectories3 = createMock(ListBlobItemDruid.class);


    blobItemDruidFactory = createMock(ListBlobItemDruidFactory.class);
  }

  @Test
  public void test_hasNext_noBlobs_returnsFalse()
  {
    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        blobItemDruidFactory,
        EMPTY_URI_PREFIXES,
        MAX_LISTING_LENGTH
    );
    boolean hasNext = azureCloudBlobIterator.hasNext();
    Assert.assertFalse(hasNext);
  }

  @Test
  public void test_next_prefixesWithMultipleBlobsAndSomeDirectories_returnsExpectedBlobs() throws Exception
  {
    EasyMock.expect(cloudBlobItemPrefixWithOnlyCloudBlobs1.isCloudBlob()).andReturn(true);
    EasyMock.expect(cloudBlobItemPrefixWithOnlyCloudBlobs1.getCloudBlob()).andReturn(
        cloudBlobDruidPrefixWithOnlyCloudBlobs1);
    EasyMock.expect(blobItemDruidFactory.create(blobItemPrefixWithOnlyCloudBlobs1)).andReturn(
        cloudBlobItemPrefixWithOnlyCloudBlobs1);

    EasyMock.expect(cloudBlobItemPrefixWithOnlyCloudBlobs2.isCloudBlob()).andReturn(true);
    EasyMock.expect(cloudBlobItemPrefixWithOnlyCloudBlobs2.getCloudBlob()).andReturn(
        cloudBlobDruidPrefixWithOnlyCloudBlobs2);
    EasyMock.expect(blobItemDruidFactory.create(blobItemPrefixWithOnlyCloudBlobs2)).andReturn(
        cloudBlobItemPrefixWithOnlyCloudBlobs2);

    EasyMock.expect(directoryItemPrefixWithCloudBlobsAndDirectories.isCloudBlob()).andReturn(false);
    EasyMock.expect(blobItemDruidFactory.create(blobItemPrefixWithCloudBlobsAndDirectories1)).andReturn(
        directoryItemPrefixWithCloudBlobsAndDirectories);

    EasyMock.expect(cloudBlobItemPrefixWithCloudBlobsAndDirectories.isCloudBlob()).andReturn(true);
    EasyMock.expect(cloudBlobItemPrefixWithCloudBlobsAndDirectories.getCloudBlob()).andReturn(
        cloudBlobDruidPrefixWithCloudBlobsAndDirectories);
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
    EasyMock.expect(resultSegmentPrefixOnlyCloudBlobs1.getContinuationToken())
            .andReturn(resultContinuationPrefixOnlyCloudBlobs);
    EasyMock.expect(resultSegmentPrefixOnlyCloudBlobs1.getResults())
            .andReturn(resultBlobItemsPrefixWithOnlyCloudBlobs1);

    EasyMock.expect(resultSegmentPrefixOnlyCloudBlobs2.getContinuationToken()).andReturn(nullResultContinuationToken);
    EasyMock.expect(resultSegmentPrefixOnlyCloudBlobs2.getResults())
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
    )).andReturn(resultSegmentPrefixOnlyCloudBlobs1);


    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(
        CONTAINER1,
        PREFIX_ONLY_CLOUD_BLOBS,
        resultContinuationPrefixOnlyCloudBlobs,
        MAX_LISTING_LENGTH
    )).andReturn(resultSegmentPrefixOnlyCloudBlobs2);

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
        PREFIXES,
        MAX_LISTING_LENGTH
    );

    List<CloudBlobDruid> expectedBlobItems = ImmutableList.of(
        cloudBlobDruidPrefixWithOnlyCloudBlobs1,
        cloudBlobDruidPrefixWithOnlyCloudBlobs2,
        cloudBlobDruidPrefixWithCloudBlobsAndDirectories
    );
    List<CloudBlobDruid> actualBlobItems = new ArrayList<>();
    while (azureCloudBlobIterator.hasNext()) {
      actualBlobItems.add(azureCloudBlobIterator.next());
    }
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
        EMPTY_URI_PREFIXES,
        MAX_LISTING_LENGTH
    );
    azureCloudBlobIterator.next();
  }

  @Test(expected = RE.class)
  public void test_fetchNextBatch_exceptionThrownInStorage_throwsREException() throws Exception
  {
    EasyMock.expect(storage.listBlobsWithPrefixInContainerSegmented(
        EasyMock.anyString(),
        EasyMock.anyString(),
        EasyMock.anyObject(),
        EasyMock.anyInt()
    )).andThrow(new URISyntaxException("", ""));
    azureCloudBlobIterator = new AzureCloudBlobIterator(
        storage,
        blobItemDruidFactory,
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
