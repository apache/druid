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
import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageExtendedErrorInformation;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.storage.azure.blob.CloudBlobHolder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class AzureDataSegmentKillerTest extends EasyMockSupport
{
  private static final String CONTAINER_NAME = "container";
  private static final String CONTAINER = "test";
  private static final String PREFIX = "test/log";
  private static final int MAX_TRIES = 3;
  private static final String BLOB_PATH = "test/2015-04-12T00:00:00.000Z_2015-04-13T00:00:00.000Z/1/0/index.zip";
  private static final int MAX_KEYS = 1;
  private static final long TIME_0 = 0L;
  private static final long TIME_1 = 1L;
  private static final long TIME_NOW = 2L;
  private static final long TIME_FUTURE = 3L;
  private static final String KEY_1 = "key1";
  private static final String KEY_2 = "key2";
  private static final URI PREFIX_URI = URI.create(StringUtils.format("azure://%s/%s", CONTAINER, PREFIX));
  private static final Exception RECOVERABLE_EXCEPTION = new StorageException("", "", null);
  private static final Exception NON_RECOVERABLE_EXCEPTION = new URISyntaxException("", "");

  private static final DataSegment DATA_SEGMENT = new DataSegment(
      "test",
      Intervals.of("2015-04-12/2015-04-13"),
      "1",
      ImmutableMap.of("containerName", CONTAINER_NAME, "blobPath", BLOB_PATH),
      null,
      null,
      NoneShardSpec.instance(),
      0,
      1
  );

  private static final StorageExtendedErrorInformation NULL_STORAGE_EXTENDED_ERROR_INFORMATION = null;
  private static final StorageExtendedErrorInformation STORAGE_EXTENDED_ERROR_INFORMATION = new StorageExtendedErrorInformation();

  private AzureDataSegmentConfig segmentConfig;
  private AzureInputDataConfig inputDataConfig;
  private AzureAccountConfig accountConfig;
  private AzureStorage azureStorage;
  private AzureCloudBlobIterableFactory azureCloudBlobIterableFactory;

  @Before
  public void before()
  {
    segmentConfig = createMock(AzureDataSegmentConfig.class);
    inputDataConfig = createMock(AzureInputDataConfig.class);
    accountConfig = createMock(AzureAccountConfig.class);
    azureStorage = createMock(AzureStorage.class);
    azureCloudBlobIterableFactory = createMock(AzureCloudBlobIterableFactory.class);
  }

  @Test
  public void killTest() throws SegmentLoadingException, URISyntaxException, StorageException
  {

    List<String> deletedFiles = new ArrayList<>();
    final String dirPath = Paths.get(BLOB_PATH).getParent().toString();

    EasyMock.expect(azureStorage.emptyCloudBlobDirectory(CONTAINER_NAME, dirPath)).andReturn(deletedFiles);

    replayAll();

    AzureDataSegmentKiller killer = new AzureDataSegmentKiller(segmentConfig, inputDataConfig, accountConfig, azureStorage, azureCloudBlobIterableFactory);

    killer.kill(DATA_SEGMENT);

    verifyAll();
  }

  @Test(expected = SegmentLoadingException.class)
  public void test_kill_StorageExceptionExtendedErrorInformationNull_throwsException()
      throws SegmentLoadingException, URISyntaxException, StorageException
  {

    common_test_kill_StorageExceptionExtendedError_throwsException(NULL_STORAGE_EXTENDED_ERROR_INFORMATION);
  }

  @Test(expected = SegmentLoadingException.class)
  public void test_kill_StorageExceptionExtendedErrorInformationNotNull_throwsException()
      throws SegmentLoadingException, URISyntaxException, StorageException
  {

    common_test_kill_StorageExceptionExtendedError_throwsException(STORAGE_EXTENDED_ERROR_INFORMATION);
  }

  @Test(expected = SegmentLoadingException.class)
  public void test_kill_URISyntaxException_throwsException()
      throws SegmentLoadingException, URISyntaxException, StorageException
  {

    String dirPath = Paths.get(BLOB_PATH).getParent().toString();

    EasyMock.expect(azureStorage.emptyCloudBlobDirectory(CONTAINER_NAME, dirPath)).andThrow(
        new URISyntaxException(
            "",
            ""
        )
    );

    replayAll();

    AzureDataSegmentKiller killer = new AzureDataSegmentKiller(segmentConfig, inputDataConfig, accountConfig, azureStorage, azureCloudBlobIterableFactory);

    killer.kill(DATA_SEGMENT);

    verifyAll();
  }

  @Test
  public void test_killAll_segmentConfigWithNullContainerAndPrefix_throwsISEException() throws Exception
  {
    EasyMock.expect(segmentConfig.getContainer()).andReturn(null).atLeastOnce();
    EasyMock.expect(segmentConfig.getPrefix()).andReturn(null).anyTimes();

    boolean thrownISEException = false;

    try {
      AzureDataSegmentKiller killer = new AzureDataSegmentKiller(
          segmentConfig,
          inputDataConfig,
          accountConfig,
          azureStorage,
          azureCloudBlobIterableFactory
      );
      EasyMock.replay(segmentConfig, inputDataConfig, accountConfig, azureStorage, azureCloudBlobIterableFactory);
      killer.killAll();
    }
    catch (ISE e) {
      thrownISEException = true;
    }

    Assert.assertTrue(thrownISEException);
    EasyMock.verify(segmentConfig, inputDataConfig, accountConfig, azureStorage, azureCloudBlobIterableFactory);
  }

  @Test
  public void test_killAll_noException_deletesAllSegments() throws Exception
  {
    EasyMock.expect(segmentConfig.getContainer()).andReturn(CONTAINER).atLeastOnce();
    EasyMock.expect(segmentConfig.getPrefix()).andReturn(PREFIX).atLeastOnce();
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
    EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).atLeastOnce();

    CloudBlobHolder object1 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_1, TIME_0);
    CloudBlobHolder object2 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_2, TIME_1);

    AzureCloudBlobIterable azureCloudBlobIterable = AzureTestUtils.expectListObjects(
        azureCloudBlobIterableFactory,
        MAX_KEYS,
        PREFIX_URI,
        ImmutableList.of(object1, object2));

    EasyMock.replay(object1, object2);
    AzureTestUtils.expectDeleteObjects(
        azureStorage,
        ImmutableList.of(object1, object2),
        ImmutableMap.of());
    EasyMock.replay(segmentConfig, inputDataConfig, accountConfig, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
    AzureDataSegmentKiller killer = new AzureDataSegmentKiller(segmentConfig, inputDataConfig, accountConfig, azureStorage, azureCloudBlobIterableFactory);
    killer.killAll();
    EasyMock.verify(segmentConfig, inputDataConfig, accountConfig, object1, object2, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
  }

  @Test
  public void test_killAll_recoverableExceptionWhenListingObjects_deletesAllSegments() throws Exception
  {
    EasyMock.expect(segmentConfig.getContainer()).andReturn(CONTAINER).atLeastOnce();
    EasyMock.expect(segmentConfig.getPrefix()).andReturn(PREFIX).atLeastOnce();
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
    EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).atLeastOnce();

    CloudBlobHolder object1 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_1, TIME_0);

    AzureCloudBlobIterable azureCloudBlobIterable = AzureTestUtils.expectListObjects(
        azureCloudBlobIterableFactory,
        MAX_KEYS,
        PREFIX_URI,
        ImmutableList.of(object1));

    EasyMock.replay(object1);
    AzureTestUtils.expectDeleteObjects(
        azureStorage,
        ImmutableList.of(object1),
        ImmutableMap.of(object1, RECOVERABLE_EXCEPTION));
    EasyMock.replay(segmentConfig, inputDataConfig, accountConfig, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
    AzureDataSegmentKiller killer = new AzureDataSegmentKiller(segmentConfig, inputDataConfig, accountConfig, azureStorage, azureCloudBlobIterableFactory);
    killer.killAll();
    EasyMock.verify(segmentConfig, inputDataConfig, accountConfig, object1, azureCloudBlobIterable, azureCloudBlobIterableFactory, azureStorage);
  }

  @Test
  public void test_killAll_nonrecoverableExceptionWhenListingObjects_deletesAllSegments() throws Exception
  {
    boolean ioExceptionThrown = false;
    CloudBlobHolder object1 = null;
    AzureCloudBlobIterable azureCloudBlobIterable = null;
    try {
      EasyMock.expect(segmentConfig.getContainer()).andReturn(CONTAINER).atLeastOnce();
      EasyMock.expect(segmentConfig.getPrefix()).andReturn(PREFIX).atLeastOnce();
      EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
      EasyMock.expect(accountConfig.getMaxTries()).andReturn(MAX_TRIES).atLeastOnce();

      object1 = AzureTestUtils.newCloudBlobHolder(CONTAINER, KEY_1, TIME_0);

      azureCloudBlobIterable = AzureTestUtils.expectListObjects(
          azureCloudBlobIterableFactory,
          MAX_KEYS,
          PREFIX_URI,
          ImmutableList.of(object1)
      );

      EasyMock.replay(object1);
      AzureTestUtils.expectDeleteObjects(
          azureStorage,
          ImmutableList.of(),
          ImmutableMap.of(object1, NON_RECOVERABLE_EXCEPTION)
      );
      EasyMock.replay(
          segmentConfig,
          inputDataConfig,
          accountConfig,
          azureCloudBlobIterable,
          azureCloudBlobIterableFactory,
          azureStorage
      );
      AzureDataSegmentKiller killer = new AzureDataSegmentKiller(
          segmentConfig,
          inputDataConfig,
          accountConfig,
          azureStorage,
          azureCloudBlobIterableFactory
      );
      killer.killAll();
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }

    Assert.assertTrue(ioExceptionThrown);

    EasyMock.verify(
        segmentConfig,
        inputDataConfig,
        accountConfig,
        object1,
        azureCloudBlobIterable,
        azureCloudBlobIterableFactory,
        azureStorage
    );
  }

  private void common_test_kill_StorageExceptionExtendedError_throwsException(StorageExtendedErrorInformation storageExtendedErrorInformation)
      throws SegmentLoadingException, URISyntaxException, StorageException
  {
    String dirPath = Paths.get(BLOB_PATH).getParent().toString();

    EasyMock.expect(azureStorage.emptyCloudBlobDirectory(CONTAINER_NAME, dirPath)).andThrow(
        new StorageException(
            "",
            "",
            400,
            storageExtendedErrorInformation,
            null
        )
    );

    replayAll();

    AzureDataSegmentKiller killer = new AzureDataSegmentKiller(segmentConfig, inputDataConfig, accountConfig, azureStorage, azureCloudBlobIterableFactory);

    killer.kill(DATA_SEGMENT);

    verifyAll();
  }
}
