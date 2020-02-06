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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.microsoft.azure.storage.StorageException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class AzureDataSegmentPusherTest extends EasyMockSupport
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String CONTAINER_NAME = "container";
  private static final String BLOB_PATH = "test/2015-04-12T00:00:00.000Z_2015-04-13T00:00:00.000Z/1/0/index.zip";
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
  private static final byte[] DATA = new byte[]{0x0, 0x0, 0x0, 0x1};
  private static final String UNIQUE_MATCHER = "foo/20150101T000000\\.000Z_20160101T000000\\.000Z/0/0/[A-Za-z0-9-]{36}/index\\.zip";
  private static final String NON_UNIQUE_MATCHER = "foo/20150101T000000\\.000Z_20160101T000000\\.000Z/0/0/index\\.zip";

  private static final DataSegment SEGMENT_TO_PUSH = new DataSegment(
      "foo",
      Intervals.of("2015/2016"),
      "0",
      new HashMap<>(),
      new ArrayList<>(),
      new ArrayList<>(),
      NoneShardSpec.instance(),
      0,
      DATA.length
  );

  private AzureStorage azureStorage;
  private AzureAccountConfig azureAccountConfig;
  private ObjectMapper jsonMapper;

  @Before
  public void before()
  {
    azureStorage = createMock(AzureStorage.class);
    azureAccountConfig = new AzureAccountConfig();
    azureAccountConfig.setAccount("account");
    azureAccountConfig.setContainer("container");

    jsonMapper = new DefaultObjectMapper();
  }

  @Test
  public void test_push_nonUniquePath_succeeds() throws Exception
  {
    boolean useUniquePath = false;
    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorage, azureAccountConfig);

    // Create a mock segment on disk
    File tmp = tempFolder.newFile("version.bin");

    Files.write(DATA, tmp);

    String azurePath = pusher.getAzurePath(SEGMENT_TO_PUSH, useUniquePath);
    azureStorage.uploadBlob(EasyMock.anyObject(File.class), EasyMock.eq(CONTAINER_NAME), EasyMock.eq(azurePath));
    EasyMock.expectLastCall();

    replayAll();

    DataSegment segment = pusher.push(tempFolder.getRoot(), SEGMENT_TO_PUSH, useUniquePath);

    Assert.assertTrue(
        segment.getLoadSpec().get("blobPath").toString(),
        Pattern.compile(NON_UNIQUE_MATCHER).matcher(segment.getLoadSpec().get("blobPath").toString()).matches()
    );

    Assert.assertEquals(SEGMENT_TO_PUSH.getSize(), segment.getSize());

    verifyAll();
  }

  @Test
  public void test_push_uniquePath_succeeds() throws Exception
  {
    boolean useUniquePath = true;
    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorage, azureAccountConfig);

    // Create a mock segment on disk
    File tmp = tempFolder.newFile("version.bin");

    Files.write(DATA, tmp);

    String azurePath = pusher.getAzurePath(SEGMENT_TO_PUSH, useUniquePath);
    azureStorage.uploadBlob(EasyMock.anyObject(File.class), EasyMock.eq(CONTAINER_NAME), EasyMock.matches(UNIQUE_MATCHER));
    EasyMock.expectLastCall();

    replayAll();

    DataSegment segment = pusher.push(tempFolder.getRoot(), SEGMENT_TO_PUSH, useUniquePath);

    Assert.assertTrue(
        segment.getLoadSpec().get("blobPath").toString(),
        Pattern.compile(UNIQUE_MATCHER).matcher(segment.getLoadSpec().get("blobPath").toString()).matches()
    );

    Assert.assertEquals(SEGMENT_TO_PUSH.getSize(), segment.getSize());

    verifyAll();
  }

  @Test(expected = RuntimeException.class)
  public void test_push_exception_throwsException() throws Exception
  {
    boolean useUniquePath = true;
    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorage, azureAccountConfig);

    // Create a mock segment on disk
    File tmp = tempFolder.newFile("version.bin");

    Files.write(DATA, tmp);
    final long size = DATA.length;

    String azurePath = pusher.getAzurePath(SEGMENT_TO_PUSH, useUniquePath);
    azureStorage.uploadBlob(EasyMock.anyObject(File.class), EasyMock.eq(CONTAINER_NAME), EasyMock.eq(azurePath));
    EasyMock.expectLastCall().andThrow(new URISyntaxException("", ""));

    replayAll();

    DataSegment segment = pusher.push(tempFolder.getRoot(), SEGMENT_TO_PUSH, useUniquePath);

    Assert.assertTrue(
        segment.getLoadSpec().get("blobPath").toString(),
        Pattern.compile(UNIQUE_MATCHER).matcher(segment.getLoadSpec().get("blobPath").toString()).matches()
    );

    Assert.assertEquals(SEGMENT_TO_PUSH.getSize(), segment.getSize());

    verifyAll();
  }

  @Test
  public void getAzurePathsTest()
  {

    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorage, azureAccountConfig);
    final String storageDir = pusher.getStorageDir(DATA_SEGMENT, false);
    final String azurePath = pusher.getAzurePath(DATA_SEGMENT, false);

    Assert.assertEquals(
        StringUtils.format("%s/%s", storageDir, AzureStorageDruidModule.INDEX_ZIP_FILE_NAME),
        azurePath
    );
  }

  @Test
  public void uploadDataSegmentTest() throws StorageException, IOException, URISyntaxException
  {
    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorage, azureAccountConfig);
    final int binaryVersion = 9;
    final File compressedSegmentData = new File("index.zip");
    final String azurePath = pusher.getAzurePath(DATA_SEGMENT, false);

    azureStorage.uploadBlob(compressedSegmentData, CONTAINER_NAME, azurePath);
    EasyMock.expectLastCall();

    replayAll();

    DataSegment pushedDataSegment = pusher.uploadDataSegment(
        DATA_SEGMENT,
        binaryVersion,
        0, // empty file
        compressedSegmentData,
        azurePath
    );

    Assert.assertEquals(compressedSegmentData.length(), pushedDataSegment.getSize());
    Assert.assertEquals(binaryVersion, (int) pushedDataSegment.getBinaryVersion());
    Map<String, Object> loadSpec = pushedDataSegment.getLoadSpec();
    Assert.assertEquals(AzureStorageDruidModule.SCHEME, MapUtils.getString(loadSpec, "type"));
    Assert.assertEquals(azurePath, MapUtils.getString(loadSpec, "blobPath"));

    verifyAll();
  }

  @Test
  public void getPathForHadoopTest()
  {
    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorage, azureAccountConfig);
    String hadoopPath = pusher.getPathForHadoop();
    Assert.assertEquals("wasbs://container@account.blob.core.windows.net/", hadoopPath);
  }

  @Test
  public void test_getPathForHadoop_noArgs_succeeds()
  {
    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorage, azureAccountConfig);
    String hadoopPath = pusher.getPathForHadoop("");
    Assert.assertEquals("wasbs://container@account.blob.core.windows.net/", hadoopPath);
  }

  @Test
  public void test_getAllowedPropertyPrefixesForHadoop_returnsExpcetedPropertyPrefixes()
  {
    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorage, azureAccountConfig);
    List<String> actualPropertyPrefixes = pusher.getAllowedPropertyPrefixesForHadoop();
    Assert.assertEquals(AzureDataSegmentPusher.ALLOWED_PROPERTY_PREFIXES_FOR_HADOOP, actualPropertyPrefixes);
  }

  @Test
  public void storageDirContainsNoColonsTest()
  {
    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorage, azureAccountConfig);
    DataSegment withColons = DATA_SEGMENT.withVersion("2018-01-05T14:54:09.295Z");
    String segmentPath = pusher.getStorageDir(withColons, false);
    Assert.assertFalse("Path should not contain any columns", segmentPath.contains(":"));
  }
}
