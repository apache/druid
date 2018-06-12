/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.azure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.microsoft.azure.storage.StorageException;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

public class AzureDataSegmentPusherTest extends EasyMockSupport
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String containerName = "container";
  private static final String blobPath = "test/2015-04-12T00:00:00.000Z_2015-04-13T00:00:00.000Z/1/0/index.zip";
  private static final DataSegment dataSegment = new DataSegment(
      "test",
      Intervals.of("2015-04-12/2015-04-13"),
      "1",
      ImmutableMap.<String, Object>of("containerName", containerName, "blobPath", blobPath),
      null,
      null,
      NoneShardSpec.instance(),
      0,
      1
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
  public void testPush() throws Exception
  {
    testPushInternal(false, "foo/20150101T000000\\.000Z_20160101T000000\\.000Z/0/0/index\\.zip");
  }

  @Test
  public void testPushUseUniquePath() throws Exception
  {
    testPushInternal(true, "foo/20150101T000000\\.000Z_20160101T000000\\.000Z/0/0/[A-Za-z0-9-]{36}/index\\.zip");
  }

  private void testPushInternal(boolean useUniquePath, String matcher) throws Exception
  {
    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorage, azureAccountConfig, jsonMapper);

    // Create a mock segment on disk
    File tmp = tempFolder.newFile("version.bin");

    final byte[] data = new byte[]{0x0, 0x0, 0x0, 0x1};
    Files.write(data, tmp);
    final long size = data.length;

    DataSegment segmentToPush = new DataSegment(
        "foo",
        Intervals.of("2015/2016"),
        "0",
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        size
    );

    DataSegment segment = pusher.push(tempFolder.getRoot(), segmentToPush, useUniquePath);

    Assert.assertTrue(
        segment.getLoadSpec().get("blobPath").toString(),
        segment.getLoadSpec().get("blobPath").toString().matches(matcher)
    );

    Assert.assertEquals(segmentToPush.getSize(), segment.getSize());
  }

  @Test
  public void getAzurePathsTest()
  {

    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorage, azureAccountConfig, jsonMapper);
    final String storageDir = pusher.getStorageDir(dataSegment, false);
    Map<String, String> paths = pusher.getAzurePaths(dataSegment, false);

    assertEquals(
        StringUtils.format("%s/%s", storageDir, AzureStorageDruidModule.INDEX_ZIP_FILE_NAME),
        paths.get("index")
    );
    assertEquals(
        StringUtils.format("%s/%s", storageDir, AzureStorageDruidModule.DESCRIPTOR_FILE_NAME),
        paths.get("descriptor")
    );
  }

  @Test
  public void uploadDataSegmentTest() throws StorageException, IOException, URISyntaxException
  {
    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorage, azureAccountConfig, jsonMapper);
    final int binaryVersion = 9;
    final File compressedSegmentData = new File("index.zip");
    final File descriptorFile = new File("descriptor.json");
    final Map<String, String> azurePaths = pusher.getAzurePaths(dataSegment, false);

    azureStorage.uploadBlob(compressedSegmentData, containerName, azurePaths.get("index"));
    expectLastCall();
    azureStorage.uploadBlob(descriptorFile, containerName, azurePaths.get("descriptor"));
    expectLastCall();

    replayAll();

    DataSegment pushedDataSegment = pusher.uploadDataSegment(
        dataSegment,
        binaryVersion,
        0, // empty file
        compressedSegmentData,
        descriptorFile,
        azurePaths
    );

    assertEquals(compressedSegmentData.length(), pushedDataSegment.getSize());
    assertEquals(binaryVersion, (int) pushedDataSegment.getBinaryVersion());
    Map<String, Object> loadSpec = pushedDataSegment.getLoadSpec();
    assertEquals(AzureStorageDruidModule.SCHEME, MapUtils.getString(loadSpec, "type"));
    assertEquals(azurePaths.get("index"), MapUtils.getString(loadSpec, "blobPath"));

    verifyAll();
  }

  @Test
  public void getPathForHadoopTest()
  {
    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorage, azureAccountConfig, jsonMapper);
    String hadoopPath = pusher.getPathForHadoop();
    Assert.assertEquals("wasbs://container@account.blob.core.windows.net/", hadoopPath);
  }

  @Test
  public void storageDirContainsNoColonsTest()
  {
    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorage, azureAccountConfig, jsonMapper);
    DataSegment withColons = dataSegment.withVersion("2018-01-05T14:54:09.295Z");
    String segmentPath = pusher.getStorageDir(withColons, false);
    Assert.assertFalse("Path should not contain any columns", segmentPath.contains(":"));
  }
}
