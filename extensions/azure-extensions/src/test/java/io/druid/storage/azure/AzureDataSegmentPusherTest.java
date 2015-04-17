/*
 * Druid - a distributed column store.
 *  Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.druid.storage.azure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.MapUtils;
import com.microsoft.azure.storage.StorageException;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMockSupport;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;

import static org.easymock.EasyMock.*;

public class AzureDataSegmentPusherTest extends EasyMockSupport
{
  private static final DataSegment dataSegment = new DataSegment(
      "test",
      new Interval("2015-04-12/2015-04-13"),
      "1",
      ImmutableMap.<String, Object>of("storageDir", "/path/to/storage/"),
      null,
      null,
      new NoneShardSpec(),
      0,
      1
  );

  private AzureStorageContainer azureStorageContainer;
  private AzureAccountConfig azureAccountConfig;
  private ObjectMapper jsonMapper;

  @Before
  public void before()
  {
    azureStorageContainer = createMock(AzureStorageContainer.class);
    azureAccountConfig = createMock(AzureAccountConfig.class);
    jsonMapper = createMock(ObjectMapper.class);

  }

  @Test
  public void getAzurePathsTest()
  {
    final String storageDir = DataSegmentPusherUtil.getStorageDir(dataSegment);
    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorageContainer, azureAccountConfig, jsonMapper);

    Map<String, String> paths = pusher.getAzurePaths(dataSegment);

    assertEquals(storageDir, paths.get("storage"));
    assertEquals(String.format("%s/%s", storageDir, AzureStorageDruidModule.INDEX_ZIP_FILE_NAME), paths.get("index"));
    assertEquals(
        String.format("%s/%s", storageDir, AzureStorageDruidModule.DESCRIPTOR_FILE_NAME),
        paths.get("descriptor")
    );
  }

  @Test
  public void uploadThenDeleteTest() throws StorageException, IOException, URISyntaxException
  {
    final File file = AzureTestUtils.createZipTempFile("segment", "bucket");
    file.deleteOnExit();
    final String azureDestPath = "/azure/path/";
    azureStorageContainer.uploadBlob(file, azureDestPath);
    expectLastCall();

    replayAll();

    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorageContainer, azureAccountConfig, jsonMapper);

    pusher.uploadThenDelete(file, azureDestPath);

    verifyAll();
  }

  @Test
  public void uploadDataSegmentTest() throws StorageException, IOException, URISyntaxException
  {
    final int version = 9;
    final File compressedSegmentData = AzureTestUtils.createZipTempFile("segment", "bucket");
    compressedSegmentData.deleteOnExit();
    final File descriptorFile = Files.createTempFile("descriptor", ".json").toFile();
    descriptorFile.deleteOnExit();
    final Map<String, String> azurePaths = ImmutableMap.of(
        "index",
        "/path/to/azure/storage",
        "descriptor",
        "/path/to/azure/storage",
        "storage",
        "/path/to/azure/storage"
    );

    azureStorageContainer.uploadBlob(compressedSegmentData, azurePaths.get("index"));
    expectLastCall();
    azureStorageContainer.uploadBlob(descriptorFile, azurePaths.get("descriptor"));
    expectLastCall();

    replayAll();

    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorageContainer, azureAccountConfig, jsonMapper);

    DataSegment pushedDataSegment = pusher.uploadDataSegment(
        dataSegment,
        version,
        compressedSegmentData,
        descriptorFile,
        azurePaths
    );

    assertEquals(compressedSegmentData.length(), pushedDataSegment.getSize());
    assertEquals(version, (int) pushedDataSegment.getBinaryVersion());
    Map<String, Object> loadSpec = pushedDataSegment.getLoadSpec();
    assertEquals(AzureStorageDruidModule.SCHEME, MapUtils.getString(loadSpec, "type"));
    assertEquals(azurePaths.get("storage"), MapUtils.getString(loadSpec, "storageDir"));

    verifyAll();

  }

  @Test
  public void t() throws IOException
  {
    AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(azureStorageContainer, azureAccountConfig, jsonMapper);

    File dir = Files.createTempDirectory("druid").toFile();
    dir.deleteOnExit();

    File x = pusher.createCompressedSegmentDataFile(dir);

    System.out.println(x);
  }

}