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

import com.microsoft.azure.storage.StorageException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

public class AzureDataSegmentPullerTest extends EasyMockSupport
{

  private static final String SEGMENT_FILE_NAME = "segment";
  private static final String CONTAINER_NAME = "container";
  private static final String BLOB_PATH = "path/to/storage/index.zip";
  private static final String BLOB_PATH_HADOOP = AzureDataSegmentPuller.AZURE_STORAGE_HOST_ADDRESS + "/path/to/storage/index.zip";
  private AzureStorage azureStorage;

  @Before
  public void before()
  {
    azureStorage = createMock(AzureStorage.class);
  }

  @Test
  public void test_getSegmentFiles_success()
      throws SegmentLoadingException, URISyntaxException, StorageException, IOException
  {
    final String value = "bucket";
    final File pulledFile = AzureTestUtils.createZipTempFile(SEGMENT_FILE_NAME, value);
    final File toDir = FileUtils.createTempDir();
    try {
      final InputStream zipStream = new FileInputStream(pulledFile);

      EasyMock.expect(azureStorage.getBlobInputStream(CONTAINER_NAME, BLOB_PATH)).andReturn(zipStream);

      replayAll();

      AzureDataSegmentPuller puller = new AzureDataSegmentPuller(azureStorage);

      FileUtils.FileCopyResult result = puller.getSegmentFiles(CONTAINER_NAME, BLOB_PATH, toDir);

      File expected = new File(toDir, SEGMENT_FILE_NAME);
      Assert.assertEquals(value.length(), result.size());
      Assert.assertTrue(expected.exists());
      Assert.assertEquals(value.length(), expected.length());

      verifyAll();
    }
    finally {
      pulledFile.delete();
      FileUtils.deleteDirectory(toDir);
    }
  }

  @Test
  public void test_getSegmentFiles_blobPathIsHadoop_success()
      throws SegmentLoadingException, URISyntaxException, StorageException, IOException
  {
    final String value = "bucket";
    final File pulledFile = AzureTestUtils.createZipTempFile(SEGMENT_FILE_NAME, value);
    final File toDir = FileUtils.createTempDir();
    try {
      final InputStream zipStream = new FileInputStream(pulledFile);

      EasyMock.expect(azureStorage.getBlobInputStream(CONTAINER_NAME, BLOB_PATH)).andReturn(zipStream);

      replayAll();

      AzureDataSegmentPuller puller = new AzureDataSegmentPuller(azureStorage);

      FileUtils.FileCopyResult result = puller.getSegmentFiles(CONTAINER_NAME, BLOB_PATH_HADOOP, toDir);

      File expected = new File(toDir, SEGMENT_FILE_NAME);
      Assert.assertEquals(value.length(), result.size());
      Assert.assertTrue(expected.exists());
      Assert.assertEquals(value.length(), expected.length());

      verifyAll();
    }
    finally {
      pulledFile.delete();
      FileUtils.deleteDirectory(toDir);
    }
  }

  @Test(expected = RuntimeException.class)
  public void test_getSegmentFiles_nonRecoverableErrorRaisedWhenPullingSegmentFiles_doNotDeleteOutputDirectory()
      throws IOException, URISyntaxException, StorageException, SegmentLoadingException
  {

    final File outDir = FileUtils.createTempDir();
    try {
      EasyMock.expect(azureStorage.getBlobInputStream(CONTAINER_NAME, BLOB_PATH)).andThrow(
          new URISyntaxException(
              "error",
              "error",
              404
          )
      );

      replayAll();

      AzureDataSegmentPuller puller = new AzureDataSegmentPuller(azureStorage);

      puller.getSegmentFiles(CONTAINER_NAME, BLOB_PATH, outDir);
    }
    catch (Exception e) {
      Assert.assertTrue(outDir.exists());
      verifyAll();
      throw e;
    }
    finally {
      FileUtils.deleteDirectory(outDir);
    }
  }

  @Test(expected = SegmentLoadingException.class)
  public void test_getSegmentFiles_recoverableErrorRaisedWhenPullingSegmentFiles_deleteOutputDirectory()
      throws IOException, URISyntaxException, StorageException, SegmentLoadingException
  {

    final File outDir = FileUtils.createTempDir();
    try {
      EasyMock.expect(azureStorage.getBlobInputStream(CONTAINER_NAME, BLOB_PATH)).andThrow(
          new StorageException(null, null, 0, null, null)
      ).atLeastOnce();

      replayAll();

      AzureDataSegmentPuller puller = new AzureDataSegmentPuller(azureStorage);

      puller.getSegmentFiles(CONTAINER_NAME, BLOB_PATH, outDir);

      Assert.assertFalse(outDir.exists());

      verifyAll();
    }
    catch (Exception e) {
      Assert.assertFalse(outDir.exists());
      verifyAll();
      throw e;
    }
    finally {
      FileUtils.deleteDirectory(outDir);
    }
  }
}
