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

package io.druid.storage.azure;

import com.microsoft.azure.storage.StorageException;
import io.druid.java.util.common.FileUtils;
import io.druid.segment.loading.SegmentLoadingException;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AzureDataSegmentPullerTest extends EasyMockSupport
{

  private static final String SEGMENT_FILE_NAME = "segment";
  private static final String containerName = "container";
  private static final String blobPath = "/path/to/storage/index.zip";
  private AzureStorage azureStorage;

  @Before
  public void before()
  {
    azureStorage = createMock(AzureStorage.class);
  }

  @Test
  public void testZIPUncompress() throws SegmentLoadingException, URISyntaxException, StorageException, IOException
  {
    final String value = "bucket";
    final File pulledFile = AzureTestUtils.createZipTempFile(SEGMENT_FILE_NAME, value);
    final File toDir = Files.createTempDirectory("druid").toFile();
    try {
      final InputStream zipStream = new FileInputStream(pulledFile);

      expect(azureStorage.getBlobInputStream(containerName, blobPath)).andReturn(zipStream);

      replayAll();

      AzureDataSegmentPuller puller = new AzureDataSegmentPuller(azureStorage);

      FileUtils.FileCopyResult result = puller.getSegmentFiles(containerName, blobPath, toDir);

      File expected = new File(toDir, SEGMENT_FILE_NAME);
      assertEquals(value.length(), result.size());
      assertTrue(expected.exists());
      assertEquals(value.length(), expected.length());

      verifyAll();
    }
    finally {
      pulledFile.delete();
      org.apache.commons.io.FileUtils.deleteDirectory(toDir);
    }
  }

  @Test(expected = RuntimeException.class)
  public void testDeleteOutputDirectoryWhenErrorIsRaisedPullingSegmentFiles()
      throws IOException, URISyntaxException, StorageException, SegmentLoadingException
  {

    final File outDir = Files.createTempDirectory("druid").toFile();
    try {
      expect(azureStorage.getBlobInputStream(containerName, blobPath)).andThrow(
          new StorageException(
              "error",
              "error",
              404,
              null,
              null
          )
      );

      replayAll();

      AzureDataSegmentPuller puller = new AzureDataSegmentPuller(azureStorage);

      puller.getSegmentFiles(containerName, blobPath, outDir);

      assertFalse(outDir.exists());

      verifyAll();
    }
    finally {
      org.apache.commons.io.FileUtils.deleteDirectory(outDir);
    }

  }
}
