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

import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AzureDataSegmentPullerTest extends EasyMockSupport
{
  private static final String CONTAINER_NAME = "container";
  private static final String BLOB_PATH = "path/to/storage/index.zip";
  private static final String UNZIPPED_BLOB_PATH = "path/to/storage/";
  private AzureStorage azureStorage;
  private AzureByteSourceFactory byteSourceFactory;

  @BeforeEach
  public void before()
  {
    azureStorage = createMock(AzureStorage.class);
    byteSourceFactory = createMock(AzureByteSourceFactory.class);
  }

  @Test
  public void test_getSegmentFiles_success(@TempDir Path sourcePath, @TempDir Path targetPath)
      throws IOException, SegmentLoadingException
  {
    final String segmentFileName = "segment";
    final String value = "bucket";

    final File pulledFile = createZipTempFile(sourcePath, segmentFileName, value);

    final InputStream zipStream = Files.newInputStream(pulledFile.toPath());
    final AzureAccountConfig config = new AzureAccountConfig();

    EasyMock.expect(byteSourceFactory.create(CONTAINER_NAME, BLOB_PATH, azureStorage))
            .andReturn(new AzureByteSource(azureStorage, CONTAINER_NAME, BLOB_PATH));
    EasyMock.expect(azureStorage.getBlockBlobInputStream(0L, CONTAINER_NAME, BLOB_PATH)).andReturn(zipStream);

    replayAll();

    AzureDataSegmentPuller puller = new AzureDataSegmentPuller(byteSourceFactory, azureStorage, config);

    FileUtils.FileCopyResult result = puller.getSegmentFiles(CONTAINER_NAME, BLOB_PATH, targetPath.toFile());

    File expected = new File(targetPath.toFile(), segmentFileName);
    assertEquals(value.length(), result.size());
    assertTrue(expected.exists());
    assertEquals(value.length(), expected.length());

    verifyAll();
  }

  @Test
  public void test_getSegmentFiles_blobPathIsHadoop_success(@TempDir Path sourcePath, @TempDir Path targetPath)
      throws IOException, SegmentLoadingException
  {
    final String segmentFileName = "segment";
    final String value = "bucket";

    final File pulledFile = createZipTempFile(sourcePath, segmentFileName, value);

    final InputStream zipStream = Files.newInputStream(pulledFile.toPath());
    final AzureAccountConfig config = new AzureAccountConfig();

    EasyMock.expect(byteSourceFactory.create(CONTAINER_NAME, BLOB_PATH, azureStorage))
            .andReturn(new AzureByteSource(azureStorage, CONTAINER_NAME, BLOB_PATH));
    EasyMock.expect(azureStorage.getBlockBlobInputStream(0L, CONTAINER_NAME, BLOB_PATH)).andReturn(zipStream);

    replayAll();

    AzureDataSegmentPuller puller = new AzureDataSegmentPuller(byteSourceFactory, azureStorage, config);

    final String blobPathHadoop = AzureUtils.AZURE_STORAGE_HOST_ADDRESS + "/path/to/storage/index.zip";
    FileUtils.FileCopyResult result = puller.getSegmentFiles(CONTAINER_NAME, blobPathHadoop, targetPath.toFile());

    File expected = new File(targetPath.toFile(), segmentFileName);
    assertEquals(value.length(), result.size());
    assertTrue(expected.exists());
    assertEquals(value.length(), expected.length());

    verifyAll();
  }

  @Test
  public void test_getSegmentFiles_unzippedSegment_pullsEachBlob(@TempDir Path sourcePath, @TempDir Path targetPath)
      throws IOException, SegmentLoadingException
  {
    final AzureAccountConfig config = new AzureAccountConfig();
    final String versionBlob = UNZIPPED_BLOB_PATH + "version.bin";
    final String smooshBlob = UNZIPPED_BLOB_PATH + "meta.smoosh";
    final String versionValue = "version";
    final String smooshValue = "smoosh";

    EasyMock.expect(azureStorage.listBlobs(CONTAINER_NAME, UNZIPPED_BLOB_PATH, null, config.getMaxTries()))
            .andReturn(ImmutableList.of(versionBlob, smooshBlob));
    expectBlobContents(sourcePath, versionBlob, versionValue);
    expectBlobContents(sourcePath, smooshBlob, smooshValue);

    replayAll();

    AzureDataSegmentPuller puller = new AzureDataSegmentPuller(byteSourceFactory, azureStorage, config);

    FileUtils.FileCopyResult result = puller.getSegmentFiles(CONTAINER_NAME, UNZIPPED_BLOB_PATH, targetPath.toFile());

    // the blobs land in outDir under their own names, not unpacked from a zip
    final File version = new File(targetPath.toFile(), "version.bin");
    final File smoosh = new File(targetPath.toFile(), "meta.smoosh");
    assertTrue(version.exists());
    assertTrue(smoosh.exists());
    assertEquals(versionValue.length(), version.length());
    assertEquals(smooshValue.length(), smoosh.length());
    assertEquals(versionValue.length() + smooshValue.length(), result.size());

    verifyAll();
  }

  @Test
  public void test_getSegmentFiles_nonRecoverableErrorRaisedWhenPullingSegmentFiles_doNotDeleteOutputDirectory(
      @TempDir Path tempPath
  )
  {
    final AzureAccountConfig config = new AzureAccountConfig();

    EasyMock.expect(byteSourceFactory.create(CONTAINER_NAME, BLOB_PATH, azureStorage))
            .andReturn(new AzureByteSource(azureStorage, CONTAINER_NAME, BLOB_PATH));
    EasyMock.expect(azureStorage.getBlockBlobInputStream(0L, CONTAINER_NAME, BLOB_PATH))
            .andThrow(new RuntimeException("error"));

    AzureDataSegmentPuller puller = new AzureDataSegmentPuller(byteSourceFactory, azureStorage, config);

    replayAll();

    assertThrows(
        RuntimeException.class,
        () -> puller.getSegmentFiles(CONTAINER_NAME, BLOB_PATH, tempPath.toFile())
    );
    assertTrue(tempPath.toFile().exists());

    verifyAll();
  }

  @Test
  public void test_getSegmentFiles_recoverableErrorRaisedWhenPullingSegmentFiles_deleteOutputDirectory(
      @TempDir Path tempPath
  )
  {
    final AzureAccountConfig config = new AzureAccountConfig();

    final HttpResponse httpResponse = createMock(HttpResponse.class);
    EasyMock.expect(httpResponse.getStatusCode()).andReturn(500).anyTimes();
    EasyMock.replay(httpResponse);
    EasyMock.expect(byteSourceFactory.create(CONTAINER_NAME, BLOB_PATH, azureStorage))
            .andReturn(new AzureByteSource(azureStorage, CONTAINER_NAME, BLOB_PATH));
    EasyMock.expect(azureStorage.getBlockBlobInputStream(0L, CONTAINER_NAME, BLOB_PATH)).andThrow(
        new BlobStorageException("", httpResponse, null)
    ).atLeastOnce();

    EasyMock.replay(azureStorage);
    EasyMock.replay(byteSourceFactory);

    AzureDataSegmentPuller puller = new AzureDataSegmentPuller(byteSourceFactory, azureStorage, config);

    assertThrows(
        SegmentLoadingException.class,
        () -> puller.getSegmentFiles(CONTAINER_NAME, BLOB_PATH, tempPath.toFile())
    );

    assertFalse(tempPath.toFile().exists());
    verifyAll();
  }

  /**
   * Sets up {@link #byteSourceFactory} and {@link #azureStorage} to serve {@code value} as the contents of
   * {@code blobPath}, backed by a real file so that the copy has something to read.
   */
  private void expectBlobContents(final Path sourcePath, final String blobPath, final String value)
      throws IOException
  {
    final File blobFile = Files.createFile(sourcePath.resolve(Paths.get(blobPath).getFileName().toString())).toFile();
    Files.write(blobFile.toPath(), value.getBytes(StandardCharsets.UTF_8));

    EasyMock.expect(byteSourceFactory.create(CONTAINER_NAME, blobPath, azureStorage))
            .andReturn(new AzureByteSource(azureStorage, CONTAINER_NAME, blobPath));
    EasyMock.expect(azureStorage.getBlockBlobInputStream(0L, CONTAINER_NAME, blobPath))
            .andReturn(Files.newInputStream(blobFile.toPath()));
  }

  @SuppressWarnings("SameParameterValue")
  private static File createZipTempFile(
      final Path tempPath,
      final String entry,
      final String entryValue
  ) throws IOException
  {
    final File zipFile = Files.createFile(tempPath.resolve("index.zip")).toFile();

    try (ZipOutputStream zipStream = new ZipOutputStream(Files.newOutputStream(zipFile.toPath()))) {
      zipStream.putNextEntry(new ZipEntry(entry));
      zipStream.write(entryValue.getBytes(StandardCharsets.UTF_8));
    }

    return zipFile;
  }
}
