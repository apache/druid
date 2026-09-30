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

package org.apache.druid.storage.s3.output;

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.apache.druid.utils.RuntimeInfo;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class S3UploadManagerTest
{
  @TempDir
  public File temporaryFolder;

  private S3UploadManager s3UploadManager;
  private S3OutputConfig s3OutputConfig;
  private S3ExportConfig s3ExportConfig;
  private StubServiceEmitter serviceEmitter;


  @BeforeEach
  public void setUp() throws IOException
  {
    File tempDir = FileUtils.createTempDirInLocation(temporaryFolder.toPath(), "s3output");
    s3OutputConfig = new S3OutputConfig("bucket", "prefix", tempDir, new HumanReadableBytes("100MiB"), 1);
    s3ExportConfig = new S3ExportConfig("tempDir", new HumanReadableBytes("200MiB"), 1, null);
    serviceEmitter = new StubServiceEmitter();
    final RuntimeInfo runtimeInfo = new DruidProcessingConfigTest.MockRuntimeInfo(8, 0, 0);
    s3UploadManager = new S3UploadManager(s3OutputConfig, s3ExportConfig, runtimeInfo, serviceEmitter);
  }

  @Test
  public void testQueueChunkForUpload() throws Exception
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.mock(ServerSideEncryptingAmazonS3.class);

    // Create a real temp file with actual content
    File chunkFile = File.createTempFile("chunk-test", ".tmp", temporaryFolder);
    try (FileOutputStream fos = new FileOutputStream(chunkFile)) {
      fos.write(new byte[1024]);
    }

    int chunkId = 42;
    UploadPartResponse uploadPartResponse = UploadPartResponse.builder()
        .eTag("etag")
        .build();
    EasyMock.expect(s3Client.uploadPart(EasyMock.anyObject(UploadPartRequest.Builder.class), EasyMock.anyObject(RequestBody.class)))
        .andReturn(uploadPartResponse);

    EasyMock.replay(s3Client);

    Future<UploadPartResponse> result = s3UploadManager.queueChunkForUpload(s3Client, "test-key", chunkId, chunkFile, "upload-id", s3OutputConfig);

    UploadPartResponse futureResult = result.get();
    Assertions.assertNotNull(futureResult);
    Assertions.assertEquals("etag", futureResult.eTag());

    serviceEmitter.verifyEmitted("s3/upload/part/queuedTime", 1);
    serviceEmitter.verifyEmitted("s3/upload/part/queueSize", 1);
    serviceEmitter.verifyEmitted("s3/upload/part/time", 1);
  }

  @Test
  public void testComputeMaxNumChunksOnDisk() throws IOException
  {
    int maxNumConcurrentChunks = S3UploadManager.computeMaxNumChunksOnDisk(s3OutputConfig, s3ExportConfig);
    int expectedMaxNumConcurrentChunks = 25; // maxChunkSizePossible/200 MB
    assertEquals(expectedMaxNumConcurrentChunks, maxNumConcurrentChunks);
  }

  @Test
  public void testComputeMaxNumChunksOnDiskWithNullOutputConfig()
  {
    // Null S3OutputConfig
    int maxNumConcurrentChunks = S3UploadManager.computeMaxNumChunksOnDisk(null, s3ExportConfig);
    int expectedMaxNumConcurrentChunks = 25; // maxChunkSizePossible / s3ExportConfig's chunk size
    assertEquals(expectedMaxNumConcurrentChunks, maxNumConcurrentChunks);

    // Null S3OutputConfig#getChunkSize()
    maxNumConcurrentChunks = S3UploadManager.computeMaxNumChunksOnDisk(EasyMock.mock(S3OutputConfig.class), s3ExportConfig);
    assertEquals(expectedMaxNumConcurrentChunks, maxNumConcurrentChunks);
  }

  @Test
  public void testComputeMaxNumChunksOnDiskWithNullExportConfig() throws IOException
  {
    // Null S3ExportConfig
    int maxNumConcurrentChunks = S3UploadManager.computeMaxNumChunksOnDisk(s3OutputConfig, null);
    int expectedMaxNumConcurrentChunks = 51; // maxChunkSizePossible / s3OutputConfig's chunk size
    assertEquals(expectedMaxNumConcurrentChunks, maxNumConcurrentChunks);

    // Null S3ExportConfig#getChunkSize()
    maxNumConcurrentChunks = S3UploadManager.computeMaxNumChunksOnDisk(s3OutputConfig, EasyMock.mock(S3ExportConfig.class));
    assertEquals(expectedMaxNumConcurrentChunks, maxNumConcurrentChunks);
  }

  @Test
  public void testUploadPartIfPossible() throws IOException
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.mock(ServerSideEncryptingAmazonS3.class);

    // Create a real temp file with actual content
    File chunkFile = File.createTempFile("upload-part-test", ".tmp", temporaryFolder);
    try (FileOutputStream fos = new FileOutputStream(chunkFile)) {
      fos.write(new byte[1024]);
    }

    UploadPartResponse uploadPartResponse = UploadPartResponse.builder().build();
    Capture<UploadPartRequest.Builder> partRequestCapture = EasyMock.newCapture();
    EasyMock.expect(s3Client.uploadPart(EasyMock.capture(partRequestCapture), EasyMock.anyObject(RequestBody.class)))
        .andReturn(uploadPartResponse);
    EasyMock.replay(s3Client);

    UploadPartResponse result = s3UploadManager.uploadPartIfPossible(s3Client, "upload-id", "bucket", "key", 1, chunkFile);

    UploadPartRequest capturedRequest = partRequestCapture.getValue().build();
    assertEquals("upload-id", capturedRequest.uploadId());
    assertEquals("bucket", capturedRequest.bucket());
    assertEquals("key", capturedRequest.key());
    assertEquals(1, capturedRequest.partNumber().intValue());
    assertEquals(1024L, capturedRequest.contentLength().longValue());

    assertEquals(uploadPartResponse, result);
  }


  @AfterEach
  public void teardown()
  {
    s3UploadManager.stop();
  }
}
