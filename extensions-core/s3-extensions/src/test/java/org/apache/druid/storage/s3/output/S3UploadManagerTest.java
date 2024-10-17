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

import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.apache.druid.utils.RuntimeInfo;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class S3UploadManagerTest
{

  private S3UploadManager s3UploadManager;
  private S3OutputConfig s3OutputConfig;
  private S3ExportConfig s3ExportConfig;
  private StubServiceEmitter serviceEmitter;

  @Before
  public void setUp()
  {
    s3OutputConfig = new S3OutputConfig("bucket", "prefix", EasyMock.mock(File.class), new HumanReadableBytes("100MiB"), 1);
    s3ExportConfig = new S3ExportConfig("tempDir", new HumanReadableBytes("200MiB"), 1, null);
    serviceEmitter = new StubServiceEmitter();
    final RuntimeInfo runtimeInfo = new DruidProcessingConfigTest.MockRuntimeInfo(8, 0, 0);
    s3UploadManager = new S3UploadManager(s3OutputConfig, s3ExportConfig, runtimeInfo, serviceEmitter);
  }

  @Test
  public void testQueueChunkForUpload() throws Exception
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.mock(ServerSideEncryptingAmazonS3.class);

    File chunkFile = EasyMock.mock(File.class);
    EasyMock.expect(chunkFile.length()).andReturn(1024L).anyTimes();
    EasyMock.expect(chunkFile.delete()).andReturn(true).anyTimes();

    int chunkId = 42;
    UploadPartResult uploadPartResult = new UploadPartResult();
    uploadPartResult.setPartNumber(chunkId);
    uploadPartResult.setETag("etag");
    EasyMock.expect(s3Client.uploadPart(EasyMock.anyObject(UploadPartRequest.class))).andReturn(uploadPartResult);

    EasyMock.replay(chunkFile, s3Client);

    Future<UploadPartResult> result = s3UploadManager.queueChunkForUpload(s3Client, "test-key", chunkId, chunkFile, "upload-id", s3OutputConfig);

    UploadPartResult futureResult = result.get();
    Assert.assertEquals(chunkId, futureResult.getPartNumber());
    Assert.assertEquals("etag", futureResult.getETag());

    serviceEmitter.verifyEmitted("s3/upload/part/queuedTime", 1);
    serviceEmitter.verifyEmitted("s3/upload/part/queueSize", 1);
    serviceEmitter.verifyEmitted("s3/upload/part/time", 1);
  }

  @Test
  public void testComputeMaxNumChunksOnDisk()
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
  public void testComputeMaxNumChunksOnDiskWithNullExportConfig()
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
  public void testUploadPartIfPossible()
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.mock(ServerSideEncryptingAmazonS3.class);

    File chunkFile = EasyMock.mock(File.class);
    EasyMock.expect(chunkFile.length()).andReturn(1024L).anyTimes();

    UploadPartResult uploadPartResult = new UploadPartResult();
    Capture<UploadPartRequest> partRequestCapture = EasyMock.newCapture();
    EasyMock.expect(s3Client.uploadPart(EasyMock.capture(partRequestCapture))).andReturn(uploadPartResult);
    EasyMock.replay(s3Client, chunkFile);

    UploadPartResult result = s3UploadManager.uploadPartIfPossible(s3Client, "upload-id", "bucket", "key", 1, chunkFile);

    UploadPartRequest capturedRequest = partRequestCapture.getValue();
    assertEquals("upload-id", capturedRequest.getUploadId());
    assertEquals("bucket", capturedRequest.getBucketName());
    assertEquals("key", capturedRequest.getKey());
    assertEquals(1, capturedRequest.getPartNumber());
    assertEquals(chunkFile, capturedRequest.getFile());
    assertEquals(1024L, capturedRequest.getPartSize());

    assertEquals(uploadPartResult, result);
  }

  @After
  public void teardown()
  {
    s3UploadManager.stop();
  }
}
