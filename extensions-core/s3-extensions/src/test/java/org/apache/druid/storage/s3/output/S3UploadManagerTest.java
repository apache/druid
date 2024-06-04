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
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.apache.druid.utils.RuntimeInfo;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class S3UploadManagerTest
{

  private S3UploadManager s3UploadManager;
  private S3OutputConfig s3OutputConfig;
  private S3ExportConfig s3ExportConfig;
  private static ExecutorService uploadExecutor;

  @Before
  public void setUp()
  {
    s3OutputConfig = EasyMock.mock(S3OutputConfig.class);
    s3ExportConfig = EasyMock.mock(S3ExportConfig.class);
    final RuntimeInfo runtimeInfo = EasyMock.mock(RuntimeInfo.class);
    uploadExecutor = Execs.singleThreaded("UploadThreadPool-%d");

    EasyMock.expect(runtimeInfo.getAvailableProcessors()).andReturn(8).anyTimes();
    EasyMock.expect(s3OutputConfig.getChunkSize()).andReturn(100L * 1024 * 1024).anyTimes(); // 100 MB
    EasyMock.expect(s3OutputConfig.getMaxRetry()).andReturn(1).anyTimes();
    EasyMock.expect(s3OutputConfig.getBucket()).andReturn("bucket").anyTimes();
    EasyMock.expect(s3ExportConfig.getChunkSize()).andReturn(HumanReadableBytes.valueOf(200L * 1024 * 1024)).anyTimes(); // 200 MB

    EasyMock.replay(runtimeInfo, s3OutputConfig, s3ExportConfig);

    s3UploadManager = new TestS3UploadManager(s3OutputConfig, s3ExportConfig, runtimeInfo);
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
  }

  @Test
  public void testComputeMaxNumConcurrentChunks()
  {
    int maxNumConcurrentChunks = s3UploadManager.computeMaxNumConcurrentChunks(s3OutputConfig, s3ExportConfig);
    int expectedMaxNumConcurrentChunks = 25; // maxChunkSizePossible/200 MB
    assertEquals(expectedMaxNumConcurrentChunks, maxNumConcurrentChunks);
  }

  @Test
  public void testComputeMaxNumConcurrentChunksWithNullOutputConfig()
  {
    // Null S3OutputConfig
    int maxNumConcurrentChunks = s3UploadManager.computeMaxNumConcurrentChunks(null, s3ExportConfig);
    int expectedMaxNumConcurrentChunks = 25; // maxChunkSizePossible / s3ExportConfig's chunk size
    assertEquals(expectedMaxNumConcurrentChunks, maxNumConcurrentChunks);

    // Null S3OutputConfig#getChunkSize()
    maxNumConcurrentChunks = s3UploadManager.computeMaxNumConcurrentChunks(EasyMock.mock(S3OutputConfig.class), s3ExportConfig);
    assertEquals(expectedMaxNumConcurrentChunks, maxNumConcurrentChunks);
  }

  @Test
  public void testComputeMaxNumConcurrentChunksWithNullExportConfig()
  {
    // Null S3ExportConfig
    int maxNumConcurrentChunks = s3UploadManager.computeMaxNumConcurrentChunks(s3OutputConfig, null);
    int expectedMaxNumConcurrentChunks = 51; // maxChunkSizePossible / s3OutputConfig's chunk size
    assertEquals(expectedMaxNumConcurrentChunks, maxNumConcurrentChunks);

    // Null S3ExportConfig#getChunkSize()
    maxNumConcurrentChunks = s3UploadManager.computeMaxNumConcurrentChunks(s3OutputConfig, EasyMock.mock(S3ExportConfig.class));
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

  @Test
  public void testStartAndStop()
  {
    s3UploadManager.start();
    s3UploadManager.stop();

    assertTrue(uploadExecutor.isShutdown());
  }

  private static class TestS3UploadManager extends S3UploadManager
  {
    public TestS3UploadManager(S3OutputConfig s3OutputConfig, S3ExportConfig s3ExportConfig, RuntimeInfo runtimeInfo)
    {
      super(s3OutputConfig, s3ExportConfig, runtimeInfo);
    }

    @Override
    ExecutorService createExecutorService(int poolSize, int maxNumConcurrentChunks)
    {
      return uploadExecutor;
    }
  }
}
