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

import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.apache.druid.storage.s3.S3TransferConfig;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RetryableS3OutputStreamTest
{
  @TempDir
  public File temporaryFolder;

  private final TestAmazonS3 s3 = new TestAmazonS3(0);
  private final String path = "resultId";

  private S3OutputConfig config;
  private long chunkSize;

  private S3UploadManager s3UploadManager;


  @BeforeEach
  public void setup() throws IOException
  {
    final File tempDir = FileUtils.createTempDirInLocation(temporaryFolder.toPath(), "s3output");
    chunkSize = 10L;
    config = new S3OutputConfig(
        "TEST",
        "TEST",
        tempDir,
        HumanReadableBytes.valueOf(chunkSize),
        2,
        false
    )
    {
      @Override
      public File getTempDir()
      {
        return tempDir;
      }

      @Override
      public Long getChunkSize()
      {
        return chunkSize;
      }

      @Override
      public int getMaxRetry()
      {
        return 2;
      }
    };

    s3UploadManager = new S3UploadManager(
        new S3OutputConfig("bucket", "prefix", EasyMock.mock(File.class), new HumanReadableBytes("5MiB"), 1),
        new S3ExportConfig("tempDir", new HumanReadableBytes("5MiB"), 1, null),
        new DruidProcessingConfigTest.MockRuntimeInfo(10, 0, 0),
        new StubServiceEmitter());
  }

  @Test
  public void testWriteAndHappy() throws IOException
  {
    chunkSize = 10;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    try (RetryableS3OutputStream out =
             new RetryableS3OutputStream(config, s3, path, s3UploadManager)) {
      for (int i = 0; i < 25; i++) {
        bb.clear();
        bb.putInt(i);
        out.write(bb.array());
      }
    }
    // each chunk is 10 bytes, so there should be 10 chunks.
    Assertions.assertEquals(10, s3.partRequests.size());
    s3.assertCompleted(chunkSize, Integer.BYTES * 25);
  }

  @Test
  public void testWriteSizeLargerThanConfiguredMaxChunkSizeShouldSucceed() throws IOException
  {
    chunkSize = 10;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES * 3);
    try (RetryableS3OutputStream out =
             new RetryableS3OutputStream(config, s3, path, s3UploadManager)) {
      bb.clear();
      bb.putInt(1);
      bb.putInt(2);
      bb.putInt(3);
      out.write(bb.array());
    }
    // each chunk 10 bytes, so there should be 2 chunks.
    Assertions.assertEquals(2, s3.partRequests.size());
    s3.assertCompleted(chunkSize, Integer.BYTES * 3);
  }

  @Test
  public void testWriteSmallBufferShouldSucceed() throws IOException
  {
    chunkSize = 128;
    try (RetryableS3OutputStream out =
             new RetryableS3OutputStream(config, s3, path, s3UploadManager)) {
      for (int i = 0; i < 600; i++) {
        out.write(i);
      }
    }
    // each chunk 128 bytes, so there should be 5 chunks.
    Assertions.assertEquals(5, s3.partRequests.size());
    s3.assertCompleted(chunkSize, 600);
  }

  @Test
  public void testWriteSmallBufferExactChunkSizeShouldSucceed() throws IOException
  {
    chunkSize = 128;
    final int fileSize = 128 * 5;
    try (RetryableS3OutputStream out =
             new RetryableS3OutputStream(config, s3, path, s3UploadManager)) {
      for (int i = 0; i < fileSize; i++) {
        out.write(i);
      }
    }
    // each chunk 128 bytes, so there should be 5 chunks.
    Assertions.assertEquals(5, s3.partRequests.size());
    s3.assertCompleted(chunkSize, fileSize);
  }

  @Test
  public void testSuccessToUploadAfterRetry() throws IOException
  {
    final TestAmazonS3 s3 = new TestAmazonS3(1);

    chunkSize = 10;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    try (RetryableS3OutputStream out =
             new RetryableS3OutputStream(config, s3, path, s3UploadManager)) {
      for (int i = 0; i < 25; i++) {
        bb.clear();
        bb.putInt(i);
        out.write(bb.array());
      }
    }
    // each chunk is 10 bytes, so there should be 10 chunks.
    Assertions.assertEquals(10, s3.partRequests.size());
    s3.assertCompleted(chunkSize, Integer.BYTES * 25);
  }

  /**
   * A part that fails every retry leaves the multipart upload aborted and no object at the key, so close() must report
   * it. Returning normally would tell the caller its bytes are readable back when they no longer exist anywhere.
   */
  @Test
  public void testFailToUploadAfterRetries()
  {
    final TestAmazonS3 s3 = new TestAmazonS3(3);

    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    final DruidException e = Assertions.assertThrows(DruidException.class, () -> {
      try (RetryableS3OutputStream out =
               new RetryableS3OutputStream(config, s3, path, s3UploadManager)) {
        for (int i = 0; i < 2; i++) {
          bb.clear();
          bb.putInt(i);
          out.write(bb.array());
        }

        bb.clear();
        bb.putInt(3);
        out.write(bb.array());
      }
    });

    Assertions.assertTrue(e.getMessage().contains("no object was written"), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains(path), e.getMessage());
    Assertions.assertNotNull(e.getCause());
    // An aborted upload means S3 rejected the parts, which an operator can act on, rather than a Druid defect.
    Assertions.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
    Assertions.assertEquals(DruidException.Category.RUNTIME_FAILURE, e.getCategory());
    s3.assertCancelled();
  }

  /**
   * A multipart upload costs three S3 requests at minimum — create, upload part, complete — so a stream small enough
   * to need only one part should not use one. A task writes one object per output partition, and every request lands
   * on the same key prefix, so the per-object floor sets the burst rate against that prefix.
   */
  @Test
  public void testStreamFittingInOneChunkIsUploadedWithASinglePut() throws IOException
  {
    chunkSize = 10;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    try (RetryableS3OutputStream out =
             new RetryableS3OutputStream(config, s3, path, s3UploadManager)) {
      bb.putInt(1);
      out.write(bb.array());
    }

    Assertions.assertEquals(0, s3.createMultipartUploadCount);
    Assertions.assertEquals(0, s3.partRequests.size());
    Assertions.assertNull(s3.completeRequest);
    Assertions.assertEquals(ImmutableList.of((long) Integer.BYTES), s3.putObjectContentLengths);
  }

  /**
   * Once a stream outgrows a single chunk it must use multipart, costing one create, one request per part, and one
   * complete.
   */
  @Test
  public void testStreamSpanningMultipleChunksUsesMultipartUpload() throws IOException
  {
    chunkSize = 10;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    try (RetryableS3OutputStream out =
             new RetryableS3OutputStream(config, s3, path, s3UploadManager)) {
      for (int i = 0; i < 25; i++) {
        bb.clear();
        bb.putInt(i);
        out.write(bb.array());
      }
    }

    Assertions.assertEquals(1, s3.createMultipartUploadCount);
    Assertions.assertEquals(10, s3.partRequests.size());
    Assertions.assertEquals(0, s3.putObjectContentLengths.size());
    s3.assertCompleted(chunkSize, Integer.BYTES * 25);
  }

  /**
   * A stream closed without any bytes written produces no object, and should reach S3 not at all to do so.
   */
  @Test
  public void testClosingWithoutWritingCreatesNoObject() throws IOException
  {
    chunkSize = 10;
    new RetryableS3OutputStream(config, s3, path, s3UploadManager).close();

    s3.assertNoRequestsIssued();
  }

  private static class TestAmazonS3 extends ServerSideEncryptingAmazonS3
  {
    private final List<UploadPartRequest> partRequests = new ArrayList<>();
    private final List<Long> putObjectContentLengths = new ArrayList<>();

    private int uploadFailuresLeft;
    private int createMultipartUploadCount = 0;
    private boolean cancelled = false;
    @Nullable
    private CompleteMultipartUploadRequest completeRequest;

    private TestAmazonS3(int totalUploadFailures)
    {
      super(EasyMock.createMock(S3Client.class), null, new NoopServerSideEncryption(), new S3TransferConfig());
      this.uploadFailuresLeft = totalUploadFailures;
    }

    @Override
    public CreateMultipartUploadResponse createMultipartUpload(CreateMultipartUploadRequest.Builder requestBuilder)
        throws SdkClientException
    {
      ++createMultipartUploadCount;
      return CreateMultipartUploadResponse.builder()
          .uploadId("uploadId")
          .build();
    }

    @Override
    public PutObjectResponse putObject(String bucket, String key, File file)
    {
      putObjectContentLengths.add(file.length());
      return PutObjectResponse.builder().build();
    }

    private void assertNoRequestsIssued()
    {
      Assertions.assertEquals(0, createMultipartUploadCount);
      Assertions.assertEquals(0, putObjectContentLengths.size());
      Assertions.assertEquals(0, partRequests.size());
      Assertions.assertNull(completeRequest);
      Assertions.assertFalse(cancelled);
    }

    @Override
    public UploadPartResponse uploadPart(UploadPartRequest.Builder requestBuilder, RequestBody requestBody)
        throws SdkClientException
    {
      if (uploadFailuresLeft > 0) {
        throw SdkClientException.builder()
            .cause(new IOE("Upload failure test. Remaining failures [%s]", --uploadFailuresLeft))
            .build();
      }
      UploadPartRequest request = requestBuilder.build();
      synchronized (partRequests) {
        partRequests.add(request);
      }
      return UploadPartResponse.builder()
          .eTag(StringUtils.format("etag-%s", request.partNumber()))
          .build();
    }

    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest request)
    {
      cancelled = true;
    }

    @Override
    public CompleteMultipartUploadResponse completeMultipartUpload(CompleteMultipartUploadRequest request)
        throws SdkClientException
    {
      completeRequest = request;
      return CompleteMultipartUploadResponse.builder().build();
    }

    private void assertCompleted(long chunkSize, long expectedFileSize)
    {
      Assertions.assertNotNull(completeRequest);
      Assertions.assertFalse(cancelled);

      Set<Integer> partNumbersFromRequest = partRequests.stream().map(UploadPartRequest::partNumber).collect(Collectors.toSet());
      Assertions.assertEquals(partRequests.size(), partNumbersFromRequest.size());

      // Verify sizes of uploaded chunks
      int numSmallerChunks = 0;
      for (UploadPartRequest part : partRequests) {
        Assertions.assertTrue(part.contentLength() <= chunkSize);
        if (part.contentLength() < chunkSize) {
          ++numSmallerChunks;
        }
      }
      Assertions.assertTrue(numSmallerChunks <= 1);

      final List<CompletedPart> completedParts = completeRequest.multipartUpload().parts();
      Assertions.assertEquals(partRequests.size(), completedParts.size());
      Assertions.assertEquals(
          partNumbersFromRequest,
          completedParts.stream().map(CompletedPart::partNumber).collect(Collectors.toSet())
      );
      Assertions.assertEquals(
          partNumbersFromRequest.stream().map(partNumber -> "etag-" + partNumber).collect(Collectors.toSet()),
          completedParts.stream().map(CompletedPart::eTag).collect(Collectors.toSet())
      );
      Assertions.assertEquals(
          expectedFileSize,
          partRequests.stream().mapToLong(UploadPartRequest::contentLength).sum()
      );
    }

    private void assertCancelled()
    {
      Assertions.assertTrue(cancelled);
      Assertions.assertNull(completeRequest);
    }
  }
}
