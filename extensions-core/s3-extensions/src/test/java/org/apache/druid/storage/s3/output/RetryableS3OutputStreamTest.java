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

import com.amazonaws.AmazonClientException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RetryableS3OutputStreamTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final TestAmazonS3 s3 = new TestAmazonS3(0);
  private final String path = "resultId";


  private S3OutputConfig config;
  private long maxResultsSize;
  private long chunkSize;

  @Before
  public void setup() throws IOException
  {
    final File tempDir = temporaryFolder.newFolder();
    chunkSize = 10L;
    config = new S3OutputConfig(
        "TEST",
        "TEST",
        tempDir,
        HumanReadableBytes.valueOf(chunkSize),
        HumanReadableBytes.valueOf(maxResultsSize),
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
      public long getMaxResultsSize()
      {
        return maxResultsSize;
      }

      @Override
      public int getMaxRetry()
      {
        return 2;
      }
    };
  }

  @Test
  public void testWriteAndHappy() throws IOException
  {
    maxResultsSize = 1000;
    chunkSize = 10;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    try (RetryableS3OutputStream out = new RetryableS3OutputStream(
        config,
        s3,
        path,
        false
    )) {
      for (int i = 0; i < 25; i++) {
        bb.clear();
        bb.putInt(i);
        out.write(bb.array());
      }
    }
    // each chunk is 10 bytes, so there should be 10 chunks.
    Assert.assertEquals(10, s3.partRequests.size());
    s3.assertCompleted(chunkSize, Integer.BYTES * 25);
  }

  @Test
  public void testWriteSizeLargerThanConfiguredMaxChunkSizeShouldSucceed() throws IOException
  {
    maxResultsSize = 1000;
    chunkSize = 10;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES * 3);
    try (RetryableS3OutputStream out = new RetryableS3OutputStream(
        config,
        s3,
        path,
        false
    )) {
      bb.clear();
      bb.putInt(1);
      bb.putInt(2);
      bb.putInt(3);
      out.write(bb.array());
    }
    // each chunk 10 bytes, so there should be 2 chunks.
    Assert.assertEquals(2, s3.partRequests.size());
    s3.assertCompleted(chunkSize, Integer.BYTES * 3);
  }

  @Test
  public void testWriteSmallBufferShouldSucceed() throws IOException
  {
    maxResultsSize = 1000;
    chunkSize = 128;
    try (RetryableS3OutputStream out = new RetryableS3OutputStream(
        config,
        s3,
        path,
        false
    )) {
      for (int i = 0; i < 600; i++) {
        out.write(i);
      }
    }
    // each chunk 128 bytes, so there should be 5 chunks.
    Assert.assertEquals(5, s3.partRequests.size());
    s3.assertCompleted(chunkSize, 600);
  }

  @Test
  public void testHitResultsSizeLimit() throws IOException
  {
    maxResultsSize = 50;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    try (RetryableS3OutputStream out = new RetryableS3OutputStream(
        config,
        s3,
        path,
        false
    )) {
      for (int i = 0; i < 14; i++) {
        bb.clear();
        bb.putInt(i);
        out.write(bb.array());
      }

      Assert.assertThrows(
          "Exceeded max results size [50]",
          IOException.class,
          () -> {
            bb.clear();
            bb.putInt(14);
            out.write(bb.array());
          }
      );
    }

    s3.assertCancelled();
  }

  @Test
  public void testSuccessToUploadAfterRetry() throws IOException
  {
    final TestAmazonS3 s3 = new TestAmazonS3(1);

    maxResultsSize = 1000;
    chunkSize = 10;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    try (RetryableS3OutputStream out = new RetryableS3OutputStream(
        config,
        s3,
        path,
        false
    )) {
      for (int i = 0; i < 25; i++) {
        bb.clear();
        bb.putInt(i);
        out.write(bb.array());
      }
    }
    // each chunk is 10 bytes, so there should be 10 chunks.
    Assert.assertEquals(10, s3.partRequests.size());
    s3.assertCompleted(chunkSize, Integer.BYTES * 25);
  }

  @Test
  public void testFailToUploadAfterRetries() throws IOException
  {
    final TestAmazonS3 s3 = new TestAmazonS3(3);

    maxResultsSize = 1000;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    try (RetryableS3OutputStream out = new RetryableS3OutputStream(
        config,
        s3,
        path,
        false
    )) {
      for (int i = 0; i < 2; i++) {
        bb.clear();
        bb.putInt(i);
        out.write(bb.array());
      }

      expectedException.expect(RuntimeException.class);
      expectedException.expectCause(CoreMatchers.instanceOf(AmazonClientException.class));
      expectedException.expectMessage("Upload failure test. Remaining failures [1]");
      bb.clear();
      bb.putInt(3);
      out.write(bb.array());
    }

    s3.assertCancelled();
  }

  private static class TestAmazonS3 extends ServerSideEncryptingAmazonS3
  {
    private final List<UploadPartRequest> partRequests = new ArrayList<>();

    private int uploadFailuresLeft;
    private boolean cancelled = false;
    @Nullable
    private CompleteMultipartUploadRequest completeRequest;

    private TestAmazonS3(int totalUploadFailures)
    {
      super(EasyMock.createMock(AmazonS3.class), new NoopServerSideEncryption());
      this.uploadFailuresLeft = totalUploadFailures;
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
        throws SdkClientException
    {
      InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
      result.setUploadId("uploadId");
      return result;
    }

    @Override
    public UploadPartResult uploadPart(UploadPartRequest request) throws SdkClientException
    {
      if (uploadFailuresLeft > 0) {
        throw new AmazonClientException(
            new IOE("Upload failure test. Remaining failures [%s]", --uploadFailuresLeft)
        );
      }
      partRequests.add(request);
      UploadPartResult result = new UploadPartResult();
      result.setETag(StringUtils.format("%s", request.getPartNumber()));
      result.setPartNumber(request.getPartNumber());
      return result;
    }

    @Override
    public void cancelMultiPartUpload(AbortMultipartUploadRequest request) throws SdkClientException
    {
      cancelled = true;
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
        throws SdkClientException
    {
      completeRequest = request;
      return new CompleteMultipartUploadResult();
    }

    private void assertCompleted(long chunkSize, long expectedFileSize)
    {
      Assert.assertNotNull(completeRequest);
      Assert.assertFalse(cancelled);

      for (int i = 0; i < partRequests.size(); i++) {
        Assert.assertEquals(i + 1, partRequests.get(i).getPartNumber());
        if (i < partRequests.size() - 1) {
          Assert.assertEquals(chunkSize, partRequests.get(i).getPartSize());
        } else {
          Assert.assertTrue(chunkSize >= partRequests.get(i).getPartSize());
        }
      }
      final List<PartETag> eTags = completeRequest.getPartETags();
      Assert.assertEquals(partRequests.size(), eTags.size());
      Assert.assertEquals(
          partRequests.stream().map(UploadPartRequest::getPartNumber).collect(Collectors.toList()),
          eTags.stream().map(PartETag::getPartNumber).collect(Collectors.toList())
      );
      Assert.assertEquals(
          partRequests.stream().map(UploadPartRequest::getPartNumber).collect(Collectors.toList()),
          eTags.stream().map(tag -> Integer.parseInt(tag.getETag())).collect(Collectors.toList())
      );
      Assert.assertEquals(
          expectedFileSize,
          partRequests.stream().mapToLong(UploadPartRequest::getPartSize).sum()
      );
    }

    private void assertCancelled()
    {
      Assert.assertTrue(cancelled);
      Assert.assertNull(completeRequest);
    }
  }
}
