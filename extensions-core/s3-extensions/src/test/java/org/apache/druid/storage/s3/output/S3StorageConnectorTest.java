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

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.apache.druid.storage.s3.S3TransferConfig;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableCauseMatcher;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class S3StorageConnectorTest
{
  private static final String BUCKET = "BUCKET";
  private static final String PREFIX = "P/R/E/F/I/X";
  public static final String TEST_FILE = "test.csv";

  private final AmazonS3Client s3Client = EasyMock.createMock(AmazonS3Client.class);
  private final ServerSideEncryptingAmazonS3 service = new ServerSideEncryptingAmazonS3(
      s3Client,
      new NoopServerSideEncryption(),
      new S3TransferConfig()
  );
  private final ListObjectsV2Result testResult = EasyMock.createMock(ListObjectsV2Result.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private StorageConnector storageConnector;

  @Before
  public void setup()
  {
    try {
      storageConnector = new S3StorageConnector(new S3OutputConfig(
          BUCKET,
          PREFIX,
          temporaryFolder.newFolder(),
          null,
          null,
          true
      ), service);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void pathExists_yes() throws IOException
  {
    final Capture<GetObjectMetadataRequest> request = Capture.newInstance();
    EasyMock.reset(s3Client);
    EasyMock.expect(s3Client.getObjectMetadata(EasyMock.capture(request)))
            .andReturn(new ObjectMetadata());
    EasyMock.replay(s3Client);
    Assert.assertTrue(storageConnector.pathExists(TEST_FILE));
    Assert.assertEquals(BUCKET, request.getValue().getBucketName());
    Assert.assertEquals(PREFIX + "/" + TEST_FILE, request.getValue().getKey());
    EasyMock.verify(s3Client);
  }

  @Test
  public void pathExists_notFound() throws IOException
  {
    final Capture<GetObjectMetadataRequest> request = Capture.newInstance();
    final AmazonS3Exception e = new AmazonS3Exception("not found");
    e.setStatusCode(404);

    EasyMock.reset(s3Client);
    EasyMock.expect(s3Client.getObjectMetadata(EasyMock.capture(request)))
            .andThrow(e);
    EasyMock.replay(s3Client);
    Assert.assertFalse(storageConnector.pathExists(TEST_FILE));
    Assert.assertEquals(BUCKET, request.getValue().getBucketName());
    Assert.assertEquals(PREFIX + "/" + TEST_FILE, request.getValue().getKey());
    EasyMock.verify(s3Client);
  }

  @Test
  public void pathExists_error()
  {
    final Capture<GetObjectMetadataRequest> request = Capture.newInstance();
    final AmazonS3Exception e = new AmazonS3Exception("not found");
    e.setStatusCode(403);

    EasyMock.reset(s3Client);
    EasyMock.expect(s3Client.getObjectMetadata(EasyMock.capture(request)))
            .andThrow(e);
    EasyMock.replay(s3Client);
    final IOException e2 = Assert.assertThrows(
        IOException.class,
        () -> storageConnector.pathExists(TEST_FILE)
    );
    Assert.assertEquals(BUCKET, request.getValue().getBucketName());
    Assert.assertEquals(PREFIX + "/" + TEST_FILE, request.getValue().getKey());
    MatcherAssert.assertThat(e2, ThrowableCauseMatcher.hasCause(CoreMatchers.instanceOf(AmazonS3Exception.class)));
    EasyMock.verify(s3Client);
  }

  @Test
  public void pathRead() throws IOException
  {
    EasyMock.reset(s3Client);
    ObjectMetadata objectMetadata = new ObjectMetadata();
    long contentLength = "test".getBytes(StandardCharsets.UTF_8).length;
    objectMetadata.setContentLength(contentLength);
    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream("test".getBytes(StandardCharsets.UTF_8)));
    EasyMock.expect(s3Client.getObjectMetadata(EasyMock.anyObject())).andReturn(objectMetadata);
    EasyMock.expect(s3Client.getObject(
        new GetObjectRequest(BUCKET, PREFIX + "/" + TEST_FILE).withRange(0, contentLength - 1))
    ).andReturn(s3Object);
    EasyMock.replay(s3Client);

    String readText = new BufferedReader(
        new InputStreamReader(storageConnector.read(TEST_FILE), StandardCharsets.UTF_8))
        .lines()
        .collect(Collectors.joining("\n"));

    Assert.assertEquals("test", readText);
    EasyMock.reset(s3Client);
  }

  @Test
  public void testReadRange() throws IOException
  {
    String data = "test";

    // non empty reads
    for (int start = 0; start < data.length(); start++) {
      for (int length = 1; length <= data.length() - start; length++) {
        String dataQueried = data.substring(start, start + length);
        EasyMock.reset(s3Client);
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(
            new ByteArrayInputStream(dataQueried.getBytes(StandardCharsets.UTF_8))
        );
        EasyMock.expect(
            s3Client.getObject(
                new GetObjectRequest(BUCKET, PREFIX + "/" + TEST_FILE).withRange(start, start + length - 1)
            )
        ).andReturn(s3Object);
        EasyMock.replay(s3Client);

        InputStream is = storageConnector.readRange(TEST_FILE, start, length);
        byte[] dataBytes = new byte[length];
        Assert.assertEquals(length, is.read(dataBytes));
        Assert.assertEquals(-1, is.read()); // reading further produces no data
        Assert.assertEquals(dataQueried, new String(dataBytes, StandardCharsets.UTF_8));
        EasyMock.reset(s3Client);
      }
    }

    // empty read
    EasyMock.reset(s3Client);
    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(
        new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8))
    );
    EasyMock.expect(
        s3Client.getObject(
            new GetObjectRequest(BUCKET, PREFIX + "/" + TEST_FILE).withRange(0, -1)
        )
    ).andReturn(s3Object);
    EasyMock.replay(s3Client);

    InputStream is = storageConnector.readRange(TEST_FILE, 0, 0);
    byte[] dataBytes = new byte[0];
    Assert.assertEquals(is.read(dataBytes), -1);
    Assert.assertEquals("", new String(dataBytes, StandardCharsets.UTF_8));
    EasyMock.reset(s3Client);
  }

  @Test
  public void testDeleteSinglePath() throws IOException
  {
    EasyMock.reset(s3Client);
    s3Client.deleteObject(BUCKET, PREFIX + "/" + TEST_FILE);
    EasyMock.expectLastCall();
    storageConnector.deleteFile(TEST_FILE);
    EasyMock.reset(s3Client);
  }

  @Test
  public void testDeleteMultiplePaths() throws IOException
  {
    EasyMock.reset(s3Client);
    String testFile2 = "file2";
    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(BUCKET);
    deleteObjectsRequest.withKeys(PREFIX + "/" + TEST_FILE, PREFIX + "/" + testFile2);
    Capture<DeleteObjectsRequest> capturedArgument = EasyMock.newCapture();

    EasyMock.expect(s3Client.deleteObjects(EasyMock.capture(capturedArgument))).andReturn(null).once();
    EasyMock.replay(s3Client);
    storageConnector.deleteFiles(Lists.newArrayList(TEST_FILE, testFile2));

    Assert.assertEquals(
        convertDeleteObjectsRequestToString(deleteObjectsRequest),
        convertDeleteObjectsRequestToString(capturedArgument.getValue())
    );
    EasyMock.reset(s3Client);
  }

  @Test
  public void testPathDeleteRecursively() throws IOException
  {
    EasyMock.reset(s3Client, testResult);

    S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
    s3ObjectSummary.setBucketName(BUCKET);
    s3ObjectSummary.setKey(PREFIX + "/test/" + TEST_FILE);
    s3ObjectSummary.setSize(1);
    EasyMock.expect(s3Client.listObjectsV2((ListObjectsV2Request) EasyMock.anyObject()))
            .andReturn(testResult);

    EasyMock.expect(testResult.getBucketName()).andReturn("123").anyTimes();
    EasyMock.expect(testResult.getObjectSummaries()).andReturn(Collections.singletonList(s3ObjectSummary)).anyTimes();
    EasyMock.expect(testResult.isTruncated()).andReturn(false).times(1);
    EasyMock.expect(testResult.getNextContinuationToken()).andReturn(null);

    Capture<DeleteObjectsRequest> capturedArgument = EasyMock.newCapture();
    EasyMock.expect(s3Client.deleteObjects(EasyMock.and(
        EasyMock.capture(capturedArgument),
        EasyMock.isA(DeleteObjectsRequest.class)
    ))).andReturn(null);
    EasyMock.replay(s3Client, testResult);

    storageConnector.deleteRecursively("test");

    Assert.assertEquals(1, capturedArgument.getValue().getKeys().size());
    Assert.assertEquals(PREFIX + "/test/" + TEST_FILE, capturedArgument.getValue().getKeys().get(0).getKey());
    EasyMock.reset(s3Client, testResult);
  }

  @Test
  public void testListDir() throws IOException
  {
    EasyMock.reset(s3Client, testResult);

    S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
    s3ObjectSummary.setBucketName(BUCKET);
    s3ObjectSummary.setKey(PREFIX + "/test/" + TEST_FILE);
    s3ObjectSummary.setSize(1);

    EasyMock.expect(testResult.getObjectSummaries()).andReturn(Collections.singletonList(s3ObjectSummary)).times(2);
    EasyMock.expect(testResult.isTruncated()).andReturn(false);
    EasyMock.expect(testResult.getNextContinuationToken()).andReturn(null);
    EasyMock.expect(s3Client.listObjectsV2((ListObjectsV2Request) EasyMock.anyObject()))
            .andReturn(testResult);
    EasyMock.replay(s3Client, testResult);

    List<String> listDirResult = Lists.newArrayList(storageConnector.listDir("test/"));
    Assert.assertEquals(ImmutableList.of(TEST_FILE), listDirResult);
  }

  private String convertDeleteObjectsRequestToString(DeleteObjectsRequest deleteObjectsRequest)
  {
    return deleteObjectsRequest.getKeys()
                               .stream()
                               .map(keyVersion -> keyVersion.getKey() + keyVersion.getVersion())
                               .collect(
                                   Collectors.joining());
  }
}
