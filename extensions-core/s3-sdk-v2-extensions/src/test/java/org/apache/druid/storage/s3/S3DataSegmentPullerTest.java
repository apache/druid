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

package org.apache.druid.storage.s3;

import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.zip.GZIPOutputStream;

/**
 *
 */
public class S3DataSegmentPullerTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSimpleGetVersion() throws IOException
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    String expectedKey = keyPrefix + "/renames-0.gz";
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);

    final HeadObjectResponse objectMetadata = HeadObjectResponse.builder()
                                                                .lastModified(Instant.ofEpochMilli(0))
                                                                .build();

    EasyMock.expect(s3Client.getObjectMetadata(bucket, expectedKey))
            .andReturn(objectMetadata)
            .once();
    S3DataSegmentPuller puller = new S3DataSegmentPuller(s3Client);

    EasyMock.replay(s3Client);

    String version = puller.getVersion(URI.create(StringUtils.format("s3://%s/%s", bucket, expectedKey)));

    EasyMock.verify(s3Client);

    Assert.assertEquals(StringUtils.format("%d", Instant.ofEpochMilli(0).toEpochMilli()), version);
  }

  @Test
  public void testGZUncompress() throws IOException, SegmentLoadingException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0";
    final ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);
    final byte[] value = bucket.getBytes(StandardCharsets.UTF_8);

    final File tmpFile = temporaryFolder.newFile("gzTest.gz");

    try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(tmpFile))) {
      outputStream.write(value);
    }

    final String objectKey = keyPrefix + "/renames-0.gz";

    final GetObjectResponse getObjectResponse = GetObjectResponse.builder()
                                                                 .lastModified(Instant.ofEpochMilli(0))
                                                                 .build();
    final ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(
        getObjectResponse,
        AbortableInputStream.create(new FileInputStream(tmpFile))
    );

    final File tmpDir = temporaryFolder.newFolder("gzTestDir");

    EasyMock.expect(s3Client.doesObjectExist(EasyMock.eq(bucket), EasyMock.eq(objectKey)))
            .andReturn(true)
            .once();
    EasyMock.expect(s3Client.getObject(EasyMock.eq(bucket), EasyMock.eq(objectKey)))
            .andReturn(responseInputStream)
            .once();
    S3DataSegmentPuller puller = new S3DataSegmentPuller(s3Client);

    EasyMock.replay(s3Client);
    FileUtils.FileCopyResult result = puller.getSegmentFiles(
        new CloudObjectLocation(
            bucket,
            objectKey
        ), tmpDir
    );
    EasyMock.verify(s3Client);

    Assert.assertEquals(value.length, result.size());
    File expected = new File(tmpDir, "renames-0");
    Assert.assertTrue(expected.exists());
    Assert.assertEquals(value.length, expected.length());
  }

  @Test
  public void testGZUncompressOn4xxError() throws IOException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0";
    final ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);
    final byte[] value = bucket.getBytes(StandardCharsets.UTF_8);

    final File tmpFile = temporaryFolder.newFile("gzTest.gz");

    try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(tmpFile))) {
      outputStream.write(value);
    }

    final String objectKey = keyPrefix + "/renames-0.gz";

    File tmpDir = temporaryFolder.newFolder("gzTestDir");

    S3Exception exception = (S3Exception) S3Exception.builder()
                                                     .message("S3DataSegmentPullerTest")
                                                     .statusCode(404)
                                                     .awsErrorDetails(AwsErrorDetails.builder()
                                                                                     .errorCode("NoSuchKey")
                                                                                     .build())
                                                     .build();
    EasyMock.expect(s3Client.doesObjectExist(EasyMock.eq(bucket), EasyMock.eq(objectKey)))
            .andReturn(true)
            .once();
    EasyMock.expect(s3Client.getObject(EasyMock.eq(bucket), EasyMock.eq(objectKey)))
            .andThrow(exception)
            .once();
    S3DataSegmentPuller puller = new S3DataSegmentPuller(s3Client);

    EasyMock.replay(s3Client);
    Assert.assertThrows(
        SegmentLoadingException.class,
        () -> puller.getSegmentFiles(
            new CloudObjectLocation(
                bucket,
                objectKey
            ), tmpDir
        )
    );
    EasyMock.verify(s3Client);

    File expected = new File(tmpDir, "renames-0");
    Assert.assertFalse(expected.exists());
  }

  @Test
  public void testGZUncompressOn5xxError() throws IOException, SegmentLoadingException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0";
    final ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);
    final byte[] value = bucket.getBytes(StandardCharsets.UTF_8);

    final File tmpFile = temporaryFolder.newFile("gzTest.gz");

    try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(tmpFile))) {
      outputStream.write(value);
    }

    final String objectKey = keyPrefix + "/renames-0.gz";

    final GetObjectResponse getObjectResponse = GetObjectResponse.builder()
                                                                 .lastModified(Instant.ofEpochMilli(0))
                                                                 .build();
    // Need to create two file input streams since the first one will be consumed by the exception path
    final File tmpFile2 = temporaryFolder.newFile("gzTest2.gz");
    try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(tmpFile2))) {
      outputStream.write(value);
    }
    final ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(
        getObjectResponse,
        AbortableInputStream.create(new FileInputStream(tmpFile2))
    );

    File tmpDir = temporaryFolder.newFolder("gzTestDir");

    S3Exception exception = (S3Exception) S3Exception.builder()
                                                     .message("S3DataSegmentPullerTest")
                                                     .statusCode(503)
                                                     .awsErrorDetails(AwsErrorDetails.builder()
                                                                                     .errorCode("Slow Down")
                                                                                     .build())
                                                     .build();
    EasyMock.expect(s3Client.doesObjectExist(EasyMock.eq(bucket), EasyMock.eq(objectKey)))
            .andReturn(true)
            .once();
    EasyMock.expect(s3Client.getObject(EasyMock.eq(bucket), EasyMock.eq(objectKey)))
            .andThrow(exception)
            .once();
    EasyMock.expect(s3Client.getObject(EasyMock.eq(bucket), EasyMock.eq(objectKey)))
            .andReturn(responseInputStream)
            .once();
    S3DataSegmentPuller puller = new S3DataSegmentPuller(s3Client);

    EasyMock.replay(s3Client);
    FileUtils.FileCopyResult result = puller.getSegmentFiles(
        new CloudObjectLocation(
            bucket,
            objectKey
        ), tmpDir
    );
    EasyMock.verify(s3Client);

    Assert.assertEquals(value.length, result.size());
    File expected = new File(tmpDir, "renames-0");
    Assert.assertTrue(expected.exists());
    Assert.assertEquals(value.length, expected.length());
  }

  @Test
  public void testS3ObjectStream() throws IOException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0";
    final ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);
    final byte[] value = bucket.getBytes(StandardCharsets.UTF_8);

    final File tmpFile = temporaryFolder.newFile("testObjectFile");

    try (OutputStream outputStream = new FileOutputStream(tmpFile)) {
      outputStream.write(value);
    }

    final String objectKey = keyPrefix + "/test-object";
    final GetObjectResponse getObjectResponse = GetObjectResponse.builder()
                                                                 .lastModified(Instant.ofEpochMilli(0))
                                                                 .build();
    final ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(
        getObjectResponse,
        AbortableInputStream.create(new FileInputStream(tmpFile))
    );

    EasyMock.expect(s3Client.getObject(EasyMock.eq(bucket), EasyMock.eq(objectKey)))
            .andReturn(responseInputStream)
            .once();
    S3DataSegmentPuller puller = new S3DataSegmentPuller(s3Client);
    EasyMock.replay(s3Client);
    InputStream stream = puller.buildFileObject(URI.create(StringUtils.format("s3://%s/%s", bucket, objectKey)))
                               .openInputStream();
    EasyMock.verify(s3Client);
    Assert.assertEquals(bucket, IOUtils.toString(stream, StandardCharsets.UTF_8));
  }

  @Test
  public void testS3ObjectModifiedDate() throws IOException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0";
    final ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);
    final byte[] value = bucket.getBytes(StandardCharsets.UTF_8);

    final File tmpFile = temporaryFolder.newFile("testObjectFile");

    try (OutputStream outputStream = Files.newOutputStream(tmpFile.toPath())) {
      outputStream.write(value);
    }

    final String objectKey = keyPrefix + "/test-object";

    final HeadObjectResponse objectMetadata = HeadObjectResponse.builder()
                                                                .lastModified(Instant.ofEpochMilli(0))
                                                                .build();

    EasyMock.expect(s3Client.getObjectMetadata(bucket, objectKey))
            .andReturn(objectMetadata)
            .once();

    S3DataSegmentPuller puller = new S3DataSegmentPuller(s3Client);
    EasyMock.replay(s3Client);
    long modifiedDate = puller.buildFileObject(URI.create(StringUtils.format("s3://%s/%s", bucket, objectKey)))
                              .getLastModified();
    EasyMock.verify(s3Client);
    Assert.assertEquals(0, modifiedDate);
  }

  @Test
  public void testGetNozip() throws IOException, SegmentLoadingException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0/";
    final ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);
    final byte[] value = bucket.getBytes(StandardCharsets.UTF_8);

    final File tmpFile = temporaryFolder.newFile("meta.smoosh");
    final File tmpFile2 = temporaryFolder.newFile("00000.smoosh");

    try (OutputStream outputStream = new FileOutputStream(tmpFile)) {
      outputStream.write(value);
    }
    try (OutputStream outputStream = new FileOutputStream(tmpFile2)) {
      outputStream.write(value);
    }

    S3Object objectSummary1 = S3Object.builder()
                                      .key(keyPrefix + "meta.smoosh")
                                      .build();
    S3Object objectSummary2 = S3Object.builder()
                                      .key(keyPrefix + "00000.smoosh")
                                      .build();

    ListObjectsV2Response listResponse = ListObjectsV2Response.builder()
                                                              .contents(objectSummary1, objectSummary2)
                                                              .build();
    EasyMock.expect(s3Client.listObjectsV2(EasyMock.anyObject())).andReturn(listResponse).once();

    final GetObjectResponse getObjectResponse1 = GetObjectResponse.builder()
                                                                  .lastModified(Instant.ofEpochMilli(0))
                                                                  .build();
    final ResponseInputStream<GetObjectResponse> responseInputStream1 = new ResponseInputStream<>(
        getObjectResponse1,
        AbortableInputStream.create(new FileInputStream(tmpFile))
    );

    final GetObjectResponse getObjectResponse2 = GetObjectResponse.builder()
                                                                  .lastModified(Instant.ofEpochMilli(0))
                                                                  .build();
    final ResponseInputStream<GetObjectResponse> responseInputStream2 = new ResponseInputStream<>(
        getObjectResponse2,
        AbortableInputStream.create(new FileInputStream(tmpFile2))
    );

    final File tmpDir = temporaryFolder.newFolder("noZipTestDir");

    EasyMock.expect(s3Client.getObject(EasyMock.eq(bucket), EasyMock.eq(keyPrefix + "meta.smoosh")))
            .andReturn(responseInputStream1)
            .once();
    EasyMock.expect(s3Client.getObject(EasyMock.eq(bucket), EasyMock.eq(keyPrefix + "00000.smoosh")))
            .andReturn(responseInputStream2)
            .once();
    S3DataSegmentPuller puller = new S3DataSegmentPuller(s3Client);

    EasyMock.replay(s3Client);
    FileUtils.FileCopyResult result = puller.getSegmentFiles(
        new CloudObjectLocation(
            bucket,
            keyPrefix
        ),
        tmpDir
    );
    EasyMock.verify(s3Client);

    Assert.assertEquals(value.length + value.length, result.size());
    File expected = new File(tmpDir, "meta.smoosh");
    Assert.assertTrue(expected.exists());
    Assert.assertEquals(value.length, expected.length());
    expected = new File(tmpDir, "00000.smoosh");
    Assert.assertTrue(expected.exists());
    Assert.assertEquals(value.length, expected.length());
  }
}
