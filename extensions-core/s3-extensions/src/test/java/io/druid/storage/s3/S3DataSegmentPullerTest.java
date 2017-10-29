/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.s3;

import io.druid.java.util.common.FileUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.segment.loading.SegmentLoadingException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;
import java.util.zip.GZIPOutputStream;
import org.easymock.EasyMock;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
public class S3DataSegmentPullerTest
{

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSimpleGetVersion() throws ServiceException, IOException
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    RestS3Service s3Client = EasyMock.createStrictMock(RestS3Service.class);

    S3Object object0 = new S3Object();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModifiedDate(new Date(0));

    EasyMock.expect(s3Client.getObjectDetails(EasyMock.eq(bucket), EasyMock.eq(object0.getKey())))
            .andReturn(object0)
            .once();
    S3DataSegmentPuller puller = new S3DataSegmentPuller(s3Client);

    EasyMock.replay(s3Client);

    String version = puller.getVersion(URI.create(StringUtils.format("s3://%s/%s", bucket, object0.getKey())));

    EasyMock.verify(s3Client);

    Assert.assertEquals(StringUtils.format("%d", new Date(0).getTime()), version);
  }

  @Test
  public void testGZUncompress() throws ServiceException, IOException, SegmentLoadingException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0";
    final RestS3Service s3Client = EasyMock.createStrictMock(RestS3Service.class);
    final byte[] value = bucket.getBytes("utf8");

    final File tmpFile = temporaryFolder.newFile("gzTest.gz");

    try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(tmpFile))) {
      outputStream.write(value);
    }

    final S3Object object0 = new S3Object();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModifiedDate(new Date(0));
    object0.setDataInputStream(new FileInputStream(tmpFile));

    final File tmpDir = temporaryFolder.newFolder("gzTestDir");

    EasyMock.expect(s3Client.getObjectDetails(EasyMock.eq(object0.getBucketName()), EasyMock.eq(object0.getKey())))
            .andReturn(null)
            .once();
    EasyMock.expect(s3Client.getObjectDetails(EasyMock.eq(object0.getBucketName()), EasyMock.eq(object0.getKey())))
            .andReturn(object0)
            .once();
    EasyMock.expect(s3Client.getObject(EasyMock.eq(object0.getBucketName()), EasyMock.eq(object0.getKey())))
            .andReturn(object0)
            .once();
    S3DataSegmentPuller puller = new S3DataSegmentPuller(s3Client);

    EasyMock.replay(s3Client);
    FileUtils.FileCopyResult result = puller.getSegmentFiles(
        new S3DataSegmentPuller.S3Coords(
            bucket,
            object0.getKey()
        ), tmpDir
    );
    EasyMock.verify(s3Client);

    Assert.assertEquals(value.length, result.size());
    File expected = new File(tmpDir, "renames-0");
    Assert.assertTrue(expected.exists());
    Assert.assertEquals(value.length, expected.length());
  }

  @Test
  public void testGZUncompressRetries() throws ServiceException, IOException, SegmentLoadingException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0";
    final RestS3Service s3Client = EasyMock.createStrictMock(RestS3Service.class);
    final byte[] value = bucket.getBytes("utf8");

    final File tmpFile = temporaryFolder.newFile("gzTest.gz");

    try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(tmpFile))) {
      outputStream.write(value);
    }

    S3Object object0 = new S3Object();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModifiedDate(new Date(0));
    object0.setDataInputStream(new FileInputStream(tmpFile));

    File tmpDir = temporaryFolder.newFolder("gzTestDir");

    S3ServiceException exception = new S3ServiceException();
    exception.setErrorCode("NoSuchKey");
    exception.setResponseCode(404);
    EasyMock.expect(s3Client.getObjectDetails(EasyMock.eq(object0.getBucketName()), EasyMock.eq(object0.getKey())))
            .andReturn(null)
            .once();
    EasyMock.expect(s3Client.getObjectDetails(EasyMock.eq(object0.getBucketName()), EasyMock.eq(object0.getKey())))
            .andReturn(object0)
            .once();
    EasyMock.expect(s3Client.getObject(EasyMock.eq(bucket), EasyMock.eq(object0.getKey())))
            .andThrow(exception)
            .once();
    EasyMock.expect(s3Client.getObjectDetails(EasyMock.eq(object0.getBucketName()), EasyMock.eq(object0.getKey())))
            .andReturn(object0)
            .once();
    EasyMock.expect(s3Client.getObject(EasyMock.eq(bucket), EasyMock.eq(object0.getKey())))
            .andReturn(object0)
            .once();
    S3DataSegmentPuller puller = new S3DataSegmentPuller(s3Client);

    EasyMock.replay(s3Client);
    FileUtils.FileCopyResult result = puller.getSegmentFiles(
        new S3DataSegmentPuller.S3Coords(
            bucket,
            object0.getKey()
        ), tmpDir
    );
    EasyMock.verify(s3Client);

    Assert.assertEquals(value.length, result.size());
    File expected = new File(tmpDir, "renames-0");
    Assert.assertTrue(expected.exists());
    Assert.assertEquals(value.length, expected.length());
  }

}
