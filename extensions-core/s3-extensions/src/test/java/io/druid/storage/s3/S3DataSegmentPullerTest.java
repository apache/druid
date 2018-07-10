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

import com.metamx.common.FileUtils;
import io.druid.segment.loading.SegmentLoadingException;
import org.easymock.EasyMock;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.Date;
import java.util.zip.GZIPOutputStream;

/**
 *
 */
public class S3DataSegmentPullerTest
{
  @Test
  public void testSimpleGetVersion() throws S3ServiceException, IOException
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    RestS3Service s3Client = EasyMock.createStrictMock(RestS3Service.class);

    S3Object object0 = new S3Object();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModifiedDate(new Date(0));

    EasyMock.expect(s3Client.getObject(EasyMock.eq(bucket), EasyMock.eq(object0.getKey()))).andReturn(object0).once();
    S3DataSegmentPuller puller = new S3DataSegmentPuller(s3Client);

    EasyMock.replay(s3Client);

    String version = puller.getVersion(URI.create(String.format("s3://%s/%s", bucket, object0.getKey())));

    EasyMock.verify(s3Client);

    Assert.assertEquals(String.format("%d", new Date(0).getTime()), version);
  }

  @Test
  public void testGZUncompress() throws S3ServiceException, IOException, SegmentLoadingException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0";
    final RestS3Service s3Client = EasyMock.createStrictMock(RestS3Service.class);
    final byte[] value = bucket.getBytes("utf8");

    final File tmpFile = Files.createTempFile("gzTest", ".gz").toFile();
    tmpFile.deleteOnExit();
    try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(tmpFile))) {
      outputStream.write(value);
    }

    S3Object object0 = new S3Object();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModifiedDate(new Date(0));
    object0.setDataInputStream(new FileInputStream(tmpFile));

    File tmpDir = Files.createTempDirectory("gzTestDir").toFile();

    try {
      EasyMock.expect(s3Client.getObjectDetails(EasyMock.<S3Bucket>anyObject(), EasyMock.eq(object0.getKey())))
              .andReturn(null)
              .once();
      EasyMock.expect(s3Client.getObject(EasyMock.eq(bucket), EasyMock.eq(object0.getKey()))).andReturn(object0).once();
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
    finally {
      org.apache.commons.io.FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Test
  public void testGZUncompressRetries() throws S3ServiceException, IOException, SegmentLoadingException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0";
    final RestS3Service s3Client = EasyMock.createStrictMock(RestS3Service.class);
    final byte[] value = bucket.getBytes("utf8");

    final File tmpFile = Files.createTempFile("gzTest", ".gz").toFile();
    tmpFile.deleteOnExit();
    try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(tmpFile))) {
      outputStream.write(value);
    }

    S3Object object0 = new S3Object();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModifiedDate(new Date(0));
    object0.setDataInputStream(new FileInputStream(tmpFile));

    File tmpDir = Files.createTempDirectory("gzTestDir").toFile();

    S3ServiceException exception = new S3ServiceException();
    exception.setErrorCode("NoSuchKey");
    exception.setResponseCode(404);
    try {
      EasyMock.expect(s3Client.getObjectDetails(EasyMock.<S3Bucket>anyObject(), EasyMock.eq(object0.getKey())))
              .andReturn(null)
              .once();
      EasyMock.expect(s3Client.getObject(EasyMock.eq(bucket), EasyMock.eq(object0.getKey())))
              .andThrow(exception)
              .once()
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
    finally {
      org.apache.commons.io.FileUtils.deleteDirectory(tmpDir);
    }
  }

}
