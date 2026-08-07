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

package org.apache.druid.storage.aliyun;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectMetadata;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.zip.GZIPOutputStream;

/**
 *
 */
public class OssDataSegmentPullerTest
{
  @TempDir
  public File temporaryFolder;

  @Test
  public void testSimpleGetVersion() throws IOException
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    String expectedKey = keyPrefix + "/renames-0.gz";
    OSS ossClient = EasyMock.createStrictMock(OSS.class);

    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setLastModified(new Date(0));

    EasyMock.expect(ossClient.getObjectMetadata(bucket, expectedKey))
            .andReturn(objectMetadata)
            .once();
    OssDataSegmentPuller puller = new OssDataSegmentPuller(ossClient);

    EasyMock.replay(ossClient);

    String version = puller.getVersion(
        URI.create(
            StringUtils.format(
                OssStorageDruidModule.SCHEME + "://%s/%s",
                bucket,
                expectedKey
            )
        )
    );

    EasyMock.verify(ossClient);

    Assertions.assertEquals(StringUtils.format("%d", new Date(0).getTime()), version);
  }

  @Test
  public void testGZUncompress() throws IOException, SegmentLoadingException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0";
    final OSS ossClient = EasyMock.createStrictMock(OSS.class);
    final byte[] value = bucket.getBytes(StandardCharsets.UTF_8);

    final File tmpFile = new File(temporaryFolder, "gzTest.gz");

    try (final FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
         final OutputStream outputStream = new GZIPOutputStream(fileOutputStream)) {
      outputStream.write(value);
    }

    final OSSObject object0 = new OSSObject();
    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.getObjectMetadata().setLastModified(new Date(0));

    final OSSObjectSummary objectSummary = new OSSObjectSummary();
    objectSummary.setBucketName(bucket);
    objectSummary.setKey(keyPrefix + "/renames-0.gz");
    objectSummary.setLastModified(new Date(0));

    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setLastModified(new Date(1));

    final File tmpDir = newFolder(temporaryFolder, "gzTestDir");

    try (final InputStream objectContent = new FileInputStream(tmpFile)) {
      object0.setObjectContent(objectContent);
      EasyMock.expect(ossClient.doesObjectExist(EasyMock.eq(object0.getBucketName()), EasyMock.eq(object0.getKey())))
              .andReturn(true)
              .once();
      EasyMock.expect(ossClient.getObjectMetadata(object0.getBucketName(), object0.getKey()))
              .andReturn(objectMetadata)
              .once();
      EasyMock.expect(ossClient.getObject(EasyMock.eq(object0.getBucketName()), EasyMock.eq(object0.getKey())))
              .andReturn(object0)
              .once();
      final OssDataSegmentPuller puller = new OssDataSegmentPuller(ossClient);

      EasyMock.replay(ossClient);
      final FileUtils.FileCopyResult result = puller.getSegmentFiles(
          new CloudObjectLocation(
              bucket,
              object0.getKey()
          ), tmpDir
      );
      EasyMock.verify(ossClient);

      Assertions.assertEquals(value.length, result.size());
      final File expected = new File(tmpDir, "renames-0");
      Assertions.assertTrue(expected.exists());
      Assertions.assertEquals(value.length, expected.length());
    }
  }

  @Test
  public void testGZUncompressRetries() throws IOException, SegmentLoadingException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0";
    final OSS ossClient = EasyMock.createStrictMock(OSS.class);
    final byte[] value = bucket.getBytes(StandardCharsets.UTF_8);

    final File tmpFile = new File(temporaryFolder, "gzTest.gz");

    try (final FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
         final OutputStream outputStream = new GZIPOutputStream(fileOutputStream)) {
      outputStream.write(value);
    }

    OSSObject object0 = new OSSObject();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.getObjectMetadata().setLastModified(new Date(0));

    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setLastModified(new Date(0));

    File tmpDir = newFolder(temporaryFolder, "gzTestDir");

    OSSException exception = new OSSException("OssDataSegmentPullerTest", "NoSuchKey", null, null, null, null, null);
    try (final InputStream objectContent = new FileInputStream(tmpFile)) {
      object0.setObjectContent(objectContent);
      EasyMock.expect(ossClient.doesObjectExist(EasyMock.eq(object0.getBucketName()), EasyMock.eq(object0.getKey())))
              .andReturn(true)
              .once();
      EasyMock.expect(ossClient.getObjectMetadata(bucket, object0.getKey()))
              .andReturn(objectMetadata)
              .once();
      EasyMock.expect(ossClient.getObject(EasyMock.eq(bucket), EasyMock.eq(object0.getKey())))
              .andThrow(exception)
              .once();
      EasyMock.expect(ossClient.getObjectMetadata(bucket, object0.getKey()))
              .andReturn(objectMetadata)
              .once();
      EasyMock.expect(ossClient.getObject(EasyMock.eq(bucket), EasyMock.eq(object0.getKey())))
              .andReturn(object0)
              .once();
      final OssDataSegmentPuller puller = new OssDataSegmentPuller(ossClient);

      EasyMock.replay(ossClient);
      final FileUtils.FileCopyResult result = puller.getSegmentFiles(
          new CloudObjectLocation(
              bucket,
              object0.getKey()
          ), tmpDir
      );
      EasyMock.verify(ossClient);

      Assertions.assertEquals(value.length, result.size());
      final File expected = new File(tmpDir, "renames-0");
      Assertions.assertTrue(expected.exists());
      Assertions.assertEquals(value.length, expected.length());
    }
  }

  private static File newFolder(File root, String... subDirs) throws IOException
  {
    final String subFolder = String.join("/", subDirs);
    final File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }

}
