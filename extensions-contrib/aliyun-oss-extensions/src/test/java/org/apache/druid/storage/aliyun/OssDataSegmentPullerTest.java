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
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSimpleGetVersion() throws IOException
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    OSS ossClient = EasyMock.createStrictMock(OSS.class);

    final OSSObjectSummary objectSummary = new OSSObjectSummary();
    objectSummary.setBucketName(bucket);
    objectSummary.setKey(keyPrefix + "/renames-0.gz");
    objectSummary.setLastModified(new Date(0));

    final ObjectListing result = new ObjectListing();
    result.getObjectSummaries().add(objectSummary);

    EasyMock.expect(ossClient.listObjects(EasyMock.anyObject(ListObjectsRequest.class)))
            .andReturn(result)
            .once();
    OssDataSegmentPuller puller = new OssDataSegmentPuller(ossClient);

    EasyMock.replay(ossClient);

    String version = puller.getVersion(URI.create(StringUtils.format(OssStorageDruidModule.SCHEME + "://%s/%s", bucket, objectSummary.getKey())));

    EasyMock.verify(ossClient);

    Assert.assertEquals(StringUtils.format("%d", new Date(0).getTime()), version);
  }

  @Test
  public void testGZUncompress() throws IOException, SegmentLoadingException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0";
    final OSS ossClient = EasyMock.createStrictMock(OSS.class);
    final byte[] value = bucket.getBytes(StandardCharsets.UTF_8);

    final File tmpFile = temporaryFolder.newFile("gzTest.gz");

    try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(tmpFile))) {
      outputStream.write(value);
    }

    final OSSObject object0 = new OSSObject();
    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.getObjectMetadata().setLastModified(new Date(0));
    object0.setObjectContent(new FileInputStream(tmpFile));

    final OSSObjectSummary objectSummary = new OSSObjectSummary();
    objectSummary.setBucketName(bucket);
    objectSummary.setKey(keyPrefix + "/renames-0.gz");
    objectSummary.setLastModified(new Date(0));

    final ObjectListing listObjectsResult = new ObjectListing();
    listObjectsResult.getObjectSummaries().add(objectSummary);

    final File tmpDir = temporaryFolder.newFolder("gzTestDir");

    EasyMock.expect(ossClient.doesObjectExist(EasyMock.eq(object0.getBucketName()), EasyMock.eq(object0.getKey())))
            .andReturn(true)
            .once();
    EasyMock.expect(ossClient.listObjects(EasyMock.anyObject(ListObjectsRequest.class)))
            .andReturn(listObjectsResult)
            .once();
    EasyMock.expect(ossClient.getObject(EasyMock.eq(object0.getBucketName()), EasyMock.eq(object0.getKey())))
            .andReturn(object0)
            .once();
    OssDataSegmentPuller puller = new OssDataSegmentPuller(ossClient);

    EasyMock.replay(ossClient);
    FileUtils.FileCopyResult result = puller.getSegmentFiles(
        new CloudObjectLocation(
            bucket,
            object0.getKey()
        ), tmpDir
    );
    EasyMock.verify(ossClient);

    Assert.assertEquals(value.length, result.size());
    File expected = new File(tmpDir, "renames-0");
    Assert.assertTrue(expected.exists());
    Assert.assertEquals(value.length, expected.length());
  }

  @Test
  public void testGZUncompressRetries() throws IOException, SegmentLoadingException
  {
    final String bucket = "bucket";
    final String keyPrefix = "prefix/dir/0";
    final OSS ossClient = EasyMock.createStrictMock(OSS.class);
    final byte[] value = bucket.getBytes(StandardCharsets.UTF_8);

    final File tmpFile = temporaryFolder.newFile("gzTest.gz");

    try (OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(tmpFile))) {
      outputStream.write(value);
    }

    OSSObject object0 = new OSSObject();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.getObjectMetadata().setLastModified(new Date(0));
    object0.setObjectContent(new FileInputStream(tmpFile));

    final OSSObjectSummary objectSummary = new OSSObjectSummary();
    objectSummary.setBucketName(bucket);
    objectSummary.setKey(keyPrefix + "/renames-0.gz");
    objectSummary.setLastModified(new Date(0));

    final ObjectListing listObjectsResult = new ObjectListing();
    listObjectsResult.getObjectSummaries().add(objectSummary);

    File tmpDir = temporaryFolder.newFolder("gzTestDir");

    OSSException exception = new OSSException("OssDataSegmentPullerTest", "NoSuchKey", null, null, null, null, null);
    EasyMock.expect(ossClient.doesObjectExist(EasyMock.eq(object0.getBucketName()), EasyMock.eq(object0.getKey())))
            .andReturn(true)
            .once();
    EasyMock.expect(ossClient.listObjects(EasyMock.anyObject(ListObjectsRequest.class)))
            .andReturn(listObjectsResult)
            .once();
    EasyMock.expect(ossClient.getObject(EasyMock.eq(bucket), EasyMock.eq(object0.getKey())))
            .andThrow(exception)
            .once();
    EasyMock.expect(ossClient.listObjects(EasyMock.anyObject(ListObjectsRequest.class)))
            .andReturn(listObjectsResult)
            .once();
    EasyMock.expect(ossClient.getObject(EasyMock.eq(bucket), EasyMock.eq(object0.getKey())))
            .andReturn(object0)
            .once();
    OssDataSegmentPuller puller = new OssDataSegmentPuller(ossClient);

    EasyMock.replay(ossClient);
    FileUtils.FileCopyResult result = puller.getSegmentFiles(
        new CloudObjectLocation(
            bucket,
            object0.getKey()
        ), tmpDir
    );
    EasyMock.verify(ossClient);

    Assert.assertEquals(value.length, result.size());
    File expected = new File(tmpDir, "renames-0");
    Assert.assertTrue(expected.exists());
    Assert.assertEquals(value.length, expected.length());
  }

}
