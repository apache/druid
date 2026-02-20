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

import org.apache.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.time.Instant;
import java.util.regex.Pattern;

public class S3TimestampVersionedDataFinderTest
{

  @Test
  public void testSimpleLatestVersion()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);

    S3Object object0 = S3Object.builder()
        .key(keyPrefix + "/renames-0.gz")
        .lastModified(Instant.ofEpochMilli(0))
        .size(10L)
        .build();

    S3Object object1 = S3Object.builder()
        .key(keyPrefix + "/renames-1.gz")
        .lastModified(Instant.ofEpochMilli(1))
        .size(10L)
        .build();

    final ListObjectsV2Response result = ListObjectsV2Response.builder()
        .contents(object0, object1)
        .keyCount(2)
        .isTruncated(false)
        .build();

    EasyMock.expect(s3Client.listObjectsV2(EasyMock.anyObject(ListObjectsV2Request.class)))
            .andReturn(result)
            .once();
    S3TimestampVersionedDataFinder finder = new S3TimestampVersionedDataFinder(s3Client);

    Pattern pattern = Pattern.compile("renames-[0-9]*\\.gz");

    EasyMock.replay(s3Client);


    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("s3://%s/%s", bucket, keyPrefix)), pattern);

    EasyMock.verify(s3Client);

    URI expected = URI.create(StringUtils.format("s3://%s/%s", bucket, object1.key()));

    Assert.assertEquals(expected, latest);
  }

  @Test
  public void testMissing()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);

    final ListObjectsV2Response result = ListObjectsV2Response.builder()
        .keyCount(0)
        .isTruncated(false)
        .build();

    EasyMock.expect(s3Client.listObjectsV2(EasyMock.anyObject(ListObjectsV2Request.class)))
            .andReturn(result)
            .once();
    S3TimestampVersionedDataFinder finder = new S3TimestampVersionedDataFinder(s3Client);

    Pattern pattern = Pattern.compile("renames-[0-9]*\\.gz");

    EasyMock.replay(s3Client);


    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("s3://%s/%s", bucket, keyPrefix)), pattern);

    EasyMock.verify(s3Client);

    Assert.assertEquals(null, latest);
  }

  @Test
  public void testFindSelf()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);

    S3Object object0 = S3Object.builder()
        .key(keyPrefix + "/renames-0.gz")
        .lastModified(Instant.ofEpochMilli(0))
        .size(10L)
        .build();

    final ListObjectsV2Response result = ListObjectsV2Response.builder()
        .contents(object0)
        .keyCount(1)
        .isTruncated(false)
        .build();

    EasyMock.expect(s3Client.listObjectsV2(EasyMock.anyObject(ListObjectsV2Request.class)))
            .andReturn(result)
            .once();
    S3TimestampVersionedDataFinder finder = new S3TimestampVersionedDataFinder(s3Client);

    Pattern pattern = Pattern.compile("renames-[0-9]*\\.gz");

    EasyMock.replay(s3Client);


    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("s3://%s/%s", bucket, keyPrefix)), pattern);

    EasyMock.verify(s3Client);

    URI expected = URI.create(StringUtils.format("s3://%s/%s", bucket, object0.key()));

    Assert.assertEquals(expected, latest);
  }

  @Test
  public void testFindExact()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);

    S3Object object0 = S3Object.builder()
        .key(keyPrefix + "/renames-0.gz")
        .lastModified(Instant.ofEpochMilli(0))
        .size(10L)
        .build();

    final ListObjectsV2Response result = ListObjectsV2Response.builder()
        .contents(object0)
        .keyCount(1)
        .isTruncated(false)
        .build();

    EasyMock.expect(s3Client.listObjectsV2(EasyMock.anyObject(ListObjectsV2Request.class)))
            .andReturn(result)
            .once();
    S3TimestampVersionedDataFinder finder = new S3TimestampVersionedDataFinder(s3Client);

    EasyMock.replay(s3Client);

    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("s3://%s/%s", bucket, object0.key())), null);

    EasyMock.verify(s3Client);

    URI expected = URI.create(StringUtils.format("s3://%s/%s", bucket, object0.key()));

    Assert.assertEquals(expected, latest);
  }
}
