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

import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Date;
import java.util.regex.Pattern;

public class S3TimestampVersionedDataFinderTest
{

  @Test
  public void testSimpleLatestVersion()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);

    S3ObjectSummary object0 = new S3ObjectSummary(), object1 = new S3ObjectSummary();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModified(new Date(0));
    object0.setSize(10);

    object1.setBucketName(bucket);
    object1.setKey(keyPrefix + "/renames-1.gz");
    object1.setLastModified(new Date(1));
    object1.setSize(10);

    final ListObjectsV2Result result = new ListObjectsV2Result();
    result.getObjectSummaries().add(object0);
    result.getObjectSummaries().add(object1);
    result.setKeyCount(2);
    result.setTruncated(false);

    EasyMock.expect(s3Client.listObjectsV2(EasyMock.anyObject(ListObjectsV2Request.class)))
            .andReturn(result)
            .once();
    S3TimestampVersionedDataFinder finder = new S3TimestampVersionedDataFinder(s3Client);

    Pattern pattern = Pattern.compile("renames-[0-9]*\\.gz");

    EasyMock.replay(s3Client);


    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("s3://%s/%s", bucket, keyPrefix)), pattern);

    EasyMock.verify(s3Client);

    URI expected = URI.create(StringUtils.format("s3://%s/%s", bucket, object1.getKey()));

    Assert.assertEquals(expected, latest);
  }

  @Test
  public void testMissing()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);

    final ListObjectsV2Result result = new ListObjectsV2Result();
    result.setKeyCount(0);
    result.setTruncated(false);

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

    S3ObjectSummary object0 = new S3ObjectSummary();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModified(new Date(0));
    object0.setSize(10);

    final ListObjectsV2Result result = new ListObjectsV2Result();
    result.getObjectSummaries().add(object0);
    result.setKeyCount(1);
    result.setTruncated(false);

    EasyMock.expect(s3Client.listObjectsV2(EasyMock.anyObject(ListObjectsV2Request.class)))
            .andReturn(result)
            .once();
    S3TimestampVersionedDataFinder finder = new S3TimestampVersionedDataFinder(s3Client);

    Pattern pattern = Pattern.compile("renames-[0-9]*\\.gz");

    EasyMock.replay(s3Client);


    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("s3://%s/%s", bucket, keyPrefix)), pattern);

    EasyMock.verify(s3Client);

    URI expected = URI.create(StringUtils.format("s3://%s/%s", bucket, object0.getKey()));

    Assert.assertEquals(expected, latest);
  }

  @Test
  public void testFindExact()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);

    S3ObjectSummary object0 = new S3ObjectSummary();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModified(new Date(0));
    object0.setSize(10);

    final ListObjectsV2Result result = new ListObjectsV2Result();
    result.getObjectSummaries().add(object0);
    result.setKeyCount(1);
    result.setTruncated(false);

    EasyMock.expect(s3Client.listObjectsV2(EasyMock.anyObject(ListObjectsV2Request.class)))
            .andReturn(result)
            .once();
    S3TimestampVersionedDataFinder finder = new S3TimestampVersionedDataFinder(s3Client);

    EasyMock.replay(s3Client);

    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("s3://%s/%s", bucket, object0.getKey())), null);

    EasyMock.verify(s3Client);

    URI expected = URI.create(StringUtils.format("s3://%s/%s", bucket, object0.getKey()));

    Assert.assertEquals(expected, latest);
  }
}
