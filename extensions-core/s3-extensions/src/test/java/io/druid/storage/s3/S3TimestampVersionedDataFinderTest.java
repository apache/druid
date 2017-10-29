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

import io.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Date;
import java.util.regex.Pattern;

public class S3TimestampVersionedDataFinderTest
{

  @Test
  public void testSimpleLatestVersion() throws S3ServiceException
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    RestS3Service s3Client = EasyMock.createStrictMock(RestS3Service.class);

    S3Object object0 = new S3Object(), object1 = new S3Object();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModifiedDate(new Date(0));

    object1.setBucketName(bucket);
    object1.setKey(keyPrefix + "/renames-1.gz");
    object1.setLastModifiedDate(new Date(1));

    EasyMock.expect(s3Client.listObjects(EasyMock.eq(bucket), EasyMock.anyString(), EasyMock.<String>isNull())).andReturn(
        new S3Object[]{object0, object1}
    ).once();
    S3TimestampVersionedDataFinder finder = new S3TimestampVersionedDataFinder(s3Client);

    Pattern pattern = Pattern.compile("renames-[0-9]*\\.gz");

    EasyMock.replay(s3Client);


    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("s3://%s/%s", bucket, keyPrefix)), pattern);

    EasyMock.verify(s3Client);

    URI expected = URI.create(StringUtils.format("s3://%s/%s", bucket, object1.getKey()));

    Assert.assertEquals(expected, latest);
  }

  @Test
  public void testMissing() throws S3ServiceException
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    RestS3Service s3Client = EasyMock.createStrictMock(RestS3Service.class);

    S3Object object0 = new S3Object(), object1 = new S3Object();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModifiedDate(new Date(0));

    object1.setBucketName(bucket);
    object1.setKey(keyPrefix + "/renames-1.gz");
    object1.setLastModifiedDate(new Date(1));

    EasyMock.expect(s3Client.listObjects(EasyMock.eq(bucket), EasyMock.anyString(), EasyMock.<String>isNull())).andReturn(
        null
    ).once();
    S3TimestampVersionedDataFinder finder = new S3TimestampVersionedDataFinder(s3Client);

    Pattern pattern = Pattern.compile("renames-[0-9]*\\.gz");

    EasyMock.replay(s3Client);


    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("s3://%s/%s", bucket, keyPrefix)), pattern);

    EasyMock.verify(s3Client);

    Assert.assertEquals(null, latest);
  }

  @Test
  public void testFindSelf() throws S3ServiceException
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    RestS3Service s3Client = EasyMock.createStrictMock(RestS3Service.class);

    S3Object object0 = new S3Object();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModifiedDate(new Date(0));

    EasyMock.expect(s3Client.listObjects(EasyMock.eq(bucket), EasyMock.anyString(), EasyMock.<String>isNull())).andReturn(
        new S3Object[]{object0}
    ).once();
    S3TimestampVersionedDataFinder finder = new S3TimestampVersionedDataFinder(s3Client);

    Pattern pattern = Pattern.compile("renames-[0-9]*\\.gz");

    EasyMock.replay(s3Client);


    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("s3://%s/%s", bucket, keyPrefix)), pattern);

    EasyMock.verify(s3Client);

    URI expected = URI.create(StringUtils.format("s3://%s/%s", bucket, object0.getKey()));

    Assert.assertEquals(expected, latest);
  }

  @Test
  public void testFindExact() throws S3ServiceException
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    RestS3Service s3Client = EasyMock.createStrictMock(RestS3Service.class);

    S3Object object0 = new S3Object();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModifiedDate(new Date(0));

    EasyMock.expect(s3Client.listObjects(EasyMock.eq(bucket), EasyMock.anyString(), EasyMock.<String>isNull())).andReturn(
        new S3Object[]{object0}
    ).once();
    S3TimestampVersionedDataFinder finder = new S3TimestampVersionedDataFinder(s3Client);


    EasyMock.replay(s3Client);


    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("s3://%s/%s", bucket, object0.getKey())), null);

    EasyMock.verify(s3Client);

    URI expected = URI.create(StringUtils.format("s3://%s/%s", bucket, object0.getKey()));

    Assert.assertEquals(expected, latest);
  }
}
