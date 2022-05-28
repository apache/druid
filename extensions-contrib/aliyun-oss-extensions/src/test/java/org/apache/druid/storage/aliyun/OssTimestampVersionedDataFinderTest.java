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
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Date;
import java.util.regex.Pattern;

public class OssTimestampVersionedDataFinderTest
{

  @Test
  public void testSimpleLatestVersion()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    OSS client = EasyMock.createStrictMock(OSS.class);

    OSSObjectSummary object0 = new OSSObjectSummary(), object1 = new OSSObjectSummary();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModified(new Date(0));
    object0.setSize(10);

    object1.setBucketName(bucket);
    object1.setKey(keyPrefix + "/renames-1.gz");
    object1.setLastModified(new Date(1));
    object1.setSize(10);

    final ObjectListing result = new ObjectListing();
    result.getObjectSummaries().add(object0);
    result.getObjectSummaries().add(object1);
    result.setTruncated(false);

    EasyMock.expect(client.listObjects(EasyMock.anyObject(ListObjectsRequest.class)))
            .andReturn(result)
            .once();
    OssTimestampVersionedDataFinder finder = new OssTimestampVersionedDataFinder(client);

    Pattern pattern = Pattern.compile("renames-[0-9]*\\.gz");

    EasyMock.replay(client);


    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("%s://%s/%s", OssStorageDruidModule.SCHEME, bucket, keyPrefix)), pattern);

    EasyMock.verify(client);

    URI expected = URI.create(StringUtils.format("%s://%s/%s", OssStorageDruidModule.SCHEME, bucket, object1.getKey()));

    Assert.assertEquals(expected, latest);
  }

  @Test
  public void testMissing()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    OSS oss = EasyMock.createStrictMock(OSS.class);

    final ObjectListing result = new ObjectListing();
    result.setTruncated(false);

    EasyMock.expect(oss.listObjects(EasyMock.anyObject(ListObjectsRequest.class)))
            .andReturn(result)
            .once();
    OssTimestampVersionedDataFinder finder = new OssTimestampVersionedDataFinder(oss);

    Pattern pattern = Pattern.compile("renames-[0-9]*\\.gz");

    EasyMock.replay(oss);


    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("%s://%s/%s", OssStorageDruidModule.SCHEME, bucket, keyPrefix)), pattern);

    EasyMock.verify(oss);

    Assert.assertEquals(null, latest);
  }

  @Test
  public void testFindSelf()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    OSS ossClient = EasyMock.createStrictMock(OSS.class);

    OSSObjectSummary object0 = new OSSObjectSummary();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModified(new Date(0));
    object0.setSize(10);

    final ObjectListing result = new ObjectListing();
    result.getObjectSummaries().add(object0);
    result.setTruncated(false);

    EasyMock.expect(ossClient.listObjects(EasyMock.anyObject(ListObjectsRequest.class)))
            .andReturn(result)
            .once();
    OssTimestampVersionedDataFinder finder = new OssTimestampVersionedDataFinder(ossClient);

    Pattern pattern = Pattern.compile("renames-[0-9]*\\.gz");

    EasyMock.replay(ossClient);


    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("%s://%s/%s", OssStorageDruidModule.SCHEME, bucket, keyPrefix)), pattern);

    EasyMock.verify(ossClient);

    URI expected = URI.create(StringUtils.format("%s://%s/%s", OssStorageDruidModule.SCHEME, bucket, object0.getKey()));

    Assert.assertEquals(expected, latest);
  }

  @Test
  public void testFindExact()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";
    OSS ossClient = EasyMock.createStrictMock(OSS.class);

    OSSObjectSummary object0 = new OSSObjectSummary();

    object0.setBucketName(bucket);
    object0.setKey(keyPrefix + "/renames-0.gz");
    object0.setLastModified(new Date(0));
    object0.setSize(10);

    final ObjectListing result = new ObjectListing();
    result.getObjectSummaries().add(object0);
    result.setTruncated(false);

    EasyMock.expect(ossClient.listObjects(EasyMock.anyObject(ListObjectsRequest.class)))
            .andReturn(result)
            .once();
    OssTimestampVersionedDataFinder finder = new OssTimestampVersionedDataFinder(ossClient);

    EasyMock.replay(ossClient);

    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("%s://%s/%s", OssStorageDruidModule.SCHEME, bucket, object0.getKey())), null);

    EasyMock.verify(ossClient);

    URI expected = URI.create(StringUtils.format("%s://%s/%s", OssStorageDruidModule.SCHEME, bucket, object0.getKey()));

    Assert.assertEquals(expected, latest);
  }
}
