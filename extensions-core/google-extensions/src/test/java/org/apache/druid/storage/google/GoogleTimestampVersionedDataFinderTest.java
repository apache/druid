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

package org.apache.druid.storage.google;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.regex.Pattern;

public class GoogleTimestampVersionedDataFinderTest
{
  @Test
  public void getLatestVersion()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0";

    // object for directory prefix/dir/0/
    final GoogleStorageObjectMetadata storageObject1 = ObjectStorageIteratorTest.makeStorageObject(bucket, keyPrefix + "//", 0);
    storageObject1.setLastUpdateTime(System.currentTimeMillis());
    final GoogleStorageObjectMetadata storageObject2 = ObjectStorageIteratorTest.makeStorageObject(bucket, keyPrefix + "/v1", 1);
    storageObject2.setLastUpdateTime(System.currentTimeMillis());
    final GoogleStorageObjectMetadata storageObject3 = ObjectStorageIteratorTest.makeStorageObject(bucket, keyPrefix + "/v2", 1);
    storageObject3.setLastUpdateTime(System.currentTimeMillis() + 100);
    final GoogleStorageObjectMetadata storageObject4 = ObjectStorageIteratorTest.makeStorageObject(bucket, keyPrefix + "/other", 4);
    storageObject4.setLastUpdateTime(System.currentTimeMillis() + 100);
    final GoogleStorage storage = ObjectStorageIteratorTest.makeMockClient(ImmutableList.of(storageObject1, storageObject2, storageObject3, storageObject4));

    final GoogleTimestampVersionedDataFinder finder = new GoogleTimestampVersionedDataFinder(storage);
    Pattern pattern = Pattern.compile("v.*");
    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("gs://%s/%s", bucket, keyPrefix)), pattern);
    URI expected = URI.create(StringUtils.format("gs://%s/%s", bucket, storageObject3.getName()));
    Assert.assertEquals(expected, latest);
  }

  @Test
  public void getLatestVersionTrailingSlashKeyPrefix()
  {
    String bucket = "bucket";
    String keyPrefix = "prefix/dir/0/";

    // object for directory prefix/dir/0/
    final GoogleStorageObjectMetadata storageObject1 = ObjectStorageIteratorTest.makeStorageObject(bucket, keyPrefix + "/", 0);
    storageObject1.setLastUpdateTime(System.currentTimeMillis());
    final GoogleStorageObjectMetadata storageObject2 = ObjectStorageIteratorTest.makeStorageObject(bucket, keyPrefix + "v1", 1);
    storageObject2.setLastUpdateTime(System.currentTimeMillis());
    final GoogleStorageObjectMetadata storageObject3 = ObjectStorageIteratorTest.makeStorageObject(bucket, keyPrefix + "v2", 1);
    storageObject3.setLastUpdateTime(System.currentTimeMillis() + 100);
    final GoogleStorageObjectMetadata storageObject4 = ObjectStorageIteratorTest.makeStorageObject(bucket, keyPrefix + "other", 4);
    storageObject4.setLastUpdateTime(System.currentTimeMillis() + 100);
    final GoogleStorage storage = ObjectStorageIteratorTest.makeMockClient(ImmutableList.of(storageObject1, storageObject2, storageObject3, storageObject4));

    final GoogleTimestampVersionedDataFinder finder = new GoogleTimestampVersionedDataFinder(storage);
    Pattern pattern = Pattern.compile("v.*");
    URI latest = finder.getLatestVersion(URI.create(StringUtils.format("gs://%s/%s", bucket, keyPrefix)), pattern);
    URI expected = URI.create(StringUtils.format("gs://%s/%s", bucket, storageObject3.getName()));
    Assert.assertEquals(expected, latest);
  }
}
