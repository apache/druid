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

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class S3DataSegmentMoverTest
{
  private static final DataSegment SOURCE_SEGMENT = new DataSegment(
      "test",
      Intervals.of("2013-01-01/2013-01-02"),
      "1",
      ImmutableMap.of(
          "key",
          "baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip",
          "bucket",
          "main"
      ),
      ImmutableList.of("dim1", "dim1"),
      ImmutableList.of("metric1", "metric2"),
      NoneShardSpec.instance(),
      0,
      1
  );

  @Test
  public void testMove() throws Exception
  {
    MockAmazonS3Client mockS3Client = new MockAmazonS3Client();
    S3DataSegmentMover mover = new S3DataSegmentMover(mockS3Client, new S3DataSegmentPusherConfig());

    mockS3Client.putObject(
        "main",
        "baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip"
    );

    DataSegment movedSegment = mover.move(
        SOURCE_SEGMENT,
        ImmutableMap.of("baseKey", "targetBaseKey", "bucket", "archive")
    );

    Map<String, Object> targetLoadSpec = movedSegment.getLoadSpec();
    Assert.assertEquals("targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip", MapUtils.getString(targetLoadSpec, "key"));
    Assert.assertEquals("archive", MapUtils.getString(targetLoadSpec, "bucket"));
    Assert.assertTrue(mockS3Client.didMove());
  }

  @Test
  public void testMoveNoop() throws Exception
  {
    MockAmazonS3Client mockS3Client = new MockAmazonS3Client();
    S3DataSegmentMover mover = new S3DataSegmentMover(mockS3Client, new S3DataSegmentPusherConfig());

    mockS3Client.putObject(
        "archive",
        "targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip"
    );

    DataSegment movedSegment = mover.move(
        SOURCE_SEGMENT,
        ImmutableMap.of("baseKey", "targetBaseKey", "bucket", "archive")
    );

    Map<String, Object> targetLoadSpec = movedSegment.getLoadSpec();

    Assert.assertEquals("targetBaseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip", MapUtils.getString(targetLoadSpec, "key"));
    Assert.assertEquals("archive", MapUtils.getString(targetLoadSpec, "bucket"));
    Assert.assertFalse(mockS3Client.didMove());
  }

  @Test(expected = SegmentLoadingException.class)
  public void testMoveException() throws Exception
  {
    MockAmazonS3Client mockS3Client = new MockAmazonS3Client();
    S3DataSegmentMover mover = new S3DataSegmentMover(mockS3Client, new S3DataSegmentPusherConfig());

    mover.move(
        SOURCE_SEGMENT,
        ImmutableMap.of("baseKey", "targetBaseKey", "bucket", "archive")
    );
  }
  
  @Test
  public void testIgnoresGoneButAlreadyMoved() throws Exception
  {
    MockAmazonS3Client mockS3Client = new MockAmazonS3Client();
    S3DataSegmentMover mover = new S3DataSegmentMover(mockS3Client, new S3DataSegmentPusherConfig());
    mover.move(new DataSegment(
        "test",
        Intervals.of("2013-01-01/2013-01-02"),
        "1",
        ImmutableMap.of(
            "key",
            "baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip",
            "bucket",
            "DOES NOT EXIST"
        ),
        ImmutableList.of("dim1", "dim1"),
        ImmutableList.of("metric1", "metric2"),
        NoneShardSpec.instance(),
        0,
        1
    ), ImmutableMap.of("bucket", "DOES NOT EXIST", "baseKey", "baseKey"));
  }

  @Test(expected = SegmentLoadingException.class)
  public void testFailsToMoveMissing() throws Exception
  {
    MockAmazonS3Client mockS3Client = new MockAmazonS3Client();
    S3DataSegmentMover mover = new S3DataSegmentMover(mockS3Client, new S3DataSegmentPusherConfig());
    mover.move(new DataSegment(
        "test",
        Intervals.of("2013-01-01/2013-01-02"),
        "1",
        ImmutableMap.of(
            "key",
            "baseKey/test/2013-01-01T00:00:00.000Z_2013-01-02T00:00:00.000Z/1/0/index.zip",
            "bucket",
            "DOES NOT EXIST"
        ),
        ImmutableList.of("dim1", "dim1"),
        ImmutableList.of("metric1", "metric2"),
        NoneShardSpec.instance(),
        0,
        1
    ), ImmutableMap.of("bucket", "DOES NOT EXIST", "baseKey", "baseKey2"));
  }

  private static class MockAmazonS3Client extends ServerSideEncryptingAmazonS3
  {
    Map<String, Set<String>> storage = new HashMap<>();
    boolean copied = false;
    boolean deletedOld = false;

    private MockAmazonS3Client()
    {
      super(new AmazonS3Client(), new NoopServerSideEncryption());
    }

    public boolean didMove()
    {
      return copied && deletedOld;
    }

    @Override
    public AccessControlList getBucketAcl(String bucketName)
    {
      final AccessControlList acl = new AccessControlList();
      acl.setOwner(new Owner("ownerId", "owner"));
      acl.grantAllPermissions(new Grant(new CanonicalGrantee(acl.getOwner().getId()), Permission.FullControl));
      return acl;
    }

    @Override
    public boolean doesObjectExist(String bucketName, String objectKey)
    {
      Set<String> objects = storage.get(bucketName);
      return (objects != null && objects.contains(objectKey));
    }

    @Override
    public ListObjectsV2Result listObjectsV2(ListObjectsV2Request listObjectsV2Request)
    {
      final String bucketName = listObjectsV2Request.getBucketName();
      final String objectKey = listObjectsV2Request.getPrefix();
      if (doesObjectExist(bucketName, objectKey)) {
        final S3ObjectSummary objectSummary = new S3ObjectSummary();
        objectSummary.setBucketName(bucketName);
        objectSummary.setKey(objectKey);
        objectSummary.setStorageClass(StorageClass.Standard.name());

        final ListObjectsV2Result result = new ListObjectsV2Result();
        result.setBucketName(bucketName);
        result.setPrefix(objectKey);
        result.setKeyCount(1);
        result.getObjectSummaries().add(objectSummary);
        result.setTruncated(true);
        return result;
      } else {
        return new ListObjectsV2Result();
      }
    }

    @Override
    public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest)
    {
      final String sourceBucketName = copyObjectRequest.getSourceBucketName();
      final String sourceObjectKey = copyObjectRequest.getSourceKey();
      final String destinationBucketName = copyObjectRequest.getDestinationBucketName();
      final String destinationObjectKey = copyObjectRequest.getDestinationKey();
      copied = true;
      if (doesObjectExist(sourceBucketName, sourceObjectKey)) {
        storage.computeIfAbsent(destinationBucketName, k -> new HashSet<>())
               .add(destinationObjectKey);
        return new CopyObjectResult();
      } else {
        final AmazonS3Exception exception = new AmazonS3Exception("S3DataSegmentMoverTest");
        exception.setErrorCode("NoSuchKey");
        exception.setStatusCode(404);
        throw exception;
      }
    }

    @Override
    public void deleteObject(String bucket, String objectKey)
    {
      deletedOld = true;
      storage.get(bucket).remove(objectKey);
    }

    public PutObjectResult putObject(String bucketName, String key)
    {
      return putObject(bucketName, key, (File) null);
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, File file)
    {
      storage.putIfAbsent(bucketName, new HashSet<>());
      storage.get(bucketName).add(key);
      return new PutObjectResult();
    }
  }
}
